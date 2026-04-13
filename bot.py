import asyncio
import logging
import math
import os
import re
import asyncpg
import random
import string
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, FSInputFile,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.exceptions import TelegramBadRequest
from openai import AsyncOpenAI
import aiohttp

# ---------------------------------------------------------------------------
# КОНФИГУРАЦИЯ
# ---------------------------------------------------------------------------
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL       = os.getenv("DATABASE_URL")

_raw_keys = [
    os.getenv("OPENROUTER_API_KEY"),
    os.getenv("OPENROUTER_API_KEY_1"),
    os.getenv("OPENROUTER_API_KEY_2"),
    os.getenv("OPENROUTER_API_KEY_3"),
    os.getenv("OPENROUTER_API_KEY_4"),
]
API_KEYS: list[str] = [k for k in _raw_keys if k]
if not API_KEYS:
    raise RuntimeError("Не задан ни один OPENROUTER_API_KEY")

_clients = [
    AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=key, timeout=120.0)
    for key in API_KEYS
]
_key_index = 0
_key_lock  = None

def _next_client() -> AsyncOpenAI:
    global _key_index
    client = _clients[_key_index % len(_clients)]
    _key_index += 1
    return client

MAX_CONCURRENT_TASKS = 25
_generation_semaphore: asyncio.Semaphore | None = None
_active_tasks: int = 0

API_RETRY_ATTEMPTS   = 3
API_RETRY_BASE_DELAY = 2.0

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()
logging.basicConfig(level=logging.INFO)

db_pool: asyncpg.Pool | None = None

@dp.startup()
async def on_startup():
    global _generation_semaphore, _key_lock, db_pool, _active_tasks
    _generation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    _key_lock = asyncio.Lock()
    _active_tasks = 0
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10, ssl="prefer")
    await init_db()
    logging.info(f"Готов | API ключей: {len(API_KEYS)} | Очередь: {MAX_CONCURRENT_TASKS}")

@dp.shutdown()
async def on_shutdown():
    if db_pool:
        await db_pool.close()

# ---------------------------------------------------------------------------
# КОНСТАНТЫ ГЕНЕРАЦИИ
# ---------------------------------------------------------------------------
WORDS_PER_MINUTE  = 130
WORDS_PER_CHAPTER = 400

VOLUME_OVERREQUEST_FACTORS: dict[str, float] = {
    "anthropic/claude-haiku-4.5":    1.45,
    "anthropic/claude-sonnet-4.6":   1.30,
    "openai/gpt-5.1":                0.83,
    "google/gemini-2.5-flash-lite":  1.25,
    "x-ai/grok-4.1-fast":            1.25,
}
VOLUME_OVERREQUEST_FACTOR_DEFAULT = 1.20

MIN_CHAPTER_RATIO  = 0.80
MAX_REGEN_ATTEMPTS = 2
MAX_CTA_PER_SCRIPT = 5
SECONDS_PER_CHUNK  = 18

MODEL_NAMES = {
    "anthropic/claude-haiku-4.5":    "Claude Haiku",
    "anthropic/claude-sonnet-4.6":   "Claude Sonnet ✨",
    "openai/gpt-5.1":                "ChatGPT",
    "google/gemini-2.5-flash-lite":  "Gemini",
    "x-ai/grok-4.1-fast":            "Grok",
}

# ---------------------------------------------------------------------------
# МОНЕТИЗАЦИЯ
# ---------------------------------------------------------------------------
USD_TO_RUB = 86.0  # фиксированный курс

# Цена в кредитах за минуту (1 кредит = 1 рубль)
MODEL_PRICE_PER_MINUTE: dict[str, float] = {
    "google/gemini-2.5-flash-lite":  0.15,
    "x-ai/grok-4.1-fast":            0.25,
    "anthropic/claude-haiku-4.5":    0.75,
    "openai/gpt-5.1":                1.25,
    "anthropic/claude-sonnet-4.6":   2.00,
}

WELCOME_CREDITS = 50  # стартовый бонус новому пользователю

REFERRAL_BONUS_INVITER     = 25   # кредитов рефереру после первого пополнения приглашённого
REFERRAL_BONUS_INVITEE_PCT = 10   # % бонуса приглашённому к первому пополнению

# Пакеты: (цена ₽, базовых кредитов, бонус кредитов, название)
CREDIT_PACKAGES = [
    (99,   100,   0,   "Старт"),
    (249,  250,   20,  "Базовый"),
    (499,  500,   60,  "Продвинутый"),
    (999,  1000,  200, "Профи"),
    (1999, 2000,  600, "Макс"),
]


def calc_cost(model_id: str, duration_min: int) -> float:
    """Стоимость генерации в кредитах, округление вверх до 0.5."""
    price_per_min = MODEL_PRICE_PER_MINUTE.get(model_id, 1.0)
    return math.ceil(price_per_min * duration_min * 2) / 2


def _now() -> str:
    return datetime.now().strftime("%d.%m.%Y %H:%M:%S")


def _gen_ref_code() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=8))

# ---------------------------------------------------------------------------
# БАЗА ДАННЫХ
# ---------------------------------------------------------------------------

async def init_db():
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                name   TEXT PRIMARY KEY,
                prompt TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                user_id       BIGINT PRIMARY KEY,
                model_name    TEXT,
                credits       NUMERIC(12,2) DEFAULT 0,
                referral_code TEXT UNIQUE,
                referred_by   BIGINT,
                first_topup   BOOLEAN DEFAULT FALSE
            )
        """)
        # Миграция: добавляем колонки если таблица уже существовала без них
        for col, definition in [
            ("credits",       "NUMERIC(12,2) DEFAULT 0"),
            ("referral_code", "TEXT"),
            ("referred_by",   "BIGINT"),
            ("first_topup",   "BOOLEAN DEFAULT FALSE"),
        ]:
            try:
                await conn.execute(
                    f"ALTER TABLE settings ADD COLUMN IF NOT EXISTS {col} {definition}"
                )
            except Exception:
                pass
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id    TEXT PRIMARY KEY,
                user_id    BIGINT,
                topic      TEXT,
                model      TEXT,
                status     TEXT,
                created_at TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id          SERIAL PRIMARY KEY,
                user_id     BIGINT,
                amount      NUMERIC(12,2),
                description TEXT,
                created_at  TEXT
            )
        """)
        count = await conn.fetchval("SELECT COUNT(*) FROM templates")
        if count == 0:
            await conn.execute(
                "INSERT INTO templates (name, prompt) VALUES ($1, $2)",
                "🎬 Стандартный", "Напиши увлекательный сценарий для YouTube.",
            )


async def get_or_create_user(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM settings WHERE user_id=$1", user_id)
        if row:
            row_dict = dict(row)
            needs_update = False
            updates = []
            params  = []
            idx     = 1

            # Существующий пользователь, но credits = NULL после миграции
            if row_dict.get("credits") is None:
                updates.append(f"credits=${idx}")
                params.append(WELCOME_CREDITS)
                idx += 1
                needs_update = True
                # Записываем стартовый бонус в транзакции
                await conn.execute(
                    "INSERT INTO transactions (user_id, amount, description, created_at) "
                    "VALUES ($1,$2,$3,$4)",
                    user_id, WELCOME_CREDITS, "🎁 Стартовый бонус (миграция)", _now(),
                )

            # referral_code = NULL после миграции
            if not row_dict.get("referral_code"):
                ref_code = _gen_ref_code()
                updates.append(f"referral_code=${idx}")
                params.append(ref_code)
                idx += 1
                needs_update = True

            if needs_update:
                params.append(user_id)
                await conn.execute(
                    f"UPDATE settings SET {', '.join(updates)} WHERE user_id=${idx}",
                    *params,
                )
                row_dict = dict(await conn.fetchrow(
                    "SELECT * FROM settings WHERE user_id=$1", user_id,
                ))
            return row_dict

        # Новый пользователь
        ref_code = _gen_ref_code()
        await conn.execute(
            "INSERT INTO settings (user_id, model_name, credits, referral_code, referred_by, first_topup) "
            "VALUES ($1,$2,$3,$4,NULL,FALSE)",
            user_id, "x-ai/grok-4.1-fast", WELCOME_CREDITS, ref_code,
        )
        await conn.execute(
            "INSERT INTO transactions (user_id, amount, description, created_at) VALUES ($1,$2,$3,$4)",
            user_id, WELCOME_CREDITS, "🎁 Стартовый бонус", _now(),
        )
        return dict(await conn.fetchrow("SELECT * FROM settings WHERE user_id=$1", user_id))


async def get_balance(user_id: int) -> float:
    user = await get_or_create_user(user_id)
    return float(user["credits"] or 0)


async def add_credits(user_id: int, amount: float, description: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE settings SET credits = credits + $1 WHERE user_id=$2", amount, user_id,
        )
        await conn.execute(
            "INSERT INTO transactions (user_id, amount, description, created_at) VALUES ($1,$2,$3,$4)",
            user_id, amount, description, _now(),
        )


async def deduct_credits(user_id: int, amount: float, description: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE settings SET credits = credits - $1 WHERE user_id=$2", amount, user_id,
        )
        await conn.execute(
            "INSERT INTO transactions (user_id, amount, description, created_at) VALUES ($1,$2,$3,$4)",
            user_id, -amount, description, _now(),
        )


async def get_transactions(user_id: int, limit: int = 7) -> list:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT amount, description, created_at FROM transactions "
            "WHERE user_id=$1 ORDER BY id DESC LIMIT $2",
            user_id, limit,
        )
    return [dict(r) for r in rows]


async def apply_referral(new_user_id: int, ref_code: str) -> bool:
    async with db_pool.acquire() as conn:
        referrer = await conn.fetchrow("SELECT user_id FROM settings WHERE referral_code=$1", ref_code)
        if not referrer or referrer["user_id"] == new_user_id:
            return False
        row = await conn.fetchrow("SELECT referred_by FROM settings WHERE user_id=$1", new_user_id)
        if row and row["referred_by"] is not None:
            return False
        await conn.execute(
            "UPDATE settings SET referred_by=$1 WHERE user_id=$2", referrer["user_id"], new_user_id,
        )
    return True


async def process_first_topup(user_id: int, topup_amount: float):
    """Начисляет реферальные бонусы при первом пополнении."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT referred_by, first_topup FROM settings WHERE user_id=$1", user_id,
        )
        if not row or row["first_topup"]:
            return
        await conn.execute("UPDATE settings SET first_topup=TRUE WHERE user_id=$1", user_id)
        referrer_id = row["referred_by"]

    bonus_invitee = round(topup_amount * REFERRAL_BONUS_INVITEE_PCT / 100, 2)
    if bonus_invitee > 0:
        await add_credits(user_id, bonus_invitee, f"🎁 Реферальный бонус +{REFERRAL_BONUS_INVITEE_PCT}%")
    if referrer_id:
        await add_credits(referrer_id, REFERRAL_BONUS_INVITER, "👥 Бонус за приглашённого")
        logging.info(f"Реф. бонус {REFERRAL_BONUS_INVITER} кред. → user {referrer_id}")


async def log_task(task_id, user_id, topic, model):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tasks VALUES ($1,$2,$3,$4,$5,$6)",
            task_id, user_id, topic, model, "In Progress", _now(),
        )


async def update_task_status(task_id, status):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE tasks SET status=$1 WHERE task_id=$2", status, task_id)


async def get_task_status(task_id) -> str | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM tasks WHERE task_id=$1", task_id)
    return row["status"] if row else None


async def get_user_model(user_id) -> str:
    user = await get_or_create_user(user_id)
    return user["model_name"] or "x-ai/grok-4.1-fast"


async def set_user_model(user_id, model_name):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE settings SET model_name=$1 WHERE user_id=$2", model_name, user_id)


async def get_templates() -> dict:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT name, prompt FROM templates")
    return {row["name"]: row["prompt"] for row in rows}


async def add_template(name, prompt):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO templates (name,prompt) VALUES ($1,$2) "
            "ON CONFLICT (name) DO UPDATE SET prompt=EXCLUDED.prompt",
            name, prompt,
        )


async def delete_template(name):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM templates WHERE name=$1", name)

# ---------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ---------------------------------------------------------------------------

async def api_call_with_retry(model_id: str, messages: list, max_tokens: int) -> str:
    last_exc = None
    for attempt in range(API_RETRY_ATTEMPTS):
        async with _key_lock:
            c = _next_client()
        try:
            resp = await c.chat.completions.create(
                model=model_id, messages=messages, max_tokens=max_tokens,
            )
            return resp.choices[0].message.content
        except Exception as e:
            last_exc = e
            err_str = str(e).lower()
            if any(x in err_str for x in ("rate", "429", "502", "503", "timeout")):
                delay = API_RETRY_BASE_DELAY * (2 ** attempt)
                logging.warning(f"API retry {attempt+1}/{API_RETRY_ATTEMPTS}: {delay:.1f}с")
                await asyncio.sleep(delay)
            else:
                raise
    raise last_exc


def clean_chapter_text(text: str) -> str:
    text = re.sub(r'^[-*_=~]{3,}\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^#{1,6}\s+.*$', '', text, flags=re.MULTILINE)
    text = re.sub(
        r'^(Глава|Часть|Раздел|Блок|Chapter|Part|Section)\s*[\dIVXivxабвгАБВГ]*[.:\-–—)]*\s*.*$',
        '', text, flags=re.MULTILINE | re.IGNORECASE,
    )
    text = re.sub(r'^\*\*[^*\n]+\*\*\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^\*[^*\n]+\*\s*$',     '', text, flags=re.MULTILINE)
    text = re.sub(r'^__[^_\n]+__\s*$',      '', text, flags=re.MULTILINE)
    text = re.sub(
        r'^\*{0,2}(Проверка|Итого|Подсчёт|Слов|Всего|Объём|Word count|Total)[^\n]{0,60}$',
        '', text, flags=re.MULTILINE | re.IGNORECASE,
    )
    text = re.sub(r'^\[?\(?\d{2,4}\s*(слов|words|сл\.)\)?\]?\s*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_plan_chapters(plan_text: str, target_count: int) -> list[str]:
    lines = []
    for line in plan_text.splitlines():
        line = line.strip()
        if not line or len(line) < 4:
            continue
        line = re.sub(r'^[\d]+[.)]\s+', '', line)
        line = re.sub(r'^[-–—•*]\s+',   '', line)
        line = line.strip()
        if len(line) > 3:
            lines.append(line)
    if not lines:
        return [f"Часть {i+1}" for i in range(target_count)]
    if len(lines) > target_count:
        lines = lines[:target_count]
    return lines


def compute_cta_positions(total_chapters: int, max_cta: int = MAX_CTA_PER_SCRIPT) -> set[int]:
    if total_chapters <= 1:
        return set()
    count = min(max_cta, total_chapters - 1)
    step  = (total_chapters - 1) / count
    return {round(1 + step * i) for i in range(count)}


def build_progress_text(task_id, friendly_model, total_chapters,
                        done_chapters, remaining_chunks, phase="generate"):
    bar_len = 10
    filled  = round(bar_len * done_chapters / max(total_chapters, 1))
    bar     = "▓" * filled + "░" * (bar_len - filled)
    pct     = round(100 * done_chapters / max(total_chapters, 1))

    if phase == "regen":
        status_line = "🔧 <i>Доработка коротких частей...</i>"
    elif phase == "done":
        status_line = "✅ <b>Готово!</b>"
    else:
        rem_sec = remaining_chunks * SECONDS_PER_CHUNK
        rem_min, rem_s = divmod(rem_sec, 60)
        time_str = f"{rem_min} мин. {rem_s} сек." if rem_min else f"{rem_s} сек."
        status_line = f"⏱ Осталось примерно: <b>{time_str}</b>"

    return (
        f"⏳ <b>Генерация сценария</b>\n\n"
        f"🆔 ID: <code>{task_id}</code>\n"
        f"🤖 Модель: <b>{friendly_model}</b>\n\n"
        f"[{bar}] {pct}%\n"
        f"📄 Частей готово: <b>{done_chapters}</b> из <b>{total_chapters}</b>\n\n"
        f"{status_line}"
    )


async def safe_edit(msg: types.Message, text: str, reply_markup=None):
    try:
        await msg.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logging.warning(f"edit_text: {e}")
    except Exception as e:
        logging.warning(f"safe_edit: {e}")


async def upload_to_backup(file_path: str) -> str | None:
    try:
        async with aiohttp.ClientSession() as session:
            with open(file_path, "rb") as f:
                async with session.post("https://file.io/?expires=14d", data={"file": f}) as resp:
                    res = await resp.json()
                    return res.get("link")
    except Exception as e:
        logging.error(f"Бэкап: {e}")
        return None


async def generate_master_doc(model_id: str, topic: str, style_prompt: str, duration_min: int) -> str:
    """
    Генерирует мастер-документ (синопсис) ДО написания глав.
    Содержит всех персонажей с именами, конфликт, предысторию, развязку.
    Передаётся в каждую главу как жёсткий каркас истории.
    """
    prompt = (
        f"Ты готовишься написать сценарий для YouTube на тему: «{topic}»\n"
        f"Стиль и жанр: {style_prompt[:300]}\n"
        f"Хронометраж: ~{duration_min} минут\n\n"
        f"Перед написанием сценария создай МАСТЕР-ДОКУМЕНТ истории.\n\n"
        f"Обязательно укажи:\n"
        f"1. ПЕРСОНАЖИ: полное имя, возраст, внешность, характер, роль в истории — для каждого\n"
        f"2. ГЛАВНЫЙ КОНФЛИКТ: суть противостояния или проблемы\n"
        f"3. ПРЕДЫСТОРИЯ: что произошло до начала истории\n"
        f"4. КЛЮЧЕВЫЕ СОБЫТИЯ: 3-5 поворотных моментов\n"
        f"5. РАЗВЯЗКА: чем всё заканчивается\n"
        f"6. МЕСТО И ВРЕМЯ: где и когда происходит действие\n\n"
        f"Пиши конкретно и чётко. Это внутренний документ — никакого художественного текста, "
        f"только структурированные факты. Максимум 400 слов."
    )
    try:
        result = await api_call_with_retry(
            model_id,
            [{"role": "user", "content": prompt}],
            800,
        )
        return result.strip() if result else ""
    except Exception as e:
        logging.warning(f"Мастер-документ: не удалось создать: {e}")
        return ""

# ---------------------------------------------------------------------------
# СОСТОЯНИЯ
# ---------------------------------------------------------------------------
class ScriptMaker(StatesGroup):
    waiting_for_topic    = State()
    waiting_for_duration = State()
    waiting_for_template = State()

class TemplateManager(StatesGroup):
    waiting_for_new_name    = State()
    waiting_for_new_prompt  = State()
    waiting_for_delete_name = State()
    waiting_for_edit_name   = State()   # отдельное состояние для редактирования

# ---------------------------------------------------------------------------
# КЛАВИАТУРЫ
# ---------------------------------------------------------------------------
def get_main_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎬 Создать сценарий")],
        [KeyboardButton(text="🗂 Массовая генерация")],
        [KeyboardButton(text="💰 Баланс"),   KeyboardButton(text="💳 Пополнить")],
        [KeyboardButton(text="📋 Тарифы"),   KeyboardButton(text="📦 Пакеты")],
        [KeyboardButton(text="📁 Шаблоны"),  KeyboardButton(text="⚙️ Настройки")],
        [KeyboardButton(text="👥 Реферальная программа")],
        [KeyboardButton(text="❓ FAQ"),       KeyboardButton(text="🏠 Главная")],
    ], resize_keyboard=True)

def get_models_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Gemini"),         KeyboardButton(text="Grok")],
        [KeyboardButton(text="Claude Haiku"),   KeyboardButton(text="ChatGPT")],
        [KeyboardButton(text="Claude Sonnet ✨")],
        [KeyboardButton(text="🔙 Назад в меню")],
    ], resize_keyboard=True)

def get_templates_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить шаблон"), KeyboardButton(text="✏️ Изменить шаблон")],
        [KeyboardButton(text="🗑 Удалить шаблон"),  KeyboardButton(text="🔙 Назад в меню")],
    ], resize_keyboard=True)

async def get_dynamic_templates_kb(for_generation: bool = False):
    templates = await get_templates()
    kb = [[KeyboardButton(text=name)] for name in templates]
    if not for_generation:
        kb.append([KeyboardButton(text="➕ Создать новый шаблон")])
    kb.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# ---------------------------------------------------------------------------
# ТЕКСТЫ
# ---------------------------------------------------------------------------
WELCOME_TEXT = (
    "👋 <b>Добро пожаловать в генератор сценариев!</b>\n\n"
    "Создавайте готовые YouTube-сценарии без ограничений.\n\n"
    "🟢 <b>Gemini</b> — бюджет\n"
    "🔵 <b>Grok</b> — дерзость\n"
    "🟡 <b>Claude Haiku</b> — стиль\n"
    "🟠 <b>ChatGPT</b> — логика\n"
    "🔴 <b>Claude Sonnet ✨</b> — элита\n\n"
    f"🎁 Вам начислено <b>{WELCOME_CREDITS} стартовых кредитов</b>!"
)

TARIFFS_TEXT = (
    "💰 <b>Тарифы на генерацию</b>\n\n"
    "Стоимость = <b>цена за минуту × длительность</b>\n"
    "1 кредит = 1 рубль\n\n"
    "──────────────────────────\n"
    "🟢 <b>Gemini</b> — 0.15 кред./мин\n"
    "30 мин: 4.5₽  |  60 мин: 9₽  |  90 мин: 13.5₽  |  120 мин: 18₽\n\n"
    "🔵 <b>Grok</b> — 0.25 кред./мин\n"
    "30 мин: 7.5₽  |  60 мин: 15₽  |  90 мин: 22.5₽  |  120 мин: 30₽\n\n"
    "🟡 <b>Claude Haiku</b> — 0.75 кред./мин\n"
    "30 мин: 22.5₽  |  60 мин: 45₽  |  90 мин: 67.5₽  |  120 мин: 90₽\n\n"
    "🟠 <b>ChatGPT (GPT-5.1)</b> — 1.25 кред./мин\n"
    "30 мин: 37.5₽  |  60 мин: 75₽  |  90 мин: 112.5₽  |  120 мин: 150₽\n\n"
    "🔴 <b>Claude Sonnet ✨</b> — 2.00 кред./мин\n"
    "30 мин: 60₽  |  60 мин: 120₽  |  90 мин: 180₽  |  120 мин: 240₽\n"
    "──────────────────────────\n\n"
    "<i>Стоимость показывается перед каждой генерацией.\n"
    "Кредиты списываются только после успешного завершения.\n"
    "Если баланса не хватает — генерация не начнётся.</i>"
)


def build_packages_text() -> str:
    lines = ["📦 <b>Пакеты пополнения</b>\n\n"
             "Покупай выгоднее — чем больше пакет, тем ниже цена за кредит.\n\n"]

    for price, base, bonus, name in CREDIT_PACKAGES:
        total = base + bonus
        discount = round((1 - price / total) * 100) if total > price else 0
        discount_str = f" (<b>скидка {discount}%</b>)" if discount else ""
        bonus_str    = f" + {bonus} бонус" if bonus else ""

        # Примеры на что хватит
        examples = []
        for mid, ppm in MODEL_PRICE_PER_MINUTE.items():
            mname = MODEL_NAMES[mid].replace(" ✨", "")
            for mins in [120, 60, 30]:
                cost_one = ppm * mins
                if cost_one <= total:
                    n = int(total // cost_one)
                    examples.append(f"{mname}: {n}× {mins} мин.")
                    break

        lines.append(
            f"──────────────────────────\n"
            f"<b>{name}</b> — {price}₽{discount_str}\n"
            f"💳 {base}{bonus_str} = <b>{total} кредитов</b>\n"
            f"<i>Например:</i>\n"
        )
        for ex in examples[:3]:
            lines.append(f"  • {ex}\n")
        lines.append("\n")

    lines.append(
        "──────────────────────────\n"
        "<i>Для оплаты нажми «💳 Пополнить»</i>"
    )
    return "".join(lines)

# ---------------------------------------------------------------------------
# НАВИГАЦИЯ
# ---------------------------------------------------------------------------

@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    user_id  = message.from_user.id
    username = message.from_user.username
    args     = message.text.split()

    # Проверяем существовал ли пользователь ДО вызова get_or_create_user
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT user_id, credits, first_topup FROM settings WHERE user_id=$1", user_id)
    is_new = existing is None

    user = await get_or_create_user(user_id)

    if username:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE settings SET username=$1 WHERE user_id=$2", username.lower(), user_id)

    # /start oferta
    if len(args) > 1:
        if args[1].lower() == "oferta":
            await message.answer(
                "📜 <b>Публичная оферта Script AI</b>\n\n"
                "Самозанятый: Панферов Кирилл Алексеевич\n"
                "ИНН: 616810872170\n"
                "📩 kkpanferovvai@gmail.com | 💬 @aass11463",
                parse_mode="HTML",
            )
            try:
                await message.answer_document(FSInputFile("oferta_scriptai.docx"), caption="Полный текст договора-оферты")
            except Exception:
                await message.answer(OFERTA_TEXT, parse_mode="HTML")
            return
        applied = await apply_referral(user_id, args[1])
        if applied:
            await message.answer(
                "✅ Реферальная ссылка применена!\n"
                f"Вы получите бонус +{REFERRAL_BONUS_INVITEE_PCT}% к первому пополнению.",
                parse_mode="HTML",
            )

    balance = float(user["credits"] or 0)

    if is_new:
        welcome = (
            "👋 <b>Добро пожаловать в Авто Сценарист!</b>\n\n"
            "Генерируй готовые YouTube-сценарии за минуты — "
            "без копирайтеров, без ограничений по длине, без склеек.\n\n"
            "✨ <b>Почему выбирают нас:</b>\n"
            "⚡ Сценарий на 60 минут — за 3-5 минут\n"
            "🗂 Массовая генерация — до 5 сценариев за раз\n"
            "🤖 5 топовых моделей — GPT, Claude, Gemini, Grok\n"
            "💰 От 9₽ за полный 60-минутный сценарий\n\n"
            f"🎁 Тебе начислено <b>{WELCOME_CREDITS} стартовых кредитов</b>!\n\n"
            f"💰 Ваш баланс: <b>{balance:.1f} кредитов</b>"
        )
    else:
        welcome = (
            "👋 <b>С возвращением!</b>\n\n"
            "Готов создавать новые сценарии — жми кнопку и поехали 🚀\n\n"
            f"💰 Баланс: <b>{balance:.1f} кредитов</b>"
        )

    await message.answer(welcome, reply_markup=get_main_kb(), parse_mode="HTML")


@dp.message(F.text == "🔄 Перезапустить")
async def restart_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    balance = await get_balance(message.from_user.id)
    await message.answer(
        WELCOME_TEXT + f"\n\n💰 Ваш баланс: <b>{balance:.1f} кредитов</b>",
        reply_markup=get_main_kb(), parse_mode="HTML",
    )


@dp.message(F.text == "🔙 Назад в меню")
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=get_main_kb())

# ---------------------------------------------------------------------------
# БАЛАНС
# ---------------------------------------------------------------------------

@dp.message(Command("balance"))
@dp.message(F.text == "💰 Баланс")
async def balance_cmd(message: types.Message):
    user_id = message.from_user.id
    balance = await get_balance(user_id)
    txs     = await get_transactions(user_id, limit=7)
    user    = await get_or_create_user(user_id)

    text = f"💰 <b>Баланс: {balance:.1f} кредитов</b>\n\n📋 <b>Последние операции:</b>\n"
    for tx in txs:
        sign = "+" if tx["amount"] > 0 else ""
        text += f"  {sign}{tx['amount']:.1f} — {tx['description']} <i>({tx['created_at']})</i>\n"
    if not txs:
        text += "  <i>Операций нет</i>\n"

    text += f"\n🔗 Реферальный код: <code>{user['referral_code']}</code>"
    await message.answer(text, parse_mode="HTML")

# ---------------------------------------------------------------------------
# ПОПОЛНЕНИЕ
# ---------------------------------------------------------------------------

@dp.message(F.text == "💳 Пополнить")
async def topup_cmd(message: types.Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{name} — {price}₽  →  {base+bonus} кред.",
            callback_data=f"pkg_{i}",
        )]
        for i, (price, base, bonus, name) in enumerate(CREDIT_PACKAGES)
    ])
    await message.answer(
        "💳 <b>Выберите пакет пополнения:</b>\n\n"
        "<i>Платёжная система подключается — скоро будет автоплатёж.\n"
        "Пока свяжитесь с поддержкой для ручного пополнения.</i>",
        reply_markup=kb, parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("pkg_"))
async def pkg_selected(call: types.CallbackQuery):
    idx = int(call.data.split("_")[1])
    price, base, bonus, name = CREDIT_PACKAGES[idx]
    total = base + bonus
    await call.message.answer(
        f"📦 Пакет <b>{name}</b>\n"
        f"💳 Сумма: <b>{price}₽</b>\n"
        f"🎁 Получите: <b>{total} кредитов</b>\n\n"
        f"Для оплаты напишите в поддержку с указанием:\n"
        f"• Пакет: <b>{name}</b>\n"
        f"• Ваш ID: <code>{call.from_user.id}</code>",
        parse_mode="HTML",
    )
    await call.answer()

# ---------------------------------------------------------------------------
# ТАРИФЫ И ПАКЕТЫ
# ---------------------------------------------------------------------------

@dp.message(F.text == "📋 Тарифы")
async def tariffs_cmd(message: types.Message):
    await message.answer(TARIFFS_TEXT, parse_mode="HTML")


@dp.message(F.text == "📦 Пакеты")
async def packages_cmd(message: types.Message):
    await message.answer(build_packages_text(), parse_mode="HTML")

# ---------------------------------------------------------------------------
# РЕФЕРАЛЬНАЯ ПРОГРАММА
# ---------------------------------------------------------------------------

@dp.message(F.text == "👥 Реферальная программа")
async def referral_cmd(message: types.Message):
    user     = await get_or_create_user(message.from_user.id)
    bot_info = await bot.get_me()
    ref_code = user["referral_code"]
    ref_link = f"https://t.me/{bot_info.username}?start={ref_code}"

    # Inline-кнопка позволяет поделиться ссылкой в один тап
    share_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📤 Поделиться ссылкой",
            url=f"https://t.me/share/url?url={ref_link}&text=Попробуй%20этот%20бот%20для%20генерации%20YouTube-сценариев!",
        )],
    ])

    await message.answer(
        "👥 <b>Реферальная программа</b>\n\n"
        "Приглашайте друзей и получайте бонусы!\n\n"
        f"🔗 Ваша ссылка (нажми чтобы скопировать):\n"
        f"<code>{ref_link}</code>\n\n"
        "<b>Условия:</b>\n"
        f"• Вы получаете <b>{REFERRAL_BONUS_INVITER} кредитов</b> "
        f"когда приглашённый делает первое пополнение\n"
        f"• Приглашённый получает <b>+{REFERRAL_BONUS_INVITEE_PCT}%</b> "
        f"бонусных кредитов к первому пополнению\n\n"
        "<i>Бонус начисляется автоматически после первой оплаты друга.</i>",
        reply_markup=share_kb,
        parse_mode="HTML",
    )

# ---------------------------------------------------------------------------
# НАСТРОЙКИ
# ---------------------------------------------------------------------------

@dp.message(F.text == "⚙️ Настройки")
async def settings_menu(message: types.Message):
    cur      = await get_user_model(message.from_user.id)
    friendly = MODEL_NAMES.get(cur, cur)
    await message.answer(
        f"⚙️ <b>Настройки</b>\n\nТекущая модель: <b>{friendly}</b>",
        reply_markup=get_models_kb(), parse_mode="HTML",
    )


@dp.message(F.text.in_(MODEL_NAMES.values()))
async def change_model(message: types.Message):
    inv = {v: k for k, v in MODEL_NAMES.items()}
    await set_user_model(message.from_user.id, inv[message.text])
    await message.answer(
        f"✅ Модель изменена на: <b>{message.text}</b>",
        reply_markup=get_main_kb(), parse_mode="HTML",
    )

# ---------------------------------------------------------------------------
# ШАБЛОНЫ
# ---------------------------------------------------------------------------

@dp.message(F.text == "📁 Шаблоны")
async def templates_menu(message: types.Message):
    templates = await get_templates()
    text = "📂 <b>Твои шаблоны:</b>\n\n"
    if not templates:
        text += "Пусто."
    else:
        for name, prompt in templates.items():
            text += f"🔹 <b>{name}</b>\n<i>{prompt[:60]}...</i>\n\n"
    await message.answer(text, reply_markup=get_templates_menu_kb(), parse_mode="HTML")


@dp.message(F.text == "➕ Добавить шаблон")
async def add_template_start(message: types.Message, state: FSMContext):
    await message.answer("Введи название:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(TemplateManager.waiting_for_new_name)


@dp.message(F.text == "✏️ Изменить шаблон")
async def edit_template_start(message: types.Message, state: FSMContext):
    await message.answer("Какой шаблон хочешь изменить?", reply_markup=await get_dynamic_templates_kb())
    await state.set_state(TemplateManager.waiting_for_edit_name)


@dp.message(TemplateManager.waiting_for_edit_name)
async def edit_template_pick(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    templates = await get_templates()
    if message.text not in templates:
        await message.answer("Выбери из списка:", reply_markup=await get_dynamic_templates_kb())
        return
    current_prompt = templates[message.text]
    await state.update_data(template_name=message.text)
    await message.answer(
        f"✏️ Шаблон: <b>{message.text}</b>\n\n"
        f"📄 Текущий промпт:\n<blockquote>{current_prompt}</blockquote>\n\n"
        f"Отправь новый текст промпта:",
        reply_markup=types.ReplyKeyboardRemove(), parse_mode="HTML",
    )
    await state.set_state(TemplateManager.waiting_for_new_prompt)


@dp.message(TemplateManager.waiting_for_new_name)
async def add_template_name(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    await state.update_data(template_name=message.text)
    await message.answer("Отправь текст промпта:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(TemplateManager.waiting_for_new_prompt)


@dp.message(TemplateManager.waiting_for_new_prompt)
async def add_template_prompt(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await add_template(data["template_name"], message.text)
    if "topic" in data:
        await message.answer(
            f"✅ Шаблон <b>{data['template_name']}</b> сохранён!\n"
            f"Продолжаем: <i>{data['topic']}</i>. Выберите шаблон:",
            reply_markup=await get_dynamic_templates_kb(for_generation=True), parse_mode="HTML",
        )
        await state.set_state(ScriptMaker.waiting_for_template)
    else:
        await message.answer("✅ Сохранён!", reply_markup=get_main_kb())
        await state.clear()


@dp.message(F.text == "🗑 Удалить шаблон")
async def delete_template_start(message: types.Message, state: FSMContext):
    await message.answer("Что удалить?", reply_markup=await get_dynamic_templates_kb())
    await state.set_state(TemplateManager.waiting_for_delete_name)


@dp.message(TemplateManager.waiting_for_delete_name)
async def delete_template_confirm(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    await delete_template(message.text)
    await message.answer(f"🗑 Удалено: {message.text}", reply_markup=get_templates_menu_kb())
    await state.clear()

# ---------------------------------------------------------------------------
# ОТМЕНА
# ---------------------------------------------------------------------------

@dp.callback_query(F.data.startswith("cancel_"))
async def cancel_task_handler(call: types.CallbackQuery):
    task_id = call.data.replace("cancel_", "")
    await update_task_status(task_id, "Cancelled")
    try:
        await call.message.edit_text(
            f"🛑 <b>Генерация отменена</b>\n\n🆔 ID: <code>{task_id}</code>",
            parse_mode="HTML",
        )
    except Exception:
        pass
    await call.message.answer(
        "Хочешь создать новый сценарий?",
        reply_markup=ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="🎬 Создать сценарий")],
            [KeyboardButton(text="🔙 Назад в меню")],
        ], resize_keyboard=True),
    )
    await call.answer()

# ---------------------------------------------------------------------------
# СОЗДАНИЕ СЦЕНАРИЯ
# ---------------------------------------------------------------------------

@dp.message(F.text == "🎬 Создать сценарий")
async def start_script(message: types.Message, state: FSMContext):
    await message.answer("О чём видео?", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ScriptMaker.waiting_for_topic)


@dp.message(ScriptMaker.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    await state.update_data(topic=message.text)
    await message.answer("Длительность (мин):")
    await state.set_state(ScriptMaker.waiting_for_duration)


@dp.message(ScriptMaker.waiting_for_duration)
async def process_duration(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) < 1:
        await message.answer("⚠️ Введите число, например: <b>60</b>", parse_mode="HTML")
        return

    duration    = int(message.text)
    model_id    = await get_user_model(message.from_user.id)
    cost        = calc_cost(model_id, duration)
    balance     = await get_balance(message.from_user.id)
    model_name  = MODEL_NAMES.get(model_id, model_id)

    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)

    cost_line = (
        f"<i>💰 Стоимость ({model_name}, {duration} мин): "
        f"<b>{cost:.1f} кред.</b> | Баланс: <b>{balance:.1f} кред.</b></i>\n\n"
    )

    if balance < cost:
        await message.answer(
            f"❌ <b>Недостаточно кредитов</b>\n\n{cost_line}"
            "Нажми <b>💳 Пополнить</b> чтобы пополнить баланс.",
            reply_markup=get_main_kb(), parse_mode="HTML",
        )
        await state.clear()
        return

    await message.answer(
        f"{cost_line}"
        "<i>⚠️ Хронометраж может отличаться на ±10 мин.</i>\n\nВыбери шаблон:",
        reply_markup=await get_dynamic_templates_kb(for_generation=True), parse_mode="HTML",
    )
    await state.set_state(ScriptMaker.waiting_for_template)

# ---------------------------------------------------------------------------
# ГЕНЕРАЦИЯ
# ---------------------------------------------------------------------------

@dp.message(ScriptMaker.waiting_for_template)
async def generate_script(message: types.Message, state: FSMContext):
    global _active_tasks

    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    if message.text == "➕ Создать новый шаблон":
        await message.answer("Введите название:", reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(TemplateManager.waiting_for_new_name)
        return

    templates = await get_templates()
    if message.text not in templates:
        await message.answer(
            "Выбери из списка:",
            reply_markup=await get_dynamic_templates_kb(for_generation=True),
        )
        return

    style_prompt   = templates[message.text]
    data           = await state.get_data()
    model_id       = await get_user_model(message.from_user.id)
    friendly_model = MODEL_NAMES.get(model_id, "AI")
    words_target   = data["words_target"]
    duration       = data["duration"]
    user_id        = message.from_user.id

    # Финальная проверка баланса
    cost    = calc_cost(model_id, duration)
    balance = await get_balance(user_id)
    if balance < cost:
        await message.answer(
            f"❌ <b>Недостаточно кредитов</b>\n"
            f"Нужно: <b>{cost:.1f}</b> | Баланс: <b>{balance:.1f}</b>",
            reply_markup=get_main_kb(), parse_mode="HTML",
        )
        await state.clear()
        return

    task_id   = f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"
    file_name = f"script_{task_id.replace(' ', '_')}.txt"
    await log_task(task_id, user_id, data["topic"], friendly_model)

    if _generation_semaphore is None:
        await message.answer("❌ Бот ещё не готов, попробуй через несколько секунд.")
        await state.clear()
        return

    if _active_tasks >= MAX_CONCURRENT_TASKS:
        await message.answer(
            "⏳ <b>Все слоты заняты.</b> Задача в очереди — начнётся автоматически.",
            parse_mode="HTML",
        )

    await _generation_semaphore.acquire()
    _active_tasks += 1

    try:
        temp_msg = await message.answer("⏳ <i>Подготовка структуры...</i>", parse_mode="HTML")

        # ── МАСТЕР-ДОКУМЕНТ (синопсис) — генерируется ДО плана ─────────────
        target_chapters = max(1, round(words_target / WORDS_PER_CHAPTER))
        master_doc      = ""
        # Синопсис нужен только для длинных сценариев (>3 глав) или нарративных жанров
        if target_chapters > 3:
            master_doc = await generate_master_doc(model_id, data['topic'], style_prompt, duration)
            if master_doc:
                logging.info(f"[{task_id}] 📋 Мастер-документ: {len(master_doc.split())} слов")

        # ── ПЛАН ───────────────────────────────────────────────────────────
        master_block = f"\nОПИРАЙСЯ СТРОГО НА ЭТОТ СИНОПСИС:\n{master_doc}\n\n" if master_doc else ""
        plan_prompt = (
            f"Составь план YouTube-видео на тему: «{data['topic']}».\n"
            f"Нужно ровно {target_chapters} пунктов — не больше, не меньше.\n"
            f"{master_block}"
            f"ЖЁСТКИЕ ТРЕБОВАНИЯ:\n"
            f"1. Каждый пункт — отдельный самостоятельный аспект (5–10 слов).\n"
            f"2. Биография и контекст эпохи — ТОЛЬКО в 1-м пункте.\n"
            f"3. Каждый пункт отвечает на ДРУГОЙ вопрос (кто/что/почему/как/когда).\n"
            f"4. Запрещены похожие по смыслу пункты.\n"
            f"5. Только нумерованный список без пояснений."
        )
        plan_raw = await api_call_with_retry(
            model_id, [{"role": "user", "content": plan_prompt}], 2500,
        )

        validate_prompt = (
            f"Вот план ({target_chapters} пунктов):\n\n{plan_raw}\n\n"
            f"Найди пункты, пересекающиеся по смыслу, перепиши их.\n"
            f"Верни ровно {target_chapters} пунктов нумерованным списком. "
            f"Если дублей нет — верни исходный список."
        )
        validated = await api_call_with_retry(
            model_id, [{"role": "user", "content": validate_prompt}], 2500,
        )
        if validated and validated.strip():
            plan_raw = validated

        chapters = parse_plan_chapters(plan_raw, target_chapters)
        n        = len(chapters)

        words_per_chapter  = words_target // n
        overrequest_factor = VOLUME_OVERREQUEST_FACTORS.get(model_id, VOLUME_OVERREQUEST_FACTOR_DEFAULT)
        words_to_request   = max(50, int(words_per_chapter * overrequest_factor))
        max_tokens_chapter = min(int(words_to_request * 2.4), 2048)
        cta_positions      = compute_cta_positions(n)
        full_plan_str      = "\n".join(f"{i+1}. {t}" for i, t in enumerate(chapters))
        chunk_size         = 5
        total_chunks       = math.ceil(n / chunk_size)

        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_{task_id}")]
        ])
        try:
            await temp_msg.delete()
        except Exception:
            pass

        status_msg = await message.answer(
            build_progress_text(task_id, friendly_model, n, 0, total_chunks),
            reply_markup=cancel_kb, parse_mode="HTML",
        )

        # ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ГЕНЕРАЦИИ ──────────────────────────────
        full_script_parts: list[str] = [""] * n
        SEQ_INTRO_CHAPTERS = min(3, n)
        covered_summary    = ""
        project_bible      = ""  # библия извлекается после первых глав (доп. слой)

        def build_chapter_prompt(index, title, prev_text="", covered="", bible="", include_cta=False):
            cta_instr = (
                "\n5. CTA: В конце — ненавязчивый призыв написать одно слово в комментариях."
                if include_cta else "\n5. CTA: В этой части НЕТ призыва."
            )
            prev_block = (
                f"\nПРЕДЫДУЩАЯ ЧАСТЬ (не повторяй):\n{prev_text[-800:]}\n"
                if prev_text else ""
            )
            covered_block = (
                f"\nУЖЕ РАСКРЫТО (нельзя повторять):\n{covered}\n"
                if covered else ""
            )
            # Мастер-документ — главный источник истины для всех глав
            master_block = (
                f"\nМАСТЕР-ДОКУМЕНТ (строго соблюдай — имена, факты, события):\n{master_doc}\n"
                if master_doc else ""
            )
            # Библия — дополнительный слой после первых глав
            bible_block = (
                f"\nДОПОЛНИТЕЛЬНЫЕ ФАКТЫ ИЗ НАПИСАННОГО:\n{bible}\n"
                if bible else ""
            )
            return (
                f"Ты пишешь часть сценария для YouTube-видео.\n\n"
                f"ТЕМА: {data['topic']}\n\n"
                f"ПЛАН ({n} частей):\n{full_plan_str}\n\n"
                f"ЗАДАЧА: написать ТОЛЬКО часть №{index+1} — «{title}».\n"
                f"Не повторяй тезисы из других частей."
                f"{master_block}{bible_block}{prev_block}{covered_block}\n"
                f"СТИЛЬ: {style_prompt}\n\n"
                f"ПРАВИЛА:\n"
                f"1. ОБЪЁМ: ровно {words_to_request} слов. Никаких пометок.\n"
                f"2. ФОРМАТ: только сплошной текст. Без заголовков, #, *, ---, списков.\n"
                f"3. НАЧАЛО: сразу с первого слова.\n"
                f"4. КОНЕЦ: завершай мысль естественно."
                f"{cta_instr}"
            )

        async def generate_one(index, title, prev_text="", covered="", bible="", is_regen=False):
            if await get_task_status(task_id) == "Cancelled":
                return
            prompt = build_chapter_prompt(
                index, title, prev_text, covered, bible,
                include_cta=(index in cta_positions) and not is_regen,
            )
            try:
                raw   = await api_call_with_retry(
                    model_id, [{"role": "user", "content": prompt}], max_tokens_chapter,
                )
                clean = clean_chapter_text(raw)
                full_script_parts[index] = clean + "\n\n"
                logging.info(f"[{task_id}] ✅ {index+1}/{n} — {len(clean.split())} слов")
            except Exception as e:
                logging.error(f"[{task_id}] ❌ {index+1}: {e}")
                full_script_parts[index] = ""

        async def get_covered_summary(texts):
            combined = " ".join(t[:600] for t in texts if t)
            if not combined.strip():
                return ""
            try:
                return (await api_call_with_retry(
                    model_id,
                    [{"role": "user", "content":
                      f"Перечисли в 3-4 предложениях ключевые тезисы. Только суть:\n\n{combined}"}],
                    300,
                )).strip()
            except Exception:
                return ""

        # ── ГЕНЕРАЦИЯ ЧАСТЕЙ ───────────────────────────────────────────────
        done_count = 0

        # Последовательно — первые 3 (вводные)
        for i in range(SEQ_INTRO_CHAPTERS):
            if await get_task_status(task_id) == "Cancelled":
                return
            prev = full_script_parts[i-1] if i > 0 else ""
            await generate_one(i, chapters[i], prev_text=prev, covered=covered_summary, bible=project_bible)
            done_count += 1
            await safe_edit(
                status_msg,
                build_progress_text(task_id, friendly_model, n, done_count,
                                    max(0, math.ceil((n - done_count) / chunk_size))),
                reply_markup=cancel_kb,
            )

        # После первых глав — извлекаем только сводку тезисов
        covered_summary = await get_covered_summary(
            [full_script_parts[i] for i in range(SEQ_INTRO_CHAPTERS)]
        )

        # Параллельно — остальные пачками
        remaining = list(range(SEQ_INTRO_CHAPTERS, n))
        for batch_start in range(0, len(remaining), chunk_size):
            if await get_task_status(task_id) == "Cancelled":
                return
            batch = remaining[batch_start:batch_start + chunk_size]
            await asyncio.gather(*[
                generate_one(i, chapters[i], covered=covered_summary, bible=project_bible) for i in batch
            ])
            done_count += len(batch)
            await safe_edit(
                status_msg,
                build_progress_text(task_id, friendly_model, n, done_count,
                                    max(0, math.ceil((n - done_count) / chunk_size))),
                reply_markup=cancel_kb,
            )
            await asyncio.sleep(2)
            if batch_start + chunk_size < len(remaining):
                new_sum = await get_covered_summary([full_script_parts[i] for i in batch])
                if new_sum:
                    covered_summary = (covered_summary + "\n" + new_sum)[-1200:]

        if await get_task_status(task_id) == "Cancelled":
            return

        # Доработка коротких частей
        short_indices = [
            i for i, p in enumerate(full_script_parts)
            if len(p.split()) < words_per_chapter * MIN_CHAPTER_RATIO
        ]
        if short_indices:
            await safe_edit(
                status_msg,
                build_progress_text(task_id, friendly_model, n, n, 0, phase="regen"),
                reply_markup=cancel_kb,
            )
            for attempt in range(MAX_REGEN_ATTEMPTS):
                if await get_task_status(task_id) == "Cancelled":
                    return
                if not short_indices:
                    break
                await asyncio.gather(*[
                    generate_one(i, chapters[i], covered=covered_summary, bible=project_bible, is_regen=True)
                    for i in short_indices
                ])
                await asyncio.sleep(2)
                short_indices = [
                    i for i in short_indices
                    if len(full_script_parts[i].split()) < words_per_chapter * MIN_CHAPTER_RATIO
                ]
                logging.info(f"[{task_id}] 🔧 Попытка {attempt+1}: осталось коротких {len(short_indices)}")

        if await get_task_status(task_id) == "Cancelled":
            return

        # ── СБОРКА ─────────────────────────────────────────────────────────
        full_script = "".join(full_script_parts).strip()
        full_script = re.sub(r'\n{3,}', '\n\n', full_script)
        word_count  = len(full_script.split())
        deviation   = word_count - words_target
        logging.info(f"[{task_id}] 📊 {word_count} слов | цель {words_target} | {deviation:+d}")

        with open(file_name, "w", encoding="utf-8") as f:
            f.write(full_script)

        # Списываем кредиты
        await deduct_credits(user_id, cost, f"🎬 Сценарий {task_id} ({friendly_model}, {duration} мин)")
        new_balance = await get_balance(user_id)

        caption = (
            f"📄 Сценарий ID: {task_id}\n"
            f"📊 ~{word_count} слов (~{word_count // WORDS_PER_MINUTE} мин.)\n"
            f"🎯 Цель: {words_target} слов ({duration} мин.)\n"
            f"💰 Списано: {cost:.1f} кред. | Остаток: {new_balance:.1f} кред."
        )

        await safe_edit(
            status_msg,
            build_progress_text(task_id, friendly_model, n, n, 0, phase="done"),
            reply_markup=None,
        )

        try:
            await message.answer_document(FSInputFile(file_name), caption=caption)
        except Exception as e:
            logging.error(f"[{task_id}] ТГ: {e}")
            backup = await upload_to_backup(file_name)
            if backup:
                await message.answer(
                    f"⚠️ Telegram не смог отправить файл:\n"
                    f"🔗 <a href='{backup}'>Скачать</a> <i>(удалится после скачивания)</i>",
                    parse_mode="HTML",
                )
            else:
                await message.answer(f"❌ Файл {task_id} не отправился.")

        await update_task_status(task_id, "Completed")

        await message.answer(
            f"✅ Готово! Списано <b>{cost:.1f} кред.</b> | Остаток: <b>{new_balance:.1f} кред.</b>\n\n"
            "Создать ещё один?",
            reply_markup=ReplyKeyboardMarkup(keyboard=[
                [KeyboardButton(text="🎬 Создать сценарий")],
                [KeyboardButton(text="🔙 Назад в меню")],
            ], resize_keyboard=True),
            parse_mode="HTML",
        )

    except Exception as e:
        logging.error(f"[{task_id}] Критическая ошибка: {e}")
        try:
            await message.answer(
                f"❌ <b>Ошибка</b> {task_id}\n\n<code>{str(e)}</code>",
                parse_mode="HTML",
            )
        except Exception:
            pass
        await update_task_status(task_id, f"Error: {str(e)}")

    finally:
        _active_tasks -= 1
        _generation_semaphore.release()
        await state.clear()
        if os.path.exists(file_name):
            os.remove(file_name)

# ---------------------------------------------------------------------------
# ЗАПУСК
# ---------------------------------------------------------------------------
async def main():
    print("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
