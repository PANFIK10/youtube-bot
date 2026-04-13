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
    "anthropic/claude-haiku-4.5":    1.75,
    "anthropic/claude-sonnet-4.6":   1.65,
    "openai/gpt-5.1":                1.00,
    "google/gemini-2.5-flash-lite":  1.55,
    "x-ai/grok-4.1-fast":            1.55,
}
VOLUME_OVERREQUEST_FACTOR_DEFAULT = 1.50

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
    """Максимальная стоимость (резерв) — по запрошенной длительности. Округление вверх до 0.5."""
    price_per_min = MODEL_PRICE_PER_MINUTE.get(model_id, 1.0)
    return math.ceil(price_per_min * duration_min * 2) / 2


def calc_cost_by_words(model_id: str, word_count: int) -> float:
    """Фактическая стоимость — по реальному количеству слов в сценарии. Округление вверх до 0.5."""
    price_per_min = MODEL_PRICE_PER_MINUTE.get(model_id, 1.0)
    actual_minutes = word_count / WORDS_PER_MINUTE
    return math.ceil(price_per_min * actual_minutes * 2) / 2


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
    text = re.sub(r'^#{1,6}[\s#].*$', '', text, flags=re.MULTILINE)   # # комментарии модели
    text = re.sub(r'^#\s*[А-ЯЁA-Z][^\n]{0,80}$', '', text, flags=re.MULTILINE)  # # Единый отрывок и т.п.
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
    # Слипшиеся слова: строчная кириллица + заглавная без пробела (артефакт стыков)
    text = re.sub(r'([а-яё])([А-ЯЁ])', r'\1 \2', text)
    # Латинские буквы внутри кириллических слов (напр. Акupрессура → Акупрессура)
    text = re.sub(r'([а-яёА-ЯЁ])([a-zA-Z])([а-яёА-ЯЁ])', r'\1\3', text)
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


def strip_cta_from_text(text: str) -> str:
    """
    Вырезает CTA-фразы которые модель вставила сама.
    Ищет паттерны «Напиши в комментарии...», «Поставь лайк...» и т.п.
    и удаляет их вместе с абзацем.
    """
    # Паттерны CTA которые модель генерирует
    cta_patterns = [
        r'[Нн]апиши\s+в\s+комментар[иях]+[^.!?\n]{0,200}[.!?\n]?',
        r'[Нн]апишите\s+в\s+комментар[иях]+[^.!?\n]{0,200}[.!?\n]?',
        r'[Пп]оставь\s+(лайк|подписку|палец)[^.!?\n]{0,150}[.!?\n]?',
        r'[Пп]одпишитесь\s+на[^.!?\n]{0,150}[.!?\n]?',
        r'[Пп]одписывайтесь[^.!?\n]{0,150}[.!?\n]?',
        r'[Нн]е\s+забудь\s+подпис[а-я]+[^.!?\n]{0,150}[.!?\n]?',
        r'[Пп]иши\s+в\s+комментар[иях]+[^.!?\n]{0,200}[.!?\n]?',
        r'[Оо]ставь\s+(своё мнение|комментарий|ответ)[^.!?\n]{0,150}[.!?\n]?',
        r'[Чч]то\s+думаешь[^.!?\n]{0,150}[.!?\n]?',
    ]
    for pattern in cta_patterns:
        text = re.sub(pattern, '', text)
    # Убираем пустые строки образовавшиеся после удаления
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def inject_cta(parts: list[str], cta_positions: set[int],
               style_prompt: str, topic: str) -> list[str]:
    """
    Вставляет CTA программно после указанных глав.
    Генерирует тематический вопрос на основе темы сценария.
    """
    if not cta_positions:
        return parts

    result = list(parts)
    for idx in sorted(cta_positions):
        if idx >= len(result) or not result[idx]:
            continue
        # Простой CTA — одно предложение в конце главы
        cta_text = f"\n\nНапиши в комментарии одно слово — что ты думаешь об этом?"
        result[idx] = result[idx].rstrip() + cta_text + "\n\n"
    return result


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


# Максимум слов для полного прогона полировки за один запрос
POLISH_CHUNK_WORDS = 2500

async def polish_script(model_id: str, text: str, topic: str,
                        style_prompt: str, task_id: str) -> str:
    """
    Финальная полировка готового сценария.
    Для коротких (<= POLISH_CHUNK_WORDS) — один запрос на весь текст.
    Для длинных — прогон по перекрывающимся чанкам с сохранением стыков.
    """
    words = text.split()
    total = len(words)
    logging.info(f"[{task_id}] ✨ Полировка: {total} слов")

    polish_instruction = (
        f"Ты — финальный редактор сценария для YouTube.\n"
        f"ТЕМА: «{topic}»\n"
        f"СТИЛЬ: {style_prompt[:200]}\n\n"
        f"ЗАДАЧИ РЕДАКТОРА:\n"
        f"1. СВЯЗНОСТЬ: убери все места где текст звучит как начало нового видео "
        f"('Сегодня мы поговорим', 'В этом видео', 'Привет всем'). "
        f"Замени плавным продолжением предыдущей мысли.\n"
        f"2. ПОВТОРЫ: найди факты, фразы, идеи которые встречаются дважды — "
        f"удали или перефразируй второе вхождение.\n"
        f"3. ПЕРЕХОДЫ: там где между абзацами резкий обрыв — добавь 1-2 слова-мостика "
        f"('Именно тогда...', 'Но это было только начало...', 'Между тем...').\n"
        f"4. РИТМ: разбей слишком длинные предложения (>25 слов). "
        f"Чередуй длинные и короткие для живого звучания.\n\n"
        f"СТРОГО ЗАПРЕЩЕНО:\n"
        f"— Менять факты, имена, даты\n"
        f"— Добавлять новые сюжетные линии\n"
        f"— Сокращать объём более чем на 10%\n"
        f"— Добавлять призывы к действию\n\n"
        f"Верни ТОЛЬКО исправленный текст без комментариев."
    )

    if total <= POLISH_CHUNK_WORDS:
        # Короткий — один запрос
        try:
            result = await api_call_with_retry(
                model_id,
                [{"role": "user", "content": f"{polish_instruction}\n\nТЕКСТ:\n{text}"}],
                min(int(total * 1.3 * 1.5), 8000),
            )
            if result and len(result.split()) > total * 0.7:
                logging.info(f"[{task_id}] ✨ Полировка завершена ({len(result.split())} слов)")
                return result.strip()
        except Exception as e:
            logging.warning(f"[{task_id}] ✨ Полировка не удалась: {e}")
        return text

    # Длинный — чанки с перекрытием 200 слов для сохранения контекста стыков
    chunk_size = POLISH_CHUNK_WORDS
    overlap    = 200
    polished_chunks: list[str] = []
    pos = 0

    while pos < total:
        chunk_words = words[pos: pos + chunk_size]
        chunk_text  = " ".join(chunk_words)

        # Передаём хвост предыдущего чанка как контекст (не редактируем его)
        context_block = ""
        if polished_chunks:
            prev_tail = " ".join(polished_chunks[-1].split()[-overlap:])
            context_block = (
                f"КОНЕЦ ПРЕДЫДУЩЕГО БЛОКА (только для контекста, не редактировать):\n"
                f"«...{prev_tail}»\n\n"
            )

        try:
            result = await api_call_with_retry(
                model_id,
                [{"role": "user", "content":
                  f"{polish_instruction}\n\n{context_block}ТЕКСТ ДЛЯ РЕДАКТИРОВАНИЯ:\n{chunk_text}"}],
                min(int(len(chunk_words) * 1.3 * 1.5), 8000),
            )
            if result and len(result.split()) > len(chunk_words) * 0.7:
                polished_chunks.append(result.strip())
            else:
                polished_chunks.append(chunk_text)
        except Exception as e:
            logging.warning(f"[{task_id}] ✨ Чанк {len(polished_chunks)+1} не отполирован: {e}")
            polished_chunks.append(chunk_text)

        pos += chunk_size
        await asyncio.sleep(1)  # небольшая пауза между запросами

    polished = "\n\n".join(polished_chunks)
    polished = re.sub(r'\n{3,}', '\n\n', polished)
    logging.info(f"[{task_id}] ✨ Полировка завершена ({len(polished.split())} слов, "
                 f"{len(polished_chunks)} чанков)")
    return polished


async def generate_master_doc(
    model_id: str, topic: str, style_prompt: str,
    duration_min: int, script_style: str = "documentary", factual: str = ""
) -> str:
    """
    Генерирует мастер-документ ДО написания глав.
    Промпт зависит от жанра: fiction / documentary / educational.
    """
    factual_block = (
        f"\nПОЛЬЗОВАТЕЛЬСКАЯ ФАКТУРА (используй как основу, не придумывай лишнего):\n"
        f"{factual}\n"
    ) if factual else ""

    if script_style == "fiction":
        # ── ХУДОЖЕСТВЕННЫЙ ─────────────────────────────────────────────────
        prompt = (
            f"Ты — главный сценарист. Создай МАСТЕР-ДОКУМЕНТ для художественного "
            f"YouTube-сценария.\n"
            f"ТЕМА: «{topic}»\n"
            f"СТИЛЬ: {style_prompt[:200]}\n"
            f"ХРОНОМЕТРАЖ: ~{duration_min} минут\n"
            f"{factual_block}\n"
            f"СОЗДАЙ И ЖЁСТКО ЗАФИКСИ:\n\n"
            f"1. ПЕРСОНАЖИ (минимум 2-3):\n"
            f"   Для каждого: полные ФИО (придумай конкретные имена, они не должны "
            f"   повторяться и не должны путаться между собой), точный возраст, "
            f"   профессия, главная черта характера, тайна или боль персонажа.\n"
            f"   СТОП-СЛОВА: нельзя использовать 'молодая женщина', 'опытный врач' — "
            f"   только конкретные имена и детали.\n\n"
            f"2. ПРЕДЫСТОРИЯ КОНФЛИКТА:\n"
            f"   Что произошло в прошлом? Конкретные события, даты, имена. "
            f"   Кто виноват на самом деле?\n\n"
            f"3. ХРОНОЛОГИЯ СЮЖЕТА:\n"
            f"   — Завязка: с чего конкретно начинается история\n"
            f"   — Поворотный момент: неожиданное событие меняющее всё\n"
            f"   — Кульминация: самый напряжённый момент\n"
            f"   — Развязка: чем всё заканчивается\n\n"
            f"4. СЕТТИНГ: конкретный город, место, год или время года.\n\n"
            f"5. НЕРУШИМЫЕ ДЕТАЛИ (7-10 фактов):\n"
            f"   Конкретные детали которые НЕЛЬЗЯ менять во всём сценарии: "
            f"   имена родственников, возраст, даты событий, названия мест, "
            f"   профессии, физические особенности персонажей.\n"
            f"   Пример: «Дочь главного героя зовётся Виолетта, ей 9 лет, "
            f"   она учится в 3-м классе школы №47 в Екатеринбурге».\n\n"
            f"Объём: 400-500 слов. Только структурированные факты.\n"
            f"ЗАКОН: все имена и факты из этого документа ЗАПРЕЩЕНО менять "
            f"в любой части сценария."
        )

    elif script_style == "documentary":
        # ── ДОКУМЕНТАЛЬНЫЙ ─────────────────────────────────────────────────
        prompt = (
            f"Ты — главный редактор документального канала. Создай МАСТЕР-ДОКУМЕНТ "
            f"для документального YouTube-сценария.\n"
            f"ТЕМА: «{topic}»\n"
            f"СТИЛЬ: {style_prompt[:200]}\n"
            f"ХРОНОМЕТРАЖ: ~{duration_min} минут\n"
            f"{factual_block}\n"
            f"ВАЖНО: Используй ТОЛЬКО реальные факты. Ничего не придумывай. "
            f"Если факт неизвестен — так и пиши 'точная дата неизвестна'.\n\n"
            f"СОСТАВЬ:\n\n"
            f"1. ГЛАВНЫЕ РЕАЛЬНЫЕ ФИГУРЫ:\n"
            f"   Реальные люди упоминаемые в теме: ФИО, роль, годы жизни если известны.\n\n"
            f"2. КЛЮЧЕВЫЕ ФАКТЫ И ДАТЫ:\n"
            f"   8-12 конкретных фактов с датами которые составляют основу сценария. "
            f"   Только то что можно проверить.\n\n"
            f"3. ХРОНОЛОГИЯ СОБЫТИЙ:\n"
            f"   Временная линия — что за чем происходило.\n\n"
            f"4. ГЛАВНЫЙ ТЕЗИС:\n"
            f"   Что должен вынести зритель после просмотра? "
            f"   Какой главный вывод или открытие?\n\n"
            f"5. СПОРНЫЕ ИЛИ НЕИЗВЕСТНЫЕ МОМЕНТЫ:\n"
            f"   Что точно неизвестно? Где есть версии? "
            f"   (Чтобы сценарий не выдавал домыслы за факты.)\n\n"
            f"6. НЕЛЬЗЯ ПРИДУМЫВАТЬ:\n"
            f"   Перечисли конкретные вещи которые модель может захотеть додумать — "
            f"   и которые нельзя.\n\n"
            f"Объём: 350-450 слов. Только проверяемые факты.\n"
            f"ЗАКОН: никакой выдумки, никаких несуществующих цитат, "
            f"никаких додуманных дат."
        )

    else:
        # ── ПОЗНАВАТЕЛЬНЫЙ ─────────────────────────────────────────────────
        prompt = (
            f"Ты — главный редактор образовательного канала. Создай МАСТЕР-ДОКУМЕНТ "
            f"для познавательного YouTube-сценария.\n"
            f"ТЕМА: «{topic}»\n"
            f"СТИЛЬ: {style_prompt[:200]}\n"
            f"ХРОНОМЕТРАЖ: ~{duration_min} минут\n"
            f"{factual_block}\n"
            f"СОСТАВЬ:\n\n"
            f"1. ГЛАВНЫЙ ВОПРОС:\n"
            f"   На какой главный вопрос отвечает этот сценарий? "
            f"   Сформулируй одним предложением.\n\n"
            f"2. КЛЮЧЕВЫЕ ПОНЯТИЯ (5-8):\n"
            f"   Термины и концепции которые будут объяснены. "
            f"   Для каждого — одно точное определение.\n\n"
            f"3. СТРУКТУРА ОБЪЯСНЕНИЯ:\n"
            f"   — Отправная точка: с чего начинаем (что зритель уже знает)\n"
            f"   — Ключевые открытия: 3-5 идей которые удивят или просветят\n"
            f"   — Главный вывод: что зритель поймёт в конце\n\n"
            f"4. ФАКТЫ И ПРИМЕРЫ:\n"
            f"   6-10 конкретных примеров, цифр, историй которые будут "
            f"   использованы для объяснения.\n\n"
            f"5. НЕЛЬЗЯ УПРОЩАТЬ:\n"
            f"   Какие нюансы темы нельзя потерять ради доступности?\n\n"
            f"Объём: 350-450 слов.\n"
            f"ЗАКОН: все факты и определения из этого документа остаются "
            f"неизменными во всём сценарии."
        )

    try:
        result = await api_call_with_retry(
            model_id,
            [{"role": "user", "content": prompt}],
            1000,
        )
        return result.strip() if result else ""
    except Exception as e:
        logging.warning(f"Мастер-документ: не удалось создать: {e}")
        return ""


async def generate_mini_briefs(
    model_id: str, chapters: list[str], master_doc: str,
    topic: str, style_prompt: str, script_style: str, task_id: str
) -> list[str]:
    """
    Для каждой главы генерирует мини-бриф одним запросом.
    Для документального жанра дополнительно назначает каждой главе
    эксклюзивный набор фактов — больше ни одна глава их не использует.
    """
    full_plan = "\n".join(f"{i+1}. {t}" for i, t in enumerate(chapters))
    master_block = f"\nСИНОПСИС:\n{master_doc}\n" if master_doc else ""
    n = len(chapters)

    genre_hint = {
        "fiction":     "художественный сценарий — сцены, диалоги, эмоции персонажей",
        "documentary": "документальный сценарий — факты, хронология, реальные события",
        "educational": "познавательный сценарий — объяснения, аналогии, примеры, выводы",
    }.get(script_style, "сценарий для YouTube")

    if script_style == "documentary":
        # Для документального: явное разграничение фактов между главами
        prompt = (
            f"Ты — главный редактор документального канала. "
            f"Составь мини-брифы для {n} частей сценария о «{topic}».\n"
            f"ЖАНР: {genre_hint}\n"
            f"СТИЛЬ: {style_prompt[:150]}\n"
            f"{master_block}\n"
            f"ПЛАН:\n{full_plan}\n\n"
            f"КРИТИЧЕСКИ ВАЖНО: каждый факт, дата, цитата из синопсиса должна появиться "
            f"РОВНО В ОДНОЙ части — и ни в какой другой. Распредели факты без пересечений.\n\n"
            f"Для КАЖДОЙ части напиши строго в формате:\n"
            f"[N] НАЧАЛО: [с чего начать, подхватывая предыдущую часть]\n"
            f"    СУТЬ: [что раскрыть, 1-2 предложения]\n"
            f"    ФАКТЫ ТОЛЬКО ЭТОЙ ЧАСТИ: [конкретные даты/имена/события — только те, что ещё не были в предыдущих частях]\n"
            f"    ЗАПРЕЩЕНО УПОМИНАТЬ: [факты из других частей, которые могут соблазнить к повтору]\n"
            f"    КРЮЧОК: [на чём оборвать]\n\n"
            f"Не пиши текст сценария. Только бриф-карточки."
        )
    else:
        prompt = (
            f"Ты — главный редактор. Составь мини-бриф для каждой из {n} частей сценария.\n"
            f"ЖАНР: {genre_hint}\n"
            f"ТЕМА: «{topic}»\n"
            f"СТИЛЬ: {style_prompt[:150]}\n"
            f"{master_block}\n"
            f"ПЛАН:\n{full_plan}\n\n"
            f"Для КАЖДОЙ части напиши строго в формате:\n"
            f"[N] НАЧАЛО: [с какой мысли/события начать, подхватывая предыдущую часть]\n"
            f"    СУТЬ: [главное что раскрыть, 1-2 предложения]\n"
            f"    КРЮЧОК: [на чём оборвать — вопрос или напряжение без ответа]\n\n"
            f"Не пиши текст сценария. Только структурированные бриф-карточки."
        )
    try:
        raw = await api_call_with_retry(model_id, [{"role": "user", "content": prompt}], 2000)
        briefs: list[str] = [""] * n
        current_idx = -1
        current_lines: list[str] = []
        for line in raw.splitlines():
            m = re.match(r'^\[(\d+)\]', line.strip())
            if m:
                if current_idx >= 0 and current_idx < n:
                    briefs[current_idx] = "\n".join(current_lines).strip()
                current_idx = int(m.group(1)) - 1
                current_lines = [line]
            elif current_idx >= 0:
                current_lines.append(line)
        if current_idx >= 0 and current_idx < n:
            briefs[current_idx] = "\n".join(current_lines).strip()
        logging.info(f"[{task_id}] 📝 Мини-брифы: {sum(1 for b in briefs if b)}/{n}")
        return briefs
    except Exception as e:
        logging.warning(f"[{task_id}] Мини-брифы не удалось сгенерировать: {e}")
        return [""] * n


# ---------------------------------------------------------------------------
# СОСТОЯНИЯ
# ---------------------------------------------------------------------------
class ScriptMaker(StatesGroup):
    waiting_for_topic    = State()
    waiting_for_style    = State()   # выбор жанра
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

def get_style_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎭 Художественный")],
        [KeyboardButton(text="📰 Документальный")],
        [KeyboardButton(text="🎓 Познавательный")],
        [KeyboardButton(text="🔙 Назад в меню")],
    ], resize_keyboard=True)


def detect_factual_content(topic: str) -> tuple[str, str]:
    """
    Если тема длиннее 300 символов — считаем что пользователь вставил
    фактуру вместе с темой. Разделяем их.
    Возвращает (чистая_тема, фактура).
    """
    topic = topic.strip()
    if len(topic) <= 300:
        return topic, ""
    # Берём первую строку как тему, остальное — фактура
    lines = topic.splitlines()
    clean_topic = lines[0].strip()
    factual     = "\n".join(lines[1:]).strip()
    return clean_topic, factual


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

FAQ_TEXT = (
    "❓ <b>Частые вопросы</b>\n\n"
    "──────────────────────────\n"
    "<b>Как создать сценарий?</b>\n"
    "Нажми «🎬 Создать сценарий», введи тему, выбери жанр, длительность и шаблон.\n\n"

    "<b>Что такое жанр и как выбрать?</b>\n\n"

    "🎭 <b>Художественный</b> — история с персонажами, диалогами и сюжетом.\n"
    "Подходит когда нужна эмоциональная история от лица конкретного человека: "
    "его путь, испытания, решения. Бот выстраивает сцены, добавляет детали и драму.\n"
    "<i>Подойдёт для:</i> биографических драм, историй успеха и провала, "
    "жизненных случаев, криминальных историй с персонажами.\n\n"

    "📰 <b>Документальный</b> — факты, хронология, реальные события.\n"
    "Подходит когда нужно рассказать о том, что произошло на самом деле — "
    "без выдумки, только проверяемые данные в логичной последовательности.\n"
    "<i>Подойдёт для:</i> исторических событий, расследований, "
    "биографий реальных людей, разборов громких дел.\n\n"

    "🎓 <b>Познавательный</b> — объяснения, аналогии, «почему» и «как».\n"
    "Подходит когда нужно разобрать явление, процесс или концепцию — "
    "простым языком, с примерами и логикой.\n"
    "<i>Подойдёт для:</i> научпопа, психологии, финансов, технологий, "
    "любой темы где главное — объяснить, а не рассказать историю.\n\n"

    "──────────────────────────\n"
    "<b>Как считается хронометраж?</b>\n"
    f"Длина сценария рассчитывается из <b>{WORDS_PER_MINUTE} слов в минуту</b> — "
    "это средний темп профессиональной озвучки.\n"
    "Итоговый хронометраж зависит от диктора: кто-то читает быстрее, кто-то медленнее. "
    "Рекомендуем заказывать на <b>10–15 минут больше</b> нужного.\n\n"
    "<i>Пример: хочешь видео на 20 мин — заказывай 30–35 мин.</i>\n"
    "Списывается только за реально сгенерированные слова.\n\n"

    "──────────────────────────\n"
    "<b>Что такое кредиты?</b>\n"
    "1 кредит = 1 рубль. Списываются по факту — по количеству слов в готовом сценарии.\n"
    "Стартовый бонус — 50 кредитов бесплатно.\n\n"
    "<b>Чем отличаются модели?</b>\n"
    "🟢 Gemini — быстро и дёшево\n"
    "🔵 Grok — живой и дерзкий стиль\n"
    "🟡 Claude Haiku — литературный стиль\n"
    "🟠 ChatGPT — чёткая логика и структура\n"
    "🔴 Claude Sonnet — наивысшее качество\n\n"
    "<b>Что такое шаблоны?</b>\n"
    "Шаблоны задают стиль подачи: эмоциональный, сухой, от первого лица и т.д.\n"
    "Можно создавать свои в разделе «📁 Шаблоны».\n\n"
    "<b>Что такое массовая генерация?</b>\n"
    "Позволяет создать до 5 сценариев за один раз по разным темам.\n\n"
    "<b>Как работает реферальная программа?</b>\n"
    "Приглашай друзей по своей ссылке — получай 25 кредитов после их первой оплаты.\n\n"
    "──────────────────────────\n"
    "По всем вопросам: @aass11463"
)

OFERTA_TEXT = (
    "📜 <b>Публичная оферта Script AI</b>\n\n"
    "Самозанятый: Панферов Кирилл Алексеевич\n"
    "ИНН: 616810872170\n\n"
    "Оказываемая услуга: автоматическая генерация текстовых сценариев "
    "с использованием нейросетевых технологий.\n\n"
    "Оплата производится в кредитах (1 кредит = 1 рубль). "
    "Кредиты списываются только после успешного завершения генерации. "
    "Возврат неиспользованных кредитов осуществляется по запросу.\n\n"
    "Сгенерированный контент предоставляется «как есть». "
    "Сервис не несёт ответственности за точность фактических данных в сценариях.\n\n"
    "📩 kkpanferovvai@gmail.com | 💬 @aass11463"
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


@dp.message(F.text == "🏠 Главная")
async def home_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    balance = await get_balance(message.from_user.id)
    await message.answer(
        f"🏠 <b>Главное меню</b>\n\n💰 Баланс: <b>{balance:.1f} кредитов</b>",
        reply_markup=get_main_kb(), parse_mode="HTML",
    )


@dp.message(F.text == "❓ FAQ")
async def faq_cmd(message: types.Message):
    await message.answer(FAQ_TEXT, parse_mode="HTML")


# ---------------------------------------------------------------------------
# МАССОВАЯ ГЕНЕРАЦИЯ
# ---------------------------------------------------------------------------

class BulkMaker(StatesGroup):
    waiting_for_topics    = State()
    waiting_for_style     = State()
    waiting_for_duration  = State()
    waiting_for_template  = State()


@dp.message(F.text == "🗂 Массовая генерация")
async def bulk_start(message: types.Message, state: FSMContext):
    await message.answer(
        "🗂 <b>Массовая генерация</b>\n\n"
        "Введи темы сценариев — <b>каждая с новой строки</b>, до 5 тем:\n\n"
        "<i>Пример:\n"
        "История Tesla\n"
        "Как работает ChatGPT\n"
        "Тайны Бермудского треугольника</i>",
        reply_markup=types.ReplyKeyboardRemove(), parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_topics)


@dp.message(BulkMaker.waiting_for_topics)
async def bulk_topics(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    topics = [t.strip() for t in message.text.strip().splitlines() if t.strip()]
    if not topics:
        await message.answer("⚠️ Введи хотя бы одну тему.")
        return
    if len(topics) > 5:
        topics = topics[:5]
        await message.answer(f"⚠️ Взяты первые 5 тем из {len(message.text.strip().splitlines())}.")
    await state.update_data(topics=topics)
    await message.answer(
        f"✅ Принято {len(topics)} тем.\n\nВыбери жанр (для всех сразу):",
        reply_markup=get_style_kb(), parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_style)


@dp.message(BulkMaker.waiting_for_style)
async def bulk_style(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    if message.text not in STYLE_MAP:
        await message.answer("Выбери жанр из списка:", reply_markup=get_style_kb())
        return
    await state.update_data(script_style=STYLE_MAP[message.text])
    description = STYLE_DESCRIPTIONS.get(message.text, "")
    await message.answer(
        f"{description}\n\n"
        "Укажи длительность каждого сценария в минутах:",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_duration)


@dp.message(BulkMaker.waiting_for_duration)
async def bulk_duration(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) < 1:
        await message.answer("⚠️ Введи число, например: <b>30</b>", parse_mode="HTML")
        return
    duration   = int(message.text)
    model_id   = await get_user_model(message.from_user.id)
    data       = await state.get_data()
    topics     = data["topics"]
    cost_each  = calc_cost(model_id, duration)
    cost_total = cost_each * len(topics)
    balance    = await get_balance(message.from_user.id)
    model_name = MODEL_NAMES.get(model_id, model_id)

    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)

    disclaimer = (
        f"ℹ️ <b>Как считается хронометраж</b>\n"
        f"Расчёт идёт из <b>{WORDS_PER_MINUTE} слов/мин</b> — средний темп озвучки.\n"
        f"Каждый диктор говорит в своём темпе. Рекомендуем закладывать "
        f"на <b>10–15 мин больше</b> желаемого хронометража.\n"
        f"Списывается фактически — по словам в готовом сценарии.\n\n"
    )

    if balance < cost_total:
        await message.answer(
            f"❌ <b>Недостаточно кредитов</b>\n\n"
            f"Нужно: <b>{cost_total:.1f}</b> ({len(topics)} × {cost_each:.1f})\n"
            f"Баланс: <b>{balance:.1f}</b>\n\n"
            "Нажми <b>💳 Пополнить</b> для пополнения.",
            reply_markup=get_main_kb(), parse_mode="HTML",
        )
        await state.clear()
        return

    await message.answer(
        f"{disclaimer}"
        f"💰 Макс. итого: <b>{cost_total:.1f} кред.</b> ({len(topics)} × {cost_each:.1f}, {model_name}, {duration} мин)\n"
        f"Баланс: <b>{balance:.1f} кред.</b> | Списывается по факту.\n\n"
        "Выбери шаблон (применится ко всем темам):",
        reply_markup=await get_dynamic_templates_kb(for_generation=True), parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_template)


@dp.message(BulkMaker.waiting_for_template)
async def bulk_generate(message: types.Message, state: FSMContext):
    global _active_tasks
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    if message.text == "➕ Создать новый шаблон":
        await message.answer("Введи название:", reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(TemplateManager.waiting_for_new_name)
        return

    templates = await get_templates()
    if message.text not in templates:
        await message.answer("Выбери из списка:", reply_markup=await get_dynamic_templates_kb(for_generation=True))
        return

    style_prompt = templates[message.text]
    data         = await state.get_data()
    topics       = data["topics"]
    model_id     = await get_user_model(message.from_user.id)
    duration     = data["duration"]
    cost_each    = calc_cost(model_id, duration)
    cost_total   = cost_each * len(topics)
    balance      = await get_balance(message.from_user.id)

    if balance < cost_total:
        await message.answer(
            f"❌ Баланс изменился. Нужно: <b>{cost_total:.1f}</b>, есть: <b>{balance:.1f}</b>",
            reply_markup=get_main_kb(), parse_mode="HTML",
        )
        await state.clear()
        return

    await message.answer(
        f"🚀 Запускаю генерацию <b>{len(topics)}</b> сценариев...\n"
        f"Каждый будет отправлен по готовности.",
        reply_markup=get_main_kb(), parse_mode="HTML",
    )
    await state.clear()

    for topic in topics:
        # Подготавливаем данные как для одиночной генерации
        fake_state_data = {
            "topic": topic,
            "factual": "",
            "script_style": data.get("script_style", "documentary"),
            "duration": duration,
            "words_target": duration * WORDS_PER_MINUTE,
        }
        task_id   = f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"
        file_name = f"script_{task_id.replace(' ', '_')}.txt"
        friendly_model = MODEL_NAMES.get(model_id, "AI")

        if _generation_semaphore is None:
            await message.answer(f"❌ Ошибка: бот не готов. Тема «{topic}» пропущена.")
            continue

        await log_task(task_id, message.from_user.id, topic, friendly_model)
        asyncio.create_task(
            _run_generation(message, fake_state_data, style_prompt, model_id,
                            friendly_model, duration, cost_each, task_id, file_name)
        )
        await asyncio.sleep(1)  # небольшая пауза между запусками

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
    await message.answer(
        "О чём видео?\n\n"
        "<i>Можешь вставить просто тему или тему + фактуру (цитаты, даты, факты) — "
        "бот разберётся сам.</i>",
        reply_markup=types.ReplyKeyboardRemove(), parse_mode="HTML",
    )
    await state.set_state(ScriptMaker.waiting_for_topic)


@dp.message(ScriptMaker.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    text = message.text.strip()
    if len(text) > 4096:
        await message.answer("⚠️ Слишком длинный текст. Сократи до 4096 символов.")
        return

    topic, factual = detect_factual_content(text)
    await state.update_data(topic=topic, factual=factual)

    hint = ""
    if factual:
        hint = f"\n\n✅ <i>Обнаружена фактура ({len(factual.split())} слов) — будет использована при генерации.</i>"

    await message.answer(
        f"📌 Тема: <b>{topic[:100]}</b>{hint}\n\nВыбери жанр сценария:",
        reply_markup=get_style_kb(), parse_mode="HTML",
    )
    await state.set_state(ScriptMaker.waiting_for_style)


STYLE_MAP = {
    "🎭 Художественный": "fiction",
    "📰 Документальный": "documentary",
    "🎓 Познавательный": "educational",
}

STYLE_DESCRIPTIONS = {
    "🎭 Художественный": (
        "🎭 <b>Художественный</b>\n\n"
        "История рассказывается через персонажей: их мысли, диалоги, поступки и эмоции. "
        "Бот создаёт сцены, выстраивает напряжение и ведёт зрителя через сюжет.\n\n"
        "<b>Когда выбирать:</b> если в центре — конкретный человек и его история. "
        "Важно не что случилось, а <i>как это переживалось</i>.\n\n"
        "<b>Примеры тем:</b>\n"
        "— Биографическая драма о реальном человеке\n"
        "— История краха и возрождения\n"
        "— Криминальный сюжет с несколькими героями\n"
        "— Жизненная история с неожиданным финалом"
    ),
    "📰 Документальный": (
        "📰 <b>Документальный</b>\n\n"
        "Строгая хронология, только реальные факты и события. "
        "Бот работает как журналист: выстраивает цепочку событий, называет даты, имена и последствия.\n\n"
        "<b>Когда выбирать:</b> если важна достоверность и последовательность. "
        "Зритель должен узнать <i>что произошло</i> — без выдумки.\n\n"
        "<b>Примеры тем:</b>\n"
        "— Хроника реального события или скандала\n"
        "— Биография с акцентом на факты и даты\n"
        "— Расследование с доказательствами\n"
        "— История организации или явления"
    ),
    "🎓 Познавательный": (
        "🎓 <b>Познавательный</b>\n\n"
        "Объяснение через аналогии, примеры и логику. "
        "Бот берёт сложную тему и раскладывает её по полочкам — от простого к сложному, "
        "так чтобы любой понял с первого раза.\n\n"
        "<b>Когда выбирать:</b> если главное — <i>объяснить</i>, а не рассказать историю. "
        "Зритель должен уйти с новым пониманием.\n\n"
        "<b>Примеры тем:</b>\n"
        "— Как работает какое-то явление или процесс\n"
        "— Почему люди принимают те или иные решения\n"
        "— Разбор психологии, науки, экономики\n"
        "— История идеи или открытия"
    ),
}


@dp.message(ScriptMaker.waiting_for_style)
async def process_style(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    if message.text not in STYLE_MAP:
        await message.answer("Выбери жанр из списка:", reply_markup=get_style_kb())
        return
    await state.update_data(script_style=STYLE_MAP[message.text])
    description = STYLE_DESCRIPTIONS.get(message.text, "")
    await message.answer(
        f"{description}\n\n"
        "Укажи длительность сценария в минутах:",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode="HTML",
    )
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
    words_est   = duration * WORDS_PER_MINUTE

    await state.update_data(duration=duration, words_target=words_est)

    cost_line = (
        f"<i>💰 Макс. стоимость ({model_name}, {duration} мин): "
        f"<b>{cost:.1f} кред.</b> | Баланс: <b>{balance:.1f} кред.</b>\n"
        f"Списывается фактически — по количеству слов в готовом сценарии.</i>\n\n"
    )

    disclaimer = (
        f"ℹ️ <b>Как считается хронометраж</b>\n"
        f"Расчёт идёт из <b>{WORDS_PER_MINUTE} слов/мин</b> — средний темп озвучки.\n"
        f"Каждый диктор говорит в своём темпе: кто-то быстрее, кто-то медленнее.\n"
        f"Рекомендуем закладывать на <b>10–15 мин больше</b> желаемого хронометража.\n\n"
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
        f"{disclaimer}"
        f"{cost_line}"
        "Выбери шаблон:",
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

    asyncio.create_task(
        _run_generation(message, data, style_prompt, model_id,
                        friendly_model, duration, cost, task_id, file_name)
    )
    await state.clear()


async def _run_generation(message: types.Message, data: dict, style_prompt: str,
                          model_id: str, friendly_model: str, duration: int,
                          cost: float, task_id: str, file_name: str):
    global _active_tasks

    user_id      = message.from_user.id
    words_target = data.get("words_target", duration * WORDS_PER_MINUTE)

    await _generation_semaphore.acquire()
    _active_tasks += 1

    try:
        temp_msg = await message.answer("⏳ <i>Подготовка структуры...</i>", parse_mode="HTML")

        # ── МАСТЕР-ДОКУМЕНТ (синопсис) — генерируется ДО плана ─────────────
        target_chapters = max(1, round(words_target / WORDS_PER_CHAPTER))
        master_doc      = ""
        script_style    = data.get("script_style", "documentary")
        factual         = data.get("factual", "")

        if target_chapters > 3:
            master_doc = await generate_master_doc(
                model_id, data['topic'], style_prompt, duration,
                script_style=script_style, factual=factual,
            )
            if master_doc:
                logging.info(f"[{task_id}] 📋 Мастер-документ ({script_style}): {len(master_doc.split())} слов")

        # ── ПЛАН С ЗОНАМИ ОТВЕТСТВЕННОСТИ ──────────────────────────────────
        master_block = f"\nОПИРАЙСЯ СТРОГО НА ЭТОТ СИНОПСИС:\n{master_doc}\n\n" if master_doc else ""
        if script_style == "fiction":
            plan_prompt = (
                f"Составь сюжетный план для художественного YouTube-сценария: «{data['topic']}».\n"
                f"Нужно ровно {target_chapters} пунктов — не больше, не меньше.\n"
                f"{master_block}"
                f"ТРЕБОВАНИЯ К ПЛАНУ:\n"
                f"1. Каждый пункт — отдельная сцена или акт. Укажи: место, персонажи, событие, эмоциональный поворот.\n"
                f"   Формат: «Название сцены | место/время, что происходит, чем заканчивается»\n"
                f"   Пример: «Встреча в больнице | ночь, палата, Игорь впервые видит диагноз дочери и уходит не ответив»\n"
                f"2. Пункты образуют единую сюжетную дугу: завязка → нарастание → кульминация → развязка.\n"
                f"3. Каждая сцена двигает историю вперёд. Нет повторяющихся событий.\n"
                f"4. Только нумерованный список."
            )
        elif script_style == "educational":
            plan_prompt = (
                f"Составь план для познавательного YouTube-сценария: «{data['topic']}».\n"
                f"Нужно ровно {target_chapters} пунктов — не больше, не меньше.\n"
                f"{master_block}"
                f"ТРЕБОВАНИЯ К ПЛАНУ:\n"
                f"1. Каждый пункт — одна ключевая идея или концепция. Зритель после него должен понять что-то новое.\n"
                f"   Формат: «Тезис | что объясняем, какой пример/аналогию используем»\n"
                f"   Пример: «Почему мозг забывает | механизм забывания, аналогия с RAM компьютера, эксперимент Эббингауза»\n"
                f"2. Пункты выстроены от простого к сложному — каждый опирается на предыдущий.\n"
                f"3. Каждый пункт раскрывает НОВЫЙ аспект темы. Без повторов.\n"
                f"4. Только нумерованный список."
            )
        else:  # documentary
            plan_prompt = (
                f"Составь хронологический план для документального YouTube-сценария: «{data['topic']}».\n"
                f"Нужно ровно {target_chapters} пунктов — не больше, не меньше.\n"
                f"{master_block}"
                f"ТРЕБОВАНИЯ К ПЛАНУ:\n"
                f"1. Каждый пункт — уникальный период или событие. Укажи конкретные факты только этого пункта.\n"
                f"   Формат: «Заголовок | период/дата, ключевые события, действующие лица»\n"
                f"   Пример: «Детство в Алма-Ате | 1946-1964, отец-юрист Вольф Эйдельштейн, переезд в Москву в 1969»\n"
                f"2. Пункты выстроены как единый нарратив — каждый вытекает из предыдущего.\n"
                f"3. Каждый пункт отвечает на ДРУГОЙ вопрос. Не повторять факты между пунктами.\n"
                f"4. Только нумерованный список."
            )
        plan_raw = await api_call_with_retry(
            model_id, [{"role": "user", "content": plan_prompt}], 3000,
        )

        validate_prompt = (
            f"Вот план ({target_chapters} пунктов):\n\n{plan_raw}\n\n"
            f"Найди пункты, пересекающиеся по смыслу или по фактам, перепиши их.\n"
            f"Убедись что каждый пункт содержит уникальные события/факты.\n"
            f"Верни ровно {target_chapters} пунктов нумерованным списком. "
            f"Если дублей нет — верни исходный список."
        )
        validated = await api_call_with_retry(
            model_id, [{"role": "user", "content": validate_prompt}], 3000,
        )
        if validated and validated.strip():
            plan_raw = validated

        chapters = parse_plan_chapters(plan_raw, target_chapters)
        n        = len(chapters)

        words_per_chapter  = words_target // n
        overrequest_factor = VOLUME_OVERREQUEST_FACTORS.get(model_id, VOLUME_OVERREQUEST_FACTOR_DEFAULT)
        words_to_request   = max(50, int(words_per_chapter * overrequest_factor))
        max_tokens_chapter = min(int(words_to_request * 2.4), 4096)
        # CTA только если пользователь явно упомянул это в шаблоне
        _cta_keywords = ("cta", "комментар", "призыв", "подписк", "лайк",
                         "вопрос к зрител", "байт", "одно слово", "комментари")
        _user_wants_cta = any(kw in style_prompt.lower() for kw in _cta_keywords)
        cta_positions  = compute_cta_positions(n) if _user_wants_cta else set()
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

        def build_chapter_prompt(index, title, prev_text="", narrative_state="", mini_brief=""):
            # Для документального — мастер-документ только в первых главах,
            # дальше каждая глава работает со своим куском фактов из брифа.
            # Это предотвращает повтор: модель не видит всю фактуру сразу.
            if script_style == "documentary" and index >= 3 and mini_brief:
                lore_block = (
                    f"СПРАВОЧНИК ФАКТОВ (только для этой части, остальное уже использовано):\n"
                    f"{mini_brief}\n\n"
                )
                mini_brief_for_prompt = ""  # бриф уже вставлен в lore_block
            else:
                lore_block = (
                    f"╔══════════════════════════════════╗\n"
                    f"║         МАСТЕР-ДОКУМЕНТ          ║\n"
                    f"║  Используй ТОЛЬКО эти имена и    ║\n"
                    f"║  факты. Ничего не придумывай.    ║\n"
                    f"╚══════════════════════════════════╝\n"
                    f"{master_doc}\n"
                    f"════════════════════════════════════\n\n"
                ) if master_doc else ""
                mini_brief_for_prompt = mini_brief

            # Хвост предыдущей части — увеличен до 1500 символов
            prev_block = (
                f"ТАК ЗАКАНЧИВАЕТСЯ ПРЕДЫДУЩАЯ ЧАСТЬ — ПРОДОЛЖИ ОТСЮДА:\n"
                f"«...{prev_text[-1500:]}»\n\n"
            ) if prev_text else ""

            # Нарративный стейт — богатый контекст вместо голых фактов
            state_block = (
                f"НАРРАТИВНАЯ ЭСТАФЕТА (что было до тебя):\n{narrative_state}\n\n"
            ) if narrative_state else ""

            # Мини-бриф — конкретное задание для этой части
            brief_block = (
                f"ТВОЁ ЗАДАНИЕ ДЛЯ ЭТОЙ ЧАСТИ:\n{mini_brief_for_prompt}\n\n"
            ) if mini_brief_for_prompt else ""

            # Подсказка по позиции в структуре
            if index == 0:
                position_hint = "Это ПЕРВАЯ часть — начни с сильного зацепляющего момента, без вступлений и приветствий."
            elif index == n - 1:
                position_hint = "Это ПОСЛЕДНЯЯ часть — подведи к финальному выводу, дай ощущение завершённости истории."
            else:
                position_hint = f"Это часть {index+1} из {n} — продолжай нарратив, не начинай заново."

            # Жанровая инструкция
            genre_instructions = {
                "fiction": (
                    "ЖАНР: художественный сценарий.\n"
                    "Пиши через персонажей: их действия, диалоги, внутренние монологи. "
                    "Покажи эмоции через детали и поступки, а не через прямые описания чувств. "
                    "Каждая сцена должна менять ситуацию или характер героя."
                ),
                "documentary": (
                    "ЖАНР: документальный сценарий.\n"
                    "Пиши от лица рассказчика: факты, даты, реальные события. "
                    "Ничего не придумывай. Если факт неизвестен — так и говори. "
                    "Используй только имена и даты из мастер-документа."
                ),
                "educational": (
                    "ЖАНР: познавательный сценарий.\n"
                    "Объясняй через аналогии, примеры и вопросы к зрителю. "
                    "Веди от простого к сложному. Каждый абзац должен добавлять одно новое понимание. "
                    "Избегай жаргона — объясняй так, как объяснял бы умному другу без специального образования."
                ),
            }.get(script_style, "")

            genre_block = f"{genre_instructions}\n\n" if genre_instructions else ""

            return (
                f"{lore_block}"
                f"ПЛАН ВСЕГО СЦЕНАРИЯ ({n} частей):\n{full_plan_str}\n\n"
                f"{prev_block}"
                f"{state_block}"
                f"{brief_block}"
                f"{genre_block}"
                f"СЕЙЧАС ПИШЕШЬ: часть №{index+1} из {n} — «{title}»\n"
                f"{position_hint}\n\n"
                f"ГЛАВНОЕ ПРАВИЛО: этот текст — фрагмент единого сплошного повествования. "
                f"Не начинай с нуля. Не делай вводных предложений типа «Сегодня мы поговорим», "
                f"«В этом видео», «Привет». Продолжай историю там, где она прервалась. "
                f"В конце оборви на крючке из брифа — не подводи итог.\n\n"
                f"СТИЛЬ: {style_prompt}\n\n"
                f"ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ:\n"
                f"1. Объём: ровно {words_to_request} слов.\n"
                f"2. Формат: сплошной текст без заголовков, списков, символов #*-.\n"
                f"3. ЗАПРЕЩЕНО: вступления, подведение итогов, призывы к зрителям, "
                f"повтор фактов из нарративной эстафеты."
            )

        async def generate_one(index, title, prev_text="", narrative_state="",
                               mini_brief="", is_regen=False):
            if await get_task_status(task_id) == "Cancelled":
                return
            prompt = build_chapter_prompt(index, title, prev_text, narrative_state, mini_brief)
            try:
                raw   = await api_call_with_retry(
                    model_id, [{"role": "user", "content": prompt}], max_tokens_chapter,
                )
                clean = strip_cta_from_text(clean_chapter_text(raw))
                full_script_parts[index] = clean + "\n\n"
                logging.info(f"[{task_id}] ✅ {index+1}/{n} — {len(clean.split())} слов")
            except Exception as e:
                logging.error(f"[{task_id}] ❌ {index+1}: {e}")
                full_script_parts[index] = ""

        async def extract_narrative_state(text: str, chapter_index: int, chapter_title: str) -> str:
            """
            После каждой главы создаёт «нарративную эстафету» для следующего автора.
            Для документального жанра — расширенный список использованных фактов.
            """
            if not text or not text.strip():
                return ""
            sample = text[-3000:]
            if script_style == "documentary":
                prompt_text = (
                    f"Ты — редактор документального сценария. Глава №{chapter_index+1} «{chapter_title}» написана.\n"
                    f"Составь передачу эстафеты.\n\n"
                    f"ХВОСТ ГЛАВЫ:\n{sample}\n\n"
                    f"Ответь СТРОГО в этом формате:\n"
                    f"ПОСЛЕДНЕЕ СОБЫТИЕ: [одно предложение — где остановились хронологически]\n"
                    f"ПЕРИОД ЗАКРЫТ: [какой временной период или тема исчерпана — больше не возвращаться]\n"
                    f"ОТКРЫТЫЙ КРЮЧОК: [какой вопрос оставлен без ответа]\n"
                    f"ИСПОЛЬЗОВАННЫЕ ФАКТЫ — ЗАПРЕЩЕНО ПОВТОРЯТЬ:\n"
                    f"[перечисли все конкретные даты, имена, цифры, цитаты, события из этой главы — каждый с новой строки]\n"
                    f"СЛЕДУЮЩАЯ ГЛАВА ДОЛЖНА НАЧАТЬ С: [конкретная точка во времени или событии]"
                )
            else:
                prompt_text = (
                    f"Ты — сценарный редактор. Глава №{chapter_index+1} «{chapter_title}» написана.\n"
                    f"Составь передачу эстафеты следующему автору.\n\n"
                    f"ХВОСТ ГЛАВЫ:\n{sample}\n\n"
                    f"Ответь СТРОГО в этом формате (без лишних слов):\n"
                    f"ПОСЛЕДНЕЕ СОБЫТИЕ: [одно предложение — чем закончилась эта часть]\n"
                    f"ОТКРЫТЫЙ КРЮЧОК: [какой вопрос или напряжение оставлено без ответа]\n"
                    f"ЭМОЦИОНАЛЬНЫЙ ТОН: [одно слово: тревога/надежда/удивление/напряжение/грусть/триумф]\n"
                    f"АКТИВНЫЕ ЛИЦА: [кто последний раз упоминался]\n"
                    f"НЕ ПОВТОРЯТЬ: [3-5 конкретных факта или фразы которые уже прозвучали]"
                )
            try:
                return (await api_call_with_retry(
                    model_id,
                    [{"role": "user", "content": prompt_text}],
                    500 if script_style == "documentary" else 350,
                )).strip()
            except Exception:
                return ""

        async def stitch_seams(parts: list[str]) -> list[str]:
            """
            Для каждой пары соседних частей сглаживает стык:
            берёт хвост части N и голову части N+1, просит модель переписать
            их как единый фрагмент без разрыва. Все вызовы параллельны.
            """
            if len(parts) <= 1:
                return parts

            HALF = 250  # слов с каждой стороны стыка
            result = list(parts)

            async def fix_seam(i: int):
                words_a = result[i].split()
                words_b = result[i + 1].split()
                if len(words_a) < 60 or len(words_b) < 60:
                    return
                tail_a = " ".join(words_a[-HALF:])
                head_b = " ".join(words_b[:HALF])
                try:
                    stitched = await api_call_with_retry(
                        model_id,
                        [{"role": "user", "content":
                          f"Два фрагмента одного сценария идут подряд, но переход резкий.\n"
                          f"Перепиши их как ЕДИНЫЙ плавный отрывок.\n"
                          f"Сохрани все факты и события. Убери любые «начальные» фразы из второго фрагмента.\n"
                          f"Объём — примерно такой же. Только текст, без комментариев.\n\n"
                          f"КОНЕЦ ПЕРВОГО:\n«...{tail_a}»\n\n"
                          f"НАЧАЛО ВТОРОГО:\n«{head_b}...»"}],
                        min(HALF * 4, 3000),
                    )
                    if not stitched or len(stitched.split()) < HALF * 0.6:
                        return
                    stitched_words = stitched.split()
                    mid = len(stitched_words) // 2
                    new_tail = " ".join(stitched_words[:mid])
                    new_head = " ".join(stitched_words[mid:])

                    # Дедупликация: убираем из new_head слова которые уже есть в new_tail
                    tail_set = set(new_tail.lower().split()[-40:])
                    head_words = new_head.split()
                    # Пропускаем начало new_head пока оно дублирует конец new_tail (до 30 слов)
                    skip = 0
                    for w_idx in range(min(30, len(head_words))):
                        window = " ".join(head_words[w_idx:w_idx + 8]).lower()
                        if all(w in tail_set for w in window.split()[:4]):
                            skip = w_idx + 1
                    if skip:
                        new_head = " ".join(head_words[skip:])

                    result[i]     = " ".join(words_a[:-HALF]) + " " + new_tail
                    result[i + 1] = new_head + " " + " ".join(words_b[HALF:])
                    result[i]     = result[i].strip()
                    result[i + 1] = result[i + 1].strip()
                    logging.info(f"[{task_id}] 🔗 Стык {i+1}↔{i+2} сглажен (skip={skip})")
                except Exception as e:
                    logging.warning(f"[{task_id}] Стык {i+1}: {e}")

            await asyncio.gather(*[fix_seam(i) for i in range(len(result) - 1)])
            return result

        # ── МИНИ-БРИФЫ ─────────────────────────────────────────────────────
        await safe_edit(
            status_msg,
            f"⏳ <b>Генерация сценария</b>\n\n"
            f"🆔 ID: <code>{task_id}</code>\n"
            f"🤖 Модель: <b>{friendly_model}</b>\n\n"
            f"[░░░░░░░░░░] 0%\n"
            f"📝 <i>Составляем план для каждой части...</i>",
            reply_markup=cancel_kb,
        )
        mini_briefs = await generate_mini_briefs(
            model_id, chapters, master_doc, data['topic'], style_prompt, script_style, task_id
        )

        # ── ГЕНЕРАЦИЯ ЧАСТЕЙ: якорь + цепочно-параллельная генерация ─────────
        # Первые SEQ_ANCHOR глав — строго последовательно (закладывают мир).
        # Остальные — параллельные цепочки по CHAIN_SIZE глав каждая.
        # Внутри цепочки связность через prev_text, между цепочками — через
        # общую сводку нарратива (narrative_summary).
        SEQ_ANCHOR = min(6, n)
        CHAIN_SIZE = 3

        done_count      = 0
        narrative_state = ""

        # ── Фаза 1: якорные главы последовательно ──────────────────────────
        for i in range(SEQ_ANCHOR):
            if await get_task_status(task_id) == "Cancelled":
                return
            prev = full_script_parts[i - 1] if i > 0 else ""
            await generate_one(
                i, chapters[i],
                prev_text=prev,
                narrative_state=narrative_state,
                mini_brief=mini_briefs[i] if i < len(mini_briefs) else "",
            )
            done_count += 1
            await safe_edit(
                status_msg,
                build_progress_text(task_id, friendly_model, n, done_count,
                                    max(0, n - done_count)),
                reply_markup=cancel_kb,
            )
            new_state = await extract_narrative_state(
                full_script_parts[i], i, chapters[i]
            )
            if new_state:
                narrative_state = new_state

        # ── Сводка нарратива после якоря — используется всеми цепочками ───
        async def build_narrative_summary(parts_so_far: list[str]) -> str:
            """Краткая сводка всего написанного — общий контекст для параллельных цепочек."""
            combined = " ".join(p[-1500:] for p in parts_so_far if p)
            if not combined.strip():
                return narrative_state
            try:
                return (await api_call_with_retry(
                    model_id,
                    [{"role": "user", "content":
                      f"Ты — сценарный редактор. Написана первая часть сценария.\n"
                      f"Составь сводку для авторов следующих глав.\n\n"
                      f"НАПИСАННОЕ:\n{combined[-4000:]}\n\n"
                      f"Ответь в формате (до 300 слов):\n"
                      f"ГДЕ МЫ: [текущая точка сюжета, 2-3 предложения]\n"
                      f"ПЕРСОНАЖИ: [активные герои и их статус]\n"
                      f"ОТКРЫТЫЕ ЛИНИИ: [что ещё не разрешено]\n"
                      f"НЕЛЬЗЯ ПОВТОРЯТЬ: [5-7 фактов/фраз уже прозвучавших]\n"
                      f"ТОН: [эмоциональный тон финала якорных глав]"}],
                    600,
                )).strip()
            except Exception:
                return narrative_state

        # ── Фаза 2: параллельные цепочки ───────────────────────────────────
        remaining = list(range(SEQ_ANCHOR, n))

        if remaining:
            narrative_summary = await build_narrative_summary(
                full_script_parts[:SEQ_ANCHOR]
            )

            async def run_chain(chain_indices: list[int]):
                """Одна цепочка: главы генерируются последовательно внутри цепочки."""
                chain_narrative = narrative_summary  # общая сводка как стартовый контекст
                for idx in chain_indices:
                    if await get_task_status(task_id) == "Cancelled":
                        return
                    # prev_text — предыдущая глава ВНУТРИ цепочки, если есть
                    chain_pos = chain_indices.index(idx)
                    if chain_pos > 0:
                        prev = full_script_parts[chain_indices[chain_pos - 1]]
                    else:
                        # Первая глава цепочки — берём хвост последней якорной главы
                        prev = full_script_parts[SEQ_ANCHOR - 1] if SEQ_ANCHOR > 0 else ""
                    await generate_one(
                        idx, chapters[idx],
                        prev_text=prev,
                        narrative_state=chain_narrative,
                        mini_brief=mini_briefs[idx] if idx < len(mini_briefs) else "",
                    )
                    # Обновляем нарратив внутри цепочки
                    new_state = await extract_narrative_state(
                        full_script_parts[idx], idx, chapters[idx]
                    )
                    if new_state:
                        chain_narrative = new_state

            # Разбиваем remaining на цепочки и запускаем параллельно батчами
            PARALLEL_CHAINS = 3  # сколько цепочек идут одновременно
            chain_idx = 0
            while chain_idx < len(remaining):
                if await get_task_status(task_id) == "Cancelled":
                    return

                # Нарезаем следующий батч цепочек
                batch_chains = []
                for _ in range(PARALLEL_CHAINS):
                    start = chain_idx
                    end   = min(start + CHAIN_SIZE, len(remaining))
                    if start >= len(remaining):
                        break
                    batch_chains.append(remaining[start:end])
                    chain_idx = end

                await asyncio.gather(*[run_chain(c) for c in batch_chains])

                done_count = sum(1 for p in full_script_parts if p)
                await safe_edit(
                    status_msg,
                    build_progress_text(task_id, friendly_model, n, done_count,
                                        max(0, n - done_count)),
                    reply_markup=cancel_kb,
                )

                # Обновляем общую сводку между батчами
                if chain_idx < len(remaining):
                    narrative_summary = await build_narrative_summary(
                        [p for p in full_script_parts if p]
                    )

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
                for i in short_indices:
                    prev = full_script_parts[i - 1] if i > 0 else ""
                    await generate_one(
                        i, chapters[i],
                        prev_text=prev,
                        narrative_state=narrative_state,
                        mini_brief=mini_briefs[i] if i < len(mini_briefs) else "",
                        is_regen=True,
                    )
                await asyncio.sleep(2)
                short_indices = [
                    i for i in short_indices
                    if len(full_script_parts[i].split()) < words_per_chapter * MIN_CHAPTER_RATIO
                ]
                logging.info(f"[{task_id}] 🔧 Попытка {attempt+1}: осталось коротких {len(short_indices)}")

        if await get_task_status(task_id) == "Cancelled":
            return

        # ── СБОРКА ─────────────────────────────────────────────────────────
        assembled_parts = inject_cta(full_script_parts, cta_positions, style_prompt, data['topic'])

        # Сглаживаем стыки между частями
        await safe_edit(
            status_msg,
            f"⏳ <b>Генерация сценария</b>\n\n"
            f"🆔 ID: <code>{task_id}</code>\n"
            f"🤖 Модель: <b>{friendly_model}</b>\n\n"
            f"[▓▓▓▓▓▓▓▓░░] 80%\n"
            f"🔗 <i>Сглаживание переходов между частями...</i>",
            reply_markup=None,
        )
        assembled_parts = await stitch_seams(assembled_parts)

        full_script = "".join(assembled_parts).strip()
        full_script = re.sub(r'\n{3,}', '\n\n', full_script)
        word_count  = len(full_script.split())
        deviation   = word_count - words_target
        logging.info(f"[{task_id}] 📊 {word_count} слов | цель {words_target} | {deviation:+d}")

        # ── ФИНАЛЬНАЯ ПОЛИРОВКА ─────────────────────────────────────────────
        await safe_edit(
            status_msg,
            f"⏳ <b>Генерация сценария</b>\n\n"
            f"🆔 ID: <code>{task_id}</code>\n"
            f"🤖 Модель: <b>{friendly_model}</b>\n\n"
            f"[▓▓▓▓▓▓▓▓▓▓] 100%\n"
            f"✨ <i>Финальная полировка текста...</i>",
            reply_markup=None,
        )
        full_script = await polish_script(
            model_id, full_script, data['topic'], style_prompt, task_id
        )
        word_count = len(full_script.split())

        with open(file_name, "w", encoding="utf-8") as f:
            f.write(full_script)

        # Фактическая стоимость — по реальному количеству слов
        actual_cost     = calc_cost_by_words(model_id, word_count)
        actual_minutes  = round(word_count / WORDS_PER_MINUTE, 1)
        saving          = round(cost - actual_cost, 1)

        await deduct_credits(
            user_id, actual_cost,
            f"🎬 Сценарий {task_id} ({friendly_model}, ~{actual_minutes} мин. / {word_count} слов)"
        )
        new_balance = await get_balance(user_id)

        saving_str = f"\n💚 Сэкономлено: <b>{saving:.1f} кред.</b>" if saving > 0 else ""

        caption = (
            f"📄 Сценарий ID: {task_id}\n"
            f"📊 {word_count} слов ≈ {actual_minutes} мин. при {WORDS_PER_MINUTE} сл/мин\n"
            f"🎯 Запрошено: {duration} мин. | Сгенерировано: ~{actual_minutes} мин.\n"
            f"💰 Списано: {actual_cost:.1f} кред. (по факту) | Остаток: {new_balance:.1f} кред."
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
            f"✅ Готово! Списано <b>{actual_cost:.1f} кред.</b> (по факту){saving_str}\n"
            f"Остаток: <b>{new_balance:.1f} кред.</b>\n\n"
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
