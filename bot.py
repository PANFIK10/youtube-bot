import asyncio
import logging
import math
import os
import re
import asyncpg
import random
import string
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    FSInputFile,
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
_generating_users: set[int] = set()   # пользователи у которых прямо сейчас идёт генерация

API_RETRY_ATTEMPTS   = 3
API_RETRY_BASE_DELAY = 2.0

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()
logging.basicConfig(level=logging.INFO)

db_pool: asyncpg.Pool | None = None


class GenerationGuardMiddleware(BaseMiddleware):
    """Блокирует любые сообщения от пользователя во время генерации (кроме /start)."""
    async def __call__(self, handler, event, data):
        if isinstance(event, types.Message):
            user_id = event.from_user.id
            text    = event.text or ""
            if user_id in _generating_users and not text.startswith("/start"):
                await event.answer(
                    "⏳ <b>Идёт генерация</b> — бот временно не принимает команды.\n"
                    "Дождись результата или нажми <b>❌ Отменить</b> в сообщении с прогрессом.",
                    parse_mode="HTML",
                )
                return
        return await handler(event, data)

@dp.startup()
async def on_startup():
    global _generation_semaphore, _key_lock, db_pool, _active_tasks
    _generation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    _key_lock = asyncio.Lock()
    _active_tasks = 0
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10, ssl="prefer")
    await init_db()
    dp.message.middleware(GenerationGuardMiddleware())
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
    Вставляет CTA только после полного предложения (точка/восклицание/вопрос).
    Никогда не разрывает текст посередине.
    """
    if not cta_positions:
        return parts

    result = list(parts)
    for idx in sorted(cta_positions):
        if idx >= len(result) or not result[idx]:
            continue
        text = result[idx].rstrip()
        # Найти позицию последнего конца предложения
        last_end = max(
            text.rfind('.'), text.rfind('!'), text.rfind('?'),
            text.rfind('»'), text.rfind('…'),
        )
        if last_end > len(text) * 0.5:   # конец предложения есть и он не в самом начале
            text = text[:last_end + 1]
        cta_text = "\n\nНапиши в комментарии одно слово — что ты думаешь об этом?"
        result[idx] = text + cta_text + "\n\n"
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


# Порог глав для двухпроходной генерации (скелет → развёртка)
TWO_PASS_THRESHOLD = 10


async def complete_sentence(model_id: str, text: str) -> str:
    """
    Если глава обрывается на незавершённом предложении — дописывает его.
    Дешёвый вызов: просим не более 60 токенов.
    """
    stripped = text.rstrip()
    if stripped and stripped[-1] in '.!?»…':
        return text
    try:
        tail = stripped[-300:]
        completion = await api_call_with_retry(
            model_id,
            [{"role": "user", "content":
              f"Допиши последнее незавершённое предложение этого текста. "
              f"Верни ТОЛЬКО недостающую часть предложения (без повтора того что уже есть), "
              f"заканчивающуюся точкой, восклицательным или вопросительным знаком. "
              f"Не добавляй ничего лишнего.\n\nТЕКСТ:\n...{tail}"}],
            80,
        )
        if completion and completion.strip():
            comp = completion.strip()
            # Убираем повтор если модель всё же повторила хвост
            if stripped.endswith(comp[:20]):
                return text
            return stripped + " " + comp + "\n\n"
    except Exception:
        pass
    return text


async def consistency_check_and_fix(
    model_id: str, text: str, topic: str,
    script_style: str, master_doc: str, task_id: str
) -> str:
    """
    Финальная проверка консистентности всего сценария:
    1. Дублированные блоки (одинаковые предложения встречаются дважды)
    2. Противоречия времени/сезона/места
    3. Коллизии имён у разных персонажей
    4. Слипшиеся слова на стыках
    Работает за два вызова: сначала диагностика, потом точечные правки.
    """
    words = text.split()
    logging.info(f"[{task_id}] 🔍 Консистентность: {len(words)} слов")

    # ── Шаг 1: автоматическая чистка слипшихся слов ────────────────────────
    import re as _re
    text = _re.sub(r'([а-яё])([А-ЯЁ])', r'\1 \2', text)       # слипшиеся слова
    text = _re.sub(r'([а-zA-Zа-яёА-ЯЁ])([a-zA-Z])([а-яёА-ЯЁ])', r'\1\3', text)  # латинские буквы внутри слов

    # ── Шаг 2: поиск дублированных предложений ─────────────────────────────
    sentences = [s.strip() for s in _re.split(r'(?<=[.!?»])\s+', text) if len(s.strip()) > 50]
    seen: dict[str, int] = {}
    duplicate_phrases: list[str] = []
    for s in sentences:
        key = s[:70]
        if key in seen:
            duplicate_phrases.append(s[:120])
        else:
            seen[key] = 1
    if len(duplicate_phrases) > 0:
        logging.info(f"[{task_id}] 🔍 Найдено {len(duplicate_phrases)} дублей предложений")

    # ── Шаг 3: запрос к модели на исправление логических ошибок ───────────
    # Делим текст на чанки по 4000 слов чтобы не превышать контекст
    CHUNK = 4000
    words_list = text.split()
    total      = len(words_list)

    if total == 0:
        return text

    # Для коротких текстов — один запрос целиком
    if total <= CHUNK:
        chunks_to_fix = [text]
    else:
        chunks_to_fix = [
            " ".join(words_list[i:i+CHUNK])
            for i in range(0, total, CHUNK)
        ]

    genre_note = {
        "fiction":     "художественный сценарий с персонажами",
        "documentary": "документальный сценарий с реальными фактами",
        "educational": "познавательный сценарий с объяснениями",
    }.get(script_style, "сценарий")

    dup_block = ""
    if duplicate_phrases:
        dup_block = (
            "\nОБНАРУЖЕНЫ ДУБЛИ — удали второе вхождение каждого:\n"
            + "\n".join(f"  - «{d}...»" for d in duplicate_phrases[:8])
            + "\n"
        )

    fixed_chunks: list[str] = []
    prev_tail = ""

    for ci, chunk in enumerate(chunks_to_fix):
        ctx_block = f"КОНЕЦ ПРЕДЫДУЩЕГО БЛОКА (контекст, не редактировать):\n«...{prev_tail}»\n\n" if prev_tail else ""
        prompt = (
            f"Ты — финальный редактор {genre_note}.\n"
            f"ТЕМА: «{topic}»\n\n"
            f"ЗАДАЧА: найди и исправь ТОЛЬКО следующие проблемы:\n"
            f"1. Дублированные фрагменты — одно и то же событие/предложение описано дважды → убери второй раз\n"
            f"2. Противоречия времени/сезона/места — если в одном месте декабрь а в другом апрель без перехода → исправь\n"
            f"3. Коллизии имён — разные персонажи с одинаковым именем → переименуй второстепенного\n"
            f"4. Оборванные предложения — предложение начато но не закончено → допиши\n"
            f"{dup_block}"
            f"СТРОГО ЗАПРЕЩЕНО:\n"
            f"— Менять стиль, переформулировать нормальные предложения\n"
            f"— Добавлять новые события или персонажей\n"
            f"— Сокращать текст более чем на 5%\n\n"
            f"Если проблем не найдено — верни текст без изменений.\n"
            f"Верни ТОЛЬКО исправленный текст.\n\n"
            f"{ctx_block}"
            f"ТЕКСТ:\n{chunk}"
        )
        try:
            chunk_words = len(chunk.split())
            result = await api_call_with_retry(
                model_id,
                [{"role": "user", "content": prompt}],
                min(int(chunk_words * 1.2 * 1.5), 8000),
            )
            if result and len(result.split()) > chunk_words * 0.7:
                fixed_chunks.append(result.strip())
                prev_tail = " ".join(result.split()[-200:])
            else:
                fixed_chunks.append(chunk)
                prev_tail = " ".join(chunk.split()[-200:])
        except Exception as e:
            logging.warning(f"[{task_id}] 🔍 Чанк {ci+1} консистентности не обработан: {e}")
            fixed_chunks.append(chunk)

        await asyncio.sleep(1)

    result_text = "\n\n".join(fixed_chunks)
    result_text = _re.sub(r'\n{3,}', '\n\n', result_text)
    logging.info(f"[{task_id}] ✅ Консистентность: {len(result_text.split())} слов")
    return result_text


async def generate_skeleton_chapter(
    model_id: str, index: int, title: str, n: int,
    full_plan_str: str, master_doc: str, style_prompt: str,
    script_style: str, prev_skeleton: str = "", narrative_state: str = ""
) -> str:
    """
    Первый проход: генерирует краткий скелет главы (~150-200 слов).
    Только ключевые события, без развёрнутых описаний.
    """
    lore_block = f"МАСТЕР-ДОКУМЕНТ:\n{master_doc}\n\n" if master_doc else ""
    prev_block = f"ПРЕДЫДУЩАЯ ГЛАВА (кратко):\n«...{prev_skeleton[-600:]}»\n\n" if prev_skeleton else ""
    state_block = f"НАРРАТИВНЫЙ КОНТЕКСТ:\n{narrative_state}\n\n" if narrative_state else ""

    genre_map = {
        "fiction":     "художественный — только ключевые события и диалоги",
        "documentary": "документальный — только ключевые факты и даты",
        "educational": "познавательный — только главный тезис и один пример",
    }

    prompt = (
        f"{lore_block}"
        f"ПЛАН СЦЕНАРИЯ ({n} частей):\n{full_plan_str}\n\n"
        f"{prev_block}{state_block}"
        f"ЗАДАЧА: напиши КРАТКИЙ СКЕЛЕТ части №{index+1} — «{title}»\n"
        f"Жанр: {genre_map.get(script_style, 'сценарий')}\n"
        f"Объём: ровно 150-200 слов. Только ключевые события/факты — без развёрнутых описаний.\n"
        f"Это черновик для дальнейшего развёртывания, не финальный текст.\n"
        f"Стиль: {style_prompt[:100]}\n\n"
        f"Только текст, без заголовков и пояснений."
    )
    try:
        raw = await api_call_with_retry(model_id, [{"role": "user", "content": prompt}], 600)
        return clean_chapter_text(raw).strip() if raw else ""
    except Exception as e:
        logging.warning(f"Скелет {index+1}: {e}")
        return ""


async def expand_chapter(
    model_id: str, index: int, title: str, n: int,
    skeleton: str, full_plan_str: str, master_doc: str,
    style_prompt: str, script_style: str,
    prev_full: str = "", next_skeleton: str = "",
    narrative_state: str = "", words_to_request: int = 400,
    max_tokens: int = 4096
) -> str:
    """
    Второй проход: разворачивает скелет главы до полного объёма.
    Ключевое отличие от обычной генерации: глава видит не только предыдущую,
    но и скелет СЛЕДУЮЩЕЙ части — и знает куда она ведёт.
    """
    lore_block = (
        f"╔══════════════════════════════════╗\n"
        f"║         МАСТЕР-ДОКУМЕНТ          ║\n"
        f"╚══════════════════════════════════╝\n"
        f"{master_doc}\n\n"
    ) if master_doc else ""

    prev_block = (
        f"ТАК ЗАКАНЧИВАЕТСЯ ПРЕДЫДУЩАЯ ЧАСТЬ (продолжи отсюда):\n«...{prev_full[-1500:]}»\n\n"
    ) if prev_full else ""

    next_block = (
        f"ТАК НАЧНЁТСЯ СЛЕДУЮЩАЯ ЧАСТЬ (закончи так, чтобы туда было логично перейти):\n«{next_skeleton[:400]}...»\n\n"
    ) if next_skeleton else ""

    state_block = f"НАРРАТИВНАЯ ЭСТАФЕТА:\n{narrative_state}\n\n" if narrative_state else ""

    genre_instructions = {
        "fiction": (
            "ЖАНР: художественный. Пиши через персонажей: действия, диалоги, внутренние монологи. "
            "Эмоции — через детали и поступки, не через прямые описания."
        ),
        "documentary": (
            "ЖАНР: документальный. Только реальные факты из мастер-документа. "
            "Ничего не придумывай."
        ),
        "educational": (
            "ЖАНР: познавательный. Объясняй через аналогии и примеры. "
            "От простого к сложному."
        ),
    }.get(script_style, "")

    if index == 0:
        position = "ПЕРВАЯ часть — начни с сильного зацепляющего момента, без вступлений."
    elif index == n - 1:
        position = "ПОСЛЕДНЯЯ часть — дай ощущение завершённости."
    else:
        position = f"Часть {index+1} из {n} — продолжай нарратив, не начинай заново."

    prompt = (
        f"{lore_block}"
        f"ПЛАН СЦЕНАРИЯ ({n} частей):\n{full_plan_str}\n\n"
        f"{prev_block}{state_block}"
        f"СКЕЛЕТ ТЕКУЩЕЙ ЧАСТИ (разверни его до полного объёма):\n{skeleton}\n\n"
        f"{next_block}"
        f"{genre_instructions}\n\n"
        f"СЕЙЧАС ПИШЕШЬ: часть №{index+1} — «{title}»\n"
        f"{position}\n\n"
        f"ПРАВИЛО: это фрагмент единого сплошного повествования. "
        f"Не начинай с нуля. Не делай вводных фраз типа «Сегодня мы поговорим». "
        f"В конце оборви на крючке — не подводи итог.\n\n"
        f"СТИЛЬ: {style_prompt}\n\n"
        f"ТЕХНИЧЕСКИЕ ТРЕБОВАНИЯ:\n"
        f"1. Объём: ровно {words_to_request} слов.\n"
        f"2. Формат: сплошной текст без заголовков, списков, символов #*-.\n"
        f"3. ЗАПРЕЩЕНО: вступления, итоги, призывы к зрителям."
    )
    try:
        raw = await api_call_with_retry(model_id, [{"role": "user", "content": prompt}], max_tokens)
        return strip_cta_from_text(clean_chapter_text(raw)).strip() if raw else ""
    except Exception as e:
        logging.error(f"Развёртка {index+1}: {e}")
        return ""


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
    waiting_for_topic            = State()
    waiting_for_style            = State()
    waiting_for_duration         = State()   # ручной ввод (кнопка «Другое»)
    waiting_for_template         = State()

class BulkMaker(StatesGroup):
    waiting_for_topics    = State()
    waiting_for_style     = State()
    waiting_for_duration  = State()
    waiting_for_template  = State()

class TemplateManager(StatesGroup):
    waiting_for_new_name    = State()
    waiting_for_new_prompt  = State()
    waiting_for_delete_name = State()
    waiting_for_edit_name   = State()

# ---------------------------------------------------------------------------
# КЛАВИАТУРЫ (все inline)
# ---------------------------------------------------------------------------
def get_main_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Создать сценарий",      callback_data="m_create"),
         InlineKeyboardButton(text="🗂 Массовая генерация",    callback_data="m_bulk")],
        [InlineKeyboardButton(text="💰 Баланс",                callback_data="m_balance"),
         InlineKeyboardButton(text="💳 Пополнить",             callback_data="m_topup")],
        [InlineKeyboardButton(text="📋 Тарифы",                callback_data="m_tariffs"),
         InlineKeyboardButton(text="📦 Пакеты",                callback_data="m_packages")],
        [InlineKeyboardButton(text="📁 Шаблоны",               callback_data="m_templates"),
         InlineKeyboardButton(text="⚙️ Настройки",             callback_data="m_settings")],
        [InlineKeyboardButton(text="👥 Реферальная программа", callback_data="m_referral")],
        [InlineKeyboardButton(text="❓ FAQ",                   callback_data="m_faq")],
    ])

def get_style_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎭 Художественный", callback_data="style_fiction")],
        [InlineKeyboardButton(text="📰 Документальный", callback_data="style_documentary")],
        [InlineKeyboardButton(text="🎓 Познавательный", callback_data="style_educational")],
        [InlineKeyboardButton(text="🔙 В меню",         callback_data="m_menu")],
    ])

def get_duration_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="15 мин", callback_data="dur_15"),
         InlineKeyboardButton(text="30 мин", callback_data="dur_30"),
         InlineKeyboardButton(text="45 мин", callback_data="dur_45")],
        [InlineKeyboardButton(text="60 мин", callback_data="dur_60"),
         InlineKeyboardButton(text="90 мин", callback_data="dur_90"),
         InlineKeyboardButton(text="120 мин", callback_data="dur_120")],
        [InlineKeyboardButton(text="✏️ Другое число", callback_data="dur_custom")],
        [InlineKeyboardButton(text="🔙 Назад к жанру", callback_data="dur_back"),
         InlineKeyboardButton(text="🏠 В меню",         callback_data="m_menu")],
    ])

def get_models_inline_kb(current_model_id: str = "") -> InlineKeyboardMarkup:
    model_list = [
        ("google/gemini-2.5-flash-lite", "🟢 Gemini",        "0.15₽/мин"),
        ("x-ai/grok-4.1-fast",           "🔵 Grok",          "0.25₽/мин"),
        ("anthropic/claude-haiku-4.5",   "🟡 Claude Haiku",  "0.75₽/мин"),
        ("openai/gpt-5.1",               "🟠 ChatGPT",       "1.25₽/мин"),
        ("anthropic/claude-sonnet-4.6",  "🔴 Claude Sonnet ✨", "2.00₽/мин"),
    ]
    rows = []
    for i, (mid, name, price) in enumerate(model_list):
        mark = " ✓" if mid == current_model_id else ""
        rows.append([InlineKeyboardButton(
            text=f"{name}{mark} — {price}",
            callback_data=f"model_{i}",
        )])
    rows.append([InlineKeyboardButton(text="🔙 В меню", callback_data="m_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_templates_menu_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить шаблон",  callback_data="tmgr_add"),
         InlineKeyboardButton(text="✏️ Изменить шаблон", callback_data="tmgr_edit")],
        [InlineKeyboardButton(text="🗑 Удалить шаблон",  callback_data="tmgr_del")],
        [InlineKeyboardButton(text="🔙 В меню",          callback_data="m_menu")],
    ])

async def get_templates_inline_kb(for_generation: bool = False,
                                  action_prefix: str = "tpl") -> InlineKeyboardMarkup:
    """Динамическая клавиатура шаблонов."""
    templates = await get_templates()
    rows = []
    for i, name in enumerate(templates):
        short = name if len(name) <= 28 else name[:25] + "…"
        rows.append([InlineKeyboardButton(text=short, callback_data=f"{action_prefix}_{i}")])
    if for_generation:
        rows.append([InlineKeyboardButton(text="➕ Создать новый шаблон", callback_data="tpl_new")])
    rows.append([InlineKeyboardButton(text="🔙 Назад к длительности", callback_data="tpl_back_dur"),
                 InlineKeyboardButton(text="🏠 В меню", callback_data="m_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def get_after_gen_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Создать ещё",  callback_data="m_create"),
         InlineKeyboardButton(text="🏠 В меню",       callback_data="m_menu")],
    ])


# ---------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНАЯ: показать главное меню
# ---------------------------------------------------------------------------
async def show_menu(target, text: str = "🏠 <b>Главное меню</b>"):
    """Отправляет или редактирует сообщение с главным меню (inline).
    target: types.Message или types.CallbackQuery."""
    kb = get_main_inline_kb()
    if isinstance(target, types.CallbackQuery):
        try:
            await target.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except TelegramBadRequest:
            await target.message.answer(text, reply_markup=kb, parse_mode="HTML")
        await target.answer()
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")


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
    # Оставлен для совместимости, возвращает inline-версию
    return get_templates_menu_inline_kb()

async def get_dynamic_templates_kb(for_generation: bool = False):
    return await get_templates_inline_kb(for_generation=for_generation)

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

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🚀 <b>КАК НАЧАТЬ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Нажми «🎬 Создать сценарий» → введи тему → выбери жанр → укажи длительность → выбери шаблон.\n"
    "Готово — через несколько минут получишь файл.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🎬 <b>ЖАНРЫ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "🎭 <b>Художественный</b> — история с персонажами, диалогами и сюжетом.\n"
    "Подходит для биографических драм, историй успеха/провала, криминальных историй.\n\n"
    "📰 <b>Документальный</b> — только факты, хронология, реальные события.\n"
    "Подходит для расследований, биографий, разборов громких дел.\n\n"
    "🎓 <b>Познавательный</b> — объяснения, аналогии, «почему» и «как».\n"
    "Подходит для научпопа, психологии, финансов, технологий.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "⏱ <b>ХРОНОМЕТРАЖ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    f"Расчёт идёт из <b>{WORDS_PER_MINUTE} слов/мин</b> — средний темп озвучки.\n"
    "Каждый диктор говорит по-разному, поэтому рекомендуем заказывать на <b>10–15 мин больше</b> нужного.\n"
    "<i>Пример: хочешь видео на 20 мин — заказывай 30–35.</i>\n"
    "Кредиты списываются по факту — только за реально сгенерированные слова.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "💰 <b>КРЕДИТЫ И ОПЛАТА</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "1 кредит = 1 рубль.\n"
    "Стартовый бонус — 50 кредитов бесплатно при регистрации.\n"
    "Кредиты списываются только после успешной генерации.\n"
    "Для пополнения: раздел «💳 Пополнить» или напиши @aass11463.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🤖 <b>МОДЕЛИ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "🟢 Gemini — 0.15₽/мин — быстро и дёшево\n"
    "🔵 Grok — 0.25₽/мин — живой и дерзкий стиль\n"
    "🟡 Claude Haiku — 0.75₽/мин — литературный стиль\n"
    "🟠 ChatGPT — 1.25₽/мин — чёткая логика\n"
    "🔴 Claude Sonnet — 2.00₽/мин — наивысшее качество\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "📁 <b>ШАБЛОНЫ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Шаблон задаёт стиль подачи: эмоциональный, сухой, от первого лица и т.д.\n"
    "Создавай и редактируй свои шаблоны в разделе «📁 Шаблоны».\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "🗂 <b>МАССОВАЯ ГЕНЕРАЦИЯ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "До 5 сценариев по разным темам за один раз.\n"
    "Каждый приходит отдельным файлом по готовности.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "👥 <b>РЕФЕРАЛЬНАЯ ПРОГРАММА</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Приглашай друзей по своей ссылке.\n"
    "Ты получаешь 25 кредитов после их первой оплаты.\n"
    "Друг получает +10% бонус к первому пополнению.\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
    "📜 <b>ДОКУМЕНТЫ</b>\n"
    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Публичная оферта: /oferta\n\n"

    "━━━━━━━━━━━━━━━━━━━━━━\n"
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

    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT user_id, credits, first_topup FROM settings WHERE user_id=$1", user_id)
    is_new = existing is None
    user   = await get_or_create_user(user_id)

    if username:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE settings SET username=$1 WHERE user_id=$2", username.lower(), user_id)

    if len(args) > 1:
        if args[1].lower() == "oferta":
            await message.answer("📜 <b>Публичная оферта Script AI</b>\n\n"
                                 "Самозанятый: Панферов Кирилл Алексеевич\n"
                                 "ИНН: 616810872170\n"
                                 "📩 kkpanferovvai@gmail.com | 💬 @aass11463", parse_mode="HTML")
            try:
                await message.answer_document(FSInputFile("oferta_scriptai.docx"), caption="Полный текст договора-оферты")
            except Exception:
                await message.answer(OFERTA_TEXT, parse_mode="HTML")
            return
        applied = await apply_referral(user_id, args[1])
        if applied:
            await message.answer(f"✅ Реферальная ссылка применена!\nВы получите бонус +{REFERRAL_BONUS_INVITEE_PCT}% к первому пополнению.", parse_mode="HTML")

    balance = float(user["credits"] or 0)
    if is_new:
        text = (
            "👋 <b>Добро пожаловать в Авто Сценарист!</b>\n\n"
            "Генерируй готовые YouTube-сценарии за минуты — без копирайтеров, без ограничений по длине.\n\n"
            "✨ <b>Почему выбирают нас:</b>\n"
            "⚡ Сценарий на 60 минут — за 3–5 минут\n"
            "🗂 Массовая генерация — до 5 сценариев за раз\n"
            "🤖 5 топовых моделей — GPT, Claude, Gemini, Grok\n"
            "💰 От 9₽ за полный 60-минутный сценарий\n\n"
            f"🎁 Тебе начислено <b>{WELCOME_CREDITS} стартовых кредитов</b>!\n"
            f"💰 Баланс: <b>{balance:.1f} кредитов</b>"
        )
    else:
        text = (
            f"👋 <b>С возвращением!</b>\n\n"
            f"💰 Баланс: <b>{balance:.1f} кредитов</b>"
        )

    # Убираем reply-клавиатуру (если была) и отправляем inline-меню
    await message.answer(text, reply_markup=types.ReplyKeyboardRemove(), parse_mode="HTML")
    await message.answer("🏠 <b>Главное меню</b>", reply_markup=get_main_inline_kb(), parse_mode="HTML")


@dp.message(Command("oferta"))
async def oferta_cmd(message: types.Message):
    await message.answer("📜 <b>Публичная оферта Script AI</b>\n\n"
                         "Самозанятый: Панферов Кирилл Алексеевич\n"
                         "ИНН: 616810872170\n"
                         "📩 kkpanferovvai@gmail.com | 💬 @aass11463", parse_mode="HTML")
    try:
        await message.answer_document(FSInputFile("oferta_scriptai.docx"), caption="Полный текст договора-оферты")
    except Exception:
        await message.answer(OFERTA_TEXT, parse_mode="HTML")


@dp.message(Command("cancel"))

@dp.callback_query(F.data == "tpl_back_dur")
async def cb_tpl_back_dur(call: types.CallbackQuery, state: FSMContext):
    """Возврат от выбора шаблона к длительности."""
    cur = await state.get_state()
    is_bulk = cur and "Bulk" in (cur or "")
    await call.message.answer(
        "⏱ <b>Укажи длительность сценария:</b>",
        reply_markup=get_duration_inline_kb(), parse_mode="HTML"
    )
    await state.set_state(BulkMaker.waiting_for_duration if is_bulk else ScriptMaker.waiting_for_duration)
    await call.answer()


@dp.callback_query(F.data == "m_menu_cancel")
async def cb_menu_cancel(call: types.CallbackQuery, state: FSMContext):
    """«В меню» во время генерации — отменяет задачу."""
    user_id = call.from_user.id
    # Ищем активную задачу пользователя и отменяем
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT task_id FROM tasks WHERE user_id=$1 AND status='In Progress' ORDER BY created_at DESC LIMIT 1",
            user_id
        )
    if row:
        await update_task_status(row["task_id"], "Cancelled")
        logging.info(f"Задача {row['task_id']} отменена через кнопку В меню")
    await state.clear()
    balance = await get_balance(user_id)
    await show_menu(call, f"🏠 <b>Главное меню</b>\n💰 Баланс: <b>{balance:.1f} кред.</b>")


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
        reply_markup=get_after_gen_inline_kb(),
    )
    await call.answer()


async def cancel_cmd(message: types.Message, state: FSMContext):
    """Отменяет текущее FSM-состояние или активную генерацию."""
    user_id = message.from_user.id
    # Если идёт генерация — найти активную задачу и отменить
    if user_id in _generating_users:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT task_id FROM tasks WHERE user_id=$1 AND status='In Progress' ORDER BY created_at DESC LIMIT 1",
                user_id
            )
        if row:
            await update_task_status(row["task_id"], "Cancelled")
            await message.answer(
                f"🛑 <b>Генерация отменена</b>\n\n🆔 ID: <code>{row['task_id']}</code>",
                reply_markup=get_after_gen_inline_kb(), parse_mode="HTML"
            )
            return
    # Иначе — очищаем FSM состояние
    cur = await state.get_state()
    if cur:
        await state.clear()
        await message.answer("✅ Действие отменено.", reply_markup=get_main_inline_kb(), parse_mode="HTML")
    else:
        await message.answer("Нет активного действия для отмены.", reply_markup=get_main_inline_kb(), parse_mode="HTML")


async def back_to_main(message_or_call, state: FSMContext = None):
    """Универсальная функция возврата в меню."""
    if state:
        await state.clear()
    await show_menu(message_or_call)


# ---------------------------------------------------------------------------
# CALLBACK-ХЕНДЛЕРЫ ГЛАВНОГО МЕНЮ
# ---------------------------------------------------------------------------

@dp.callback_query(F.data == "m_menu")
async def cb_menu(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    balance = await get_balance(call.from_user.id)
    await show_menu(call, f"🏠 <b>Главное меню</b>\n💰 Баланс: <b>{balance:.1f} кред.</b>")


@dp.callback_query(F.data == "m_balance")
async def cb_balance(call: types.CallbackQuery):
    user_id = call.from_user.id
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
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В меню", callback_data="m_menu")]
    ])
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "m_topup")
async def cb_topup(call: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        *[[InlineKeyboardButton(
            text=f"{name} — {price}₽  →  {base+bonus} кред.",
            callback_data=f"pkg_{i}",
        )] for i, (price, base, bonus, name) in enumerate(CREDIT_PACKAGES)],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="m_menu")],
    ])
    try:
        await call.message.edit_text(
            "💳 <b>Выберите пакет пополнения:</b>\n\n"
            "<i>Платёжная система подключается — скоро будет автоплатёж.\n"
            "Пока свяжитесь с поддержкой для ручного пополнения.</i>",
            reply_markup=kb, parse_mode="HTML",
        )
    except TelegramBadRequest:
        await call.message.answer(
            "💳 <b>Выберите пакет пополнения:</b>",
            reply_markup=kb, parse_mode="HTML",
        )
    await call.answer()


@dp.callback_query(F.data == "m_tariffs")
async def cb_tariffs(call: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В меню", callback_data="m_menu")]
    ])
    try:
        await call.message.edit_text(TARIFFS_TEXT, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await call.message.answer(TARIFFS_TEXT, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "m_packages")
async def cb_packages(call: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Пополнить", callback_data="m_topup"),
         InlineKeyboardButton(text="🔙 В меню",    callback_data="m_menu")],
    ])
    try:
        await call.message.edit_text(build_packages_text(), reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await call.message.answer(build_packages_text(), reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "m_faq")
async def cb_faq(call: types.CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 В меню", callback_data="m_menu")]
    ])
    try:
        await call.message.edit_text(FAQ_TEXT, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await call.message.answer(FAQ_TEXT, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "m_referral")
async def cb_referral(call: types.CallbackQuery):
    user     = await get_or_create_user(call.from_user.id)
    bot_info = await bot.get_me()
    ref_code = user["referral_code"]
    ref_link = f"https://t.me/{bot_info.username}?start={ref_code}"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📤 Поделиться ссылкой",
            url=f"https://t.me/share/url?url={ref_link}&text=Попробуй%20этот%20бот%20для%20генерации%20YouTube-сценариев!",
        )],
        [InlineKeyboardButton(text="🔙 В меню", callback_data="m_menu")],
    ])
    text = (
        "👥 <b>Реферальная программа</b>\n\n"
        f"🔗 Ваша ссылка:\n<code>{ref_link}</code>\n\n"
        f"• Вы получаете <b>{REFERRAL_BONUS_INVITER} кредитов</b> когда приглашённый делает первое пополнение\n"
        f"• Приглашённый получает <b>+{REFERRAL_BONUS_INVITEE_PCT}%</b> бонусных кредитов к первому пополнению\n\n"
        "<i>Бонус начисляется автоматически.</i>"
    )
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "m_settings")
async def cb_settings(call: types.CallbackQuery):
    cur      = await get_user_model(call.from_user.id)
    kb       = get_models_inline_kb(current_model_id=cur)
    friendly = MODEL_NAMES.get(cur, cur)
    text     = f"⚙️ <b>Настройки</b>\n\nТекущая модель: <b>{friendly}</b>\n\nВыбери модель:"
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


MODEL_LIST_IDS = [
    "google/gemini-2.5-flash-lite",
    "x-ai/grok-4.1-fast",
    "anthropic/claude-haiku-4.5",
    "openai/gpt-5.1",
    "anthropic/claude-sonnet-4.6",
]


@dp.callback_query(F.data.startswith("model_"))
async def cb_change_model(call: types.CallbackQuery):
    idx = int(call.data.split("_")[1])
    if idx >= len(MODEL_LIST_IDS):
        await call.answer("Неизвестная модель")
        return
    model_id = MODEL_LIST_IDS[idx]
    await set_user_model(call.from_user.id, model_id)
    friendly = MODEL_NAMES.get(model_id, model_id)
    kb = get_models_inline_kb(current_model_id=model_id)
    await call.message.edit_text(
        f"⚙️ <b>Настройки</b>\n\nМодель изменена на: <b>{friendly}</b> ✓",
        reply_markup=kb, parse_mode="HTML",
    )
    await call.answer(f"✅ {friendly}")


@dp.callback_query(F.data == "m_templates")
async def cb_templates_menu(call: types.CallbackQuery):
    templates = await get_templates()
    text = "📂 <b>Твои шаблоны:</b>\n\n"
    if not templates:
        text += "<i>Пусто — создай первый шаблон.</i>"
    else:
        for name, prompt in templates.items():
            text += f"🔹 <b>{name}</b>\n<i>{prompt[:60]}…</i>\n\n"
    try:
        await call.message.edit_text(text, reply_markup=get_templates_menu_inline_kb(), parse_mode="HTML")
    except TelegramBadRequest:
        await call.message.answer(text, reply_markup=get_templates_menu_inline_kb(), parse_mode="HTML")
    await call.answer()


@dp.callback_query(F.data == "tmgr_add")
async def cb_tmgr_add(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Введи название нового шаблона:")
    await state.set_state(TemplateManager.waiting_for_new_name)
    await call.answer()


@dp.callback_query(F.data == "tmgr_edit")
async def cb_tmgr_edit(call: types.CallbackQuery, state: FSMContext):
    kb = await get_templates_inline_kb(action_prefix="tedit")
    await call.message.edit_text("Какой шаблон изменить?", reply_markup=kb)
    await state.set_state(TemplateManager.waiting_for_edit_name)
    await call.answer()


@dp.callback_query(F.data == "tmgr_del")
async def cb_tmgr_del(call: types.CallbackQuery, state: FSMContext):
    kb = await get_templates_inline_kb(action_prefix="tdel")
    await call.message.edit_text("Какой шаблон удалить?", reply_markup=kb)
    await state.set_state(TemplateManager.waiting_for_delete_name)
    await call.answer()


@dp.callback_query(F.data.startswith("tedit_"))
async def cb_tedit_pick(call: types.CallbackQuery, state: FSMContext):
    idx       = int(call.data.split("_")[1])
    templates = await get_templates()
    names     = list(templates.keys())
    if idx >= len(names):
        await call.answer("Шаблон не найден")
        return
    name   = names[idx]
    prompt = templates[name]
    await state.update_data(template_name=name)
    await call.message.answer(
        f"✏️ Шаблон: <b>{name}</b>\n\n"
        f"📄 Текущий промпт:\n<blockquote>{prompt}</blockquote>\n\n"
        "Отправь новый текст промпта:",
        parse_mode="HTML",
    )
    await state.set_state(TemplateManager.waiting_for_new_prompt)
    await call.answer()


@dp.callback_query(F.data.startswith("tdel_"))
async def cb_tdel_pick(call: types.CallbackQuery, state: FSMContext):
    idx       = int(call.data.split("_")[1])
    templates = await get_templates()
    names     = list(templates.keys())
    if idx >= len(names):
        await call.answer("Шаблон не найден")
        return
    name = names[idx]
    await delete_template(name)
    await state.clear()
    await call.answer(f"🗑 Удалён: {name}")
    await cb_templates_menu(call)


# ---------------------------------------------------------------------------
# НАВИГАЦИЯ (текстовые команды — для совместимости)
# ---------------------------------------------------------------------------

@dp.message(F.text == "🔙 Назад в меню")
async def back_to_main_msg(message: types.Message, state: FSMContext):
    await state.clear()
    await show_menu(message)


# ---------------------------------------------------------------------------
# СОЗДАНИЕ СЦЕНАРИЯ — ВВОД ТЕМЫ
# ---------------------------------------------------------------------------

@dp.callback_query(F.data == "genre_back")
async def cb_genre_back(call: types.CallbackQuery, state: FSMContext):
    """Возврат к выбору жанра из описания."""
    cur = await state.get_state()
    is_bulk = cur and "Bulk" in cur
    await call.message.edit_text(
        "Выбери жанр сценария:",
        reply_markup=get_style_inline_kb()
    )
    await state.set_state(BulkMaker.waiting_for_style if is_bulk else ScriptMaker.waiting_for_style)
    await call.answer()


@dp.callback_query(F.data == "m_create")
async def cb_create(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.answer(
        "🎬 <b>Новый сценарий</b>\n\n"
        "О чём видео? Напиши тему.\n\n"
        "<i>Можно вставить тему + фактуру (цитаты, даты, факты) — бот разберётся сам.</i>",
        parse_mode="HTML",
    )
    await state.set_state(ScriptMaker.waiting_for_topic)
    await call.answer()


@dp.callback_query(F.data == "m_bulk")
async def cb_bulk(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.answer(
        "🗂 <b>Массовая генерация</b>\n\n"
        "Введи темы сценариев — <b>каждая с новой строки</b>, до 5 тем:\n\n"
        "<i>Пример:\n"
        "История Tesla\n"
        "Как работает ChatGPT\n"
        "Тайны Бермудского треугольника</i>",
        parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_topics)
    await call.answer()


@dp.message(F.text == "🎬 Создать сценарий")
async def start_script(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "🎬 <b>Новый сценарий</b>\n\nО чём видео? Напиши тему.",
        parse_mode="HTML",
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
    hint = (
        f"\n\n✅ <i>Обнаружена фактура ({len(factual.split())} слов) — будет использована при генерации.</i>"
        if factual else ""
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎭 Художественный", callback_data="style_fiction")],
        [InlineKeyboardButton(text="📰 Документальный", callback_data="style_documentary")],
        [InlineKeyboardButton(text="🎓 Познавательный", callback_data="style_educational")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="m_menu")],
    ])
    await message.answer(
        f"📌 Тема: <b>{topic[:100]}</b>{hint}\n\nВыбери жанр сценария:",
        reply_markup=kb, parse_mode="HTML",
    )
    await state.set_state(ScriptMaker.waiting_for_style)


@dp.message(ScriptMaker.waiting_for_duration)
async def process_duration_manual(message: types.Message, state: FSMContext):
    """Ручной ввод длительности (кнопка «Другое»)."""
    if not message.text.isdigit() or int(message.text) < 1:
        await message.answer("⚠️ Введи число минут, например: <b>45</b>", parse_mode="HTML")
        return
    await _process_duration_value(message, state, int(message.text))


@dp.message(ScriptMaker.waiting_for_template)
async def process_template_fallback(message: types.Message, state: FSMContext):
    """Fallback — шаблон выбирается через inline-кнопки."""
    await message.answer(
        "Выбери шаблон из списка:",
        reply_markup=await get_templates_inline_kb(for_generation=True),
    )


# ---------------------------------------------------------------------------
# CALLBACK: ВЫБОР ЖАНРА
# ---------------------------------------------------------------------------

STYLE_CB_MAP = {
    "style_fiction":     "fiction",
    "style_documentary": "documentary",
    "style_educational": "educational",
}

STYLE_DESCRIPTIONS_CB = {
    "style_fiction": (
        "🎭 <b>Художественный</b>\n\n"
        "История через персонажей: их мысли, диалоги, поступки и эмоции. "
        "Бот создаёт сцены, выстраивает напряжение и ведёт зрителя через сюжет.\n\n"
        "<b>Когда выбирать:</b> если в центре — конкретный человек и его история.\n\n"
        "<b>Примеры тем:</b>\n"
        "— Биографическая драма о реальном человеке\n"
        "— История краха и возрождения\n"
        "— Криминальный сюжет с несколькими героями"
    ),
    "style_documentary": (
        "📰 <b>Документальный</b>\n\n"
        "Строгая хронология, только реальные факты и события. "
        "Бот работает как журналист: выстраивает цепочку событий, называет даты и имена.\n\n"
        "<b>Когда выбирать:</b> если важна достоверность и последовательность.\n\n"
        "<b>Примеры тем:</b>\n"
        "— Хроника реального события или скандала\n"
        "— Биография с акцентом на факты и даты\n"
        "— Расследование с доказательствами"
    ),
    "style_educational": (
        "🎓 <b>Познавательный</b>\n\n"
        "Объяснение через аналогии, примеры и логику — от простого к сложному.\n\n"
        "<b>Когда выбирать:</b> если главное — объяснить, а не рассказать историю.\n\n"
        "<b>Примеры тем:</b>\n"
        "— Как работает какое-то явление или процесс\n"
        "— Разбор психологии, науки, экономики\n"
        "— История идеи или открытия"
    ),
}


async def _after_style_cb(call: types.CallbackQuery, state: FSMContext, next_state):
    script_style = STYLE_CB_MAP.get(call.data, "documentary")
    await state.update_data(script_style=script_style)
    desc = STYLE_DESCRIPTIONS_CB.get(call.data, "")

    # Показываем описание жанра редактируя текущее сообщение (убираем кнопки выбора)
    if desc:
        kb_back = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Изменить жанр", callback_data="genre_back")],
            [InlineKeyboardButton(text="🏠 В меню", callback_data="m_menu")],
        ])
        try:
            await call.message.edit_text(desc, reply_markup=kb_back, parse_mode="HTML")
        except Exception:
            await call.message.answer(desc, reply_markup=kb_back, parse_mode="HTML")

    # Отдельное сообщение с выбором длительности
    await call.message.answer(
        "⏱ <b>Укажи длительность сценария</b>\n\n"
        "Рекомендуем брать на 10–15 мин больше нужного — темп у всех дикторов разный:",
        reply_markup=get_duration_inline_kb(), parse_mode="HTML"
    )
    await state.set_state(next_state)
    await call.answer()


@dp.callback_query(ScriptMaker.waiting_for_style, F.data.startswith("style_"))
async def cb_style_script(call: types.CallbackQuery, state: FSMContext):
    await _after_style_cb(call, state, ScriptMaker.waiting_for_duration)


@dp.callback_query(BulkMaker.waiting_for_style, F.data.startswith("style_"))
async def cb_style_bulk(call: types.CallbackQuery, state: FSMContext):
    await _after_style_cb(call, state, BulkMaker.waiting_for_duration)


# ---------------------------------------------------------------------------
# CALLBACK: ВЫБОР ДЛИТЕЛЬНОСТИ
# ---------------------------------------------------------------------------

DURATION_PRESETS = {
    "dur_15": 15, "dur_30": 30, "dur_45": 45,
    "dur_60": 60, "dur_90": 90, "dur_120": 120,
}


async def _process_duration_value(target, state: FSMContext, duration: int, is_bulk: bool = False):
    user_id    = target.from_user.id
    model_id   = await get_user_model(user_id)
    cost       = calc_cost(model_id, duration)
    balance    = await get_balance(user_id)
    model_name = MODEL_NAMES.get(model_id, model_id)

    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)

    disclaimer = (
        f"ℹ️ <b>Как считается хронометраж</b>\n"
        f"Расчёт идёт из <b>{WORDS_PER_MINUTE} слов/мин</b> — средний темп озвучки.\n"
        f"Каждый диктор говорит в своём темпе. "
        f"Рекомендуем закладывать на <b>10–15 мин больше</b> желаемого.\n"
        f"Списывается фактически — по словам в готовом сценарии.\n\n"
    )
    cost_line = (
        f"💰 Макс. стоимость ({model_name}, {duration} мин): <b>{cost:.1f} кред.</b>\n"
        f"Баланс: <b>{balance:.1f} кред.</b>\n\n"
    )

    if balance < cost:
        msg = f"❌ <b>Недостаточно кредитов</b>\n\n{cost_line}Нажми <b>💳 Пополнить</b>."
        kb  = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Пополнить", callback_data="m_topup"),
             InlineKeyboardButton(text="🔙 В меню",    callback_data="m_menu")],
        ])
        if isinstance(target, types.CallbackQuery):
            await target.message.answer(msg, reply_markup=kb, parse_mode="HTML")
            await target.answer()
        else:
            await target.answer(msg, reply_markup=kb, parse_mode="HTML")
        await state.clear()
        return False

    kb   = await get_templates_inline_kb(for_generation=True)
    text = f"{disclaimer}{cost_line}Выбери шаблон:"
    if isinstance(target, types.CallbackQuery):
        await target.message.answer(text, reply_markup=kb, parse_mode="HTML")
        await target.answer()
    else:
        await target.answer(text, reply_markup=kb, parse_mode="HTML")

    next_state = BulkMaker.waiting_for_template if is_bulk else ScriptMaker.waiting_for_template
    await state.set_state(next_state)
    return True


@dp.callback_query(ScriptMaker.waiting_for_duration, F.data.in_(DURATION_PRESETS))
async def cb_dur_script(call: types.CallbackQuery, state: FSMContext):
    await _process_duration_value(call, state, DURATION_PRESETS[call.data])


@dp.callback_query(BulkMaker.waiting_for_duration, F.data.in_(DURATION_PRESETS))
async def cb_dur_bulk(call: types.CallbackQuery, state: FSMContext):
    await _process_duration_value(call, state, DURATION_PRESETS[call.data], is_bulk=True)


@dp.callback_query(F.data == "dur_custom")
async def cb_dur_custom(call: types.CallbackQuery):
    await call.message.answer("Введи длительность в минутах (число):")
    await call.answer()


@dp.callback_query(F.data == "dur_back")
async def cb_dur_back(call: types.CallbackQuery, state: FSMContext):
    await call.message.answer("Выбери жанр сценария:", reply_markup=get_style_inline_kb())
    cur = await state.get_state()
    if cur and "Bulk" in (cur or ""):
        await state.set_state(BulkMaker.waiting_for_style)
    else:
        await state.set_state(ScriptMaker.waiting_for_style)
    await call.answer()


# ---------------------------------------------------------------------------
# CALLBACK: ВЫБОР ШАБЛОНА
# ---------------------------------------------------------------------------

async def _get_template_by_index(idx: int):
    templates = await get_templates()
    names = list(templates.keys())
    if idx >= len(names):
        return None
    name = names[idx]
    return name, templates[name]


@dp.callback_query(ScriptMaker.waiting_for_template, F.data.startswith("tpl_"))
async def cb_tpl_script(call: types.CallbackQuery, state: FSMContext):
    if call.data == "tpl_new":
        await call.message.answer("Введи название нового шаблона:")
        await state.set_state(TemplateManager.waiting_for_new_name)
        await call.answer()
        return
    idx = int(call.data.split("_")[1])
    result = await _get_template_by_index(idx)
    if not result:
        await call.answer("Шаблон не найден")
        return
    tpl_name, style_prompt = result
    data           = await state.get_data()
    model_id       = await get_user_model(call.from_user.id)
    friendly_model = MODEL_NAMES.get(model_id, "AI")
    duration       = data["duration"]
    cost           = calc_cost(model_id, duration)
    balance        = await get_balance(call.from_user.id)

    if balance < cost:
        await call.message.answer(
            f"❌ <b>Недостаточно кредитов</b>\nНужно: <b>{cost:.1f}</b> | Баланс: <b>{balance:.1f}</b>",
            reply_markup=get_main_inline_kb(), parse_mode="HTML",
        )
        await state.clear()
        await call.answer()
        return

    task_id   = f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"
    file_name = f"script_{task_id.replace(' ', '_')}.txt"
    await log_task(task_id, call.from_user.id, data["topic"], friendly_model)
    await call.answer("🚀 Запускаю…")
    asyncio.create_task(
        _run_generation(call.message, data, style_prompt, model_id,
                        friendly_model, duration, cost, task_id, file_name)
    )
    await state.clear()


@dp.callback_query(BulkMaker.waiting_for_template, F.data.startswith("tpl_"))
async def cb_tpl_bulk(call: types.CallbackQuery, state: FSMContext):
    if call.data == "tpl_new":
        await call.message.answer("Введи название нового шаблона:")
        await state.set_state(TemplateManager.waiting_for_new_name)
        await call.answer()
        return
    idx = int(call.data.split("_")[1])
    result = await _get_template_by_index(idx)
    if not result:
        await call.answer("Шаблон не найден")
        return
    tpl_name, style_prompt = result
    data       = await state.get_data()
    topics     = data["topics"]
    model_id   = await get_user_model(call.from_user.id)
    duration   = data["duration"]
    cost_each  = calc_cost(model_id, duration)
    cost_total = cost_each * len(topics)
    balance    = await get_balance(call.from_user.id)

    if balance < cost_total:
        await call.message.answer(
            f"❌ Баланс изменился. Нужно: <b>{cost_total:.1f}</b>, есть: <b>{balance:.1f}</b>",
            reply_markup=get_main_inline_kb(), parse_mode="HTML",
        )
        await state.clear()
        await call.answer()
        return

    await call.message.answer(
        f"🚀 Запускаю генерацию <b>{len(topics)}</b> сценариев…\nКаждый будет отправлен по готовности.",
        parse_mode="HTML",
    )
    await state.clear()
    await call.answer()

    friendly_model = MODEL_NAMES.get(model_id, "AI")
    for topic in topics:
        fake_data = {
            "topic": topic, "factual": "",
            "script_style": data.get("script_style", "documentary"),
            "duration": duration, "words_target": duration * WORDS_PER_MINUTE,
        }
        task_id   = f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"
        file_name = f"script_{task_id.replace(' ', '_')}.txt"
        await log_task(task_id, call.from_user.id, topic, friendly_model)
        asyncio.create_task(
            _run_generation(call.message, fake_data, style_prompt, model_id,
                            friendly_model, duration, cost_each, task_id, file_name)
        )
        await asyncio.sleep(1)



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
        reply_markup=get_style_inline_kb(), parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_style)


@dp.message(BulkMaker.waiting_for_style)
async def bulk_style(message: types.Message, state: FSMContext):
    # Жанр теперь выбирается через inline-кнопки (cb_style_bulk)
    # Это сообщение — fallback если пользователь написал текстом
    await message.answer("Выбери жанр из списка:", reply_markup=get_style_inline_kb())


@dp.message(BulkMaker.waiting_for_duration)
async def bulk_duration(message: types.Message, state: FSMContext):
    """Ручной ввод длительности для массовой генерации (кнопка «Другое»)."""
    if not message.text.isdigit() or int(message.text) < 1:
        await message.answer("⚠️ Введи число, например: <b>30</b>", parse_mode="HTML")
        return
    await _process_duration_value(message, state, int(message.text), is_bulk=True)


@dp.message(BulkMaker.waiting_for_template)
async def bulk_generate(message: types.Message, state: FSMContext):
    # Шаблон выбирается через inline-кнопки (cb_tpl_bulk)
    await message.answer("Выбери шаблон из списка:", reply_markup=await get_templates_inline_kb(for_generation=True))

async def _run_generation(message: types.Message, data: dict, style_prompt: str,
                          model_id: str, friendly_model: str, duration: int,
                          cost: float, task_id: str, file_name: str):
    global _active_tasks

    user_id      = message.from_user.id
    words_target = data.get("words_target", duration * WORDS_PER_MINUTE)
    _generating_users.add(user_id)

    await _generation_semaphore.acquire()
    _active_tasks += 1

    try:
        # Сразу показываем кнопку отмены — ещё до генерации плана
        cancel_kb_early = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_{task_id}"),
             InlineKeyboardButton(text="🏠 В меню",   callback_data="m_menu_cancel")]
        ])
        temp_msg = await message.answer(
            f"⏳ <b>Подготовка структуры</b>\n\n"
            f"🆔 ID: <code>{task_id}</code>\n"
            f"🤖 Модель: <b>{friendly_model}</b>\n\n"
            f"⚙️ <i>Генерируем мастер-документ и план...</i>",
            reply_markup=cancel_kb_early, parse_mode="HTML"
        )

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
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_{task_id}"),
             InlineKeyboardButton(text="🏠 В меню",   callback_data="m_menu_cancel")]
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
            if script_style == "documentary" and index >= 3 and mini_brief:
                lore_block = (
                    f"СПРАВОЧНИК ФАКТОВ (только для этой части, остальное уже использовано):\n"
                    f"{mini_brief}\n\n"
                )
                mini_brief_for_prompt = ""
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

            # Реестр имён — только для художественного жанра
            # Предотвращает: разные персонажи с одним именем
            name_registry_block = ""
            if script_style == "fiction" and master_doc:
                name_registry_block = (
                    "РЕЕСТР ГЛАВНЫХ ПЕРСОНАЖЕЙ — их имена НЕЛЬЗЯ давать второстепенным:\n"
                    "(извлечены из мастер-документа выше)\n"
                    "Любой новый второстепенный персонаж должен называться иначе "
                    "или оставаться без имени.\n\n"
                )

            prev_block = (
                f"ТАК ЗАКАНЧИВАЕТСЯ ПРЕДЫДУЩАЯ ЧАСТЬ — ПРОДОЛЖИ ОТСЮДА:\n"
                f"«...{prev_text[-1500:]}»\n\n"
            ) if prev_text else ""

            state_block = (
                f"НАРРАТИВНАЯ ЭСТАФЕТА (что было до тебя):\n{narrative_state}\n\n"
            ) if narrative_state else ""

            brief_block = (
                f"ТВОЁ ЗАДАНИЕ ДЛЯ ЭТОЙ ЧАСТИ:\n{mini_brief_for_prompt}\n\n"
            ) if mini_brief_for_prompt else ""

            if index == 0:
                position_hint = "Это ПЕРВАЯ часть — начни с сильного зацепляющего момента, без вступлений и приветствий."
            elif index == n - 1:
                position_hint = "Это ПОСЛЕДНЯЯ часть — подведи к финальному выводу, дай ощущение завершённости истории."
            else:
                position_hint = f"Это часть {index+1} из {n} — продолжай нарратив, не начинай заново."

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
                f"{name_registry_block}"
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
                # Layer 2: Проверка на обрыв предложения
                clean = await complete_sentence(model_id, clean)
                clean = clean.rstrip()
                full_script_parts[index] = clean + "\n\n"
                logging.info(f"[{task_id}] ✅ {index+1}/{n} — {len(clean.split())} слов")
            except Exception as e:
                logging.error(f"[{task_id}] ❌ {index+1}: {e}")
                full_script_parts[index] = ""

        async def extract_narrative_state(text: str, chapter_index: int, chapter_title: str) -> str:
            """
            После каждой главы создаёт «нарративную эстафету» для следующего автора.
            Включает время/место — ключ к устранению противоречий типа «декабрь → апрель».
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
                    f"ВРЕМЕННОЙ КОНТЕКСТ: [точный период/дата/время суток когда закончилась эта глава]\n"
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
                    f"ВРЕМЕННОЙ КОНТЕКСТ: [когда и где происходит действие: время суток, сезон, место]\n"
                    f"ОТКРЫТЫЙ КРЮЧОК: [какой вопрос или напряжение оставлено без ответа]\n"
                    f"ЭМОЦИОНАЛЬНЫЙ ТОН: [одно слово: тревога/надежда/удивление/напряжение/грусть/триумф]\n"
                    f"АКТИВНЫЕ ЛИЦА: [кто последний раз упоминался]\n"
                    f"НЕ ПОВТОРЯТЬ: [3-5 конкретных факта или фразы которые уже прозвучали]"
                )
            try:
                return (await api_call_with_retry(
                    model_id,
                    [{"role": "user", "content": prompt_text}],
                    600 if script_style == "documentary" else 450,
                )).strip()
            except Exception:
                return ""

        async def stitch_seams(parts: list[str]) -> list[str]:
            """
            Сглаживает переходы между частями.
            Вместо перезаписи больших блоков (что вызывало дубли) —
            генерирует 1-3 мостиковых предложения между частями и вставляет их.
            Оригинальный текст обеих частей не изменяется.
            """
            if len(parts) <= 1:
                return parts

            TAIL_LEN = 400   # символов с конца части A
            HEAD_LEN = 400   # символов с начала части B
            result = list(parts)

            async def bridge_seam(i: int):
                part_a = result[i].rstrip()
                part_b = result[i + 1].lstrip()
                if len(part_a) < 100 or len(part_b) < 100:
                    return

                tail_a = part_a[-TAIL_LEN:]
                head_b = part_b[:HEAD_LEN]

                # Проверяем нужен ли мост: если хвост A уже заканчивается хорошо
                # и голова B продолжает плавно — пропускаем
                tail_end = tail_a.rstrip()
                if tail_end and tail_end[-1] not in '.!?»…':
                    # Хвост A обрывается — нужна доводка
                    pass
                # В любом случае проверяем на резкий старт B

                # Всегда создаём мост (дешевле чем проверять)
                try:
                    bridge = await api_call_with_retry(
                        model_id,
                        [{"role": "user", "content":
                          f"Два фрагмента одного сценария идут подряд. "
                          f"Напиши 1-2 коротких связующих предложения которые обеспечат плавный переход между ними. "
                          f"Предложения должны органично вытекать из конца первого фрагмента и вести к началу второго. "
                          f"НЕ повторяй текст из фрагментов. Только переход.\n\n"
                          f"КОНЕЦ ПЕРВОГО:\n«...{tail_a}»\n\n"
                          f"НАЧАЛО ВТОРОГО:\n«{head_b}...»\n\n"
                          f"Верни только 1-2 связующих предложения."}],
                        150,
                    )
                    if bridge and bridge.strip() and len(bridge.split()) < 60:
                        bridge_clean = clean_chapter_text(bridge).strip()
                        # Вставляем мост в конец части A (не меняя часть B)
                        result[i] = part_a + " " + bridge_clean + "\n\n"
                        logging.info(f"[{task_id}] 🔗 Мост {i+1}↔{i+2}: «{bridge_clean[:60]}...»")
                except Exception as e:
                    logging.warning(f"[{task_id}] Мост {i+1}: {e}")

            await asyncio.gather(*[bridge_seam(i) for i in range(len(result) - 1)])

            # После всех мостов — чистим артефакты слипшихся слов
            return [clean_chapter_text(p) if p else p for p in result]

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


        # ── ВЫБОР РЕЖИМА ГЕНЕРАЦИИ ──────────────────────────────────────────
        use_two_pass = n >= TWO_PASS_THRESHOLD
        logging.info(f"[{task_id}] Режим: {'двухпроходной' if use_two_pass else 'однопроходной'} ({n} глав)")

        SEQ_ANCHOR = min(6, n)
        CHAIN_SIZE = 3
        done_count      = 0
        narrative_state = ""

        if use_two_pass:
            # ════════════════════════════════════════════════════════════════
            # ДВУХПРОХОДНАЯ ГЕНЕРАЦИЯ
            # Проход 1: скелет всех глав (быстро, последовательно)
            # Проход 2: развёртка до полного объёма (каждая глава видит следующую)
            # ════════════════════════════════════════════════════════════════
            await safe_edit(
                status_msg,
                f"⏳ <b>Генерация сценария</b>\n\n"
                f"🆔 ID: <code>{task_id}</code>\n"
                f"🤖 Модель: <b>{friendly_model}</b>\n\n"
                f"[░░░░░░░░░░] 0%\n"
                f"📋 <i>Проход 1: строим каркас сценария...</i>",
                reply_markup=cancel_kb,
            )

            # ── Проход 1: скелет ──────────────────────────────────────────
            skeletons: list[str] = [""] * n
            skel_narrative = ""
            for i in range(n):
                if await get_task_status(task_id) == "Cancelled":
                    return
                skel = await generate_skeleton_chapter(
                    model_id, i, chapters[i], n, full_plan_str,
                    master_doc, style_prompt, script_style,
                    prev_skeleton=skeletons[i-1] if i > 0 else "",
                    narrative_state=skel_narrative,
                )
                skeletons[i] = skel
                if skel:
                    try:
                        skel_narrative = (await api_call_with_retry(
                            model_id,
                            [{"role": "user", "content":
                              f"Кратко (2-3 предложения): где мы в сюжете после этого фрагмента?\n"
                              f"Время/место/персонажи/открытый вопрос.\n\n{skel[-500:]}"}],
                            150,
                        )).strip()
                    except Exception:
                        pass
                pct = round(50 * (i + 1) / n)
                bar = "▓" * (pct // 5) + "░" * (10 - pct // 5)
                await safe_edit(
                    status_msg,
                    f"⏳ <b>Генерация сценария</b>\n\n"
                    f"🆔 ID: <code>{task_id}</code>\n"
                    f"🤖 Модель: <b>{friendly_model}</b>\n\n"
                    f"[{bar}] {pct}%\n"
                    f"📋 <i>Каркас: {i+1}/{n} частей...</i>",
                    reply_markup=cancel_kb,
                )
            logging.info(f"[{task_id}] ✅ Скелет готов: {sum(1 for s in skeletons if s)}/{n}")

            # ── Проход 2: развёртка ───────────────────────────────────────
            await safe_edit(
                status_msg,
                f"⏳ <b>Генерация сценария</b>\n\n"
                f"🆔 ID: <code>{task_id}</code>\n"
                f"🤖 Модель: <b>{friendly_model}</b>\n\n"
                f"[▓▓▓▓▓░░░░░] 50%\n"
                f"✍️ <i>Проход 2: разворачиваем в полный текст...</i>",
                reply_markup=cancel_kb,
            )
            exp_narrative = ""
            for i in range(n):
                if await get_task_status(task_id) == "Cancelled":
                    return
                expanded = await expand_chapter(
                    model_id, i, chapters[i], n,
                    skeleton=skeletons[i],
                    full_plan_str=full_plan_str,
                    master_doc=master_doc,
                    style_prompt=style_prompt,
                    script_style=script_style,
                    prev_full=full_script_parts[i-1] if i > 0 else "",
                    next_skeleton=skeletons[i+1] if i + 1 < n else "",
                    narrative_state=exp_narrative,
                    words_to_request=words_to_request,
                    max_tokens=max_tokens_chapter,
                )
                if expanded:
                    clean = await complete_sentence(model_id, expanded)
                    full_script_parts[i] = clean.rstrip() + "\n\n"
                done_count += 1
                pct = 50 + round(50 * done_count / n)
                bar = "▓" * (pct // 10) + "░" * (10 - pct // 10)
                await safe_edit(
                    status_msg,
                    f"⏳ <b>Генерация сценария</b>\n\n"
                    f"🆔 ID: <code>{task_id}</code>\n"
                    f"🤖 Модель: <b>{friendly_model}</b>\n\n"
                    f"[{bar}] {pct}%\n"
                    f"✍️ <i>Развёртка: {done_count}/{n} частей...</i>",
                    reply_markup=cancel_kb,
                )
                new_state = await extract_narrative_state(full_script_parts[i], i, chapters[i])
                if new_state:
                    exp_narrative = new_state

        else:
            # ════════════════════════════════════════════════════════════════
            # ОДНОПРОХОДНАЯ ГЕНЕРАЦИЯ (якорь + параллельные цепочки)
            # ════════════════════════════════════════════════════════════════
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
                    build_progress_text(task_id, friendly_model, n, done_count, max(0, n - done_count)),
                    reply_markup=cancel_kb,
                )
                new_state = await extract_narrative_state(full_script_parts[i], i, chapters[i])
                if new_state:
                    narrative_state = new_state

            async def build_narrative_summary(parts_so_far: list[str]) -> str:
                combined = " ".join(p[-1500:] for p in parts_so_far if p)
                if not combined.strip():
                    return narrative_state
                try:
                    return (await api_call_with_retry(
                        model_id,
                        [{"role": "user", "content":
                          f"Составь сводку для авторов следующих глав (до 300 слов).\n"
                          f"НАПИСАННОЕ:\n{combined[-4000:]}\n\n"
                          f"ГДЕ МЫ / ВРЕМЕННОЙ КОНТЕКСТ / ПЕРСОНАЖИ / ОТКРЫТЫЕ ЛИНИИ / НЕЛЬЗЯ ПОВТОРЯТЬ / ТОН:"}],
                        600,
                    )).strip()
                except Exception:
                    return narrative_state

            remaining = list(range(SEQ_ANCHOR, n))
            if remaining:
                narrative_summary = await build_narrative_summary(full_script_parts[:SEQ_ANCHOR])

                async def run_chain(chain_indices: list[int]):
                    chain_narrative = narrative_summary
                    for idx in chain_indices:
                        if await get_task_status(task_id) == "Cancelled":
                            return
                        chain_pos = chain_indices.index(idx)
                        prev = full_script_parts[chain_indices[chain_pos-1]] if chain_pos > 0 else (full_script_parts[SEQ_ANCHOR-1] if SEQ_ANCHOR > 0 else "")
                        await generate_one(
                            idx, chapters[idx],
                            prev_text=prev,
                            narrative_state=chain_narrative,
                            mini_brief=mini_briefs[idx] if idx < len(mini_briefs) else "",
                        )
                        new_state = await extract_narrative_state(full_script_parts[idx], idx, chapters[idx])
                        if new_state:
                            chain_narrative = new_state

                PARALLEL_CHAINS = 3
                chain_idx = 0
                while chain_idx < len(remaining):
                    if await get_task_status(task_id) == "Cancelled":
                        return
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
                        build_progress_text(task_id, friendly_model, n, done_count, max(0, n - done_count)),
                        reply_markup=cancel_kb,
                    )
                    if chain_idx < len(remaining):
                        narrative_summary = await build_narrative_summary([p for p in full_script_parts if p])


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
            f"[▓▓▓▓▓▓▓▓▓░] 90%\n"
            f"✨ <i>Финальная полировка текста...</i>",
            reply_markup=None,
        )
        full_script = await polish_script(
            model_id, full_script, data['topic'], style_prompt, task_id
        )

        # ── ПРОВЕРКА КОНСИСТЕНТНОСТИ ─────────────────────────────────────────
        await safe_edit(
            status_msg,
            f"⏳ <b>Генерация сценария</b>\n\n"
            f"🆔 ID: <code>{task_id}</code>\n"
            f"🤖 Модель: <b>{friendly_model}</b>\n\n"
            f"[▓▓▓▓▓▓▓▓▓▓] 100%\n"
            f"🔍 <i>Проверка логики и устранение повторов...</i>",
            reply_markup=None,
        )
        full_script = await consistency_check_and_fix(
            model_id, full_script, data['topic'], script_style, master_doc, task_id
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
            f"Остаток: <b>{new_balance:.1f} кред.</b>",
            reply_markup=get_after_gen_inline_kb(),
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
        _generating_users.discard(user_id)
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
