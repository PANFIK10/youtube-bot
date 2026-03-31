import asyncio
import logging
import math
import os
import re
import asyncpg
import random
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

# ── Пул API-ключей OpenRouter ──────────────────────────────────────────────
# Основной ключ + до 4 дополнительных. Берём все что есть в env.
_raw_keys = [
    os.getenv("OPENROUTER_API_KEY"),
    os.getenv("OPENROUTER_API_KEY_1"),
    os.getenv("OPENROUTER_API_KEY_2"),
    os.getenv("OPENROUTER_API_KEY_3"),
    os.getenv("OPENROUTER_API_KEY_4"),
]
API_KEYS: list[str] = [k for k in _raw_keys if k]   # убираем незаданные
if not API_KEYS:
    raise RuntimeError("Не задан ни один OPENROUTER_API_KEY в переменных окружения")

# Создаём отдельный AsyncOpenAI-клиент на каждый ключ
_clients = [
    AsyncOpenAI(base_url="https://openrouter.ai/api/v1", api_key=key, timeout=120.0)
    for key in API_KEYS
]
_key_index = 0          # курсор round-robin
_key_lock  = None       # asyncio.Lock — инициализируется в on_startup

def _next_client() -> AsyncOpenAI:
    """Round-robin по пулу клиентов. Потокобезопасно для asyncio."""
    global _key_index
    client = _clients[_key_index % len(_clients)]
    _key_index += 1
    return client

# ── Семафор очереди ────────────────────────────────────────────────────────
# Не более MAX_CONCURRENT_TASKS задач генерируются одновременно.
# Остальные ждут в очереди — пользователь видит сообщение об этом.
MAX_CONCURRENT_TASKS = 25  # 50 пользователей / ~2 мин средняя очередь
_generation_semaphore: asyncio.Semaphore | None = None  # инициализируется в on_startup
_active_tasks: int = 0   # счётчик активных задач (без обращения к приватным атрибутам)

# ── Retry-параметры ────────────────────────────────────────────────────────
API_RETRY_ATTEMPTS = 3          # сколько раз повторять при ошибке API
API_RETRY_BASE_DELAY = 2.0      # начальная пауза (сек), удваивается каждый раз

bot = Bot(token=TELEGRAM_TOKEN)
dp  = Dispatcher()
logging.basicConfig(level=logging.INFO)

# Глобальный пул соединений asyncpg
db_pool: asyncpg.Pool | None = None

# Инициализируем asyncio-объекты при старте бота
@dp.startup()
async def on_startup():
    global _generation_semaphore, _key_lock, db_pool, _active_tasks
    _generation_semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    _key_lock = asyncio.Lock()
    _active_tasks = 0

    # Создаём пул asyncpg (min 2, max 10 соединений)
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        ssl="prefer",
    )
    await init_db()
    logging.info(
        f"БД: asyncpg пул готов | "
        f"Ключей API: {len(API_KEYS)} | "
        f"Очередь: max {MAX_CONCURRENT_TASKS} задач"
    )

@dp.shutdown()
async def on_shutdown():
    if db_pool:
        await db_pool.close()
    logging.info("БД: пул соединений закрыт")

# ---------------------------------------------------------------------------
# КОНСТАНТЫ ГЕНЕРАЦИИ
# ---------------------------------------------------------------------------
WORDS_PER_MINUTE = 130          # темп речи при озвучке
WORDS_PER_CHAPTER = 400         # целевой объём одной части (слов)

# Каждая модель ведёт себя по-разному относительно запрошенного объёма.
# Коэффициенты получены эмпирически из реальных прогонов:
#   Haiku 4.5  — недодаёт ~7 %   → запрашиваем на 45 % больше  (1.35 / 0.931 = 1.45)
#   GPT-5.1    — передаёт ~63 %  → запрашиваем на 17 % меньше  (1.35 / 1.633 = 0.83)
#   Gemini     — передаёт ~8 %   → запрашиваем на 25 % больше  (1.35 / 1.079 = 1.25)
#   Grok 4.1   — передаёт ~8 %   → запрашиваем на 25 % больше  (1.35 / 1.084 = 1.25)
VOLUME_OVERREQUEST_FACTORS: dict[str, float] = {
    "anthropic/claude-haiku-4.5":   1.45,
    "openai/gpt-5.1":               0.83,
    "google/gemini-2.5-flash-lite": 1.25,
    "x-ai/grok-4.1-fast":           1.25,
}
VOLUME_OVERREQUEST_FACTOR_DEFAULT = 1.20  # запасной для неизвестных моделей

# Если после генерации часть короче этого порога — она идёт на доработку
MIN_CHAPTER_RATIO = 0.80        # 80 % от цели = допустимо
MAX_REGEN_ATTEMPTS = 2          # максимум перегенераций одной части

MAX_CTA_PER_SCRIPT = 5          # максимум призывов к комментариям на весь сценарий
SECONDS_PER_CHUNK  = 18         # реальное время одной пачки из 5 параллельных запросов

MODEL_NAMES = {
    "anthropic/claude-haiku-4.5":   "Claude",
    "openai/gpt-5.1":               "ChatGPT",
    "google/gemini-2.5-flash-lite": "Gemini",
    "x-ai/grok-4.1-fast":           "Grok",
}

# ---------------------------------------------------------------------------
# РАБОТА С PostgreSQL (asyncpg)
# ---------------------------------------------------------------------------

async def init_db():
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS templates (
                name TEXT PRIMARY KEY,
                prompt TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                user_id BIGINT PRIMARY KEY,
                model_name TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                task_id TEXT PRIMARY KEY,
                user_id BIGINT,
                topic TEXT,
                model TEXT,
                status TEXT,
                created_at TEXT
            )
        """)
        count = await conn.fetchval("SELECT COUNT(*) FROM templates")
        if count == 0:
            await conn.execute(
                "INSERT INTO templates (name, prompt) VALUES ($1, $2)",
                "🎬 Стандартный", "Напиши увлекательный сценарий для YouTube.",
            )


async def log_task(task_id, user_id, topic, model):
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO tasks VALUES ($1,$2,$3,$4,$5,$6)",
            task_id, user_id, topic, model, "In Progress", now,
        )


async def update_task_status(task_id, status):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE tasks SET status=$1 WHERE task_id=$2",
            status, task_id,
        )


async def get_task_status(task_id) -> str | None:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status FROM tasks WHERE task_id=$1", task_id
        )
    return row["status"] if row else None


async def get_user_model(user_id) -> str:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT model_name FROM settings WHERE user_id=$1", user_id
        )
    return row["model_name"] if row else "x-ai/grok-4.1-fast"


async def set_user_model(user_id, model_name):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO settings (user_id, model_name) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET model_name = EXCLUDED.model_name
            """,
            user_id, model_name,
        )


async def get_templates() -> dict:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT name, prompt FROM templates")
    return {row["name"]: row["prompt"] for row in rows}


async def add_template(name, prompt):
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO templates (name, prompt) VALUES ($1, $2)
            ON CONFLICT (name) DO UPDATE SET prompt = EXCLUDED.prompt
            """,
            name, prompt,
        )


async def delete_template(name):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM templates WHERE name=$1", name)


# ---------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ---------------------------------------------------------------------------

async def api_call_with_retry(model_id: str, messages: list, max_tokens: int) -> str:
    """
    Вызов OpenRouter с автоматическим retry и ротацией ключей.
    При ошибке rate-limit или сетевой ошибке — ждёт и пробует следующий ключ.
    Возвращает текст ответа или бросает исключение после всех попыток.
    """
    last_exc = None
    for attempt in range(API_RETRY_ATTEMPTS):
        async with _key_lock:
            c = _next_client()
        try:
            resp = await c.chat.completions.create(
                model=model_id,
                messages=messages,
                max_tokens=max_tokens,
            )
            return resp.choices[0].message.content
        except Exception as e:
            last_exc = e
            err_str = str(e).lower()
            # Rate limit или сервер перегружен — ждём с экспоненциальной задержкой
            if any(x in err_str for x in ("rate", "429", "502", "503", "timeout")):
                delay = API_RETRY_BASE_DELAY * (2 ** attempt)
                logging.warning(
                    f"API ошибка (попытка {attempt+1}/{API_RETRY_ATTEMPTS}): {e} "
                    f"— повтор через {delay:.1f}с"
                )
                await asyncio.sleep(delay)
            else:
                # Не сетевая ошибка — сразу бросаем
                raise
    raise last_exc


def clean_chapter_text(text: str) -> str:
    """
    Удаляет все технические артефакты форматирования из текста части:
    заголовки Markdown, горизонтальные разделители, слова «Глава/Часть/Раздел»,
    жирный/курсивный текст на отдельных строках, лишние переносы.
    """
    # Горизонтальные разделители: ---, ***, ___, ===, ~~~
    text = re.sub(r'^[-*_=~]{3,}\s*$', '', text, flags=re.MULTILINE)
    # Markdown-заголовки: # ## ###
    text = re.sub(r'^#{1,6}\s+.*$', '', text, flags=re.MULTILINE)
    # «Глава 1», «Часть II», «Раздел 3:», «Chapter One», «Part 2» и т.п.
    text = re.sub(
        r'^(Глава|Часть|Раздел|Блок|Chapter|Part|Section)\s*[\dIVXivxабвгАБВГ]*'
        r'[.:\-–—)]*\s*.*$',
        '', text, flags=re.MULTILINE | re.IGNORECASE,
    )
    # Строки из одного **жирного** блока
    text = re.sub(r'^\*\*[^*\n]+\*\*\s*$', '', text, flags=re.MULTILINE)
    # Строки из одного *курсивного* блока
    text = re.sub(r'^\*[^*\n]+\*\s*$', '', text, flags=re.MULTILINE)
    # Строки из одного __подчёркнутого__ блока
    text = re.sub(r'^__[^_\n]+__\s*$', '', text, flags=re.MULTILINE)
    # Строки-отчёты модели: «Проверка: 540 слов ✓», «Итого: 512 слов», «Слов: 498» и т.п.
    text = re.sub(
        r'^\*{0,2}(Проверка|Итого|Подсчёт|Слов|Всего|Объём|Word count|Total)[^\n]{0,60}$',
        '', text, flags=re.MULTILINE | re.IGNORECASE,
    )
    # Строки вида «(540 слов)», «[512 words]» — скобочные аннотации объёма
    text = re.sub(r'^\[?\(?\d{2,4}\s*(слов|words|сл\.)\)?\]?\s*$', '', text, flags=re.MULTILINE)
    # Тройные и более переносы → двойной
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_plan_chapters(plan_text: str, target_count: int) -> list[str]:
    """
    Парсит нумерованный план → список чистых названий глав.
    Обрезает до target_count, если модель вернула больше.
    """
    lines = []
    for line in plan_text.splitlines():
        line = line.strip()
        if not line or len(line) < 4:
            continue
        line = re.sub(r'^[\d]+[.)]\s+', '', line)   # «1. » / «1) »
        line = re.sub(r'^[-–—•*]\s+', '', line)      # «- » / «• »
        line = line.strip()
        if len(line) > 3:
            lines.append(line)

    if not lines:
        logging.warning("План пустой — использую заглушки")
        return [f"Часть {i + 1}" for i in range(target_count)]

    if len(lines) > target_count:
        lines = lines[:target_count]
    elif len(lines) < target_count:
        logging.warning(f"План вернул {len(lines)} из {target_count} глав")

    return lines


def compute_cta_positions(total_chapters: int, max_cta: int = MAX_CTA_PER_SCRIPT) -> set[int]:
    """
    Равномерно распределяет позиции CTA по сценарию.
    Никогда не ставит CTA в первую главу.
    Возвращает множество индексов (0-based).
    """
    if total_chapters <= 1:
        return set()
    # Делим диапазон [1 .. total-1] на max_cta равных отрезков
    count = min(max_cta, total_chapters - 1)
    step  = (total_chapters - 1) / count
    return {round(1 + step * i) for i in range(count)}


def build_progress_text(
    task_id: str,
    friendly_model: str,
    total_chapters: int,
    done_chapters: int,
    remaining_chunks: int,
    phase: str = "generate",   # "generate" | "regen" | "done"
) -> str:
    """
    Формирует HTML-текст статусного сообщения с прогресс-баром.
    Редактируется после каждой пачки.
    """
    bar_len   = 10
    filled    = round(bar_len * done_chapters / max(total_chapters, 1))
    bar       = "▓" * filled + "░" * (bar_len - filled)
    pct       = round(100 * done_chapters / max(total_chapters, 1))

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
    """
    Безопасное редактирование сообщения.
    Передаём reply_markup при каждом edit — иначе Telegram сбрасывает кнопки.
    Игнорирует «message is not modified» и другие мелкие ошибки.
    """
    try:
        await msg.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            logging.warning(f"edit_text error: {e}")
    except Exception as e:
        logging.warning(f"safe_edit failed: {e}")


async def upload_to_backup(file_path: str) -> str | None:
    try:
        async with aiohttp.ClientSession() as session:
            with open(file_path, 'rb') as f:
                async with session.post('https://file.io/?expires=14d', data={'file': f}) as resp:
                    res = await resp.json()
                    return res.get('link')
    except Exception as e:
        logging.error(f"Ошибка бэкапа: {e}")
        return None

# ---------------------------------------------------------------------------
# СОСТОЯНИЯ
# ---------------------------------------------------------------------------
class ScriptMaker(StatesGroup):
    waiting_for_topic    = State()
    waiting_for_duration = State()
    waiting_for_template = State()

class TemplateManager(StatesGroup):
    waiting_for_new_name   = State()
    waiting_for_new_prompt = State()
    waiting_for_delete_name = State()

# ---------------------------------------------------------------------------
# КЛАВИАТУРЫ
# ---------------------------------------------------------------------------
def get_main_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎬 Создать сценарий")],
        [KeyboardButton(text="📁 Мои шаблоны"), KeyboardButton(text="⚙️ Настройки")],
        [KeyboardButton(text="🔄 Перезапустить")],
    ], resize_keyboard=True)

def get_models_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Claude"),  KeyboardButton(text="ChatGPT")],
        [KeyboardButton(text="Gemini"),  KeyboardButton(text="Grok")],
        [KeyboardButton(text="🔙 Назад в меню")],
    ], resize_keyboard=True)

def get_templates_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить шаблон"), KeyboardButton(text="✏️ Изменить шаблон")],
        [KeyboardButton(text="🗑 Удалить шаблон"),  KeyboardButton(text="🔙 Назад в меню")],
    ], resize_keyboard=True)

async def get_dynamic_templates_kb(for_generation: bool = False):
    """
    for_generation=True  — выбор шаблона при создании сценария (без кнопки «Создать новый»)
    for_generation=False — управление шаблонами (с кнопкой «Создать новый»)
    """
    templates = await get_templates()
    kb = [[KeyboardButton(text=name)] for name in templates]
    if not for_generation:
        kb.append([KeyboardButton(text="➕ Создать новый шаблон")])
    kb.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# ---------------------------------------------------------------------------
# ХЭНДЛЕРЫ — навигация
# ---------------------------------------------------------------------------
WELCOME_TEXT = (
    "👋 <b>Добро пожаловать в генератор сценариев!</b>\n\n"
    "Создавайте готовые сценарии без борьбы с лимитами и склеек.\n\n"
    "🎭 <b>Claude</b> — стиль.\n🧠 <b>ChatGPT</b> — логика.\n"
    "⚡ <b>Gemini</b> — скорость.\n🔥 <b>Grok</b> — дерзость."
)

@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(WELCOME_TEXT, reply_markup=get_main_kb(), parse_mode="HTML")


@dp.message(F.text == "🔄 Перезапустить")
async def restart_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(WELCOME_TEXT, reply_markup=get_main_kb(), parse_mode="HTML")


@dp.message(F.text == "🔙 Назад в меню")
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=get_main_kb())


@dp.message(F.text == "⚙️ Настройки")
async def settings_menu(message: types.Message):
    cur = await get_user_model(message.from_user.id)
    friendly = MODEL_NAMES.get(cur, cur)
    await message.answer(
        f"⚙️ <b>Настройки интеллекта</b>\n\nТекущая модель: <b>{friendly}</b>",
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
# ХЭНДЛЕРЫ — шаблоны
# ---------------------------------------------------------------------------
@dp.message(F.text == "📁 Мои шаблоны")
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
    await message.answer("Введи название нового шаблона:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(TemplateManager.waiting_for_new_name)


@dp.message(F.text == "✏️ Изменить шаблон")
async def edit_template_start(message: types.Message, state: FSMContext):
    await message.answer("Какой шаблон хочешь изменить?", reply_markup=await get_dynamic_templates_kb())
    await state.set_state(TemplateManager.waiting_for_new_name)


@dp.message(TemplateManager.waiting_for_new_name)
async def add_template_name(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    await state.update_data(template_name=message.text)
    await message.answer(
        "Отправь новый текст промпта для этого шаблона:",
        reply_markup=types.ReplyKeyboardRemove(),
    )
    await state.set_state(TemplateManager.waiting_for_new_prompt)


@dp.message(TemplateManager.waiting_for_new_prompt)
async def add_template_prompt(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await add_template(data['template_name'], message.text)
    if 'topic' in data:
        await message.answer(
            f"✅ Шаблон <b>{data['template_name']}</b> сохранён!\n"
            f"Продолжаем работу над сценарием: <i>{data['topic']}</i>.\n\n"
            "Выберите шаблон для генерации:",
            reply_markup=await get_dynamic_templates_kb(for_generation=True), parse_mode="HTML",
        )
        await state.set_state(ScriptMaker.waiting_for_template)
    else:
        await message.answer("✅ Шаблон успешно сохранён!", reply_markup=get_main_kb())
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
    # Показываем клавиатуру для быстрого старта нового сценария
    new_script_kb = ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎬 Создать сценарий")],
        [KeyboardButton(text="🔙 Назад в меню")],
    ], resize_keyboard=True)
    await call.message.answer(
        "Хочешь создать новый сценарий?",
        reply_markup=new_script_kb,
    )
    await call.answer()

# ---------------------------------------------------------------------------
# ХЭНДЛЕРЫ — создание сценария
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
        await message.answer("⚠️ Введите длительность числом, например: <b>60</b>", parse_mode="HTML")
        return
    duration = int(message.text)
    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)
    await message.answer(
        "<i>⚠️ Обратите внимание: итоговый хронометраж может отличаться на ±10 минут, "
        "так как скорость речи и паузы при озвучке у всех индивидуальны.</i>\n\n"
        "Выбери шаблон:",
        reply_markup=await get_dynamic_templates_kb(for_generation=True), parse_mode="HTML",
    )
    await state.set_state(ScriptMaker.waiting_for_template)


# ---------------------------------------------------------------------------
# ГЛАВНАЯ ФУНКЦИЯ ГЕНЕРАЦИИ
# ---------------------------------------------------------------------------
@dp.message(ScriptMaker.waiting_for_template)
async def generate_script(message: types.Message, state: FSMContext):
    global _active_tasks
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)
    if message.text == "➕ Создать новый шаблон":
        await message.answer(
            "Введите название для нового шаблона:",
            reply_markup=types.ReplyKeyboardRemove(),
        )
        await state.set_state(TemplateManager.waiting_for_new_name)
        return

    templates = await get_templates()
    if message.text not in templates:
        await message.answer(
            "Выбери шаблон из списка:",
            reply_markup=await get_dynamic_templates_kb(for_generation=True),
        )
        return

    style_prompt  = templates[message.text]
    data          = await state.get_data()
    model_id      = await get_user_model(message.from_user.id)
    friendly_model = MODEL_NAMES.get(model_id, "AI")
    words_target  = data['words_target']

    task_id = f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"
    await log_task(task_id, message.from_user.id, data['topic'], friendly_model)

    file_name = f"script_{task_id.replace(' ', '_')}.txt"

    global _active_tasks

    # Защита от старта до инициализации (on_startup не отработал)
    if _generation_semaphore is None:
        await message.answer("❌ Бот ещё не готов, попробуй через несколько секунд.")
        return

    # Если все слоты заняты — предупреждаем пользователя и ждём в очереди
    if _active_tasks >= MAX_CONCURRENT_TASKS:
        await message.answer(
            "⏳ <b>Все слоты заняты.</b> Твоя задача поставлена в очередь — "
            "генерация начнётся автоматически, как только освободится место.",
            parse_mode="HTML",
        )
    await _generation_semaphore.acquire()

    _active_tasks += 1

    try:
        # ── ФАЗА 0: уведомление ────────────────────────────────────────────
        temp_msg = await message.answer(
            "⏳ <i>Подготовка структуры сценария...</i>", parse_mode="HTML"
        )

        # ── ФАЗА 1: генерация плана ────────────────────────────────────────
        target_chapters = max(1, round(words_target / WORDS_PER_CHAPTER))

        plan_prompt = (
            f"Составь план YouTube-видео на тему: «{data['topic']}».\n"
            f"Нужно ровно {target_chapters} пунктов — не больше, не меньше.\n"
            f"Каждый пункт — уникальная и конкретная подтема (5–10 слов).\n"
            f"Темы НЕ должны пересекаться или повторять друг друга.\n"
            f"Формат: только нумерованный список без пояснений и вступлений."
        )
        resp_text = await api_call_with_retry(
            model_id,
            [{"role": "user", "content": plan_prompt}],
            max_tokens=2500,
        )
        plan_text = resp_text
        chapters  = parse_plan_chapters(plan_text, target_chapters)
        n         = len(chapters)

        # Целевое число слов на одну часть (точное)
        words_per_chapter = words_target // n

        # Сколько слов просить у модели — с поправкой под конкретную модель
        overrequest_factor = VOLUME_OVERREQUEST_FACTORS.get(
            model_id, VOLUME_OVERREQUEST_FACTOR_DEFAULT
        )
        words_to_request = max(50, int(words_per_chapter * overrequest_factor))

        # max_tokens: ~2 токена на русское слово + 20 % буфер
        max_tokens_chapter = min(int(words_to_request * 2.4), 2048)

        # Позиции CTA (0-based индексы глав)
        cta_positions = compute_cta_positions(n)

        # Контекст: строка со всем планом (для передачи в каждую главу)
        full_plan_str = "\n".join(
            f"{i+1}. {title}" for i, title in enumerate(chapters)
        )

        # ── ФАЗА 2: статусное сообщение ────────────────────────────────────
        chunk_size   = 5
        total_chunks = math.ceil(n / chunk_size)

        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="❌ Отменить генерацию",
                callback_data=f"cancel_{task_id}",
            )]
        ])
        try:
            await temp_msg.delete()
        except Exception:
            pass

        status_msg = await message.answer(
            build_progress_text(task_id, friendly_model, n, 0, total_chunks),
            reply_markup=cancel_kb, parse_mode="HTML",
        )

        # ── ФАЗА 3: параллельная генерация пачками ─────────────────────────
        full_script_parts: list[str] = [""] * n

        async def generate_chapter(index: int, title: str, is_regen: bool = False) -> None:
            """Генерирует одну часть сценария и кладёт результат в full_script_parts."""
            if await get_task_status(task_id) == "Cancelled":
                return

            include_cta = (index in cta_positions) and not is_regen

            cta_instruction = (
                "\n5. CTA: В конце этой части — естественный, ненавязчивый призыв написать "
                "в комментариях одно слово или короткий ответ на вопрос, связанный с темой. "
                "Органично вплети в текст, не выделяй отдельным абзацем."
                if include_cta
                else "\n5. CTA: В этой части НЕТ призыва к комментариям."
            )

            prompt = (
                f"Ты пишешь часть сценария для YouTube-видео.\n\n"
                f"ОБЩАЯ ТЕМА ВИДЕО: {data['topic']}\n\n"
                f"ПОЛНЫЙ ПЛАН СЦЕНАРИЯ (все {n} частей):\n{full_plan_str}\n\n"
                f"ТВОЯ ЗАДАЧА: написать ТОЛЬКО часть №{index+1} — «{title}».\n"
                f"Все остальные части уже распределены. "
                f"Не повторяй тезисы и примеры из других частей плана.\n\n"
                f"СТИЛЬ: {style_prompt}\n\n"
                f"СТРОГИЕ ПРАВИЛА:\n"
                f"1. ОБЪЁМ: Напиши ровно {words_to_request} слов "
                f"(это {round(words_to_request/WORDS_PER_MINUTE, 1)} мин. речи). "
                f"Не пиши ничего кроме самого текста сценария — никаких пометок, подсчётов или проверок.\n"
                f"2. ФОРМАТ: ТОЛЬКО сплошной текст. Запрещено: заголовки, символы #, *, "
                f"слова «Глава», «Часть», «Раздел», горизонтальные линии (---), списки.\n"
                f"3. НАЧАЛО: сразу с первого слова текста, без вступлений.\n"
                f"4. КОНЕЦ: завершай мысль естественно, не обрывай на полуслове."
                f"{cta_instruction}"
            )
            try:
                raw   = await api_call_with_retry(
                    model_id,
                    [{"role": "user", "content": prompt}],
                    max_tokens=max_tokens_chapter,
                )
                clean = clean_chapter_text(raw)
                full_script_parts[index] = clean + "\n\n"
                wc = len(clean.split())
                logging.info(
                    f"[{task_id}] ✅ Часть {index+1}/{n} — {wc} слов "
                    f"(цель {words_per_chapter}, запрошено {words_to_request})"
                )
            except Exception as e:
                logging.error(f"[{task_id}] ❌ Часть {index+1}: {e}")
                full_script_parts[index] = ""

        # Пачки по chunk_size параллельных запросов
        done_count = 0
        for batch_start in range(0, n, chunk_size):
            if await get_task_status(task_id) == "Cancelled":
                return

            batch_indices = list(range(batch_start, min(batch_start + chunk_size, n)))
            batch_num     = batch_start // chunk_size + 1

            logging.info(
                f"[{task_id}] 🚀 Пачка {batch_num}/{total_chunks}: "
                f"части {batch_start+1}–{batch_indices[-1]+1}"
            )
            tasks = [generate_chapter(i, chapters[i]) for i in batch_indices]
            await asyncio.gather(*tasks)

            done_count += len(batch_indices)
            remaining  = total_chunks - batch_num

            await safe_edit(
                status_msg,
                build_progress_text(task_id, friendly_model, n, done_count, remaining),
                reply_markup=cancel_kb,
            )
            await asyncio.sleep(2)

        if await get_task_status(task_id) == "Cancelled":
            return

        # ── ФАЗА 4: доработка коротких частей ─────────────────────────────
        short_indices = [
            i for i, part in enumerate(full_script_parts)
            if len(part.split()) < words_per_chapter * MIN_CHAPTER_RATIO
        ]

        if short_indices:
            logging.info(
                f"[{task_id}] 🔧 Коротких частей для доработки: {len(short_indices)}"
            )
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

                regen_tasks = [generate_chapter(i, chapters[i], is_regen=True)
                               for i in short_indices]
                await asyncio.gather(*regen_tasks)
                await asyncio.sleep(2)

                # Оставляем в списке только те, что всё ещё коротки
                short_indices = [
                    i for i in short_indices
                    if len(full_script_parts[i].split()) < words_per_chapter * MIN_CHAPTER_RATIO
                ]
                logging.info(
                    f"[{task_id}] 🔧 Попытка {attempt+1}: осталось коротких — {len(short_indices)}"
                )

        if await get_task_status(task_id) == "Cancelled":
            return

        # ── ФАЗА 5: сборка и отправка ──────────────────────────────────────
        full_script = "".join(full_script_parts).strip()
        full_script = re.sub(r'\n{3,}', '\n\n', full_script)

        word_count = len(full_script.split())
        deviation  = word_count - words_target
        logging.info(
            f"[{task_id}] 📊 ИТОГО: {word_count} слов | "
            f"цель: {words_target} | отклонение: {deviation:+d} ({deviation/words_target*100:+.1f}%)"
        )

        with open(file_name, "w", encoding="utf-8") as f:
            f.write(full_script)

        caption = (
            f"📄 Сценарий ID: {task_id}\n"
            f"📊 Объём: ~{word_count} слов (~{word_count // WORDS_PER_MINUTE} мин.)\n"
            f"🎯 Цель была: {words_target} слов ({data['duration']} мин.)"
        )

        await safe_edit(
            status_msg,
            build_progress_text(task_id, friendly_model, n, n, 0, phase="done"),
            reply_markup=None,
        )

        try:
            await message.answer_document(FSInputFile(file_name), caption=caption)
        except Exception as e:
            logging.error(f"[{task_id}] ТГ не отправил файл: {e}")
            backup = await upload_to_backup(file_name)
            if backup:
                await message.answer(
                    f"⚠️ <b>Telegram не смог отправить файл напрямую</b>, "
                    f"но я сохранил его в облаке:\n\n"
                    f"🔗 <a href='{backup}'>Скачать готовый сценарий</a>\n"
                    f"<i>(Ссылка удалится после скачивания)</i>",
                    parse_mode="HTML",
                )
            else:
                await message.answer(f"❌ Критическая ошибка: файл {task_id} не отправился.")

        await update_task_status(task_id, "Completed")

        # Кнопка быстрого перехода к новому сценарию
        new_script_kb = ReplyKeyboardMarkup(keyboard=[
            [KeyboardButton(text="🎬 Создать сценарий")],
            [KeyboardButton(text="🔙 Назад в меню")],
        ], resize_keyboard=True)
        await message.answer(
            "✅ Готово! Хочешь создать ещё один сценарий?",
            reply_markup=new_script_kb,
        )

    except Exception as e:
        logging.error(f"[{task_id}] Критическая ошибка: {e}")
        try:
            await message.answer(
                f"❌ <b>Ошибка задачи</b> {task_id}\n\n<code>{str(e)}</code>",
                parse_mode="HTML",
            )
        except Exception:
            pass
        await update_task_status(task_id, f"Error: {str(e)}")

    finally:
        # Гарантированно освобождаем слот очереди, очищаем state и файл
        _active_tasks -= 1
        _generation_semaphore.release()
        await state.clear()
        if os.path.exists(file_name):
            os.remove(file_name)


# ---------------------------------------------------------------------------
# ЗАПУСК
# ---------------------------------------------------------------------------
async def main():
    print("Бот запущен и готов к работе!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
