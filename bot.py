import asyncio
import logging
import os
import math
import sqlite3
import random
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from openai import AsyncOpenAI
from aiogram.exceptions import TelegramBadRequest

# --- КОНФИГУРАЦИЯ ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

client = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

WORDS_PER_MINUTE = 130

# --- СЛОВАРЬ ИМЕН ---
MODEL_NAMES = {
    "anthropic/claude-haiku-4.5": "Claude",
    "openai/gpt-5.1": "ChatGPT",
    "google/gemini-2.5-flash-lite": "Gemini",
    "x-ai/grok-4.1-fast": "Grok",
    "qwen/qwen3.5-flash-02-23": "Qwen"
}

# --- БАЗА ДАННЫХ ---
def init_db():
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS templates (name TEXT PRIMARY KEY, prompt TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (user_id INTEGER PRIMARY KEY, model_name TEXT)''')
    # Новая таблица для логов задач
    c.execute('''CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, 
                    user_id INTEGER, 
                    topic TEXT, 
                    model TEXT, 
                    status TEXT, 
                    created_at TEXT)''')
    
    c.execute("SELECT COUNT(*) FROM templates")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO templates (name, prompt) VALUES (?, ?)", 
                  ("🎬 Стандартный", "Напиши увлекательный сценарий для YouTube."))
    conn.commit()
    conn.close()

def log_task(task_id, user_id, topic, model):
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    c.execute("INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?)", 
              (task_id, user_id, topic, model, "In Progress", now))
    conn.commit()
    conn.close()

def update_task_status(task_id, status):
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute("UPDATE tasks SET status = ? WHERE task_id = ?", (status, task_id))
    conn.commit()
    conn.close()

def get_user_model(user_id):
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute("SELECT model_name FROM settings WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else "x-ai/grok-4.1-fast"

def set_user_model(user_id, model_name):
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute("REPLACE INTO settings (user_id, model_name) VALUES (?, ?)", (user_id, model_name))
    conn.commit()
    conn.close()

def get_templates():
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute("SELECT name, prompt FROM templates")
    res = {row[0]: row[1] for row in c.fetchall()}
    conn.close()
    return res

def add_template(name, prompt):
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute("REPLACE INTO templates (name, prompt) VALUES (?, ?)", (name, prompt))
    conn.commit()
    conn.close()

def delete_template(name):
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute("DELETE FROM templates WHERE name = ?", (name,))
    conn.commit()
    conn.close()

init_db()

# --- FSM ---
class ScriptMaker(StatesGroup):
    waiting_for_topic = State()
    waiting_for_duration = State()
    waiting_for_template = State()

class TemplateManager(StatesGroup):
    waiting_for_new_name = State()
    waiting_for_new_prompt = State()
    waiting_for_delete_name = State()

# --- КЛАВИАТУРЫ ---
def get_main_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎬 Создать сценарий")],
        [KeyboardButton(text="📁 Мои шаблоны"), KeyboardButton(text="⚙️ Настройки")]
    ], resize_keyboard=True)

def get_models_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="Claude"), KeyboardButton(text="ChatGPT")],
        [KeyboardButton(text="Gemini"), KeyboardButton(text="Grok")],
        [KeyboardButton(text="Qwen")],
        [KeyboardButton(text="🔙 Назад в меню")]
    ], resize_keyboard=True)

def get_templates_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить шаблон"), KeyboardButton(text="🗑 Удалить шаблон")],
        [KeyboardButton(text="🔙 Назад в меню")]
    ], resize_keyboard=True)

def get_dynamic_templates_kb():
    templates = get_templates()
    kb = [[KeyboardButton(text=name)] for name in templates.keys()]
    kb.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# --- ЛОГИКА ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Бот-сценарист СССР готов! Выбирай действие:", reply_markup=get_main_kb())

@dp.message(F.text == "🔙 Назад в меню")
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=get_main_kb())

@dp.message(F.text == "⚙️ Настройки")
async def settings_menu(message: types.Message):
    current_id = get_user_model(message.from_user.id)
    friendly = MODEL_NAMES.get(current_id, current_id)
    info = (
        "⚙️ **Настройки интеллекта**\n\n"
        f"Текущая модель: **{friendly}**\n\n"
        "📜 **Доступные варианты:**\n"
        "• **Claude** — Стиль и живой язык.\n"
        "• **ChatGPT** — Логика и точность.\n"
        "• **Gemini** — Огромная память.\n"
        "• **Grok** — 🚀 Дерзкие сценарии.\n"
        "• **Qwen** — Скорость и дисциплина.\n"
    )
    await message.answer(info, reply_markup=get_models_kb(), parse_mode="Markdown")

@dp.message(F.text.in_(MODEL_NAMES.values()))
async def change_model(message: types.Message):
    inv_map = {v: k for k, v in MODEL_NAMES.items()}
    selected_id = inv_map[message.text]
    set_user_model(message.from_user.id, selected_id)
    await message.answer(f"✅ Модель изменена на: **{message.text}**", reply_markup=get_main_kb())

# --- ШАБЛОНЫ ---
@dp.message(F.text == "📁 Мои шаблоны")
async def templates_menu(message: types.Message):
    templates = get_templates()
    text = "📂 **Твои шаблоны:**\n\n"
    if not templates: text += "Пусто."
    else:
        for name, prompt in templates.items(): text += f"🔹 **{name}**\n_{prompt[:60]}..._\n\n"
    await message.answer(text, reply_markup=get_templates_menu_kb(), parse_mode="Markdown")

@dp.message(F.text == "➕ Добавить шаблон")
async def add_template_start(message: types.Message, state: FSMContext):
    await message.answer("Введи название шаблона:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(TemplateManager.waiting_for_new_name)

@dp.message(TemplateManager.waiting_for_new_name)
async def add_template_name(message: types.Message, state: FSMContext):
    await state.update_data(template_name=message.text)
    await message.answer("Текст промпта:")
    await state.set_state(TemplateManager.waiting_for_new_prompt)

@dp.message(TemplateManager.waiting_for_new_prompt)
async def add_template_prompt(message: types.Message, state: FSMContext):
    data = await state.get_data()
    add_template(data['template_name'], message.text)
    await message.answer("✅ Сохранено!", reply_markup=get_main_kb())
    await state.clear()

@dp.message(F.text == "🗑 Удалить шаблон")
async def delete_template_start(message: types.Message, state: FSMContext):
    await message.answer("Что удалить?", reply_markup=get_dynamic_templates_kb())
    await state.set_state(TemplateManager.waiting_for_delete_name)

@dp.message(TemplateManager.waiting_for_delete_name)
async def delete_template_confirm(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню": return await back_to_main(message, state)
    delete_template(message.text)
    await message.answer(f"🗑 Удалено: {message.text}", reply_markup=get_templates_menu_kb())
    await state.clear()

# --- ГЕНЕРАЦИЯ ---
@dp.message(F.text == "🎬 Создать сценарий")
async def start_script(message: types.Message, state: FSMContext):
    await message.answer("О чем видео?", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ScriptMaker.waiting_for_topic)

@dp.message(ScriptMaker.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    await state.update_data(topic=message.text)
    await message.answer("Длительность (мин):")
    await state.set_state(ScriptMaker.waiting_for_duration)

@dp.message(ScriptMaker.waiting_for_duration)
async def process_duration(message: types.Message, state: FSMContext):
    if not message.text.isdigit(): return
    duration = int(message.text)
    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)
    await message.answer("Выбери шаблон:", reply_markup=get_dynamic_templates_kb())
    await state.set_state(ScriptMaker.waiting_for_template)

@dp.message(ScriptMaker.waiting_for_template)
async def generate_script(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню": return await back_to_main(message, state)
    
    templates = get_templates()
    if message.text not in templates: return
    
    style_prompt = templates[message.text]
    data = await state.get_data()
    model_id = get_user_model(message.from_user.id)
    
    # Исправляем отображение: берем красивое имя модели для сообщения
    friendly_model = MODEL_NAMES.get(model_id, "AI")
    
    task_id = f"{random.randint(100, 999)} {random.randint(100, 999)} {random.randint(100, 999)}"
    log_task(task_id, message.from_user.id, data['topic'], friendly_model)

    # Теперь здесь пишется чистое название (Grok, Gemini и т.д.)
    status_msg = await message.answer(
        f"⏳ **Задача создана**\n\n"
        f"🆔 ID: `{task_id}`\n"
        f"🤖 Модель: **{friendly_model}**\n"
        f"📊 Статус: Подготовка структуры...",
        reply_markup=get_main_kb(), parse_mode="Markdown"
    )

    try:
        parts_count = math.ceil(data['words_target'] / 1000)
        if parts_count < 1: parts_count = 1

        plan_prompt = f"Составь план для видео '{data['topic']}' на {parts_count} глав. Только список названий."
        response = await client.chat.completions.create(
            model=model_id,
            messages=[{"role": "user", "content": plan_prompt}]
        )
        plan_text = response.choices[0].message.content
        chapters = [c for c in plan_text.split('\n') if c.strip() and any(char.isdigit() for char in c[:3])]
        
        # full_script теперь пустой в начале, без заголовков и планов
        full_script = ""

        for i, chapter in enumerate(chapters):
            try:
                await status_msg.edit_text(
                    f"🚀 **Генерация в процессе**\n\n"
                    f"🆔 ID: `{task_id}`\n"
                    f"🤖 Модель: **{friendly_model}**\n"
                    f"✍️ Пишу часть: `{i+1} из {len(chapters)}`",
                    parse_mode="Markdown"
                )
            except Exception: pass

            # Усиленный промпт, чтобы ИИ не писал заголовки глав сам
            chapter_prompt = (
                f"Тема видео: {data['topic']}\n"
                f"Пиши текст ТОЛЬКО для этого раздела: {chapter}\n"
                f"ТВОИ ПРАВИЛА: {style_prompt}\n\n"
                f"ВАЖНО: Начни сразу с текста для озвучки. НЕ ПИШИ название главы, цифры или вступления."
            )
            
            resp = await client.chat.completions.create(
                model=model_id,
                messages=[{"role": "user", "content": chapter_prompt}]
            )
            
            # Склеиваем только чистый текст
            full_script += resp.choices[0].message.content + "\n\n"
            await asyncio.sleep(5)

        file_name = f"script_{task_id.replace(' ', '_')}.txt"
        with open(file_name, "w", encoding="utf-8") as f: 
            f.write(full_script)
        
        await status_msg.edit_text(f"✅ **Готово!**\n🆔 ID: `{task_id}`\n\nСценарий отправлен файлом ниже.", parse_mode="Markdown")
        await message.answer_document(FSInputFile(file_name))
        
        update_task_status(task_id, "Completed")
        os.remove(file_name)

    except Exception as e:
        await status_msg.edit_text(f"❌ **Ошибка задачи** `{task_id}`\n\nТекст ошибки: {e}")
        update_task_status(task_id, f"Error: {e}")

    await state.clear()

async def main():
    print("Бот запущен. Система тасков активна.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
