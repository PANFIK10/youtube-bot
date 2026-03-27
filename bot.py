import asyncio
import logging
import os
import math
import sqlite3
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from openai import AsyncOpenAI

# --- КОНФИГУРАЦИЯ ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")

# Инициализация клиента OpenRouter
client = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

WORDS_PER_MINUTE = 130

# --- РАБОТА С БАЗОЙ ДАННЫХ ---
def init_db():
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    # Таблица для шаблонов
    c.execute('''CREATE TABLE IF NOT EXISTS templates (name TEXT PRIMARY KEY, prompt TEXT)''')
    # Таблица для настроек пользователей (выбор модели)
    c.execute('''CREATE TABLE IF NOT EXISTS settings (user_id INTEGER PRIMARY KEY, model_name TEXT)''')
    
    # Дефолтный шаблон, если пусто
    c.execute("SELECT COUNT(*) FROM templates")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO templates (name, prompt) VALUES (?, ?)", 
                  ("🎬 Стандартный", "Напиши увлекательный сценарий для YouTube. Раздели на смысловые блоки."))
    conn.commit()
    conn.close()

def get_user_model(user_id):
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    c.execute("SELECT model_name FROM settings WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else "x-ai/grok-4.1-fast" # Грок по умолчанию

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
    result = {row[0]: row[1] for row in c.fetchall()}
    conn.close()
    return result

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

# --- МАШИНЫ СОСТОЯНИЙ (FSM) ---
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
    kb = [
        [KeyboardButton(text="🎬 Создать сценарий")],
        [KeyboardButton(text="📁 Мои шаблоны"), KeyboardButton(text="⚙️ Настройки")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_models_kb():
    kb = [
        [KeyboardButton(text="Claude"), KeyboardButton(text="ChatGPT")],
        [KeyboardButton(text="Gemini"), KeyboardButton(text="Grok")],
        [KeyboardButton(text="Qwen")],
        [KeyboardButton(text="🔙 Назад в меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_templates_menu_kb():
    kb = [
        [KeyboardButton(text="➕ Добавить шаблон"), KeyboardButton(text="🗑 Удалить шаблон")],
        [KeyboardButton(text="🔙 Назад в меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_dynamic_templates_kb():
    templates = get_templates()
    kb = [[KeyboardButton(text=name)] for name in templates.keys()]
    kb.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# --- ЛОГИКА: МЕНЮ И НАСТРОЙКИ ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Бот-сценарист готов к работе! Выбирай действие:", reply_markup=get_main_kb())

@dp.message(F.text == "🔙 Назад в меню")
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=get_main_kb())

@dp.message(F.text == "⚙️ Настройки")
async def settings_menu(message: types.Message):
    current_model_id = get_user_model(message.from_user.id)
    
    # Краткие справки по моделям для интерфейса
    info = (
        "⚙️ **Настройки интеллекта**\n\n"
        f"Текущая модель: `{current_model_id}`\n\n"
        "📜 **Доступные варианты:**\n"
        "• **Claude** (200к окно) — Идеальный стиль и живой язык.\n"
        "• **ChatGPT** (400к окно) — Стабильная логика и точность.\n"
        "• **Gemini** (1.05М окно) — Сверхпамять для длинных видео.\n"
        "• **Grok** (2М окно) — 🚀 Дерзкие и нестандартные сценарии.\n"
        "• **Qwen** (1М окно) — Высокая скорость и послушность промпту.\n"
    )
    await message.answer(info, reply_markup=get_models_kb(), parse_mode="Markdown")

@dp.message(F.text.in_(["Claude", "ChatGPT", "Gemini", "Grok", "Qwen"]))
async def change_model(message: types.Message):
    model_map = {
        "Claude": "anthropic/claude-haiku-4.5",
        "ChatGPT": "openai/gpt-5.1",
        "Gemini": "google/gemini-2.5-flash-lite",
        "Grok": "x-ai/grok-4.1-fast",
        "Qwen": "qwen/qwen3.5-flash-02-23"
    }
    selected = model_map[message.text]
    set_user_model(message.from_user.id, selected)
    
    resp = f"✅ Выбрана модель **{message.text}**"
    if message.text == "Grok":
        resp += "\n\n🔥 Режим дерзкого сценариста активирован!"
    
    await message.answer(resp, reply_markup=get_main_kb(), parse_mode="Markdown")

# --- ЛОГИКА: УПРАВЛЕНИЕ ШАБЛОНАМИ ---
@dp.message(F.text == "📁 Мои шаблоны")
async def templates_menu(message: types.Message):
    templates = get_templates()
    text = "📂 **Твои сохраненные шаблоны:**\n\n"
    if not templates: text += "Список пуст."
    else:
        for name, prompt in templates.items(): text += f"🔹 **{name}**\n_{prompt[:60]}..._\n\n"
    await message.answer(text, reply_markup=get_templates_menu_kb(), parse_mode="Markdown")

@dp.message(F.text == "➕ Добавить шаблон")
async def add_template_start(message: types.Message, state: FSMContext):
    await message.answer("Введи название (коротко, для кнопки):", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(TemplateManager.waiting_for_new_name)

@dp.message(TemplateManager.waiting_for_new_name)
async def add_template_name(message: types.Message, state: FSMContext):
    await state.update_data(template_name=message.text)
    await message.answer("Теперь отправь полный текст промпта (правила стиля, ограничения):")
    await state.set_state(TemplateManager.waiting_for_new_prompt)

@dp.message(TemplateManager.waiting_for_new_prompt)
async def add_template_prompt(message: types.Message, state: FSMContext):
    data = await state.get_data()
    add_template(data['template_name'], message.text)
    await message.answer(f"✅ Шаблон '{data['template_name']}' сохранен!", reply_markup=get_main_kb())
    await state.clear()

@dp.message(F.text == "🗑 Удалить шаблон")
async def delete_template_start(message: types.Message, state: FSMContext):
    await message.answer("Выбери шаблон для удаления:", reply_markup=get_dynamic_templates_kb())
    await state.set_state(TemplateManager.waiting_for_delete_name)

@dp.message(TemplateManager.waiting_for_delete_name)
async def delete_template_confirm(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню": return await back_to_main(message, state)
    delete_template(message.text)
    await message.answer(f"🗑 Шаблон '{message.text}' удален.", reply_markup=get_templates_menu_kb())
    await state.clear()

# --- ЛОГИКА: СОЗДАНИЕ СЦЕНАРИЯ ---
@dp.message(F.text == "🎬 Создать сценарий")
async def start_script(message: types.Message, state: FSMContext):
    await message.answer("О чем будет видео? Введи тему или краткое описание:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ScriptMaker.waiting_for_topic)

@dp.message(ScriptMaker.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    await state.update_data(topic=message.text)
    await message.answer("Желаемая длительность в минутах (только число):")
    await state.set_state(ScriptMaker.waiting_for_duration)

@dp.message(ScriptMaker.waiting_for_duration)
async def process_duration(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Пожалуйста, введи только число минут.")
        return
    duration = int(message.text)
    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)
    await message.answer("Выбери стиль написания (шаблон):", reply_markup=get_dynamic_templates_kb())
    await state.set_state(ScriptMaker.waiting_for_template)

@dp.message(ScriptMaker.waiting_for_template)
async def generate_script(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню": return await back_to_main(message, state)
    
    templates = get_templates()
    if message.text not in templates: return
    
    style_prompt = templates[message.text]
    data = await state.get_data()
    user_model = get_user_model(message.from_user.id)

    await message.answer(f"🚀 Начинаю генерацию через {user_model}...\nЭто может занять несколько минут.", reply_markup=get_main_kb())

    try:
        # Разбиваем на части по ~1000 слов
        parts_count = math.ceil(data['words_target'] / 1000)
        if parts_count < 1: parts_count = 1

        # 1. Генерируем структуру
        plan_prompt = f"Составь подробный план для видео '{data['topic']}' на {parts_count} глав. Выдай только нумерованный список названий глав."
        response = await client.chat.completions.create(
            model=user_model,
            messages=[{"role": "user", "content": plan_prompt}]
        )
        plan_text = response.choices[0].message.content
        chapters = [c for c in plan_text.split('\n') if c.strip() and any(char.isdigit() for char in c[:3])]
        
        await message.answer(f"📋 Структура готова ({len(chapters)} глав). Начинаю детальную проработку текста...")

        full_script = f"ТЕМА: {data['topic']}\nХРОНОМЕТРАЖ: {data['duration']} мин.\nИНТЕЛЛЕКТ: {user_model}\n\n--- ПЛАН ---\n{plan_text}\n\n--- СЦЕНАРИЙ ---\n"

        # 2. Генерируем каждую главу
        for i, chapter in enumerate(chapters):
            await message.answer(f"⏳ Пишу главу {i+1} из {len(chapters)}...")
            chapter_prompt = (
                f"Тема видео: {data['topic']}\n"
                f"Текущая глава для написания: {chapter}\n"
                f"Инструкции по стилю: {style_prompt}\n"
                f"ПИШИ ОБЪЕМНО. Сразу текст для диктора. Без вступлений типа 'Конечно, вот текст'."
            )
            
            resp = await client.chat.completions.create(
                model=user_model,
                messages=[{"role": "user", "content": chapter_prompt}]
            )
            full_script += f"\n\n[ {chapter.upper()} ]\n\n" + resp.choices[0].message.content
            
            # Пауза для стабильности API
            await asyncio.sleep(6)

        # 3. Сохранение в файл
        file_name = f"script_{message.from_user.id}.txt"
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(full_script)

        # 4. Отправка документа
        script_file = FSInputFile(file_name)
        await message.answer_document(script_file, caption=f"🎉 Сценарий на {data['duration']} мин. полностью готов!")
        
        # Удаляем временный файл
        os.remove(file_name)

    except Exception as e:
        await message.answer(f"❌ Ошибка в процессе генерации: {e}")

    await state.clear()

# --- ЗАПУСК ---
async def main():
    print("Бот запущен через OpenRouter. Ожидание сообщений...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())