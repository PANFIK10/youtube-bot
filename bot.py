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
from google import genai

# Достаем ключи из сейфа хостинга
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

client = genai.Client(api_key=GOOGLE_API_KEY)
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

WORDS_PER_MINUTE = 130

# --- БАЗА ДАННЫХ ДЛЯ ШАБЛОНОВ ---
def init_db():
    conn = sqlite3.connect('templates.db')
    c = conn.cursor()
    # Создаем таблицу, если ее нет
    c.execute('''CREATE TABLE IF NOT EXISTS templates (name TEXT PRIMARY KEY, prompt TEXT)''')
    
    # Добавим один базовый шаблон для старта, если база пустая
    c.execute("SELECT COUNT(*) FROM templates")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO templates (name, prompt) VALUES (?, ?)", 
                  ("🎬 Стандартный", "Напиши увлекательный сценарий для YouTube. Раздели на смысловые блоки."))
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

# Запускаем создание БД при старте кода
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
        [KeyboardButton(text="📁 Мои шаблоны")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_templates_menu_kb():
    kb = [
        [KeyboardButton(text="➕ Добавить шаблон"), KeyboardButton(text="🗑 Удалить шаблон")],
        [KeyboardButton(text="🔙 Назад в меню")]
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

def get_dynamic_templates_kb(for_deletion=False):
    templates = get_templates()
    kb = [[KeyboardButton(text=name)] for name in templates.keys()]
    kb.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)


# --- ЛОГИКА: ГЛАВНОЕ МЕНЮ ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Привет! Я твой бот-сценарист. Выбирай действие:", reply_markup=get_main_kb())

@dp.message(F.text == "🔙 Назад в меню")
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=get_main_kb())


# --- ЛОГИКА: УПРАВЛЕНИЕ ШАБЛОНАМИ ---
@dp.message(F.text == "📁 Мои шаблоны")
async def templates_menu(message: types.Message):
    templates = get_templates()
    text = "📂 **Твои сохраненные шаблоны:**\n\n"
    if not templates:
        text += "У тебя пока нет шаблонов."
    else:
        for name, prompt in templates.items():
            text += f"🔹 **{name}**\n_{prompt[:50]}..._\n\n"
    
    await message.answer(text, reply_markup=get_templates_menu_kb(), parse_mode="Markdown")

@dp.message(F.text == "➕ Добавить шаблон")
async def add_template_start(message: types.Message, state: FSMContext):
    await message.answer("Придумай короткое название для кнопки шаблона (например: 'Для шортсов', 'Стиль Жириновского'):", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(TemplateManager.waiting_for_new_name)

@dp.message(TemplateManager.waiting_for_new_name)
async def add_template_name(message: types.Message, state: FSMContext):
    await state.update_data(template_name=message.text)
    await message.answer("Отлично. А теперь отправь сам текст промпта (системные инструкции), по которому нейросеть будет писать сценарий:")
    await state.set_state(TemplateManager.waiting_for_new_prompt)

@dp.message(TemplateManager.waiting_for_new_prompt)
async def add_template_prompt(message: types.Message, state: FSMContext):
    data = await state.get_data()
    name = data['template_name']
    prompt = message.text
    add_template(name, prompt)
    await message.answer(f"✅ Шаблон **{name}** успешно сохранен!", reply_markup=get_templates_menu_kb(), parse_mode="Markdown")
    await state.clear()

@dp.message(F.text == "🗑 Удалить шаблон")
async def delete_template_start(message: types.Message, state: FSMContext):
    await message.answer("Выбери шаблон для удаления:", reply_markup=get_dynamic_templates_kb())
    await state.set_state(TemplateManager.waiting_for_delete_name)

@dp.message(TemplateManager.waiting_for_delete_name)
async def delete_template_confirm(message: types.Message, state: FSMContext):
    name = message.text
    if name == "🔙 Назад в меню":
        await back_to_main(message, state)
        return
    delete_template(name)
    await message.answer(f"🗑 Шаблон **{name}** удален.", reply_markup=get_templates_menu_kb(), parse_mode="Markdown")
    await state.clear()


# --- ЛОГИКА: ГЕНЕРАЦИЯ СЦЕНАРИЯ ---
@dp.message(F.text == "🎬 Создать сценарий")
async def start_script(message: types.Message, state: FSMContext):
    await message.answer("О чем будет видео? Напиши тему или идею:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ScriptMaker.waiting_for_topic)

@dp.message(ScriptMaker.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    await state.update_data(topic=message.text)
    await message.answer("Сколько минут должно длиться видео?\n*Напиши просто число (например: 15)*")
    await state.set_state(ScriptMaker.waiting_for_duration)

@dp.message(ScriptMaker.waiting_for_duration)
async def process_duration(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        await message.answer("Ошибка! Введи только число минут.")
        return
    
    duration = int(message.text)
    words_target = duration * WORDS_PER_MINUTE
    await state.update_data(duration=duration, words_target=words_target)

    templates = get_templates()
    if not templates:
        await message.answer("У тебя нет ни одного шаблона! Сначала добавь его в меню 'Мои шаблоны'.", reply_markup=get_main_kb())
        await state.clear()
        return

    await message.answer(
        f"⏱ Хронометраж: {duration} мин.\n"
        f"📝 Примерный объем текста: ~{words_target} слов.\n\n"
        f"Выбери шаблон промпта из твоей базы:",
        reply_markup=get_dynamic_templates_kb()
    )
    await state.set_state(ScriptMaker.waiting_for_template)

@dp.message(ScriptMaker.waiting_for_template)
async def generate_script(message: types.Message, state: FSMContext):
    template_name = message.text
    if template_name == "🔙 Назад в меню":
        await back_to_main(message, state)
        return

    templates = get_templates()
    if template_name not in templates:
        await message.answer("Пожалуйста, выбери шаблон, нажав на кнопку внизу.")
        return

    style_prompt = templates[template_name]
    data = await state.get_data()
    topic = data['topic']
    words_target = data['words_target']

    await message.answer("🚀 Принято! Анализирую тему и составляю структуру...", reply_markup=get_main_kb())

    try:
        parts_count = math.ceil(words_target / 1000)
        if parts_count < 1: parts_count = 1

        plan_prompt = f"Составь план для YouTube видео на тему '{topic}'. Мне нужно ровно {parts_count} глав. Напиши только нумерованный список названий глав."
        plan_response = client.models.generate_content(model='gemini-1.5-flash', contents=plan_prompt)
        
        chapters = [c for c in plan_response.text.split('\n') if c.strip()]
        await message.answer(f"📋 План готов. Глав в сценарии: {len(chapters)}. Начинаю писать текст для каждой главы. Это займет время...")

        full_script = f"ТЕМА: {topic}\nХРОНОМЕТРАЖ: {data['duration']} мин.\nШАБЛОН: {template_name}\n\n--- ПЛАН ---\n{plan_response.text}\n\n--- СЦЕНАРИЙ ---\n\n"

        for i, chapter in enumerate(chapters):
            if i % 2 == 0:
                await message.answer(f"⏳ Пишу главу {i+1} из {len(chapters)}...")
            
            chapter_prompt = (
                f"Ты пишешь сценарий для YouTube видео на тему '{topic}'.\n"
                f"Твоя задача — написать текст ТОЛЬКО для этой конкретной главы плана:\n{chapter}\n\n"
                f"Твои строгие системные инструкции и стиль:\n{style_prompt}\n\n"
                f"Пиши объемно и сразу текст, который диктор будет читать с суфлера. Не пиши приветствия, только текст главы."
            )
            
            part_response = client.models.generate_content(model='gemini-1.5-flash', contents=chapter_prompt)
            full_script += f"\n\n[ГЛАВА: {chapter}]\n\n" + part_response.text
            
            await asyncio.sleep(3) 

        file_path = f"script_{message.from_user.id}.txt"
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(full_script)

        script_file = FSInputFile(file_path)
        await message.answer_document(script_file, caption=f"🎉 Сценарий на {data['duration']} мин. готов!")

        os.remove(file_path)

    except Exception as e:
        await message.answer(f"❌ Ошибка генерации (возможно, превышен лимит слов или тема заблокирована нейросетью): {e}")

    await state.clear()

async def main():
    print("Бот успешно запущен на сервере!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())