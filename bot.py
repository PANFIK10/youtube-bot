import asyncio
import logging
import os
import math
import psycopg2
import random
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from openai import AsyncOpenAI
from aiogram.exceptions import TelegramBadRequest
import aiohttp

# --- КОНФИГУРАЦИЯ ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

# Клиент OpenRouter
client = AsyncOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
    timeout=120.0 
)

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

WORDS_PER_MINUTE = 130
WORDS_PER_CHAPTER = 330 # Реальный объем одной выдачи (около 2000-2500 символов)
MODEL_NAMES = {
    "anthropic/claude-haiku-4.5": "Claude",
    "openai/gpt-5.1": "ChatGPT",
    "google/gemini-2.5-flash-lite": "Gemini",
    "x-ai/grok-4.1-fast": "Grok",
    "qwen/qwen3.5-flash-02-23": "Qwen"
}

# --- РАБОТА С PostgreSQL ---
def get_db_connection():
    return psycopg2.connect(DATABASE_URL, sslmode='prefer')

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS templates (name TEXT PRIMARY KEY, prompt TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS settings (user_id BIGINT PRIMARY KEY, model_name TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY, user_id BIGINT, 
                    topic TEXT, model TEXT, status TEXT, created_at TEXT)''')
    
    c.execute("SELECT COUNT(*) FROM templates")
    if c.fetchone()[0] == 0:
        c.execute("INSERT INTO templates (name, prompt) VALUES (%s, %s)", 
                  ("🎬 Стандартный", "Напиши увлекательный сценарий для YouTube."))
    conn.commit()
    c.close()
    conn.close()

def log_task(task_id, user_id, topic, model):
    conn = get_db_connection()
    c = conn.cursor()
    now = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    c.execute("INSERT INTO tasks VALUES (%s, %s, %s, %s, %s, %s)", 
              (task_id, user_id, topic, model, "In Progress", now))
    conn.commit()
    c.close()
    conn.close()

def update_task_status(task_id, status):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("UPDATE tasks SET status = %s WHERE task_id = %s", (status, task_id))
    conn.commit()
    c.close()
    conn.close()

def get_task_status(task_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT status FROM tasks WHERE task_id = %s", (task_id,))
    row = c.fetchone()
    c.close()
    conn.close()
    return row[0] if row else None

def get_user_model(user_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT model_name FROM settings WHERE user_id = %s", (user_id,))
    row = c.fetchone()
    c.close()
    conn.close()
    return row[0] if row else "x-ai/grok-4.1-fast"

def set_user_model(user_id, model_name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("INSERT INTO settings (user_id, model_name) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET model_name = EXCLUDED.model_name", (user_id, model_name))
    conn.commit()
    c.close()
    conn.close()

def get_templates():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT name, prompt FROM templates")
    res = {row[0]: row[1] for row in c.fetchall()}
    c.close()
    conn.close()
    return res

def add_template(name, prompt):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("INSERT INTO templates (name, prompt) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET prompt = EXCLUDED.prompt", (name, prompt))
    conn.commit()
    c.close()
    conn.close()

def delete_template(name):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM templates WHERE name = %s", (name,))
    conn.commit()
    c.close()
    conn.close()

init_db()

async def upload_to_backup(file_path):
    try:
        async with aiohttp.ClientSession() as session:
            with open(file_path, 'rb') as f:
                # Ссылка живет 14 дней или до 1-го скачивания
                async with session.post('https://file.io/?expires=14d', data={'file': f}) as resp:
                    res = await resp.json()
                    return res.get('link')
    except Exception as e:
        logging.error(f"Ошибка создания бэкапа: {e}")
        return None

# --- СОСТОЯНИЯ ---
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
        [KeyboardButton(text="➕ Добавить шаблон"), KeyboardButton(text="✏️ Изменить шаблон")],
        [KeyboardButton(text="🗑 Удалить шаблон"), KeyboardButton(text="🔙 Назад в меню")]
    ], resize_keyboard=True)

def get_dynamic_templates_kb():
    templates = get_templates()
    kb = [[KeyboardButton(text=name)] for name in templates.keys()]
    kb.append([KeyboardButton(text="➕ Создать новый шаблон")]) 
    kb.append([KeyboardButton(text="🔙 Назад в меню")])
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

# --- ЛОГИКА ---
@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    welcome_text = (
        "👋 <b>Добро пожаловать в генератор сценариев!</b>\n\n"
        "Создавайте готовые сценарии без борьбы с лимитами и склеек.\n\n"
        "🎭 <b>Claude</b> — стиль.\n🧠 <b>ChatGPT</b> — логика.\n⚡ <b>Gemini</b> — память.\n🔥 <b>Grok</b> — дерзость.\n🐲 <b>Qwen</b> — скорость."
    )
    await message.answer(welcome_text, reply_markup=get_main_kb(), parse_mode="HTML")

@dp.message(F.text == "🔙 Назад в меню")
async def back_to_main(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=get_main_kb())

@dp.message(F.text == "⚙️ Настройки")
async def settings_menu(message: types.Message):
    current_id = get_user_model(message.from_user.id)
    friendly = MODEL_NAMES.get(current_id, current_id)
    await message.answer(f"⚙️ <b>Настройки интеллекта</b>\n\nТекущая модель: <b>{friendly}</b>", 
                         reply_markup=get_models_kb(), parse_mode="HTML")

@dp.message(F.text.in_(MODEL_NAMES.values()))
async def change_model(message: types.Message):
    inv_map = {v: k for k, v in MODEL_NAMES.items()}
    selected_id = inv_map[message.text]
    set_user_model(message.from_user.id, selected_id)
    await message.answer(f"✅ Модель изменена на: <b>{message.text}</b>", reply_markup=get_main_kb(), parse_mode="HTML")

# --- УПРАВЛЕНИЕ ШАБЛОНАМИ ---
@dp.message(F.text == "📁 Мои шаблоны")
async def templates_menu(message: types.Message):
    templates = get_templates()
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
    await message.answer("Какой шаблон хочешь изменить?", reply_markup=get_dynamic_templates_kb())
    await state.set_state(TemplateManager.waiting_for_new_name)

@dp.message(TemplateManager.waiting_for_new_name)
async def add_template_name(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню": return await back_to_main(message, state)
    await state.update_data(template_name=message.text)
    await message.answer("Отправь новый текст промпта для этого шаблона:", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(TemplateManager.waiting_for_new_prompt)

@dp.message(TemplateManager.waiting_for_new_prompt)
async def add_template_prompt(message: types.Message, state: FSMContext):
    data = await state.get_data()
    add_template(data['template_name'], message.text)
    
    if 'topic' in data:
        await message.answer(
            f"✅ Шаблон <b>{data['template_name']}</b> сохранен!\n"
            f"Продолжаем работу над сценарием: <i>{data['topic']}</i>.\n\n"
            "Выберите шаблон для генерации:", 
            reply_markup=get_dynamic_templates_kb(), 
            parse_mode="HTML"
        )
        await state.set_state(ScriptMaker.waiting_for_template)
    else:
        await message.answer("✅ Шаблон успешно сохранен!", reply_markup=get_main_kb())
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

@dp.callback_query(F.data.startswith("cancel_"))
async def cancel_task_handler(call: types.CallbackQuery):
    task_id = call.data.replace("cancel_", "")
    update_task_status(task_id, "Cancelled") 
    
    try:
        await call.message.edit_text(f"🛑 <b>Генерация отменена пользователем</b>\n\n🆔 ID: <code>{task_id}</code>", parse_mode="HTML")
    except Exception:
        pass 
        
    await call.answer("Задача успешно отменена!", show_alert=True)

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
    
    await message.answer(
        "<i>⚠️ Обратите внимание: итоговый хронометраж может отличаться на ±10 минут, так как скорость речи и паузы при озвучке у всех индивидуальны.</i>\n\n"
        "Выбери шаблон:", 
        reply_markup=get_dynamic_templates_kb(),
        parse_mode="HTML"
    )
    await state.set_state(ScriptMaker.waiting_for_template)

@dp.message(ScriptMaker.waiting_for_template)
async def generate_script(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню": return await back_to_main(message, state)
    if message.text == "➕ Создать новый шаблон":
        await message.answer("Введите название для нового шаблона:", reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(TemplateManager.waiting_for_new_name)
        return
    
    templates = get_templates()
    if message.text not in templates: return
    
    style_prompt = templates[message.text]
    data = await state.get_data()
    model_id = get_user_model(message.from_user.id)
    friendly_model = MODEL_NAMES.get(model_id, "AI")
    
    task_id = f"{random.randint(100, 999)} {random.randint(100, 999)} {random.randint(100, 999)}"
    log_task(task_id, message.from_user.id, data['topic'], friendly_model)

    try:
        # Уведомляем, что начали думать над планом
        temp_msg = await message.answer("⏳ <i>Подготовка структуры сценария...</i>", parse_mode="HTML")
        
        # Жесткий расчет количества глав
        target_chapters = math.ceil(data['words_target'] / WORDS_PER_CHAPTER)
        
        plan_prompt = f"Составь план для видео '{data['topic']}' ровно на {target_chapters} глав. Только пронумерованный список названий."
        response = await client.chat.completions.create(
            model=model_id, 
            messages=[{"role": "user", "content": plan_prompt}],
            max_tokens=2000 # Снимаем лимит на длину плана
        )
        plan_text = response.choices[0].message.content
        
        chapters = [c.strip() for c in plan_text.split('\n') if len(c.strip()) > 5]
        if not chapters: chapters = [data['topic']]
        
        full_script = ""
        total_chapters = len(chapters)
        sec_per_chapter = 12 # Реальное время генерации одной главы (5 сек пауза + 7 сек ответ)
        
        # Считаем общее время ОДИН раз
        est_seconds = total_chapters * sec_per_chapter
        est_min, est_sec = divmod(est_seconds, 60)
        time_str = f"{est_min} мин. {est_sec} сек." if est_min > 0 else f"{est_sec} сек."

        # Создаем кнопку отмены
        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить генерацию", callback_data=f"cancel_{task_id}")]
        ])

        # Удаляем временное сообщение и шлем финальное статичное
        try:
            await temp_msg.delete()
        except Exception:
            pass
            
        status_msg = await message.answer(
            f"⏳ <b>Задача принята в работу!</b>\n\n"
            f"🆔 ID: <code>{task_id}</code>\n"
            f"🤖 Модель: <b>{friendly_model}</b>\n"
            f"📚 Количество частей: <b>{total_chapters}</b>\n"
            f"⏱ Примерное время ожидания: <b>{time_str}</b>\n\n"
            f"<i>Пожалуйста, подождите. Готовый файл будет отправлен сюда.</i>",
            reply_markup=cancel_kb, parse_mode="HTML"
        )

        # === ПАРАЛЛЕЛЬНАЯ ГЕНЕРАЦИЯ (СУПЕРСКОРОСТЬ) ===
        full_script_parts = [""] * total_chapters
        
        async def generate_single_chapter(index, chapter_title):
            if get_task_status(task_id) == "Cancelled":
                return
            
            chapter_prompt = (
                f"Тема видео: {data['topic']}\n"
                f"Суть этой части: {chapter_title}\n"
                f"Стиль: {style_prompt}\n\n"
                f"КРИТИЧЕСКИЕ ПРАВИЛА (ШТРАФ ЗА НАРУШЕНИЕ):\n"
                f"1. ЖЕСТКИЙ ОБЪЕМ: Напиши строго 2000-2500 символов (это около 20-25 длинных предложений или 2,5 минуты непрерывной речи). Заверши мысль, уложившись в этот лимит.\n"
                f"2. ФОРМАТ: КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО писать заголовки! Никаких символов '#', никаких 'Глава', 'Часть'. Пиши ТОЛЬКО сплошной текст.\n"
                f"3. НАЧАЛО: Начинай сразу с первого предложения текста, без вступлений."
            )
            try:
                resp = await client.chat.completions.create(
                    model=model_id, 
                    messages=[{"role": "user", "content": chapter_prompt}],
                    max_tokens=1300
                )
                full_script_parts[index] = resp.choices[0].message.content + "\n\n"
                # --- ЛОГ УСПЕШНОЙ ГЕНЕРАЦИИ ---
                logging.info(f"[{task_id}] ✅ Часть {index+1} из {total_chapters} успешно готова!")
            except Exception as e:
                logging.error(f"[{task_id}] ❌ Ошибка в части {index+1}: {e}")
                full_script_parts[index] = f"[Ошибка генерации части {index+1}]\n\n"

        # Запускаем пачками по 5 запросов одновременно
        chunk_size = 5
        for i in range(0, total_chapters, chunk_size):
            if get_task_status(task_id) == "Cancelled":
                return
                
            chunk = chapters[i:i + chunk_size]
            # --- ЛОГ ЗАПУСКА ПАЧКИ ---
            logging.info(f"[{task_id}] 🚀 ОТПРАВЛЯЮ ПАЧКУ: части с {i+1} по {min(i+chunk_size, total_chapters)}...")
            
            tasks = [generate_single_chapter(i + j, title) for j, title in enumerate(chunk)]
            await asyncio.gather(*tasks) # Ждем завершения всей пачки
            
            await asyncio.sleep(2) # Короткая пауза между пачками для безопасности API

        full_script = "".join(full_script_parts)

        # Контрольная проверка на отмену в самом конце
        if get_task_status(task_id) == "Cancelled":
            return

        # === СОХРАНЕНИЕ И ОТПРАВКА ===
        file_name = f"script_{task_id.replace(' ', '_')}.txt"
        with open(file_name, "w", encoding="utf-8") as f: 
            f.write(full_script)
        
        # 1. Пробуем отправить файл в Telegram
        try:
            await message.answer_document(FSInputFile(file_name), caption=f"📄 Сценарий ID: {task_id}")
            # Если успешно — просто статус "Готово"
            try:
                await status_msg.edit_text(f"✅ <b>Готово!</b>\n🆔 ID: <code>{task_id}</code>\n\nСценарий успешно отправлен.", parse_mode="HTML")
            except: pass

        except Exception as e:
            logging.error(f"ТГ не смог отправить файл {task_id}: {e}")
            
            # 2. ТГ затупил -> Создаем бэкап-ссылку через нашу новую функцию
            backup_link = await upload_to_backup(file_name)
            if backup_link:
                await message.answer(
                    f"⚠️ <b>Telegram не смог отправить файл напрямую</b>, но я сохранил его в облаке:\n\n"
                    f"🔗 <a href='{backup_link}'>Скачать готовый сценарий</a>\n"
                    f"<i>(Ссылка удалится после скачивания)</i>", 
                    parse_mode="HTML"
                )
                try:
                    await status_msg.edit_text(f"✅ <b>Готово!</b>\n🆔 ID: <code>{task_id}</code>\n\nФайл доступен по ссылке выше.", parse_mode="HTML")
                except: pass
            else:
                await message.answer(f"❌ Критическая ошибка: файл {task_id} не отправился.")

        update_task_status(task_id, "Completed")
        os.remove(file_name)

    except Exception as e:
        logging.error(f"Ошибка в задаче {task_id}: {e}")
        try:
            await message.answer(f"❌ <b>Ошибка задачи</b> {task_id}\n\nТекст: <code>{str(e)}</code>", parse_mode="HTML")
        except Exception:
            pass
        update_task_status(task_id, f"Error: {str(e)}")

    await state.clear()

async def main():
    print("Бот на PostgreSQL запущен и готов к работе!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
