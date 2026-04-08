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
import aiohttp.web

# ---------------------------------------------------------------------------
# КОНФИГУРАЦИЯ
# ---------------------------------------------------------------------------
TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL       = os.getenv("DATABASE_URL")
CRYPTOBOT_TOKEN    = os.getenv("CRYPTOBOT_TOKEN", "")
WEBHOOK_HOST       = os.getenv("WEBHOOK_HOST", "")  # https://scriptai.bothost.ru
# Уведомления об ошибках администратору
_raw_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: set[int] = {int(x.strip()) for x in _raw_admins.split(",") if x.strip().isdigit()}

CRYPTOBOT_API_URL  = "https://pay.crypt.bot/api"

# Robokassa
ROBO_LOGIN         = "ScriptAI"
ROBO_PASS1         = os.getenv("ROBO_PASS1", "")
ROBO_PASS2         = os.getenv("ROBO_PASS2", "")
ROBO_PASS1_TEST    = os.getenv("ROBO_PASS1_TEST", "")
ROBO_PASS2_TEST    = os.getenv("ROBO_PASS2_TEST", "")
ROBO_TEST_USERS    = ADMIN_IDS  # тестовый режим только для админов

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
_user_tasks: dict[int, int] = {}   # user_id → кол-во активных задач
_last_message: dict[int, float] = {}  # user_id → timestamp последнего сообщения
THROTTLE_SECONDS = 0.5  # минимальный интервал между сообщениями от одного пользователя

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

    # Middleware защита от флуда
    @dp.message.middleware()
    async def throttle_middleware(handler, event, data):
        import time
        user_id = event.from_user.id if event.from_user else None
        if user_id:
            now = time.monotonic()
            last = _last_message.get(user_id, 0)
            if now - last < THROTTLE_SECONDS:
                return  # молча игнорируем слишком частые сообщения
            _last_message[user_id] = now
        return await handler(event, data)

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

MIN_TOPUP_RUB = 100  # минимальная сумма пополнения в рублях

def rub_to_usd(rub: float) -> str:
    """Переводит рубли в доллары для отображения рядом с ценой."""
    return f"~${rub / USD_TO_RUB:.2f}"

# Цена в кредитах за минуту (1 кредит = 1 рубль)
MODEL_PRICE_PER_MINUTE: dict[str, float] = {
    "google/gemini-2.5-flash-lite":  0.15,
    "x-ai/grok-4.1-fast":            0.25,
    "anthropic/claude-haiku-4.5":    0.75,
    "openai/gpt-5.1":                1.25,
    "anthropic/claude-sonnet-4.6":   2.00,
}

WELCOME_CREDITS = 50  # стартовый бонус новому пользователю

REFERRAL_BONUS_INVITER     = 25
REFERRAL_BONUS_INVITEE_PCT = 10

MAX_TASKS_PER_USER    = 2     # макс. одновременных задач от одного пользователя
LOW_BALANCE_THRESHOLD = 20.0  # уведомлять если баланс ниже этого порога после генерации
MAX_SCRIPT_DURATION   = 300   # максимум минут для одного сценария

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


def _esc(text: str) -> str:
    """Экранирует спецсимволы HTML чтобы пользовательский ввод не ломал разметку."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def admin_notify(text: str):
    """Отправляет уведомление всем администраторам из ADMIN_IDS."""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# CRYPTOBOT — создание инвойсов и обработка вебхуков
# ---------------------------------------------------------------------------

async def cryptobot_create_invoice(amount_rub: float, user_id: int,
                                   package_idx: int, description: str) -> dict | None:
    """Создаёт инвойс в CryptoBot на сумму в рублях."""
    if not CRYPTOBOT_TOKEN:
        return None
    payload = f"{user_id}:{package_idx}"
    async with aiohttp.ClientSession() as session:
        try:
            resp = await session.get(
                f"{CRYPTOBOT_API_URL}/createInvoice",
                headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                params={
                    "currency_type": "fiat",
                    "fiat": "RUB",
                    "amount": str(round(amount_rub, 2)),
                    "description": description,
                    "payload": payload,
                    "paid_btn_name": "openBot",
                    "paid_btn_url": f"https://t.me/autoscenariobot",
                    "expires_in": 3600,  # 1 час на оплату
                },
            )
            data = await resp.json()
            if data.get("ok"):
                return data["result"]
        except Exception as e:
            logging.error(f"CryptoBot createInvoice: {e}")
    return None


async def _verify_cryptobot_signature(body: bytes, signature: str) -> bool:
    """Проверяет подпись вебхука от CryptoBot."""
    import hashlib
    import hmac
    secret = hashlib.sha256(CRYPTOBOT_TOKEN.encode()).digest()
    expected = hmac.new(secret, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


async def cryptobot_webhook_handler(request: aiohttp.web.Request):
    """Обработчик вебхука от CryptoBot — начисляет кредиты после оплаты."""
    body = await request.read()
    signature = request.headers.get("crypto-pay-api-signature", "")

    if not await _verify_cryptobot_signature(body, signature):
        logging.warning("CryptoBot: неверная подпись вебхука")
        return aiohttp.web.Response(status=401)

    import json
    data = json.loads(body)

    if data.get("update_type") != "invoice_paid":
        return aiohttp.web.Response(text="ok")

    invoice = data.get("payload", {})
    payload = invoice.get("payload", "")

    try:
        user_id_str, pkg_idx_str = payload.split(":")
        user_id  = int(user_id_str)
        pkg_idx  = int(pkg_idx_str)
        price, base, bonus, name = CREDIT_PACKAGES[pkg_idx]
        total = base + bonus
    except Exception as e:
        logging.error(f"CryptoBot webhook: ошибка разбора payload: {e}")
        return aiohttp.web.Response(text="ok")

    # Защита от двойного начисления
    invoice_id = str(invoice.get("invoice_id", ""))
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT inv_id FROM payments WHERE inv_id=$1", f"crypto_{invoice_id}"
        )
        if existing:
            logging.warning(f"CryptoBot: дубль платежа invoice_id={invoice_id}")
            return aiohttp.web.Response(text="ok")
        await conn.execute(
            "INSERT INTO payments (inv_id, user_id, amount, created_at) VALUES ($1,$2,$3,$4)",
            f"crypto_{invoice_id}", user_id, float(price), _now(),
        )

    # Начисляем кредиты
    await add_credits(user_id, total, f"💎 Оплата криптой — пакет «{name}»")

    # Обрабатываем реферальный бонус при первом пополнении
    await process_first_topup(user_id, float(price))

    new_balance = await get_balance(user_id)

    # Уведомляем пользователя
    try:
        await bot.send_message(
            user_id,
            f"✅ <b>Оплата получена!</b>\n\n"
            f"📦 Пакет: <b>{name}</b>\n"
            f"💎 Начислено: <b>{total} кредитов</b>\n"
            f"💰 Баланс: <b>{new_balance:.1f} кред.</b>\n\n"
            f"Спасибо! Можешь создавать сценарии 🎬",
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"CryptoBot: не удалось уведомить user {user_id}: {e}")

    logging.info(f"CryptoBot: начислено {total} кред. → user {user_id} (пакет {name})")
    return aiohttp.web.Response(text="ok")

# ---------------------------------------------------------------------------
# ROBOKASSA — создание платёжных ссылок и обработка результата
# ---------------------------------------------------------------------------

def robo_is_test(user_id: int) -> bool:
    """Тестовый режим только для администраторов."""
    return user_id in ROBO_TEST_USERS


def robo_make_link(amount: float, inv_id: int, desc: str, user_id: int) -> str:
    """Возвращает ссылку на наш endpoint который отдаёт POST-форму."""
    import urllib.parse
    params = urllib.parse.urlencode({
        "amount": f"{amount:.2f}",
        "inv_id": inv_id,
        "desc": desc,
        "uid": user_id,
    })
    return f"{WEBHOOK_HOST}/robokassa/pay?{params}"


def robo_build_form_html(amount: float, inv_id: int, desc: str, user_id: int) -> str:
    """Строит HTML страницу с автосабмит POST-формой для Robokassa."""
    import hashlib, json, urllib.parse
    is_test = robo_is_test(user_id)
    pass1   = ROBO_PASS1_TEST if is_test else ROBO_PASS1

    receipt_obj = {
        "items": [{
            "name": desc[:128],
            "quantity": 1,
            "sum": round(amount, 2),
            "payment_method": "full_payment",
            "payment_object": "service",
            "tax": "none",
        }]
    }
    receipt_json    = json.dumps(receipt_obj, ensure_ascii=False, separators=(',', ':'))
    receipt_encoded = urllib.parse.quote(receipt_json)

    # Подпись с URL-encoded Receipt
    sign_str  = f"{ROBO_LOGIN}:{amount:.2f}:{inv_id}:{receipt_encoded}:{pass1}:Shp_uid={user_id}"
    signature = hashlib.sha256(sign_str.encode()).hexdigest()

    is_test_val = "1" if is_test else "0"

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Переход к оплате...</title></head>
<body onload="document.forms[0].submit()">
<p>Переход к оплате...</p>
<form action="https://auth.robokassa.ru/Merchant/Index.aspx" method="POST">
  <input type="hidden" name="MerchantLogin" value="{ROBO_LOGIN}">
  <input type="hidden" name="OutSum" value="{amount:.2f}">
  <input type="hidden" name="InvId" value="{inv_id}">
  <input type="hidden" name="Description" value="{desc[:100]}">
  <input type="hidden" name="Receipt" value="{receipt_encoded}">
  <input type="hidden" name="SignatureValue" value="{signature}">
  <input type="hidden" name="IsTest" value="{is_test_val}">
  <input type="hidden" name="Culture" value="ru">
  <input type="hidden" name="Shp_uid" value="{user_id}">
  <input type="submit" value="Оплатить">
</form>
</body></html>"""


async def robokassa_pay_handler(request: aiohttp.web.Request):
    """Отдаёт HTML форму с автосабмитом для оплаты через POST."""
    import urllib.parse
    params  = request.rel_url.query
    try:
        amount  = float(params["amount"])
        inv_id  = int(params["inv_id"])
        desc    = params.get("desc", "Script AI")
        user_id = int(params["uid"])
    except Exception:
        return aiohttp.web.Response(text="bad params", status=400)

    html = robo_build_form_html(amount, inv_id, desc, user_id)
    return aiohttp.web.Response(text=html, content_type="text/html", charset="utf-8")


def robo_check_result(out_sum: str, inv_id: str, signature: str,
                      shp_uid: str, is_test: bool) -> bool:
    """Проверяет подпись Result URL от Robokassa."""
    import hashlib
    pass2 = ROBO_PASS2_TEST if is_test else ROBO_PASS2
    # Формат проверки: OutSum:InvId:Пароль#2:Shp_uid=value
    sign_str = f"{out_sum}:{inv_id}:{pass2}:Shp_uid={shp_uid}"
    expected = hashlib.sha256(sign_str.encode()).hexdigest().upper()
    return expected == signature.upper()


async def robokassa_result_handler(request: aiohttp.web.Request):
    """Обработчик Result URL от Robokassa — начисляет кредиты после оплаты."""
    try:
        data = dict(await request.post())
    except Exception:
        data = dict(request.rel_url.query)

    out_sum   = data.get("OutSum", "")
    inv_id    = data.get("InvId", "")
    signature = data.get("SignatureValue", "")
    shp_uid   = data.get("Shp_uid", data.get("shp_uid", ""))
    is_test   = data.get("IsTest", "0") == "1"

    logging.info(f"Robokassa result: inv={inv_id} sum={out_sum} uid={shp_uid} test={is_test}")

    if not robo_check_result(out_sum, inv_id, signature, shp_uid, is_test):
        logging.warning(f"Robokassa: неверная подпись inv={inv_id} | data={dict(data)}")
        return aiohttp.web.Response(text="bad sign")

    try:
        user_id = int(shp_uid)
    except Exception as e:
        logging.error(f"Robokassa: ошибка shp_uid: {e}")
        return aiohttp.web.Response(text=f"OK{inv_id}")

    # Защита от двойного начисления — берём данные из БД
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT user_id, amount, pkg_idx, paid FROM payments WHERE inv_id=$1",
            str(inv_id)
        )
        if not row:
            logging.error(f"Robokassa: inv_id={inv_id} не найден в БД")
            return aiohttp.web.Response(text=f"OK{inv_id}")
        if row["paid"]:
            logging.warning(f"Robokassa: дубль платежа inv={inv_id}, игнорируем")
            return aiohttp.web.Response(text=f"OK{inv_id}")
        # Помечаем как оплаченный
        await conn.execute(
            "UPDATE payments SET paid=TRUE WHERE inv_id=$1", str(inv_id)
        )
        pkg_idx = row["pkg_idx"]

    try:
        price, base, bonus, name = CREDIT_PACKAGES[pkg_idx]
        total = base + bonus
    except Exception as e:
        logging.error(f"Robokassa: неверный pkg_idx={pkg_idx}: {e}")
        return aiohttp.web.Response(text=f"OK{inv_id}")

    await add_credits(user_id, total, f"💳 Оплата СБП — пакет «{name}»")
    await process_first_topup(user_id, float(price))
    new_balance = await get_balance(user_id)  # ← был баг: переменная не была определена

    try:
        test_label = " (тест)" if is_test else ""
        await bot.send_message(
            user_id,
            f"✅ <b>Оплата получена{test_label}!</b>\n\n"
            f"📦 Пакет: <b>{name}</b>\n"
            f"💳 Начислено: <b>{total} кредитов</b>\n"
            f"💰 Баланс: <b>{new_balance:.1f} кред.</b>\n\n"
            f"Спасибо! Можешь создавать сценарии 🎬",
            parse_mode="HTML",
        )
    except Exception as e:
        logging.error(f"Robokassa: не удалось уведомить user {user_id}: {e}")

    logging.info(f"Robokassa: начислено {total} кред. → user {user_id} (пакет {name})")
    return aiohttp.web.Response(text=f"OK{inv_id}")

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
            ("username",      "TEXT"),
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
        # Таблица для защиты от двойного начисления
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                inv_id      TEXT PRIMARY KEY,
                user_id     BIGINT,
                amount      NUMERIC(12,2),
                pkg_idx     INT DEFAULT -1,
                paid        BOOLEAN DEFAULT FALSE,
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
        # CASE защищает от ухода в минус при гонке запросов
        result = await conn.fetchval(
            """
            UPDATE settings
            SET credits = GREATEST(credits - $1, 0)
            WHERE user_id = $2
            RETURNING credits
            """,
            amount, user_id,
        )
        await conn.execute(
            "INSERT INTO transactions (user_id, amount, description, created_at) VALUES ($1,$2,$3,$4)",
            user_id, -amount, description, _now(),
        )
    return float(result or 0)


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

# ---------------------------------------------------------------------------
# СОСТОЯНИЯ
# ---------------------------------------------------------------------------
class ScriptMaker(StatesGroup):
    waiting_for_topic    = State()
    waiting_for_duration = State()
    waiting_for_template = State()

class BulkMaker(StatesGroup):
    waiting_for_topics   = State()
    waiting_for_duration = State()
    waiting_for_template = State()

class TemplateManager(StatesGroup):
    waiting_for_new_name    = State()
    waiting_for_new_prompt  = State()
    waiting_for_delete_name = State()
    waiting_for_edit_name   = State()

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
WELCOME_NEW_TEXT = (
    "👋 <b>Добро пожаловать в Авто Сценарист!</b>\n\n"
    "Генерируй готовые YouTube-сценарии за минуты — "
    "без копирайтеров, без ограничений по длине, без склеек.\n\n"
    "✨ <b>Почему выбирают нас:</b>\n"
    "⚡ Сценарий на 60 минут — за 3-5 минут\n"
    "🗂 Массовая генерация — до 5 сценариев за раз\n"
    "🤖 5 топовых моделей — GPT, Claude, Gemini, Grok\n"
    "💰 От 9₽ за полный 60-минутный сценарий\n"
    "🎯 Любая ниша — история, бизнес, технологии, лайфстайл\n\n"
    "──────────────────────────\n"
    "🟢 <b>Gemini</b> — быстро и бюджетно\n"
    "🔵 <b>Grok</b> — дерзко и смело\n"
    "🟡 <b>Claude Haiku</b> — выдержанный стиль\n"
    "🟠 <b>ChatGPT</b> — чёткая логика\n"
    "🔴 <b>Claude Sonnet ✨</b> — элитное качество\n"
    "──────────────────────────\n\n"
    f"🎁 Тебе начислено <b>{WELCOME_CREDITS} стартовых кредитов</b> — "
    f"хватит на несколько сценариев прямо сейчас!"
)

WELCOME_RETURN_TEXT = (
    "👋 <b>С возвращением!</b>\n\n"
    "Готов создавать новые сценарии — жми кнопку и поехали 🚀"
)

FAQ_TEXT = (
    "❓ <b>Часто задаваемые вопросы</b>\n\n"

    "💰 <b>Как работают кредиты?</b>\n"
    "1 кредит = 1 рубль. Стоимость зависит от модели и длительности. "
    "Например, 60 минут на Gemini — 9 кредитов, на Claude Sonnet — 120. "
    "Точная стоимость всегда показывается перед стартом.\n\n"

    "🎁 <b>Почему у меня 50 кредитов сразу?</b>\n"
    "Это стартовый бонус — подарок, чтобы попробовать сервис без вложений.\n\n"

    "⏱ <b>Сколько времени занимает генерация?</b>\n"
    "Сценарий на 30 мин — около 2-4 минут, на 120 мин — 8-15 минут. "
    "Gemini быстрее, Claude Sonnet чуть медленнее, но качественнее.\n\n"

    "🤖 <b>Чем отличаются модели?</b>\n"
    "🟢 Gemini — дешево, быстро, для любой темы\n"
    "🔵 Grok — дерзкий стиль, хорош для острых тем\n"
    "🟡 Claude Haiku — выдержанный литературный стиль\n"
    "🟠 ChatGPT — четкая логика, структурированный текст\n"
    "🔴 Claude Sonnet — лучшее качество, топовая модель\n\n"

    "📋 <b>Что такое шаблон?</b>\n"
    "Инструкция для нейросети о стиле написания. Например: "
    "<i>«Пиши как опытный документалист, без воды, с живыми примерами»</i>. "
    "Можно создать разные шаблоны под разные форматы и переключаться между ними.\n\n"

    "🗂 <b>Как работает массовая генерация?</b>\n"
    "Вводишь до 5 тем каждую с новой строки, выбираешь длительность и шаблон. "
    "Бот списывает кредиты сразу и генерирует сценарии по одному, "
    "присылая файлы по мере готовности. Можно уйти пить чай — все придёт само.\n\n"

    "📄 <b>В каком формате приходит сценарий?</b>\n"
    "В виде .txt файла — открывается на любом устройстве, "
    "можно скопировать в Google Docs или сразу отдать на озвучку.\n\n"

    "🔗 <b>Как работает реферальная программа?</b>\n"
    "Поделись ссылкой из раздела «👥 Реферальная программа». "
    "Когда друг делает первое пополнение — ты получаешь 25 кредитов, "
    "а он +10% бонусом к своему платежу.\n\n"

    "❌ <b>Что если файл не пришёл?</b>\n"
    "Кредиты списываются только после успешной отправки файла. "
    "Попробуй снова — если проблема повторяется, напиши в "
    "<a href='https://t.me/aass11463'>поддержку</a>.\n\n"

    "💳 <b>Как пополнить баланс?</b>\n"
    "Нажми «💳 Пополнить», выбери пакет и следуй инструкции. "
    "Чем больше пакет — тем выгоднее цена за кредит.\n\n"

    "🔄 <b>Можно ли отменить генерацию?</b>\n"
    "Да — во время генерации есть кнопка «❌ Отменить». "
    "Также работает команда /cancel. Кредиты при отмене не списываются.\n\n"

    "📜 <b>Публичная оферта и условия возврата</b>\n"
    "Полный текст договора — /oferta\n\n"

    "──────────────────────────\n"
    "📌 <b>Реквизиты:</b>\n"
    "Самозанятый: Панферов Кирилл Алексеевич\n"
    "ИНН: 616810872170\n"
    "📩 kkpanferovvai@gmail.com\n"
    "💬 @aass11463"
)

OFERTA_TEXT = (
    "📜 <b>ПУБЛИЧНАЯ ОФЕРТА</b>\n"
    "<i>о заключении договора об оказании услуг</i>\n\n"

    "<b>Исполнитель:</b> Самозанятый Панферов Кирилл Алексеевич\n"
    "<b>ИНН:</b> 616810872170\n"
    "<b>Сервис:</b> Telegram-бот @autoscenariobot (Script AI)\n"
    "<b>Контакт:</b> @aass11463 | kkpanferovvai@gmail.com\n\n"

    "──────────────────────────\n"
    "<b>1. Предмет договора</b>\n"
    "Исполнитель оказывает услуги по автоматической генерации текстовых "
    "сценариев для YouTube-видео через Telegram-бот. Заказчик оплачивает "
    "услуги кредитами (1 кредит = 1 рубль).\n\n"

    "<b>2. Акцепт оферты</b>\n"
    "Договор считается заключённым с момента первого запуска бота, "
    "пополнения баланса или запуска генерации.\n\n"

    "<b>3. Стоимость и оплата</b>\n"
    "Стоимость определяется тарифами в боте (кнопка «📋 Тарифы»). "
    "Минимальное пополнение — 100₽. Кредиты списываются только после "
    "успешной генерации и доставки файла.\n\n"

    "<b>4. Получение услуги</b>\n"
    "Результат — .txt файл со сценарием, отправляемый в чат. "
    "Время генерации: 2–20 минут. Хронометраж может отличаться на ±10 мин.\n\n"

    "<b>5. Возврат средств</b>\n"
    "✅ Кредиты возвращаются если:\n"
    "— генерация не завершена по вине Исполнителя;\n"
    "— файл не был доставлен и резервная ссылка недоступна.\n\n"
    "❌ Возврат не производится если:\n"
    "— файл успешно доставлен;\n"
    "— генерация отменена самим пользователем после списания;\n"
    "— пользователь недоволен содержанием (субъективная оценка).\n\n"
    "💰 Неиспользованные кредиты возвращаются деньгами в течение 14 дней "
    "по запросу в поддержку: @aass11463\n\n"

    "<b>6. Ответственность</b>\n"
    "Максимальная ответственность Исполнителя — стоимость неоказанных услуг. "
    "Исполнитель не отвечает за содержание сценариев с точки зрения "
    "авторских прав на данные языковых моделей третьих лиц.\n\n"

    "<b>7. Конфиденциальность</b>\n"
    "Персональные данные обрабатываются согласно ФЗ-152. "
    "Темы сценариев не хранятся дольше необходимого.\n\n"

    "<b>8. Срок действия</b>\n"
    "Оферта действует с момента размещения в боте до её отзыва. "
    "Изменения публикуются в боте и вступают в силу немедленно.\n\n"

    "<b>9. Споры</b>\n"
    "Споры решаются путём переговоров. Досудебный порядок обязателен. "
    "Применимое право — законодательство РФ."
)

def build_tariffs_text() -> str:
    rates = [
        ("🟢", "Gemini",          0.15),
        ("🔵", "Grok",            0.25),
        ("🟡", "Claude Haiku",    0.75),
        ("🟠", "ChatGPT (GPT-5.1)", 1.25),
        ("🔴", "Claude Sonnet ✨", 2.00),
    ]
    lines = [
        "💰 <b>Тарифы на генерацию</b>\n\n"
        "Стоимость = <b>цена за минуту × длительность</b>\n"
        "1 кредит = 1 рубль\n\n"
        "──────────────────────────\n"
    ]
    for emoji, name, ppm in rates:
        lines.append(f"{emoji} <b>{name}</b> — {ppm} кред./мин\n")
        for mins in [30, 60, 90, 120]:
            cost = ppm * mins
            lines.append(f"  {mins} мин: <b>{cost:.1f}₽</b> ({rub_to_usd(cost)})")
            if mins < 120:
                lines.append("  |")
        lines.append("\n\n")
    lines.append(
        "──────────────────────────\n\n"
        "<i>Стоимость показывается перед каждой генерацией.\n"
        "Кредиты списываются только после успешного завершения.\n"
        "Если баланса не хватает — генерация не начнётся.</i>"
    )
    return "".join(lines)


def build_packages_text() -> str:
    lines = [
        "📦 <b>Пакеты пополнения</b>\n\n"
        f"Минимальная сумма пополнения: <b>{MIN_TOPUP_RUB}₽</b> "
        f"({rub_to_usd(MIN_TOPUP_RUB)} USDT)\n"
        "Покупай выгоднее — чем больше пакет, тем ниже цена за кредит.\n\n"
    ]

    for price, base, bonus, name in CREDIT_PACKAGES:
        total = base + bonus
        discount = round((1 - price / total) * 100) if total > price else 0
        discount_str = f" (<b>скидка {discount}%</b>)" if discount else ""
        bonus_str    = f" + {bonus} бонус" if bonus else ""

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
            f"<b>{name}</b> — {price}₽ ({rub_to_usd(price)}){discount_str}\n"
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
    username = message.from_user.username  # может быть None
    args     = message.text.split()
    user     = await get_or_create_user(user_id)

    # Сохраняем/обновляем username при каждом старте
    if username:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE settings SET username=$1 WHERE user_id=$2",
                username.lower(), user_id,
            )

    # Реферальный код в параметре /start
    if len(args) > 1:
        # /start oferta — показываем оферту (для Robokassa)
        if args[1].lower() == "oferta":
            await get_or_create_user(user_id)
            await message.answer(
                "📜 <b>Публичная оферта Script AI</b>\n\n"
                "Самозанятый: Панферов Кирилл Алексеевич\n"
                "ИНН: 616810872170\n"
                "📩 kkpanferovvai@gmail.com | 💬 @aass11463",
                parse_mode="HTML",
            )
            try:
                await message.answer_document(
                    FSInputFile("oferta_scriptai.docx"),
                    caption="Полный текст договора-оферты",
                )
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

    balance   = float(user["credits"] or 0)
    is_new    = balance == WELCOME_CREDITS and not user.get("first_topup")

    if is_new:
        welcome = WELCOME_NEW_TEXT + f"\n\n💰 Ваш баланс: <b>{balance:.1f} кредитов</b>"
    else:
        welcome = WELCOME_RETURN_TEXT + f"\n\n💰 Баланс: <b>{balance:.1f} кредитов</b>"

    await message.answer(welcome, reply_markup=get_main_kb(), parse_mode="HTML")


@dp.message(Command("menu"))
@dp.message(F.text == "🏠 Главная")
async def home_cmd(message: types.Message, state: FSMContext):
    await state.clear()
    balance = await get_balance(message.from_user.id)
    await message.answer(
        f"🏠 Главное меню\n💰 Баланс: <b>{balance:.1f} кред.</b>",
        reply_markup=get_main_kb(), parse_mode="HTML",
    )


@dp.message(Command("cancel"))
async def cancel_cmd(message: types.Message, state: FSMContext):
    current = await state.get_state()
    if current is None:
        await message.answer("Нет активных действий для отмены.", reply_markup=get_main_kb())
        return
    await state.clear()
    await message.answer("🛑 Действие отменено.", reply_markup=get_main_kb())


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
        "🏦 СБП (Robokassa) — карты, СБП\n"
        "💎 Крипта — USDT, TON, BTC и др.",
        reply_markup=kb, parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("pkg_"))
async def pkg_selected(call: types.CallbackQuery):
    idx      = int(call.data.split("_")[1])
    price, base, bonus, name = CREDIT_PACKAGES[idx]
    total    = base + bonus
    user_id  = call.from_user.id
    is_test  = robo_is_test(user_id)

    # inv_id уникален: берём последние 6 цифр unix-времени + индекс пакета
    import time as _time
    inv_id   = int(str(int(_time.time()))[-6:]) * 10 + idx

    # Сохраняем маппинг inv_id → user_id + pkg_idx в БД заранее
    async with db_pool.acquire() as _conn:
        await _conn.execute(
            "INSERT INTO payments (inv_id, user_id, amount, pkg_idx, paid, created_at) "
            "VALUES ($1,$2,$3,$4,FALSE,$5) ON CONFLICT DO NOTHING",
            str(inv_id), user_id, float(price), idx, _now(),
        )

    # Ссылка Robokassa (СБП)
    robo_url = robo_make_link(
        amount=float(price),
        inv_id=inv_id,
        desc=f"Script AI — {name} {total} кредитов",
        user_id=user_id,
    )

    # Создаём инвойс CryptoBot (крипта)
    invoice = await cryptobot_create_invoice(
        amount_rub=price,
        user_id=user_id,
        package_idx=idx,
        description=f"Script AI — пакет «{name}», {total} кредитов",
    )

    # Формируем кнопки
    buttons = []
    if robo_url:
        label = "🏦 Оплатить по СБП" + (" (тест)" if is_test else "")
        buttons.append([InlineKeyboardButton(text=label, url=robo_url)])
    if invoice:
        pay_url = invoice.get("bot_invoice_url", "")
        buttons.append([InlineKeyboardButton(text="💎 Оплатить криптой", url=pay_url)])

    if not buttons:
        buttons.append([InlineKeyboardButton(
            text="📩 Написать в поддержку",
            url="https://t.me/aass11463",
        )])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)

    test_notice = "\n⚠️ <i>Тестовый режим — деньги не спишутся</i>" if is_test else ""
    await call.message.answer(
        f"📦 Пакет <b>{name}</b>\n"
        f"💳 Сумма: <b>{price}₽</b> ({rub_to_usd(price)})\n"
        f"🎁 Получите: <b>{total} кредитов</b>\n\n"
        f"Выбери способ оплаты:{test_notice}",
        reply_markup=kb, parse_mode="HTML",
    )
    await call.answer()

# ---------------------------------------------------------------------------
# ТАРИФЫ И ПАКЕТЫ
# ---------------------------------------------------------------------------

@dp.message(Command("help"))
@dp.message(F.text == "❓ FAQ")
async def faq_cmd(message: types.Message):
    await message.answer(FAQ_TEXT, parse_mode="HTML")


@dp.message(Command("oferta"))
async def oferta_cmd(message: types.Message):
    await message.answer(
        "📜 <b>Публичная оферта Script AI</b>\n\n"
        "Самозанятый: Панферов Кирилл Алексеевич\n"
        "ИНН: 616810872170\n"
        "📩 kkpanferovvai@gmail.com | 💬 @aass11463",
        parse_mode="HTML",
    )
    try:
        await message.answer_document(
            FSInputFile("oferta_scriptai.docx"),
            caption="Полный текст договора-оферты",
        )
    except Exception:
        # Если файл не найден — отправляем текстом
        await message.answer(OFERTA_TEXT, parse_mode="HTML")


@dp.message(F.text == "📋 Тарифы")
async def tariffs_cmd(message: types.Message):
    await message.answer(build_tariffs_text(), parse_mode="HTML")


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
            url=f"https://t.me/share/url?url={ref_link}",
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
            f"Продолжаем: <i>{_esc(data['topic'])}</i>. Выберите шаблон:",
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

@dp.callback_query(F.data == "goto_topup")
async def goto_topup(call: types.CallbackQuery):
    await call.answer()
    await topup_cmd(call.message)


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
# МАССОВАЯ ГЕНЕРАЦИЯ
# ---------------------------------------------------------------------------
MAX_BULK_SCRIPTS = 5  # максимум сценариев за один раз

@dp.message(F.text == "🗂 Массовая генерация")
async def bulk_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        f"🗂 <b>Массовая генерация</b>\n\n"
        f"Введи темы сценариев — каждую с новой строки.\n"
        f"Максимум <b>{MAX_BULK_SCRIPTS} тем</b>.\n\n"
        f"<i>Пример:\n"
        f"Как Маск построил Tesla\n"
        f"История создания iPhone\n"
        f"Почему СССР распался</i>",
        reply_markup=types.ReplyKeyboardRemove(), parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_topics)


@dp.message(BulkMaker.waiting_for_topics)
async def bulk_topics(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад в меню":
        return await back_to_main(message, state)

    topics = [t.strip() for t in message.text.splitlines() if t.strip()]
    topics = [t[:4096] for t in topics]  # обрезаем каждую тему до 4096 символов
    if not topics:
        await message.answer("⚠️ Введи хотя бы одну тему.")
        return
    if len(topics) > MAX_BULK_SCRIPTS:
        await message.answer(
            f"⚠️ Максимум {MAX_BULK_SCRIPTS} тем. "
            f"Ты ввёл {len(topics)} — оставлю первые {MAX_BULK_SCRIPTS}."
        )
        topics = topics[:MAX_BULK_SCRIPTS]

    await state.update_data(topics=topics)
    await message.answer(
        f"✅ Принято <b>{len(topics)} тем</b>:\n" +
        "\n".join(f"  {i+1}. {t}" for i, t in enumerate(topics)) +
        "\n\nУкажи длительность каждого сценария (мин):",
        parse_mode="HTML",
    )
    await state.set_state(BulkMaker.waiting_for_duration)


@dp.message(BulkMaker.waiting_for_duration)
async def bulk_duration(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) < 1:
        await message.answer("⚠️ Введи число, например: <b>60</b>", parse_mode="HTML")
        return

    duration = int(message.text)
    if duration > MAX_SCRIPT_DURATION:
        await message.answer(
            f"⚠️ Максимальная длительность — <b>{MAX_SCRIPT_DURATION} минут</b>.\n"
            f"Введи число от 1 до {MAX_SCRIPT_DURATION}:",
            parse_mode="HTML",
        )
        return

    duration   = int(message.text)
    model_id   = await get_user_model(message.from_user.id)
    model_name = MODEL_NAMES.get(model_id, model_id)
    data       = await state.get_data()
    topics     = data["topics"]
    cost_each  = calc_cost(model_id, duration)
    cost_total = cost_each * len(topics)
    balance    = await get_balance(message.from_user.id)

    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)

    cost_line = (
        f"<i>💰 Стоимость: {cost_each:.1f}₽ × {len(topics)} = "
        f"<b>{cost_total:.1f}₽</b> ({rub_to_usd(cost_total)}) | "
        f"Баланс: <b>{balance:.1f}₽</b></i>\n\n"
    )

    if balance < cost_total:
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
    await state.set_state(BulkMaker.waiting_for_template)


@dp.message(BulkMaker.waiting_for_template)
async def bulk_generate(message: types.Message, state: FSMContext):
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
    topics         = data["topics"]
    model_id       = await get_user_model(message.from_user.id)
    friendly_model = MODEL_NAMES.get(model_id, "AI")
    duration       = data["duration"]
    words_target   = data["words_target"]
    user_id        = message.from_user.id

    cost_each  = calc_cost(model_id, duration)
    cost_total = cost_each * len(topics)

    # Финальная проверка баланса
    balance = await get_balance(user_id)
    if balance < cost_total:
        await message.answer(
            f"❌ <b>Недостаточно кредитов</b>\n"
            f"Нужно: <b>{cost_total:.1f}</b> | Баланс: <b>{balance:.1f}</b>",
            reply_markup=get_main_kb(), parse_mode="HTML",
        )
        await state.clear()
        return

    # Списываем всю сумму сразу
    await deduct_credits(
        user_id, cost_total,
        f"🗂 Массовая генерация {len(topics)} сц. ({friendly_model}, {duration} мин)",
    )

    await state.clear()

    if _generation_semaphore is None:
        await message.answer("❌ Бот ещё не готов, попробуй через несколько секунд.")
        return

    await message.answer(
        f"🗂 <b>Запускаю {len(topics)} сценариев</b>\n\n"
        f"Модель: <b>{friendly_model}</b> | Длительность: <b>{duration} мин</b>\n"
        f"Списано: <b>{cost_total:.1f} кред.</b>\n\n"
        f"Сценарии будут приходить по одному по мере готовности ☕",
        reply_markup=get_main_kb(), parse_mode="HTML",
    )

    # Запускаем все сценарии последовательно в фоне
    asyncio.create_task(_run_bulk(message, topics, model_id, friendly_model,
                                  duration, words_target, style_prompt, user_id))


async def _run_bulk(message, topics, model_id, friendly_model,
                    duration, words_target, style_prompt, user_id):
    """Последовательно генерирует все сценарии из списка."""
    global _active_tasks
    total = len(topics)
    done  = 0

    for i, topic in enumerate(topics):
        if _generation_semaphore is None:
            break

        if _active_tasks >= MAX_CONCURRENT_TASKS:
            await message.answer(
                f"⏳ Сценарий {i+1}/{total} в очереди — ждёт свободного слота...",
            )

        await _generation_semaphore.acquire()
        _active_tasks += 1

        try:
            await message.answer(
                f"⏳ <b>Генерирую сценарий {i+1}/{total}</b>: <i>{_esc(topic)}</i>",
                parse_mode="HTML",
            )
            file_name = await _generate_single(
                message, topic, model_id, friendly_model,
                duration, words_target, style_prompt, user_id,
                task_prefix=f"[Bulk {i+1}/{total}]",
            )
            if file_name:
                done += 1
        finally:
            _active_tasks -= 1
            _generation_semaphore.release()

    balance = await get_balance(user_id)
    await message.answer(
        f"✅ <b>Массовая генерация завершена!</b>\n\n"
        f"Готово: <b>{done}/{total}</b> сценариев\n"
        f"Остаток баланса: <b>{balance:.1f} кред.</b>",
        parse_mode="HTML",
    )


# ---------------------------------------------------------------------------
# СОЗДАНИЕ СЦЕНАРИЯ
# ---------------------------------------------------------------------------

@dp.message(F.text == "🎬 Создать сценарий")
async def start_script(message: types.Message, state: FSMContext):
    await message.answer("О чём видео?", reply_markup=types.ReplyKeyboardRemove())
    await state.set_state(ScriptMaker.waiting_for_topic)


@dp.message(ScriptMaker.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    topic = message.text.strip()
    if len(topic) > 4096:
        await message.answer(
            "⚠️ Тема слишком длинная. Сформулируй покороче — до 4096 символов."
        )
        return
    await state.update_data(topic=topic)
    await message.answer("Длительность (мин):")
    await state.set_state(ScriptMaker.waiting_for_duration)


@dp.message(ScriptMaker.waiting_for_duration)
async def process_duration(message: types.Message, state: FSMContext):
    if not message.text.isdigit() or int(message.text) < 1:
        await message.answer("⚠️ Введите число, например: <b>60</b>", parse_mode="HTML")
        return

    duration = int(message.text)
    if duration > MAX_SCRIPT_DURATION:
        await message.answer(
            f"⚠️ Максимальная длительность — <b>{MAX_SCRIPT_DURATION} минут</b>.\n"
            f"Введи число от 1 до {MAX_SCRIPT_DURATION}:",
            parse_mode="HTML",
        )
        return

    duration    = int(message.text)
    model_id    = await get_user_model(message.from_user.id)
    cost        = calc_cost(model_id, duration)
    balance     = await get_balance(message.from_user.id)
    model_name  = MODEL_NAMES.get(model_id, model_id)

    await state.update_data(duration=duration, words_target=duration * WORDS_PER_MINUTE)

    cost_line = (
        f"<i>💰 Стоимость ({model_name}, {duration} мин): "
        f"<b>{cost:.1f}₽</b> ({rub_to_usd(cost)}) | "
        f"Баланс: <b>{balance:.1f}₽</b></i>\n\n"
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

    # Проверка лимита задач на пользователя
    if _user_tasks.get(user_id, 0) >= MAX_TASKS_PER_USER:
        await message.answer(
            f"⚠️ У тебя уже {MAX_TASKS_PER_USER} задачи в работе.\n"
            f"Дождись завершения перед запуском новой.",
            reply_markup=get_main_kb(), parse_mode="HTML",
        )
        await state.clear()
        return

    if _active_tasks >= MAX_CONCURRENT_TASKS:
        await message.answer(
            "⏳ <b>Все слоты заняты.</b> Задача в очереди — начнётся автоматически.",
            parse_mode="HTML",
        )

    await _generation_semaphore.acquire()
    _active_tasks += 1
    _user_tasks[user_id] = _user_tasks.get(user_id, 0) + 1

    try:
        temp_msg = await message.answer("⏳ <i>Подготовка структуры...</i>", parse_mode="HTML")

        # ── ПЛАН ───────────────────────────────────────────────────────────
        target_chapters = max(1, round(words_target / WORDS_PER_CHAPTER))

        plan_prompt = (
            f"Составь план YouTube-видео на тему: «{data['topic']}».\n"
            f"Нужно ровно {target_chapters} пунктов — не больше, не меньше.\n\n"
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
        # CTA только если пользователь упомянул его в своём шаблоне
        _cta_keywords = ("cta", "комментар", "призыв", "подписк", "лайк")
        _user_wants_cta = any(kw in style_prompt.lower() for kw in _cta_keywords)
        cta_positions = compute_cta_positions(n) if _user_wants_cta else set()
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

        def build_chapter_prompt(index, title, prev_text="", covered="", include_cta=False):
            cta_instr = (
                "\n5. CTA: В конце — ненавязчивый призыв написать одно слово в комментариях."
                if include_cta else ""
            )
            prev_block = (
                f"\nПРЕДЫДУЩАЯ ЧАСТЬ (не повторяй):\n{prev_text[-800:]}\n"
                if prev_text else ""
            )
            covered_block = (
                f"\nУЖЕ РАСКРЫТО (нельзя повторять):\n{covered}\n"
                if covered else ""
            )
            return (
                f"Ты пишешь часть сценария для YouTube-видео.\n\n"
                f"ТЕМА: {data['topic']}\n\n"
                f"ПЛАН ({n} частей):\n{full_plan_str}\n\n"
                f"ЗАДАЧА: написать ТОЛЬКО часть №{index+1} — «{title}».\n"
                f"Не повторяй тезисы из других частей."
                f"{prev_block}{covered_block}\n"
                f"ГЛАВНОЕ ТРЕБОВАНИЕ К СТИЛЮ И ПОДАЧЕ (в приоритете):\n{style_prompt}\n\n"
                f"ТЕХНИЧЕСКИЕ ОГРАНИЧЕНИЯ (обязательны):\n"
                f"1. ОБЪЁМ: ровно {words_to_request} слов. Никаких пометок и подсчётов.\n"
                f"2. ФОРМАТ: только сплошной текст. Без заголовков, #, *, ---, списков.\n"
                f"3. НАЧАЛО: сразу с первого слова, без вступлений.\n"
                f"4. КОНЕЦ: завершай мысль естественно."
                f"{cta_instr}"
            )

        async def generate_one(index, title, prev_text="", covered="", is_regen=False):
            if await get_task_status(task_id) == "Cancelled":
                return
            prompt = build_chapter_prompt(
                index, title, prev_text, covered,
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
            await generate_one(i, chapters[i], prev_text=prev, covered=covered_summary)
            done_count += 1
            await safe_edit(
                status_msg,
                build_progress_text(task_id, friendly_model, n, done_count,
                                    max(0, math.ceil((n - done_count) / chunk_size))),
                reply_markup=cancel_kb,
            )

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
                generate_one(i, chapters[i], covered=covered_summary) for i in batch
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
                    generate_one(i, chapters[i], covered=covered_summary, is_regen=True)
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
                [KeyboardButton(text="🏠 Главная")],
            ], resize_keyboard=True),
            parse_mode="HTML",
        )

        # Мягкий призыв к рефералке каждые 3 завершённых сценария
        async with db_pool.acquire() as _conn:
            completed_count = await _conn.fetchval(
                "SELECT COUNT(*) FROM tasks WHERE user_id=$1 AND status='Completed'", user_id
            )
        if completed_count and completed_count % 3 == 0:
            await message.answer(
                "👥 <b>Кстати!</b> За каждого приглашённого друга — "
                "<b>25 кредитов</b> в подарок.\n"
                "Твоя реферальная ссылка в разделе «👥 Реферальная программа» 🎁",
                parse_mode="HTML",
            )

        # Уведомление о низком балансе
        if new_balance < LOW_BALANCE_THRESHOLD:
            await message.answer(
                f"⚠️ <b>Баланс заканчивается</b> — осталось <b>{new_balance:.1f} кред.</b>\n"
                f"Пополни чтобы продолжить генерацию.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="💳 Пополнить", callback_data="goto_topup")]
                ]),
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
        await admin_notify(
            f"🔴 <b>Ошибка генерации</b>\n\n"
            f"🆔 Task: <code>{task_id}</code>\n"
            f"👤 User: <code>{user_id}</code>\n"
            f"🤖 Модель: {friendly_model}\n"
            f"📄 Тема: {_esc(data.get('topic', '?'))}\n"
            f"❗ Ошибка: <code>{_esc(str(e))}</code>"
        )

    finally:
        _active_tasks -= 1
        _user_tasks[user_id] = max(0, _user_tasks.get(user_id, 1) - 1)
        _generation_semaphore.release()
        await state.clear()
        if os.path.exists(file_name):
            os.remove(file_name)

# ---------------------------------------------------------------------------
# ОБЩАЯ ФУНКЦИЯ ГЕНЕРАЦИИ ОДНОГО СЦЕНАРИЯ
# ---------------------------------------------------------------------------

async def _generate_single(
    message, topic, model_id, friendly_model,
    duration, words_target, style_prompt, user_id,
    task_prefix="", deduct=False, cost=0.0,
) -> bool:
    task_id   = f"{random.randint(100,999)} {random.randint(100,999)} {random.randint(100,999)}"
    file_name = f"script_{task_id.replace(' ', '_')}.txt"
    await log_task(task_id, user_id, topic, friendly_model)

    try:
        temp_msg = await message.answer("⏳ <i>Подготовка структуры...</i>", parse_mode="HTML")
        target_chapters = max(1, round(words_target / WORDS_PER_CHAPTER))

        plan_prompt = (
            f"Составь план YouTube-видео на тему: «{topic}».\n"
            f"Нужно ровно {target_chapters} пунктов — не больше, не меньше.\n\n"
            f"ЖЁСТКИЕ ТРЕБОВАНИЯ:\n"
            f"1. Каждый пункт — отдельный самостоятельный аспект (5–10 слов).\n"
            f"2. Биография и контекст эпохи — ТОЛЬКО в 1-м пункте.\n"
            f"3. Каждый пункт отвечает на ДРУГОЙ вопрос (кто/что/почему/как/когда).\n"
            f"4. Запрещены похожие по смыслу пункты.\n"
            f"5. Только нумерованный список без пояснений."
        )
        plan_raw = await api_call_with_retry(model_id, [{"role": "user", "content": plan_prompt}], 2500)
        validated = await api_call_with_retry(model_id, [{"role": "user", "content": (
            f"Вот план ({target_chapters} пунктов):\n\n{plan_raw}\n\n"
            f"Найди пункты, пересекающиеся по смыслу, перепиши их.\n"
            f"Верни ровно {target_chapters} пунктов нумерованным списком. "
            f"Если дублей нет — верни исходный список."
        )}], 2500)
        if validated and validated.strip():
            plan_raw = validated

        chapters = parse_plan_chapters(plan_raw, target_chapters)
        n        = len(chapters)

        words_per_chapter  = words_target // n
        overrequest_factor = VOLUME_OVERREQUEST_FACTORS.get(model_id, VOLUME_OVERREQUEST_FACTOR_DEFAULT)
        words_to_request   = max(50, int(words_per_chapter * overrequest_factor))
        max_tokens_chapter = min(int(words_to_request * 2.4), 2048)
        _cta_keywords   = ("cta", "комментар", "призыв", "подписк", "лайк")
        cta_positions   = compute_cta_positions(n) if any(kw in style_prompt.lower() for kw in _cta_keywords) else set()
        full_plan_str   = "\n".join(f"{i+1}. {t}" for i, t in enumerate(chapters))
        chunk_size      = 5
        total_chunks    = math.ceil(n / chunk_size)

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

        full_script_parts: list[str] = [""] * n
        SEQ_INTRO_CHAPTERS = min(3, n)
        covered_summary    = ""

        def build_p(index, title, prev_text="", covered="", include_cta=False):
            cta_i   = "\n5. CTA: В конце — ненавязчивый призыв написать одно слово." if include_cta else ""
            prev_b  = f"\nПРЕДЫДУЩАЯ ЧАСТЬ (не повторяй):\n{prev_text[-800:]}\n" if prev_text else ""
            cov_b   = f"\nУЖЕ РАСКРЫТО (нельзя повторять):\n{covered}\n" if covered else ""
            return (
                f"Ты пишешь часть сценария для YouTube-видео.\n\nТЕМА: {topic}\n\n"
                f"ПЛАН ({n} частей):\n{full_plan_str}\n\n"
                f"ЗАДАЧА: написать ТОЛЬКО часть №{index+1} — «{title}».\n"
                f"Не повторяй тезисы из других частей.{prev_b}{cov_b}\n"
                f"ГЛАВНОЕ ТРЕБОВАНИЕ К СТИЛЮ И ПОДАЧЕ (в приоритете):\n{style_prompt}\n\n"
                f"ТЕХНИЧЕСКИЕ ОГРАНИЧЕНИЯ (обязательны):\n"
                f"1. ОБЪЁМ: ровно {words_to_request} слов. Никаких пометок.\n"
                f"2. ФОРМАТ: только сплошной текст. Без заголовков, #, *, ---, списков.\n"
                f"3. НАЧАЛО: сразу с первого слова.\n"
                f"4. КОНЕЦ: завершай мысль естественно.{cta_i}"
            )

        async def gen_one(index, title, prev_text="", covered="", is_regen=False):
            if await get_task_status(task_id) == "Cancelled":
                return
            try:
                raw   = await api_call_with_retry(
                    model_id,
                    [{"role": "user", "content": build_p(
                        index, title, prev_text, covered,
                        include_cta=(index in cta_positions) and not is_regen,
                    )}],
                    max_tokens_chapter,
                )
                full_script_parts[index] = clean_chapter_text(raw) + "\n\n"
            except Exception as e:
                logging.error(f"{task_prefix}[{task_id}] ❌ {index+1}: {e}")
                full_script_parts[index] = ""

        async def get_summary(texts):
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

        done_count = 0
        for i in range(SEQ_INTRO_CHAPTERS):
            if await get_task_status(task_id) == "Cancelled":
                return False
            await gen_one(i, chapters[i], prev_text=full_script_parts[i-1] if i > 0 else "", covered=covered_summary)
            done_count += 1
            await safe_edit(status_msg,
                build_progress_text(task_id, friendly_model, n, done_count,
                                    max(0, math.ceil((n - done_count) / chunk_size))),
                reply_markup=cancel_kb)

        covered_summary = await get_summary([full_script_parts[i] for i in range(SEQ_INTRO_CHAPTERS)])

        remaining_indices = list(range(SEQ_INTRO_CHAPTERS, n))
        for batch_start in range(0, len(remaining_indices), chunk_size):
            if await get_task_status(task_id) == "Cancelled":
                return False
            batch = remaining_indices[batch_start:batch_start + chunk_size]
            await asyncio.gather(*[gen_one(i, chapters[i], covered=covered_summary) for i in batch])
            done_count += len(batch)
            await safe_edit(status_msg,
                build_progress_text(task_id, friendly_model, n, done_count,
                                    max(0, math.ceil((n - done_count) / chunk_size))),
                reply_markup=cancel_kb)
            await asyncio.sleep(2)
            if batch_start + chunk_size < len(remaining_indices):
                new_sum = await get_summary([full_script_parts[i] for i in batch])
                if new_sum:
                    covered_summary = (covered_summary + "\n" + new_sum)[-1200:]

        if await get_task_status(task_id) == "Cancelled":
            return False

        short_indices = [i for i, p in enumerate(full_script_parts)
                         if len(p.split()) < words_per_chapter * MIN_CHAPTER_RATIO]
        if short_indices:
            await safe_edit(status_msg, build_progress_text(task_id, friendly_model, n, n, 0, phase="regen"), reply_markup=cancel_kb)
            for _ in range(MAX_REGEN_ATTEMPTS):
                if not short_indices or await get_task_status(task_id) == "Cancelled":
                    break
                await asyncio.gather(*[gen_one(i, chapters[i], covered=covered_summary, is_regen=True) for i in short_indices])
                await asyncio.sleep(2)
                short_indices = [i for i in short_indices if len(full_script_parts[i].split()) < words_per_chapter * MIN_CHAPTER_RATIO]

        if await get_task_status(task_id) == "Cancelled":
            return False

        full_script = re.sub(r'\n{3,}', '\n\n', "".join(full_script_parts).strip())
        word_count  = len(full_script.split())
        with open(file_name, "w", encoding="utf-8") as f:
            f.write(full_script)

        if deduct:
            await deduct_credits(user_id, cost, f"🎬 Сценарий {task_id} ({friendly_model}, {duration} мин)")

        new_balance = await get_balance(user_id)
        prefix_str  = "Массовая: " if task_prefix else ""
        caption = (
            f"📄 {prefix_str}<i>{topic}</i>\n"
            f"📊 ~{word_count} слов (~{word_count // WORDS_PER_MINUTE} мин.)\n"
            f"💰 Баланс: {new_balance:.1f} кред."
        )

        await safe_edit(status_msg, build_progress_text(task_id, friendly_model, n, n, 0, phase="done"), reply_markup=None)
        try:
            await message.answer_document(FSInputFile(file_name), caption=caption)
        except Exception:
            backup = await upload_to_backup(file_name)
            if backup:
                await message.answer(f"⚠️ <a href='{backup}'>Скачать</a>", parse_mode="HTML")

        await update_task_status(task_id, "Completed")
        return True

    except Exception as e:
        logging.error(f"{task_prefix}[{task_id}] Ошибка: {e}")
        try:
            await message.answer(f"❌ Ошибка: «{_esc(topic)}»\n<code>{_esc(str(e))}</code>", parse_mode="HTML")
        except Exception:
            pass
        await update_task_status(task_id, f"Error: {str(e)}")
        await admin_notify(
            f"🔴 <b>Ошибка генерации</b> {task_prefix}\n\n"
            f"🆔 Task: <code>{task_id}</code>\n"
            f"👤 User: <code>{user_id}</code>\n"
            f"🤖 Модель: {friendly_model}\n"
            f"📄 Тема: {_esc(topic)}\n"
            f"❗ Ошибка: <code>{_esc(str(e))}</code>"
        )
        return False

    finally:
        if os.path.exists(file_name):
            os.remove(file_name)


# ---------------------------------------------------------------------------
# ЗАПУСК
# ---------------------------------------------------------------------------
async def main():
    print("Бот запущен!")

    # Веб-сервер для приёма вебхуков от CryptoBot
    if CRYPTOBOT_TOKEN and WEBHOOK_HOST:
        app = aiohttp.web.Application()
        app.router.add_post("/cryptobot/webhook", cryptobot_webhook_handler)
        app.router.add_post("/robokassa/result", robokassa_result_handler)
        app.router.add_get("/robokassa/result", robokassa_result_handler)
        app.router.add_get("/robokassa/pay", robokassa_pay_handler)
        runner = aiohttp.web.AppRunner(app)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, "0.0.0.0", 8080)
        await site.start()
        logging.info("Вебхук-сервер запущен на порту 8080")

        # Регистрируем вебхук в CryptoBot при старте
        webhook_url = f"{WEBHOOK_HOST}/cryptobot/webhook"
        async with aiohttp.ClientSession() as session:
            try:
                await session.get(
                    f"{CRYPTOBOT_API_URL}/setWebhook",
                    headers={"Crypto-Pay-API-Token": CRYPTOBOT_TOKEN},
                    params={"url": webhook_url},
                )
                logging.info(f"CryptoBot вебхук зарегистрирован: {webhook_url}")
            except Exception as e:
                logging.error(f"CryptoBot setWebhook: {e}")

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
