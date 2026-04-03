import asyncio
import os
import logging
import asyncpg
import random
import aiohttp
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

logging.basicConfig(level=logging.INFO)
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/db")
CRYPTO_TOKEN = os.getenv("CRYPTO_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class SearchStates(StatesGroup):
    waiting_search = State()

class WalletStates(StatesGroup):
    waiting_deposit_amount = State()
    waiting_withdraw_amount = State()

class DealStates(StatesGroup):
    waiting_role = State()
    waiting_amount = State()
    waiting_conditions = State()
    waiting_confirm = State()
    waiting_accept = State()

async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            virtual_id INT UNIQUE,
            username TEXT,
            reputation_positive INT DEFAULT 0,
            reputation_negative INT DEFAULT 0,
            balance DECIMAL DEFAULT 0,
            deposit DECIMAL DEFAULT 0,
            deals_count INT DEFAULT 0,
            deals_sum DECIMAL DEFAULT 0,
            about TEXT DEFAULT '',
            registered_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            from_user_id BIGINT,
            to_user_id BIGINT,
            review_type TEXT,
            review_text TEXT,
            photo_id TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id SERIAL PRIMARY KEY,
            invoice_id TEXT UNIQUE,
            user_id BIGINT,
            amount DECIMAL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '5 minutes'
        )
    """)
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id SERIAL PRIMARY KEY,
            deal_id TEXT UNIQUE,
            creator_id BIGINT,
            creator_role TEXT,
            buyer_id BIGINT DEFAULT 0,
            seller_id BIGINT DEFAULT 0,
            amount DECIMAL,
            amount_with_fee DECIMAL DEFAULT 0,
            amount_to_seller DECIMAL DEFAULT 0,
            conditions TEXT,
            status TEXT DEFAULT 'pending_join',
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '5 minutes'
        )
    """)
    
    try:
        await conn.execute("ALTER TABLE users ADD COLUMN virtual_id INT UNIQUE")
    except Exception:
        pass
    
    try:
        await conn.execute("ALTER TABLE users ADD COLUMN balance DECIMAL DEFAULT 0")
    except Exception:
        pass
    
    try:
        await conn.execute("ALTER TABLE deals ADD COLUMN creator_id BIGINT")
    except Exception:
        pass
    
    try:
        await conn.execute("ALTER TABLE deals ADD COLUMN creator_role TEXT")
    except Exception:
        pass
    
    try:
        await conn.execute("ALTER TABLE deals ADD COLUMN expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '5 minutes'")
    except Exception:
        pass
    
    try:
        await conn.execute("ALTER TABLE deals ADD COLUMN amount_with_fee DECIMAL DEFAULT 0")
    except Exception:
        pass
    
    try:
        await conn.execute("ALTER TABLE deals ADD COLUMN amount_to_seller DECIMAL DEFAULT 0")
    except Exception:
        pass
    
    try:
        await conn.execute("ALTER TABLE invoices ADD COLUMN expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '5 minutes'")
    except Exception:
        pass
    
    await conn.close()

async def get_conn():
    return await asyncpg.connect(DATABASE_URL)

def generate_virtual_id():
    return random.randint(10000, 99999)

def generate_deal_id():
    return random.randint(1000, 9999)

async def get_or_create_user(user_id: int, username: str):
    conn = await get_conn()
    
    user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    
    if not user:
        virtual_id = generate_virtual_id()
        while await conn.fetchval("SELECT 1 FROM users WHERE virtual_id = $1", virtual_id):
            virtual_id = generate_virtual_id()
        await conn.execute(
            "INSERT INTO users (user_id, virtual_id, username, balance) VALUES ($1, $2, $3, $4)",
            user_id, virtual_id, username, 0
        )
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
    
    await conn.close()
    return user

async def find_user_by_query(query: str):
    conn = await get_conn()
    query = query.strip()
    
    if query.startswith("@"):
        username = query[1:]
        user = await conn.fetchrow("SELECT * FROM users WHERE username ILIKE $1", username)
    elif query.isdigit():
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1 OR virtual_id = $1", int(query))
    else:
        user = None
    
    await conn.close()
    return user

async def create_invoice(amount: float, user_id: int):
    url = "https://pay.crypt.bot/api/createInvoice"
    headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
    data = {
        "asset": "USDT",
        "amount": str(amount),
        "description": f"Пополнение баланса пользователя {user_id}"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            result = await resp.json()
            if result.get("ok"):
                invoice_id = str(result["result"]["invoice_id"])
                pay_url = result["result"]["pay_url"]
                
                conn = await get_conn()
                await conn.execute(
                    "INSERT INTO invoices (invoice_id, user_id, amount, expires_at) VALUES ($1, $2, $3, NOW() + INTERVAL '5 minutes')",
                    invoice_id, user_id, amount
                )
                await conn.close()
                
                return pay_url, invoice_id
            return None, None

async def get_invoice_status(invoice_id: str):
    url = "https://pay.crypt.bot/api/getInvoices"
    headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
    params = {"invoice_ids": invoice_id}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, params=params) as resp:
            result = await resp.json()
            if result.get("ok") and result.get("result", {}).get("items"):
                return result["result"]["items"][0].get("status")
            return None

async def create_check(amount: float):
    url = "https://pay.crypt.bot/api/createCheck"
    headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
    data = {
        "asset": "USDT",
        "amount": str(amount),
        "description": "Вывод средств"
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            result = await resp.json()
            if result.get("ok"):
                return result["result"]["url"]
            return None

async def update_balance(user_id: int, amount: float):
    conn = await get_conn()
    await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)
    await conn.close()

async def mark_invoice_paid(invoice_id: str):
    conn = await get_conn()
    await conn.execute("UPDATE invoices SET status = 'paid' WHERE invoice_id = $1", invoice_id)
    await conn.close()

async def check_pending_invoices():
    while True:
        try:
            await asyncio.sleep(1)
            
            conn = await get_conn()
            pending = await conn.fetch("SELECT * FROM invoices WHERE status = 'pending' AND expires_at > NOW()")
            expired = await conn.fetch("SELECT * FROM invoices WHERE status = 'pending' AND expires_at <= NOW()")
            await conn.close()
            
            for inv in expired:
                await bot.send_message(
                    inv["user_id"],
                    f"<blockquote>❌ Счёт на пополнение истёк\n\n• Сумма: {inv['amount']:.2f} USDT\n• Создайте новый счёт</blockquote>",
                    parse_mode="HTML"
                )
                await mark_invoice_paid(inv["invoice_id"])
            
            for inv in pending:
                status = await get_invoice_status(inv["invoice_id"])
                if status == "paid":
                    amount_without_fee = inv["amount"] / 1.06
                    await update_balance(inv["user_id"], amount_without_fee)
                    await mark_invoice_paid(inv["invoice_id"])
                    
                    await bot.send_message(
                        inv["user_id"],
                        f"<blockquote>✅ Пополнение на {amount_without_fee:.2f} USDT успешно зачислено!</blockquote>",
                        parse_mode="HTML"
                    )
                    logging.info(f"Invoice {inv['invoice_id']} paid for user {inv['user_id']}")
        except Exception as e:
            logging.error(f"Error in check_pending_invoices: {e}")

async def check_expired_deals():
    while True:
        try:
            await asyncio.sleep(1)
            
            conn = await get_conn()
            expired_join = await conn.fetch("SELECT * FROM deals WHERE status = 'pending_join' AND expires_at <= NOW()")
            expired_payment = await conn.fetch("SELECT * FROM deals WHERE status = 'pending_payment' AND expires_at <= NOW()")
            
            for deal in expired_join:
                await conn.execute("UPDATE deals SET status = 'expired' WHERE deal_id = $1", deal["deal_id"])
                await bot.send_message(
                    deal["creator_id"],
                    f"<blockquote>❌ Сделка #{deal['deal_id']} закрыта\n\n• Партнёр не вступил в течение 5 минут\n• Создайте новую сделку</blockquote>",
                    parse_mode="HTML"
                )
                logging.info(f"Deal {deal['deal_id']} expired (no partner joined)")
            
            for deal in expired_payment:
                await conn.execute("UPDATE deals SET status = 'payment_expired' WHERE deal_id = $1", deal["deal_id"])
                await bot.send_message(
                    deal["buyer_id"],
                    f"<blockquote>❌ Сделка #{deal['deal_id']} закрыта\n\n• Оплата не поступила в течение 5 минут\n• Создайте новую сделку</blockquote>",
                    parse_mode="HTML"
                )
                await bot.send_message(
                    deal["seller_id"],
                    f"<blockquote>❌ Сделка #{deal['deal_id']} закрыта\n\n• Покупатель не оплатил в течение 5 минут\n• Создайте новую сделку</blockquote>",
                    parse_mode="HTML"
                )
                logging.info(f"Deal {deal['deal_id']} expired (payment not received)")
            
            await conn.close()
        except Exception as e:
            logging.error(f"Error in check_expired_deals: {e}")

async def create_deal(creator_id: int, creator_role: str, amount: float, conditions: str):
    conn = await get_conn()
    deal_id = generate_deal_id()
    while await conn.fetchval("SELECT 1 FROM deals WHERE deal_id = $1", str(deal_id)):
        deal_id = generate_deal_id()
    
    amount_with_fee = amount * 1.06
    amount_to_seller = amount * 0.94
    
    await conn.execute(
        "INSERT INTO deals (deal_id, creator_id, creator_role, amount, amount_with_fee, amount_to_seller, conditions, status, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() + INTERVAL '5 minutes')",
        str(deal_id), creator_id, creator_role, amount, amount_with_fee, amount_to_seller, conditions, "pending_join"
    )
    await conn.close()
    return deal_id

async def get_deal(deal_id: str):
    conn = await get_conn()
    deal = await conn.fetchrow("SELECT * FROM deals WHERE deal_id = $1", deal_id)
    await conn.close()
    return deal

async def update_deal(deal_id: str, buyer_id: int, seller_id: int, status: str):
    conn = await get_conn()
    await conn.execute(
        "UPDATE deals SET buyer_id = $1, seller_id = $2, status = $3, expires_at = NOW() + INTERVAL '5 minutes' WHERE deal_id = $4",
        buyer_id, seller_id, status, deal_id
    )
    await conn.close()

async def update_deal_status(deal_id: str, status: str):
    conn = await get_conn()
    await conn.execute("UPDATE deals SET status = $1 WHERE deal_id = $2", status, deal_id)
    await conn.close()

async def freeze_balance(user_id: int, amount: float):
    conn = await get_conn()
    await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id = $2", amount, user_id)
    await conn.close()

async def unfreeze_balance_to_seller(deal_id: str):
    conn = await get_conn()
    deal = await conn.fetchrow("SELECT seller_id, amount_to_seller FROM deals WHERE deal_id = $1", deal_id)
    if deal:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", float(deal["amount_to_seller"]), deal["seller_id"])
        await conn.execute("UPDATE deals SET status = 'completed' WHERE deal_id = $1", deal_id)
    await conn.close()

def format_profile(user):
    user_id = user["user_id"]
    username = user["username"] or str(user_id)
    virtual_id = user["virtual_id"] if user["virtual_id"] else user_id
    
    total_reputation = user["reputation_positive"] + user["reputation_negative"]
    positive_percent = (user["reputation_positive"] / total_reputation * 100) if total_reputation > 0 else 0
    negative_percent = (user["reputation_negative"] / total_reputation * 100) if total_reputation > 0 else 0
    
    registered_date = user["registered_at"].strftime("%d %B %Y года")
    registered_date_ru = registered_date.replace("January", "января").replace("February", "февраля").replace("March", "марта").replace("April", "апреля").replace("May", "мая").replace("June", "июня").replace("July", "июля").replace("August", "августа").replace("September", "сентября").replace("October", "октября").replace("November", "ноября").replace("December", "декабря")
    
    text = (
        f"👤 @{username} [ ID: {virtual_id} ]\n\n"
        f"<blockquote>• <b><a href='callback://rep_{user_id}'>Репутация</a></b> {total_reputation}\n"
        f"➕ • {positive_percent:.1f}%\n"
        f"➖ • {negative_percent:.1f}%</blockquote>\n"
        f"<blockquote><b>Депозит:</b> 🛟 ${float(user['deposit']):.2f} [ ≈ 0 ₽ ]</blockquote>\n"
        f"<blockquote><b>Сделки:</b> 💰 {user['deals_count']} шт · ${float(user['deals_sum']):.2f} [ ≈ 0 ₽ ]</blockquote>\n"
        f"<blockquote>❗️ <b>ВНИМАНИЕ СМОТРИТЕ ПОЛЕ «О СЕБЕ»</b></blockquote>\n\n"
        f"📅 В системе с {registered_date_ru}\n"
        f"<blockquote><b>✅ АвтоГарант — @SHIFTrepbot</b></blockquote>"
    )
    return text

def get_profile_keyboard(is_own_profile=True, target_user_id=None):
    if is_own_profile:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Кошелек", callback_data="wallet", style="primary")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_menu", style="primary")]
        ])
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡️ Репутация", callback_data=f"rep_action_{target_user_id}", style="danger")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_menu", style="primary")]
        ])
    return keyboard

def get_main_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Профиль", callback_data="profile", style="primary"), InlineKeyboardButton(text="🔍 Поиск", callback_data="search", style="primary")],
        [InlineKeyboardButton(text="🔐 АвтоГарант", callback_data="autogarant", style="success")]
    ])
    return keyboard

def get_autogarant_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡️ Создать сделку", callback_data="create_deal", style="success")],
        [InlineKeyboardButton(text="💳 Кошелек", callback_data="wallet_autogarant", style="primary"), InlineKeyboardButton(text="🔍 Мои сделки", callback_data="my_deals", style="primary")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu", style="primary")]
    ])
    return keyboard

def get_role_keyboard():
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Покупатель", callback_data="role_buyer", style="success"), InlineKeyboardButton(text="💼 Продавец", callback_data="role_seller", style="danger")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant", style="primary")]
    ])
    return keyboard

@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    args = message.text.split()
    
    if len(args) > 1 and args[1].startswith("deal_"):
        deal_id = args[1].split("_")[1]
        await deal_start(message, state, deal_id)
        return
    
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    
    await get_or_create_user(user_id, username)
    
    text = (
        "<blockquote>🛡 SHIFT | РЕПУТАЦИЯ — система репутации и доверия.\n\n"
        "• Проверяйте репутацию пользователей\n"
        "• Проводите безопасные сделки\n"
        "• Пользуйтесь гарантом\n\n"
        "Выберите действие:</blockquote>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())

async def deal_start(message: types.Message, state: FSMContext, deal_id: str):
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "pending_join":
        await message.answer("<blockquote>❌ Сделка не найдена или уже завершена\n\n• Проверьте ссылку\n• Сделка могла быть уже принята</blockquote>", parse_mode="HTML")
        return
    
    user_id = message.from_user.id
    
    if user_id == deal["creator_id"]:
        await message.answer("<blockquote>❌ Вы не можете присоединиться к своей сделке\n\n• Отправьте ссылку партнёру</blockquote>", parse_mode="HTML")
        return
    
    await state.update_data(deal_id=deal_id, amount=deal["amount"], amount_with_fee=deal["amount_with_fee"], conditions=deal["conditions"], creator_role=deal["creator_role"])
    
    your_role = "Продавец" if deal["creator_role"] == "buyer" else "Покупатель"
    
    text = (
        f"<blockquote>📩 ПРИГЛАШЕНИЕ В СДЕЛКУ #{deal_id}\n\n"
        f"• Ваша роль: {your_role}\n"
        f"• Сумма: {float(deal['amount']):.2f} USDT\n\n"
        f"📝 УСЛОВИЯ:\n{deal['conditions']}\n\n"
        f"Подтвердите участие, чтобы продолжить.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять сделку", callback_data="accept_deal", style="success")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data="reject_deal", style="danger")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(DealStates.waiting_accept)

@dp.callback_query(lambda call: call.data == "profile")
async def profile(call: types.CallbackQuery):
    user_id = call.from_user.id
    username = call.from_user.username or str(user_id)
    
    user = await get_or_create_user(user_id, username)
    text = format_profile(user)
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=True))
    await call.answer()

@dp.callback_query(lambda call: call.data == "search")
async def search(call: types.CallbackQuery, state: FSMContext):
    text = (
        "<blockquote>🔎 ПОИСК ПОЛЬЗОВАТЕЛЯ\n\n"
        "• Введите @юзернейм или ID\n"
        "• Можно использовать виртуальный ID\n"
        "• Найденный профиль откроется в нашем формате</blockquote>"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu", style="primary")]
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(SearchStates.waiting_search)
    await call.answer()

@dp.message(SearchStates.waiting_search)
async def process_search(message: types.Message, state: FSMContext):
    query = message.text.strip()
    user = await find_user_by_query(query)
    
    if not user:
        await message.answer("<blockquote>❌ ПОЛЬЗОВАТЕЛЬ НЕ НАЙДЕН\n\n• Проверьте правильность ввода\n• Используйте @юзернейм или ID</blockquote>", parse_mode="HTML")
        return
    
    text = format_profile(user)
    await message.answer(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=False, target_user_id=user["user_id"]))
    await state.clear()

@dp.callback_query(lambda call: call.data == "wallet")
async def wallet(call: types.CallbackQuery):
    user_id = call.from_user.id
    conn = await get_conn()
    balance = await conn.fetchval("SELECT balance FROM users WHERE user_id = $1", user_id)
    await conn.close()
    
    balance = float(balance) if balance else 0
    
    text = (
        f"<blockquote>💸 КОШЕЛЁК\n\n"
        f"• Баланс: {balance:.2f} USDT\n\n"
        f"• Пополнение: от 1 USDT, комиссия 6% (3% CryptoBot + 3% сервис)\n"
        f"• Вывод: от 1 USDT, комиссия 0%\n\n"
        f"Выберите действие:</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить", callback_data="deposit", style="success"), InlineKeyboardButton(text="➖ Вывести", callback_data="withdraw", style="danger")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_profile", style="primary")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data == "wallet_autogarant")
async def wallet_autogarant(call: types.CallbackQuery):
    user_id = call.from_user.id
    conn = await get_conn()
    balance = await conn.fetchval("SELECT balance FROM users WHERE user_id = $1", user_id)
    await conn.close()
    
    balance = float(balance) if balance else 0
    
    text = (
        f"<blockquote>💸 КОШЕЛЁК\n\n"
        f"• Баланс: {balance:.2f} USDT\n\n"
        f"• Пополнение: от 1 USDT, комиссия 6% (3% CryptoBot + 3% сервис)\n"
        f"• Вывод: от 1 USDT, комиссия 0%\n\n"
        f"Выберите действие:</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить", callback_data="deposit", style="success"), InlineKeyboardButton(text="➖ Вывести", callback_data="withdraw", style="danger")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant", style="primary")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data == "deposit")
async def deposit_start(call: types.CallbackQuery, state: FSMContext):
    text = (
        "<blockquote>➕ ПОПОЛНЕНИЕ БАЛАНСА\n\n"
        "• Введите сумму в USDT\n"
        "• Минимум: 1 USDT\n"
        "• Комиссия: 6% (3% CryptoBot + 3% сервис)\n\n"
        "Пример: 5 или 50</blockquote>"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="wallet", style="primary")]
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(WalletStates.waiting_deposit_amount)
    await call.answer()

@dp.message(WalletStates.waiting_deposit_amount)
async def deposit_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    try:
        amount = float(message.text.strip())
        if amount < 1:
            await message.answer("<blockquote>❌ ОШИБКА\n\n• Минимальная сумма: 1 USDT\n• Попробуйте снова</blockquote>", parse_mode="HTML")
            return
    except ValueError:
        await message.answer("<blockquote>❌ ОШИБКА\n\n• Введите число\n• Пример: 5 или 50</blockquote>", parse_mode="HTML")
        return
    
    amount_with_fee = amount * 1.06
    
    invoice_url, invoice_id = await create_invoice(amount_with_fee, user_id)
    
    if not invoice_url:
        await message.answer("<blockquote>❌ ОШИБКА СОЗДАНИЯ СЧЁТА\n\n• Попробуйте позже\n• Свяжитесь с администратором</blockquote>", parse_mode="HTML")
        await state.clear()
        return
    
    text = (
        f"<blockquote>💳 СЧЁТ СОЗДАН\n\n"
        f"• К оплате: {amount_with_fee:.2f} USDT\n"
        f"• Зачислится: {amount:.2f} USDT\n"
        f"• Счёт действителен: 5 минут\n\n"
        f"Нажмите кнопку ниже для оплаты через CryptoBot.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить", url=invoice_url)],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="wallet", style="danger")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.clear()

@dp.callback_query(lambda call: call.data == "withdraw")
async def withdraw_start(call: types.CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    conn = await get_conn()
    balance = await conn.fetchval("SELECT balance FROM users WHERE user_id = $1", user_id)
    await conn.close()
    
    balance = float(balance) if balance else 0
    
    if balance < 1:
        await call.answer("❌ Недостаточно средств. Минимальная сумма вывода: 1 USDT", show_alert=True)
        return
    
    text = (
        f"<blockquote>➖ ВЫВОД СРЕДСТВ\n\n"
        f"• Доступно: {balance:.2f} USDT\n"
        f"• Минимум: 1 USDT\n"
        f"• Комиссия: 0%\n\n"
        f"Введите сумму для вывода:</blockquote>"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="wallet", style="primary")]
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(WalletStates.waiting_withdraw_amount)
    await call.answer()

@dp.message(WalletStates.waiting_withdraw_amount)
async def withdraw_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    conn = await get_conn()
    balance = await conn.fetchval("SELECT balance FROM users WHERE user_id = $1", user_id)
    await conn.close()
    balance = float(balance) if balance else 0
    
    try:
        amount = float(message.text.strip())
        if amount < 1:
            await message.answer("<blockquote>❌ ОШИБКА\n\n• Минимальная сумма: 1 USDT</blockquote>", parse_mode="HTML")
            return
        if amount > balance:
            await message.answer(f"<blockquote>❌ ОШИБКА\n\n• Недостаточно средств\n• Доступно: {balance:.2f} USDT</blockquote>", parse_mode="HTML")
            return
    except ValueError:
        await message.answer("<blockquote>❌ ОШИБКА\n\n• Введите число\n• Пример: 5 или 50</blockquote>", parse_mode="HTML")
        return
    
    check_url = await create_check(amount)
    
    if not check_url:
        await message.answer("<blockquote>❌ ОШИБКА СОЗДАНИЯ ЧЕКА\n\n• Попробуйте позже</blockquote>", parse_mode="HTML")
        await state.clear()
        return
    
    conn = await get_conn()
    await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id = $2", amount, user_id)
    await conn.close()
    
    text = (
        f"<blockquote>✅ ЧЕК СОЗДАН\n\n"
        f"• Сумма: {amount:.2f} USDT\n"
        f"• Комиссия: 0%\n"
        f"• Средства списаны с баланса\n\n"
        f"🔗 <a href='{check_url}'>Активировать чек</a></blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="wallet", style="primary")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.clear()

@dp.callback_query(lambda call: call.data == "autogarant")
async def autogarant(call: types.CallbackQuery):
    text = (
        "<blockquote>⚡️ АВТОСДЕЛКИ\n\n"
        "• Безопасные сделки с гарантией\n"
        "• Средства замораживаются на эскроу\n"
        "• Споры решаются через арбитра\n"
        "• Комиссия сервиса: 6% (3% CryptoBot + 3% сервис)\n\n"
        "Выберите действие:</blockquote>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_autogarant_keyboard())
    await call.answer()

@dp.callback_query(lambda call: call.data == "my_deals")
async def my_deals(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await state.update_data(current_page=0)
    await show_deals_page(call, state, 0)

async def show_deals_page(call, state, page):
    user_id = call.from_user.id
    limit = 4
    offset = page * limit
    
    conn = await get_conn()
    deals = await conn.fetch(
        "SELECT * FROM deals WHERE buyer_id = $1 OR seller_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
        user_id, limit, offset
    )
    total = await conn.fetchval(
        "SELECT COUNT(*) FROM deals WHERE buyer_id = $1 OR seller_id = $1",
        user_id
    )
    await conn.close()
    
    if not deals and page == 0:
        await call.answer("У вас нет сделок", show_alert=True)
        return
    
    if not deals and page > 0:
        await call.answer("Это последняя страница", show_alert=True)
        return
    
    text = "<blockquote>📋 ВАШИ СДЕЛКИ\n\n• Нажмите на сделку для просмотра</blockquote>"
    keyboard = []
    for deal in deals:
        status_display = {
            "pending_join": "⏳ Ожидает вступления",
            "expired": "❌ Истекла",
            "pending_payment": "💳 Ожидает оплаты",
            "payment_expired": "❌ Оплата не поступила",
            "paid": "💸 Оплачено, ожидает выполнения",
            "completed": "✅ Завершена",
            "disputed": "⚠️ Спор"
        }.get(deal["status"], deal["status"])
        
        keyboard.append([InlineKeyboardButton(text=f"Сделка #{deal['deal_id']} — {status_display}", callback_data=f"my_deal_{deal['deal_id']}")])
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="Назад", callback_data=f"deals_page_{page-1}"))
    
    total_pages = (total + limit - 1) // limit
    nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="ignore"))
    
    if (page + 1) * limit < total:
        nav_buttons.append(InlineKeyboardButton(text="Вперед", callback_data=f"deals_page_{page+1}"))
    
    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant", style="primary")])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("deals_page_"))
async def deals_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split("_")[2])
    await show_deals_page(call, state, page)

@dp.callback_query(lambda call: call.data.startswith("my_deal_"))
async def my_deal_detail(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    buyer = await get_user_by_id(deal["buyer_id"])
    seller = await get_user_by_id(deal["seller_id"])
    buyer_username = buyer["username"] if buyer else str(deal["buyer_id"])
    seller_username = seller["username"] if seller else str(deal["seller_id"])
    
    status_display = {
        "pending_join": "⏳ Ожидает вступления",
        "expired": "❌ Истекла",
        "pending_payment": "💳 Ожидает оплаты",
        "payment_expired": "❌ Оплата не поступила",
        "paid": "💸 Оплачено, ожидает выполнения",
        "completed": "✅ Завершена",
        "disputed": "⚠️ Спор"
    }.get(deal["status"], deal["status"])
    
    text = (
        f"<blockquote>📋 СДЕЛКА #{deal['deal_id']}\n\n"
        f"• Покупатель: @{buyer_username}\n"
        f"• Продавец: @{seller_username}\n"
        f"• Сумма: {float(deal['amount']):.2f} USDT\n"
        f"• Комиссия: 6% (3% CryptoBot + 3% сервис)\n\n"
        f"📝 УСЛОВИЯ:\n{deal['conditions']}\n\n"
        f"• Создана: {deal['created_at'].strftime('%d.%m.%Y %H:%M')}\n"
        f"• Статус: {status_display}</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="my_deals", style="primary")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data == "create_deal")
async def create_deal_start(call: types.CallbackQuery, state: FSMContext):
    text = (
        "<blockquote>🛡 СОЗДАНИЕ СДЕЛКИ\n\n"
        "Кем вы выступаете?\n\n"
        "🛒 Покупатель — вы платите и ждёте товар или услугу\n"
        "💼 Продавец — вы передаёте товар или услугу и ждёте оплату\n\n"
        "Комиссия сервиса: 6% (3% CryptoBot + 3% сервис)</blockquote>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_role_keyboard())
    await state.set_state(DealStates.waiting_role)
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("role_"))
async def select_role(call: types.CallbackQuery, state: FSMContext):
    role = call.data.split("_")[1]
    await state.update_data(role=role)
    
    text = (
        "<blockquote>💲 ВВЕДИТЕ СУММУ СДЕЛКИ\n\n"
        "• Минимум: 1 USDT\n"
        "• Комиссия сервиса: 6% (3% CryptoBot + 3% сервис)\n"
        "• Средства замораживаются до подтверждения\n\n"
        "Пример: 50 или 12.5</blockquote>"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant", style="primary")]
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(DealStates.waiting_amount)
    await call.answer()

@dp.message(DealStates.waiting_amount)
async def deal_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount < 1:
            await message.answer("<blockquote>❌ ОШИБКА\n\n• Минимальная сумма сделки: 1 USDT</blockquote>", parse_mode="HTML")
            return
    except ValueError:
        await message.answer("<blockquote>❌ ОШИБКА\n\n• Введите число\n• Пример: 50 или 12.5</blockquote>", parse_mode="HTML")
        return
    
    await state.update_data(amount=amount)
    
    text = (
        "<blockquote>📝 ОПИШИТЕ УСЛОВИЯ СДЕЛКИ\n\n"
        "Эти условия увидит ваш партнёр. Пишите чётко — в случае спора арбитраж опирается только на них.\n\n"
        "Рекомендуемый шаблон:\n"
        "• Что передаётся\n"
        "• В какие сроки\n"
        "• Что считается выполнением\n"
        "• Что считается нарушением</blockquote>"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_role", style="primary")]
    ])
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(DealStates.waiting_conditions)

@dp.callback_query(lambda call: call.data == "back_to_role")
async def back_to_role(call: types.CallbackQuery, state: FSMContext):
    text = (
        "<blockquote>🛡 СОЗДАНИЕ СДЕЛКИ\n\n"
        "Кем вы выступаете?\n\n"
        "🛒 Покупатель — вы платите и ждёте товар или услугу\n"
        "💼 Продавец — вы передаёте товар или услугу и ждёте оплату\n\n"
        "Комиссия сервиса: 6% (3% CryptoBot + 3% сервис)</blockquote>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_role_keyboard())
    await state.set_state(DealStates.waiting_role)
    await call.answer()

@dp.message(DealStates.waiting_conditions)
async def deal_conditions(message: types.Message, state: FSMContext):
    conditions = message.text
    
    data = await state.get_data()
    role = data.get("role")
    amount = data.get("amount")
    
    role_display = "Покупатель" if role == "buyer" else "Продавец"
    
    text = (
        f"<blockquote>🏁 ПРОВЕРЬТЕ ДАННЫЕ\n\n"
        f"• Ваша роль: {role_display}\n"
        f"• Сумма сделки: {amount:.2f} USDT\n"
        f"• Комиссия: 6% (3% CryptoBot + 3% сервис)\n\n"
        f"📝 УСЛОВИЯ:\n{conditions}\n\n"
        f"После подтверждения изменить данные нельзя.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_deal", style="success")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_autogarant", style="danger")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.update_data(conditions=conditions)
    await state.set_state(DealStates.waiting_confirm)

@dp.callback_query(lambda call: call.data == "confirm_deal")
async def confirm_deal(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    role = data.get("role")
    amount = data.get("amount")
    conditions = data.get("conditions")
    creator_id = call.from_user.id
    
    deal_id = await create_deal(creator_id, role, amount, conditions)
    
    bot_username = (await bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start=deal_{deal_id}"
    
    text = (
        f"<blockquote>🔗 ПРИГЛАШЕНИЕ В СДЕЛКУ #{deal_id}\n\n"
        f"• Ваша роль: {'Продавец' if role == 'buyer' else 'Покупатель'}\n"
        f"• Сумма: {amount:.2f} USDT\n"
        f"• Комиссия: 6% (3% CryptoBot + 3% сервис)\n\n"
        f"Отправьте эту ссылку партнёру:\n"
        f"<code>{invite_link}</code>\n\n"
        f"Ссылка действительна 5 минут.\n\n"
        f"После перехода партнёр подтвердит участие.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мои сделки", callback_data="my_deals", style="primary")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant", style="primary")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.clear()
    await call.answer()

@dp.callback_query(lambda call: call.data == "accept_deal")
async def accept_deal(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    deal_id = data.get("deal_id")
    amount = data.get("amount")
    amount_with_fee = data.get("amount_with_fee")
    conditions = data.get("conditions")
    creator_role = data.get("creator_role")
    joiner_id = call.from_user.id
    
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "pending_join":
        await call.answer("Сделка не найдена или уже принята", show_alert=True)
        return
    
    if creator_role == "buyer":
        buyer_id = deal["creator_id"]
        seller_id = joiner_id
    else:
        buyer_id = joiner_id
        seller_id = deal["creator_id"]
    
    await update_deal(deal_id, buyer_id, seller_id, "pending_payment")
    
    invoice_url, invoice_id = await create_invoice(amount_with_fee, buyer_id)
    
    conn = await get_conn()
    buyer_balance = await conn.fetchval("SELECT balance FROM users WHERE user_id = $1", buyer_id)
    await conn.close()
    buyer_balance = float(buyer_balance) if buyer_balance else 0
    
    text = (
        f"<blockquote>💳 ОПЛАТА ПО СДЕЛКЕ #{deal_id}\n\n"
        f"• Ваша роль: Покупатель\n"
        f"• Сумма сделки: {amount:.2f} USDT\n"
        f"• К оплате с комиссией 6%: {amount_with_fee:.2f} USDT\n\n"
        f"📝 УСЛОВИЯ:\n{conditions}\n\n"
        f"• Ваш баланс: {buyer_balance:.2f} USDT\n\n"
        f"Счёт действителен 5 минут.\n\n"
        f"После оплаты средства заморозятся до выполнения условий.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить балансом", callback_data=f"pay_balance_{deal_id}", style="success")],
        [InlineKeyboardButton(text="💳 Оплата CryptoBot", url=invoice_url)],
        [InlineKeyboardButton(text="❌ Отменить сделку", callback_data="cancel_deal", style="danger")]
    ])
    
    await bot.send_message(buyer_id, text, parse_mode="HTML", reply_markup=keyboard)
    
    await call.answer("Вы приняли сделку", show_alert=True)
    await call.message.delete()

@dp.callback_query(lambda call: call.data == "reject_deal")
async def reject_deal(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    deal_id = data.get("deal_id")
    
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "pending_join":
        await call.answer("Сделка не найдена или уже принята", show_alert=True)
        return
    
    await update_deal_status(deal_id, "expired")
    
    creator_id = deal["creator_id"]
    
    await bot.send_message(
        creator_id,
        f"<blockquote>❌ Сделка #{deal_id} отклонена партнёром\n\n• Создайте новую сделку</blockquote>",
        parse_mode="HTML"
    )
    
    await call.answer("Вы отклонили сделку", show_alert=True)
    await call.message.delete()
    await state.clear()

@dp.callback_query(lambda call: call.data.startswith("pay_balance_"))
async def pay_balance(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "pending_payment":
        await call.answer("Сделка не найдена или уже оплачена", show_alert=True)
        return
    
    buyer_id = call.from_user.id
    amount = float(deal["amount"])
    amount_to_seller = float(deal["amount_to_seller"])
    
    conn = await get_conn()
    balance = await conn.fetchval("SELECT balance FROM users WHERE user_id = $1", buyer_id)
    await conn.close()
    balance = float(balance) if balance else 0
    
    if balance < amount:
        await call.answer(f"❌ Недостаточно средств. Доступно: {balance:.2f} USDT", show_alert=True)
        return
    
    await freeze_balance(buyer_id, amount)
    await update_deal_status(deal_id, "paid")
    
    seller_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить выполнение", callback_data=f"confirm_complete_{deal_id}", style="success")],
        [InlineKeyboardButton(text="⚠️ Открыть спор", callback_data=f"open_dispute_{deal_id}", style="danger")]
    ])
    
    await bot.send_message(
        deal["seller_id"],
        f"<blockquote>💰 ОПЛАТА ПОЛУЧЕНА #{deal_id}\n\n"
        f"• Сумма сделки: {amount:.2f} USDT\n"
        f"• Продавцу поступит: {amount_to_seller:.2f} USDT (комиссия 6% удержана)\n\n"
        f"Приступайте к выполнению условий.</blockquote>",
        parse_mode="HTML",
        reply_markup=seller_keyboard
    )
    
    await call.message.edit_text(
        f"<blockquote>✅ ОПЛАТА ПОДТВЕРЖДЕНА #{deal_id}\n\n"
        f"• Сумма: {amount:.2f} USDT заморожена\n\n"
        f"Ожидайте выполнения условий от продавца.</blockquote>",
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(lambda call: call.data == "cancel_deal")
async def cancel_deal(call: types.CallbackQuery):
    conn = await get_conn()
    deal = await conn.fetchrow("SELECT * FROM deals WHERE buyer_id = $1 AND status = 'pending_payment'", call.from_user.id)
    
    if deal:
        seller_id = deal["seller_id"]
        deal_id = deal["deal_id"]
        await update_deal_status(deal_id, "expired")
        await bot.send_message(
            seller_id,
            f"<blockquote>❌ Сделка #{deal_id} отменена покупателем</blockquote>",
            parse_mode="HTML"
        )
    await conn.close()
    
    await call.answer("Сделка отменена", show_alert=True)
    await call.message.delete()

@dp.callback_query(lambda call: call.data.startswith("confirm_complete_"))
async def confirm_complete(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "paid":
        await call.answer("Сделка не найдена или уже завершена", show_alert=True)
        return
    
    text = (
        f"<blockquote>📝 ПОДТВЕРДИТЕ ВЫПОЛНЕНИЕ\n\n"
        f"• Сделка #{deal_id}\n"
        f"• Убедитесь, что всё сделано\n"
        f"• После подтверждения покупатель получит доступ к разблокировке</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, всё выполнено", callback_data=f"confirm_done_{deal_id}", style="success")],
        [InlineKeyboardButton(text="❌ Нет, ещё работаю", callback_data="my_deals", style="danger")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("confirm_done_"))
async def confirm_done(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "paid":
        await call.answer("Сделка не найдена или уже завершена", show_alert=True)
        return
    
    buyer_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить получение", callback_data=f"confirm_receive_{deal_id}", style="success")],
        [InlineKeyboardButton(text="❌ Открыть спор", callback_data=f"open_dispute_{deal_id}", style="danger")]
    ])
    
    await bot.send_message(
        deal["buyer_id"],
        f"<blockquote>✅ ПРОДАВЕЦ ПОДТВЕРДИЛ ВЫПОЛНЕНИЕ #{deal_id}\n\n"
        f"Проверьте результат. Если всё хорошо — подтвердите получение.</blockquote>",
        parse_mode="HTML",
        reply_markup=buyer_keyboard
    )
    
    await call.message.edit_text(f"<blockquote>✅ Вы подтвердили выполнение условий сделки #{deal_id}\n\n• Ожидайте подтверждения от покупателя</blockquote>", parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("confirm_receive_"))
async def confirm_receive(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "paid":
        await call.answer("Сделка не найдена или уже завершена", show_alert=True)
        return
    
    await unfreeze_balance_to_seller(deal_id)
    
    amount_to_seller = float(deal["amount_to_seller"])
    
    await bot.send_message(
        deal["seller_id"],
        f"<blockquote>🏁 СДЕЛКА #{deal_id} ЗАВЕРШЕНА\n\n"
        f"• Сумма: {amount_to_seller:.2f} USDT разблокирована и переведена вам\n"
        f"• Комиссия сервиса: {float(deal['amount']) - amount_to_seller:.2f} USDT\n\n"
        f"Статус: ✅ Завершена</blockquote>",
        parse_mode="HTML"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мои сделки", callback_data="my_deals", style="primary")]
    ])
    
    await call.message.edit_text(
        f"<blockquote>🏁 СДЕЛКА #{deal_id} ЗАВЕРШЕНА\n\n"
        f"• Сумма: {float(deal['amount']):.2f} USDT заморожена\n"
        f"• Продавцу переведено: {amount_to_seller:.2f} USDT\n"
        f"• Комиссия сервиса: {float(deal['amount']) - amount_to_seller:.2f} USDT\n\n"
        f"Статус: ✅ Завершена</blockquote>",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("open_dispute_"))
async def open_dispute(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    await update_deal_status(deal_id, "disputed")
    
    buyer = await get_user_by_id(deal["buyer_id"])
    seller = await get_user_by_id(deal["seller_id"])
    buyer_username = buyer["username"] if buyer else str(deal["buyer_id"])
    seller_username = seller["username"] if seller else str(deal["seller_id"])
    
    dispute_text = (
        f"<blockquote>⚠️ СПОР ПО СДЕЛКЕ #{deal_id}\n\n"
        f"• Покупатель: @{buyer_username}\n"
        f"• Продавец: @{seller_username}\n"
        f"• Сумма: {float(deal['amount']):.2f} USDT\n\n"
        f"📝 УСЛОВИЯ:\n{deal['conditions']}\n\n"
        f"👤 Покупатель: <a href='tg://user?id={deal['buyer_id']}'>@{buyer_username}</a>\n"
        f"👤 Продавец: <a href='tg://user?id={deal['seller_id']}'>@{seller_username}</a></blockquote>"
    )
    
    dispute_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Связаться с пользователем", url=f"tg://user?id={call.from_user.id}")]
    ])
    
    if ADMIN_ID:
        await bot.send_message(ADMIN_ID, dispute_text, parse_mode="HTML", reply_markup=dispute_keyboard)
    else:
        logging.error("ADMIN_ID not set")
    
    await call.message.edit_text(
        f"<blockquote>⚠️ СПОР ПО СДЕЛКЕ #{deal_id} ОТКРЫТ\n\n"
        f"• Администратор скоро свяжется с вами\n"
        f"• Деньги остаются замороженными до решения</blockquote>",
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query(lambda call: call.data == "back_to_autogarant")
async def back_to_autogarant(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    text = (
        "<blockquote>⚡️ АВТОСДЕЛКИ\n\n"
        "• Безопасные сделки с гарантией\n"
        "• Средства замораживаются на эскроу\n"
        "• Споры решаются через арбитра\n"
        "• Комиссия сервиса: 6% (3% CryptoBot + 3% сервис)\n\n"
        "Выберите действие:</blockquote>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_autogarant_keyboard())
    await call.answer()

@dp.callback_query(lambda call: call.data == "back_to_menu")
async def back_to_menu(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    text = (
        "<blockquote>🛡 SHIFT | РЕПУТАЦИЯ\n\n"
        "• Проверяйте репутацию пользователей\n"
        "• Проводите безопасные сделки\n"
        "• Пользуйтесь гарантом\n\n"
        "Выберите действие:</blockquote>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_main_keyboard())
    await call.answer()

@dp.callback_query(lambda call: call.data == "back_to_profile")
async def back_to_profile(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = call.from_user.id
    username = call.from_user.username or str(user_id)
    user = await get_or_create_user(user_id, username)
    text = format_profile(user)
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=True))
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("rep_action_"))
async def rep_action(call: types.CallbackQuery, state: FSMContext):
    target_user_id = int(call.data.split("_")[2])
    user = await get_user_by_id(target_user_id)
    username = user["username"] if user else str(target_user_id)
    
    await state.update_data(target_user_id=target_user_id, target_username=username)
    
    text = f"<blockquote>Какую репутацию @{username} вы хотите посмотреть?</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все", callback_data=f"rep_type_all_{target_user_id}", style="primary")],
        [InlineKeyboardButton(text="Положительные", callback_data=f"rep_type_positive_{target_user_id}", style="success"), InlineKeyboardButton(text="Отрицательные", callback_data=f"rep_type_negative_{target_user_id}", style="danger")],
        [InlineKeyboardButton(text="Назад", callback_data=f"back_to_user_profile_{target_user_id}", style="primary")]
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

async def get_user_by_id(user_id: int):
    conn = await get_conn()
    user = await conn.fetchrow("SELECT username FROM users WHERE user_id = $1", user_id)
    await conn.close()
    return user

@dp.callback_query(lambda call: call.data.startswith("rep_type_"))
async def rep_type(call: types.CallbackQuery, state: FSMContext):
    parts = call.data.split("_")
    review_type = parts[2]
    target_user_id = int(parts[3])
    
    if review_type == "all":
        review_type = None
    
    reviews, total = await get_reviews(target_user_id, review_type)
    
    if total == 0:
        await call.answer("Репутация отсутствует", show_alert=True)
        return
    
    user = await get_user_by_id(target_user_id)
    username = user["username"] if user else str(target_user_id)
    
    type_name = "Все" if review_type is None else ("Положительные" if review_type == "positive" else "Отрицательные")
    
    await state.update_data(
        current_page=0, 
        total_reviews=total, 
        current_type=review_type, 
        target_user_id=target_user_id,
        target_username=username
    )
    
    await show_reviews_page(call, state, target_user_id, review_type, 0, username, type_name)

async def get_reviews(to_user_id: int, review_type: str = None, limit: int = 4, offset: int = 0):
    conn = await get_conn()
    if review_type:
        rows = await conn.fetch(
            "SELECT * FROM reviews WHERE to_user_id = $1 AND review_type = $2 ORDER BY created_at DESC LIMIT $3 OFFSET $4",
            to_user_id, review_type, limit, offset
        )
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM reviews WHERE to_user_id = $1 AND review_type = $2",
            to_user_id, review_type
        )
    else:
        rows = await conn.fetch(
            "SELECT * FROM reviews WHERE to_user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
            to_user_id, limit, offset
        )
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM reviews WHERE to_user_id = $1",
            to_user_id
        )
    await conn.close()
    return rows, total

async def show_reviews_page(call, state, target_user_id, review_type, page, username, type_name):
    limit = 4
    offset = page * limit
    reviews, total = await get_reviews(target_user_id, review_type, limit, offset)
    
    text = f"<blockquote>🔥 Отзывы для @{username} — {type_name} ({total})</blockquote>"
    
    keyboard_buttons = []
    for r in reviews:
        keyboard_buttons.append([InlineKeyboardButton(text=f"Отзыв #{r['id']}", callback_data=f"review_{r['id']}")])
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="Назад", callback_data=f"rep_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{((total-1)//limit)+1}", callback_data="ignore"))
    if (page + 1) * limit < total:
        nav_buttons.append(InlineKeyboardButton(text="Вперед", callback_data=f"rep_page_{page+1}"))
    
    keyboard_buttons.append(nav_buttons)
    keyboard_buttons.append([InlineKeyboardButton(text="Вернуться", callback_data=f"rep_action_{target_user_id}", style="primary")])
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("rep_page_"))
async def rep_page(call: types.CallbackQuery, state: FSMContext):
    page = int(call.data.split("_")[2])
    data = await state.get_data()
    target_user_id = data.get("target_user_id")
    review_type = data.get("current_type")
    user = await get_user_by_id(target_user_id)
    username = user["username"] if user else str(target_user_id)
    type_name = "Все" if review_type is None else ("Положительные" if review_type == "positive" else "Отрицательные")
    
    await state.update_data(current_page=page)
    await show_reviews_page(call, state, target_user_id, review_type, page, username, type_name)

@dp.callback_query(lambda call: call.data.startswith("review_"))
async def show_review(call: types.CallbackQuery):
    review_id = int(call.data.split("_")[1])
    conn = await get_conn()
    review = await conn.fetchrow("SELECT * FROM reviews WHERE id = $1", review_id)
    await conn.close()
    
    if not review:
        await call.answer("Отзыв не найден", show_alert=True)
        return
    
    from_user = await get_user_by_id(review["from_user_id"])
    to_user = await get_user_by_id(review["to_user_id"])
    
    from_username = from_user["username"] if from_user else str(review["from_user_id"])
    to_username = to_user["username"] if to_user else str(review["to_user_id"])
    
    review_type_display = "Положительный отзыв" if review["review_type"] == "positive" else "Отрицательный отзыв"
    emoji = "👍" if review["review_type"] == "positive" else "👎"
    
    text = (
        f"<blockquote>{emoji} <b>{review_type_display}</b>\n\n"
        f"<b>📤 Отправитель:</b> @{from_username}\n"
        f"<b>📥 Получатель:</b> @{to_username}\n"
        f"<b>📅 Дата отправки:</b> {review['created_at'].strftime('%d %B %Y года')}\n\n"
        f"{review['review_text']}</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_reviews")]
    ])
    
    if review["photo_id"]:
        await call.message.answer_photo(photo=review["photo_id"], caption=text, parse_mode="HTML", reply_markup=keyboard)
    else:
        await call.message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    
    await call.answer()

@dp.callback_query(lambda call: call.data == "back_to_reviews")
async def back_to_reviews(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    target_user_id = data.get("target_user_id")
    review_type = data.get("current_type")
    page = data.get("current_page", 0)
    user = await get_user_by_id(target_user_id)
    username = user["username"] if user else str(target_user_id)
    type_name = "Все" if review_type is None else ("Положительные" if review_type == "positive" else "Отрицательные")
    
    await show_reviews_page(call, state, target_user_id, review_type, page, username, type_name)

@dp.callback_query(lambda call: call.data.startswith("back_to_user_profile_"))
async def back_to_user_profile(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    target_user_id = int(call.data.split("_")[4])
    conn = await get_conn()
    user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", target_user_id)
    await conn.close()
    if user:
        text = format_profile(user)
        is_own = (user["user_id"] == call.from_user.id)
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=is_own, target_user_id=target_user_id))
    await call.answer()

@dp.callback_query(lambda call: call.data == "ignore")
async def ignore(call: types.CallbackQuery):
    await call.answer()

async def main():
    await init_db()
    
    asyncio.create_task(check_pending_invoices())
    asyncio.create_task(check_expired_deals())
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
