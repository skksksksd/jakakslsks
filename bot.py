import asyncio
import os
import logging
import asyncpg
import random
import aiohttp
import re
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

if not BOT_TOKEN:
    logging.error("BOT_TOKEN не задан!")
    exit(1)
if not CRYPTO_TOKEN:
    logging.error("CRYPTO_TOKEN не задан!")
    exit(1)

# ========== ФУНКЦИИ БД (В НАЧАЛЕ) ==========

db_pool = None

async def init_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    
    async with db_pool.acquire() as conn:
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
        
        # Добавляем недостающие колонки если есть
        for col in ['virtual_id', 'balance', 'deposit', 'deals_count', 'deals_sum', 'about']:
            try:
                await conn.execute(f"ALTER TABLE users ADD COLUMN {col} DECIMAL DEFAULT 0")
            except Exception:
                pass
        
        try:
            await conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
        except Exception:
            pass

async def get_user_by_id(user_id: int):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

async def get_or_create_user(user_id: int, username: str):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        
        if not user:
            virtual_id = random.randint(10000, 99999)
            while await conn.fetchval("SELECT 1 FROM users WHERE virtual_id = $1", virtual_id):
                virtual_id = random.randint(10000, 99999)
            await conn.execute(
                "INSERT INTO users (user_id, virtual_id, username, balance) VALUES ($1, $2, $3, $4)",
                user_id, virtual_id, username or str(user_id), 0
            )
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        
        return user

async def find_user_by_query(query: str):
    query = query.strip()
    async with db_pool.acquire() as conn:
        if query.startswith("@"):
            username = query[1:]
            return await conn.fetchrow("SELECT * FROM users WHERE username ILIKE $1", username)
        elif query.isdigit():
            return await conn.fetchrow("SELECT * FROM users WHERE user_id = $1 OR virtual_id = $1", int(query))
        return None

async def update_balance(user_id: int, amount: float):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)

async def freeze_balance(user_id: int, amount: float):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id = $2", amount, user_id)

# ========== ОСТАЛЬНЫЕ ФУНКЦИИ ==========

def generate_virtual_id():
    return random.randint(10000, 99999)

def generate_deal_id():
    return random.randint(1000, 9999)

async def create_invoice(amount: float, user_id: int):
    url = "https://pay.crypt.bot/api/createInvoice"
    headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
    data = {
        "asset": "USDT",
        "amount": str(amount),
        "description": f"Пополнение баланса пользователя {user_id}"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                result = await resp.json()
                if result.get("ok"):
                    invoice_id = str(result["result"]["invoice_id"])
                    pay_url = result["result"]["pay_url"]
                    
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO invoices (invoice_id, user_id, amount, expires_at) VALUES ($1, $2, $3, NOW() + INTERVAL '5 minutes')",
                            invoice_id, user_id, amount
                        )
                    
                    return pay_url, invoice_id
    except Exception as e:
        logging.error(f"create_invoice error: {e}")
    return None, None

async def get_invoice_status(invoice_id: str):
    url = "https://pay.crypt.bot/api/getInvoices"
    headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
    params = {"invoice_ids": invoice_id}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                result = await resp.json()
                if result.get("ok") and result.get("result", {}).get("items"):
                    return result["result"]["items"][0].get("status")
    except Exception as e:
        logging.error(f"get_invoice_status error: {e}")
    return None

async def create_check(amount: float):
    url = "https://pay.crypt.bot/api/createCheck"
    headers = {"Crypto-Pay-API-Token": CRYPTO_TOKEN}
    data = {
        "asset": "USDT",
        "amount": str(amount),
        "description": "Вывод средств"
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                result = await resp.json()
                if result.get("ok"):
                    return result["result"]["url"]
    except Exception as e:
        logging.error(f"create_check error: {e}")
    return None

async def create_deal(creator_id: int, creator_role: str, amount: float, conditions: str):
    deal_id = generate_deal_id()
    amount_with_fee = amount * 1.06
    amount_to_seller = amount * 0.94
    
    async with db_pool.acquire() as conn:
        while await conn.fetchval("SELECT 1 FROM deals WHERE deal_id = $1", str(deal_id)):
            deal_id = generate_deal_id()
        
        await conn.execute(
            "INSERT INTO deals (deal_id, creator_id, creator_role, amount, amount_with_fee, amount_to_seller, conditions, status, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW() + INTERVAL '5 minutes')",
            str(deal_id), creator_id, creator_role, amount, amount_with_fee, amount_to_seller, conditions, "pending_join"
        )
    return deal_id

async def get_deal(deal_id: str):
    async with db_pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM deals WHERE deal_id = $1", deal_id)

async def update_deal(deal_id: str, buyer_id: int, seller_id: int, status: str):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE deals SET buyer_id = $1, seller_id = $2, status = $3, expires_at = NOW() + INTERVAL '5 minutes' WHERE deal_id = $4 AND status = 'pending_join'",
            buyer_id, seller_id, status, deal_id
        )

async def update_deal_status(deal_id: str, status: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE deals SET status = $1 WHERE deal_id = $2", status, deal_id)

async def unfreeze_balance_to_seller(deal_id: str):
    async with db_pool.acquire() as conn:
        deal = await conn.fetchrow("SELECT seller_id, amount_to_seller FROM deals WHERE deal_id = $1", deal_id)
        if deal:
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", float(deal["amount_to_seller"]), deal["seller_id"])
            await conn.execute("UPDATE deals SET status = 'completed' WHERE deal_id = $1", deal_id)

async def check_pending_invoices():
    while True:
        try:
            await asyncio.sleep(1)
            
            async with db_pool.acquire() as conn:
                pending = await conn.fetch("SELECT * FROM invoices WHERE status = 'pending' AND expires_at > NOW()")
                expired = await conn.fetch("SELECT * FROM invoices WHERE status = 'pending' AND expires_at <= NOW()")
                
                for inv in expired:
                    await bot.send_message(
                        inv["user_id"],
                        f"<blockquote>❌ Счёт на пополнение истёк\n\n• Сумма: {inv['amount']:.2f} USDT</blockquote>",
                        parse_mode="HTML"
                    )
                    await conn.execute("UPDATE invoices SET status = 'expired' WHERE invoice_id = $1", inv["invoice_id"])
                
                for inv in pending:
                    status = await get_invoice_status(inv["invoice_id"])
                    if status == "paid":
                        # CryptoBot уже взял комиссию, зачисляем полную сумму
                        await update_balance(inv["user_id"], float(inv["amount"]))
                        await conn.execute("UPDATE invoices SET status = 'paid' WHERE invoice_id = $1", inv["invoice_id"])
                        
                        await bot.send_message(
                            inv["user_id"],
                            f"<blockquote>✅ Пополнение на {inv['amount']:.2f} USDT успешно зачислено!</blockquote>",
                            parse_mode="HTML"
                        )
        except Exception as e:
            logging.error(f"check_pending_invoices error: {e}")
            await asyncio.sleep(5)

async def check_expired_deals():
    while True:
        try:
            await asyncio.sleep(1)
            
            async with db_pool.acquire() as conn:
                expired_join = await conn.fetch("SELECT * FROM deals WHERE status = 'pending_join' AND expires_at <= NOW()")
                expired_payment = await conn.fetch("SELECT * FROM deals WHERE status = 'pending_payment' AND expires_at <= NOW()")
                
                for deal in expired_join:
                    await conn.execute("UPDATE deals SET status = 'expired' WHERE deal_id = $1", deal["deal_id"])
                    await bot.send_message(
                        deal["creator_id"],
                        f"<blockquote>❌ Сделка #{deal['deal_id']} закрыта\n\n• Партнёр не вступил в течение 5 минут</blockquote>",
                        parse_mode="HTML"
                    )
                
                for deal in expired_payment:
                    await conn.execute("UPDATE deals SET status = 'payment_expired' WHERE deal_id = $1", deal["deal_id"])
                    if deal["buyer_id"]:
                        await bot.send_message(deal["buyer_id"], f"<blockquote>❌ Сделка #{deal['deal_id']} закрыта\n\n• Оплата не поступила в течение 5 минут</blockquote>", parse_mode="HTML")
                    if deal["seller_id"]:
                        await bot.send_message(deal["seller_id"], f"<blockquote>❌ Сделка #{deal['deal_id']} закрыта\n\n• Покупатель не оплатил в течение 5 минут</blockquote>", parse_mode="HTML")
        except Exception as e:
            logging.error(f"check_expired_deals error: {e}")
            await asyncio.sleep(5)

def format_profile(user):
    username = user["username"] or str(user["user_id"])
    virtual_id = user["virtual_id"] if user["virtual_id"] else user["user_id"]
    
    total_reputation = user["reputation_positive"] + user["reputation_negative"]
    positive_percent = (user["reputation_positive"] / total_reputation * 100) if total_reputation > 0 else 0
    negative_percent = (user["reputation_negative"] / total_reputation * 100) if total_reputation > 0 else 0
    
    text = (
        f"👤 @{username} [ ID: {virtual_id} ]\n\n"
        f"<blockquote>• <b>Репутация</b> {total_reputation}\n"
        f"➕ • {positive_percent:.1f}%\n"
        f"➖ • {negative_percent:.1f}%</blockquote>\n"
        f"<blockquote><b>Депозит:</b> 🛟 ${float(user['deposit']):.2f}</blockquote>\n"
        f"<blockquote><b>Сделки:</b> 💰 {user['deals_count']} шт · ${float(user['deals_sum']):.2f}</blockquote>\n"
        f"<blockquote>❗️ <b>ВНИМАНИЕ СМОТРИТЕ ПОЛЕ «О СЕБЕ»</b></blockquote>\n\n"
        f"📅 В системе с {user['registered_at'].strftime('%d.%m.%Y')}\n"
        f"<blockquote><b>✅ АвтоГарант — @SHIFTrepbot</b></blockquote>"
    )
    return text

def get_main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Профиль", callback_data="profile"), InlineKeyboardButton(text="🔍 Поиск", callback_data="search")],
        [InlineKeyboardButton(text="🔐 АвтоГарант", callback_data="autogarant")]
    ])

def get_profile_keyboard(is_own_profile=True, target_user_id=None):
    if is_own_profile:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Кошелек", callback_data="wallet")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
        ])
    else:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡️ Репутация", callback_data=f"rep_action_{target_user_id}")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
        ])

def get_autogarant_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡️ Создать сделку", callback_data="create_deal")],
        [InlineKeyboardButton(text="💳 Кошелек", callback_data="wallet_autogarant"), InlineKeyboardButton(text="🔍 Мои сделки", callback_data="my_deals")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]
    ])

def get_role_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Покупатель", callback_data="role_buyer"), InlineKeyboardButton(text="💼 Продавец", callback_data="role_seller")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant")]
    ])

def get_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Постинг", callback_data="admin_post")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🚪 Выйти", callback_data="admin_exit")]
    ])

# ========== КЛАССЫ СОСТОЯНИЙ ==========

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

class AdminStates(StatesGroup):
    waiting_post = State()

# ========== БОТ И ДИСПЕТЧЕР ==========

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ========== ОБРАБОТЧИКИ ==========

@dp.message(Command("start"))
async def start(message: types.Message, state: FSMContext):
    if message.chat.type != "private":
        return
    
    args = message.text.split()
    
    if len(args) > 1 and args[1].startswith("deal_"):
        deal_id = args[1].split("_")[1]
        await deal_start(message, state, deal_id)
        return
    
    if len(args) > 1 and args[1].startswith("user_"):
        target_user_id = int(args[1].split("_")[1])
        user = await get_user_by_id(target_user_id)
        if user:
            await message.answer(format_profile(user), parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=False, target_user_id=target_user_id))
        else:
            await message.answer("<blockquote>❌ Пользователь не найден</blockquote>", parse_mode="HTML")
        return
    
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    
    await get_or_create_user(user_id, username)
    
    text = "<blockquote>🛡 SHIFT | РЕПУТАЦИЯ\n\nВыберите действие:</blockquote>"
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())

async def deal_start(message: types.Message, state: FSMContext, deal_id: str):
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "pending_join":
        await message.answer("<blockquote>❌ Сделка не найдена или уже завершена</blockquote>", parse_mode="HTML")
        return
    
    user_id = message.from_user.id
    
    if user_id == deal["creator_id"]:
        await message.answer("<blockquote>❌ Вы не можете присоединиться к своей сделке</blockquote>", parse_mode="HTML")
        return
    
    your_role = "Продавец" if deal["creator_role"] == "buyer" else "Покупатель"
    
    text = (
        f"<blockquote>📩 ПРИГЛАШЕНИЕ В СДЕЛКУ #{deal_id}\n\n"
        f"• Ваша роль: {your_role}\n"
        f"• Сумма: {float(deal['amount']):.2f} USDT\n\n"
        f"📝 УСЛОВИЯ:\n{deal['conditions']}\n\n"
        f"Подтвердите участие.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Принять сделку", callback_data=f"accept_deal_{deal_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_deal_{deal_id}")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query(lambda call: call.data.startswith("accept_deal_"))
async def accept_deal(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal or deal["status"] != "pending_join":
        await call.answer("Сделка не найдена или уже принята", show_alert=True)
        return
    
    joiner_id = call.from_user.id
    
    if deal["creator_role"] == "buyer":
        buyer_id = deal["creator_id"]
        seller_id = joiner_id
    else:
        buyer_id = joiner_id
        seller_id = deal["creator_id"]
    
    await update_deal(deal_id, buyer_id, seller_id, "pending_payment")
    
    amount = float(deal["amount"])
    amount_with_fee = float(deal["amount_with_fee"])
    conditions = deal["conditions"]
    
    invoice_url, invoice_id = await create_invoice(amount_with_fee, buyer_id)
    
    if not invoice_url:
        await call.answer("Ошибка создания счёта", show_alert=True)
        return
    
    text = (
        f"<blockquote>💳 ОПЛАТА ПО СДЕЛКЕ #{deal_id}\n\n"
        f"• Сумма: {amount:.2f} USDT\n"
        f"• К оплате: {amount_with_fee:.2f} USDT\n\n"
        f"📝 УСЛОВИЯ:\n{conditions}</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплатить", url=invoice_url)],
        [InlineKeyboardButton(text="❌ Отменить сделку", callback_data=f"cancel_deal_{deal_id}")]
    ])
    
    await bot.send_message(buyer_id, text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer("Вы приняли сделку", show_alert=True)
    await call.message.delete()

@dp.callback_query(lambda call: call.data.startswith("reject_deal_"))
async def reject_deal(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if deal and deal["status"] == "pending_join":
        await update_deal_status(deal_id, "expired")
        await bot.send_message(deal["creator_id"], f"<blockquote>❌ Сделка #{deal_id} отклонена</blockquote>", parse_mode="HTML")
    
    await call.answer("Вы отклонили сделку", show_alert=True)
    await call.message.delete()

@dp.callback_query(lambda call: call.data.startswith("cancel_deal_"))
async def cancel_deal(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if deal and deal["status"] == "pending_payment":
        await update_deal_status(deal_id, "expired")
        if deal["seller_id"]:
            await bot.send_message(deal["seller_id"], f"<blockquote>❌ Сделка #{deal_id} отменена</blockquote>", parse_mode="HTML")
    
    await call.answer("Сделка отменена", show_alert=True)
    await call.message.delete()

@dp.callback_query(lambda call: call.data == "profile")
async def profile(call: types.CallbackQuery):
    user = await get_or_create_user(call.from_user.id, call.from_user.username or str(call.from_user.id))
    await call.message.edit_text(format_profile(user), parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=True))
    await call.answer()

@dp.callback_query(lambda call: call.data == "search")
async def search(call: types.CallbackQuery, state: FSMContext):
    text = "<blockquote>🔎 ПОИСК ПОЛЬЗОВАТЕЛЯ\n\nВведите @юзернейм или ID</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back_to_menu")]])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(SearchStates.waiting_search)
    await call.answer()

@dp.message(SearchStates.waiting_search)
async def process_search(message: types.Message, state: FSMContext):
    user = await find_user_by_query(message.text.strip())
    
    if not user:
        await message.answer("<blockquote>❌ ПОЛЬЗОВАТЕЛЬ НЕ НАЙДЕН</blockquote>", parse_mode="HTML")
        return
    
    await message.answer(format_profile(user), parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=False, target_user_id=user["user_id"]))
    await state.clear()

@dp.callback_query(lambda call: call.data == "wallet")
async def wallet(call: types.CallbackQuery):
    user = await get_user_by_id(call.from_user.id)
    balance = float(user["balance"]) if user else 0
    
    text = f"<blockquote>💸 КОШЕЛЁК\n\nБаланс: {balance:.2f} USDT\n\nВыберите действие:</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить", callback_data="deposit"), InlineKeyboardButton(text="➖ Вывести", callback_data="withdraw")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_profile")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data == "wallet_autogarant")
async def wallet_autogarant(call: types.CallbackQuery):
    await wallet(call)

@dp.callback_query(lambda call: call.data == "deposit")
async def deposit_start(call: types.CallbackQuery, state: FSMContext):
    text = "<blockquote>➕ ПОПОЛНЕНИЕ\n\nВведите сумму в USDT (мин. 1 USDT)</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="wallet")]])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(WalletStates.waiting_deposit_amount)
    await call.answer()

@dp.message(WalletStates.waiting_deposit_amount)
async def deposit_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount < 1:
            raise ValueError
    except ValueError:
        await message.answer("<blockquote>❌ Введите число от 1 USDT</blockquote>", parse_mode="HTML")
        return
    
    invoice_url, invoice_id = await create_invoice(amount, message.from_user.id)
    
    if not invoice_url:
        await message.answer("<blockquote>❌ Ошибка создания счёта</blockquote>", parse_mode="HTML")
        await state.clear()
        return
    
    text = f"<blockquote>💳 СЧЁТ НА {amount:.2f} USDT\n\nДействителен 5 минут</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="💳 Оплатить", url=invoice_url)]])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.clear()

@dp.callback_query(lambda call: call.data == "withdraw")
async def withdraw_start(call: types.CallbackQuery, state: FSMContext):
    user = await get_user_by_id(call.from_user.id)
    balance = float(user["balance"]) if user else 0
    
    if balance < 1:
        await call.answer("❌ Недостаточно средств", show_alert=True)
        return
    
    text = f"<blockquote>➖ ВЫВОД\n\nДоступно: {balance:.2f} USDT\n\nВведите сумму:</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="wallet")]])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(WalletStates.waiting_withdraw_amount)
    await call.answer()

@dp.message(WalletStates.waiting_withdraw_amount)
async def withdraw_amount(message: types.Message, state: FSMContext):
    user = await get_user_by_id(message.from_user.id)
    balance = float(user["balance"]) if user else 0
    
    try:
        amount = float(message.text.strip())
        if amount < 1 or amount > balance:
            raise ValueError
    except ValueError:
        await message.answer(f"<blockquote>❌ Введите сумму от 1 до {balance:.2f} USDT</blockquote>", parse_mode="HTML")
        return
    
    check_url = await create_check(amount)
    
    if not check_url:
        await message.answer("<blockquote>❌ Ошибка создания чека</blockquote>", parse_mode="HTML")
        await state.clear()
        return
    
    await update_balance(message.from_user.id, -amount)
    
    text = f"<blockquote>✅ ЧЕК НА {amount:.2f} USDT\n\n🔗 <a href='{check_url}'>Активировать</a></blockquote>"
    await message.answer(text, parse_mode="HTML")
    await state.clear()

@dp.callback_query(lambda call: call.data == "autogarant")
async def autogarant(call: types.CallbackQuery):
    text = "<blockquote>⚡️ АВТОСДЕЛКИ\n\nБезопасные сделки с гарантией</blockquote>"
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_autogarant_keyboard())
    await call.answer()

@dp.callback_query(lambda call: call.data == "create_deal")
async def create_deal_start(call: types.CallbackQuery, state: FSMContext):
    text = "<blockquote>🛡 КЕМ ВЫСТУПАЕТЕ?</blockquote>"
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_role_keyboard())
    await state.set_state(DealStates.waiting_role)
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("role_"))
async def select_role(call: types.CallbackQuery, state: FSMContext):
    role = call.data.split("_")[1]
    await state.update_data(role=role)
    
    text = "<blockquote>💲 ВВЕДИТЕ СУММУ (мин. 1 USDT)</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant")]])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(DealStates.waiting_amount)
    await call.answer()

@dp.message(DealStates.waiting_amount)
async def deal_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount < 1:
            raise ValueError
    except ValueError:
        await message.answer("<blockquote>❌ Введите число от 1 USDT</blockquote>", parse_mode="HTML")
        return
    
    await state.update_data(amount=amount)
    
    text = "<blockquote>📝 ОПИШИТЕ УСЛОВИЯ СДЕЛКИ</blockquote>"
    await message.answer(text, parse_mode="HTML")
    await state.set_state(DealStates.waiting_conditions)

@dp.message(DealStates.waiting_conditions)
async def deal_conditions(message: types.Message, state: FSMContext):
    conditions = message.text
    data = await state.get_data()
    role = data.get("role")
    amount = data.get("amount")
    
    role_display = "Покупатель" if role == "buyer" else "Продавец"
    
    text = (
        f"<blockquote>🏁 ПРОВЕРЬТЕ\n\n"
        f"Роль: {role_display}\n"
        f"Сумма: {amount:.2f} USDT\n"
        f"Условия: {conditions}\n\n"
        f"Подтвердить?</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm_deal")],
        [InlineKeyboardButton(text="❌ Отменить", callback_data="back_to_autogarant")]
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
    
    deal_id = await create_deal(call.from_user.id, role, amount, conditions)
    
    bot_username = (await bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start=deal_{deal_id}"
    
    text = f"<blockquote>🔗 ССЫЛКА ДЛЯ ПАРТНЁРА:\n<code>{invite_link}</code>\n\nДействительна 5 минут</blockquote>"
    
    await call.message.edit_text(text, parse_mode="HTML")
    await state.clear()
    await call.answer()

@dp.callback_query(lambda call: call.data == "my_deals")
async def my_deals(call: types.CallbackQuery):
    user_id = call.from_user.id
    
    async with db_pool.acquire() as conn:
        deals = await conn.fetch("SELECT * FROM deals WHERE buyer_id = $1 OR seller_id = $1 ORDER BY created_at DESC LIMIT 10", user_id)
    
    if not deals:
        await call.answer("У вас нет сделок", show_alert=True)
        return
    
    text = "<blockquote>📋 ВАШИ СДЕЛКИ</blockquote>"
    keyboard = []
    
    for deal in deals:
        status_display = {
            "pending_join": "⏳ Ожидает",
            "pending_payment": "💳 Ожидает оплаты",
            "paid": "💸 Оплачено",
            "completed": "✅ Завершена",
        }.get(deal["status"], deal["status"])
        
        keyboard.append([InlineKeyboardButton(text=f"Сделка #{deal['deal_id']} — {status_display}", callback_data=f"my_deal_{deal['deal_id']}")])
    
    keyboard.append([InlineKeyboardButton(text="Назад", callback_data="back_to_autogarant")])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard))
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("my_deal_"))
async def my_deal_detail(call: types.CallbackQuery):
    deal_id = call.data.split("_")[2]
    deal = await get_deal(deal_id)
    
    if not deal:
        await call.answer("Сделка не найдена", show_alert=True)
        return
    
    text = (
        f"<blockquote>📋 СДЕЛКА #{deal_id}\n\n"
        f"Сумма: {float(deal['amount']):.2f} USDT\n"
        f"Статус: {deal['status']}\n"
        f"Условия: {deal['conditions']}</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="my_deals")]])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("rep_action_"))
async def rep_action(call: types.CallbackQuery):
    target_user_id = int(call.data.split("_")[2])
    user = await get_user_by_id(target_user_id)
    username = user["username"] if user else str(target_user_id)
    
    text = f"<blockquote>Репутация @{username}</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data=f"back_to_user_profile_{target_user_id}")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("back_to_user_profile_"))
async def back_to_user_profile(call: types.CallbackQuery):
    target_user_id = int(call.data.split("_")[4])
    user = await get_user_by_id(target_user_id)
    if user:
        await call.message.edit_text(format_profile(user), parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=False, target_user_id=target_user_id))
    await call.answer()

@dp.callback_query(lambda call: call.data == "back_to_menu")
async def back_to_menu(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    text = "<blockquote>🛡 SHIFT | РЕПУТАЦИЯ\n\nВыберите действие:</blockquote>"
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_main_keyboard())
    await call.answer()

@dp.callback_query(lambda call: call.data == "back_to_profile")
async def back_to_profile(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await profile(call)

@dp.callback_query(lambda call: call.data == "back_to_autogarant")
async def back_to_autogarant(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await autogarant(call)

@dp.callback_query(lambda call: call.data == "admin_post")
async def admin_post(call: types.CallbackQuery, state: FSMContext):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Доступ запрещен", show_alert=True)
        return
    
    await call.message.edit_text("<b>📢 Введите текст для рассылки:</b>", parse_mode="HTML")
    await state.set_state(AdminStates.waiting_post)
    await call.answer()

@dp.message(AdminStates.waiting_post)
async def admin_send_post(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    await state.clear()
    
    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users")
    
    success = 0
    fail = 0
    
    await message.answer("<b>📢 Начинаю рассылку...</b>", parse_mode="HTML")
    
    for user in users:
        try:
            await bot.send_message(user["user_id"], message.text or "📢 Рассылка", parse_mode="HTML")
            success += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)
    
    await message.answer(f"<b>✅ Рассылка завершена!</b>\n\nДоставлено: {success}\nОшибок: {fail}", parse_mode="HTML")
    await message.answer("<b>🤖 Админ панель</b>", parse_mode="HTML", reply_markup=get_admin_keyboard())

@dp.callback_query(lambda call: call.data == "admin_stats")
async def admin_stats(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Доступ запрещен", show_alert=True)
        return
    
    async with db_pool.acquire() as conn:
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
        total_deals = await conn.fetchval("SELECT COUNT(*) FROM deals WHERE status = 'completed'")
    
    text = f"<b>📊 СТАТИСТИКА</b>\n\n👥 Пользователей: {total_users}\n✅ Сделок: {total_deals}"
    
    await call.message.edit_text(text, parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda call: call.data == "admin_exit")
async def admin_exit(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        await call.answer("Доступ запрещен", show_alert=True)
        return
    
    await call.message.delete()
    await call.answer()

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.delete()
        return
    
    await message.delete()
    await message.answer("<b>🤖 Админ панель</b>", parse_mode="HTML", reply_markup=get_admin_keyboard())

# ========== ЗАПУСК ==========

async def main():
    print("🚀 Запуск бота...")
    
    await init_db_pool()
    print("✅ База данных подключена")
    
    asyncio.create_task(check_pending_invoices())
    asyncio.create_task(check_expired_deals())
    
    print("✅ Бот запущен")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
