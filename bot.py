import asyncio
import os
import logging
import asyncpg
import random
import aiohttp
from datetime import datetime
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

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class SearchStates(StatesGroup):
    waiting_search = State()

class WalletStates(StatesGroup):
    waiting_deposit_amount = State()
    waiting_withdraw_amount = State()

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
            created_at TIMESTAMP DEFAULT NOW()
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
    
    await conn.close()

async def get_conn():
    return await asyncpg.connect(DATABASE_URL)

def generate_virtual_id():
    return random.randint(10000, 99999)

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
                    "INSERT INTO invoices (invoice_id, user_id, amount) VALUES ($1, $2, $3)",
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
            pending = await conn.fetch("SELECT * FROM invoices WHERE status = 'pending'")
            await conn.close()
            
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

@dp.message(Command("start"))
async def start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or str(user_id)
    
    await get_or_create_user(user_id, username)
    
    text = (
        "<blockquote>🛡 SHIFT | РЕПУТАЦИЯ — система репутации и доверия.\n\n"
        "Окунись в мир безопасности. Проверяйте пользователей и проводите сделки.</blockquote>"
    )
    await message.answer(text, parse_mode="HTML", reply_markup=get_main_keyboard())

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
        "<blockquote>🔎 Введите @юзернейм или ID пользователя для поиска.</blockquote>"
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
        await message.answer("<blockquote>❌ Пользователь не найден. Проверьте данные и попробуйте снова.</blockquote>", parse_mode="HTML")
        return
    
    text = format_profile(user)
    is_own = (user["user_id"] == message.from_user.id)
    await message.answer(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=is_own, target_user_id=user["user_id"]))
    await state.clear()

@dp.callback_query(lambda call: call.data == "wallet")
async def wallet(call: types.CallbackQuery):
    user_id = call.from_user.id
    conn = await get_conn()
    balance = await conn.fetchval("SELECT balance FROM users WHERE user_id = $1", user_id)
    await conn.close()
    
    balance = float(balance) if balance else 0
    
    text = (
        f"<blockquote>💸 Кошелёк\n\n"
        f"💲 Баланс: {balance:.2f} USDT\n\n"
        f"➕ Пополнение — от 1 USDT · комиссия 6%\n"
        f"➖ Вывод — от 1 USDT · комиссия 0%</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Пополнить", callback_data="deposit", style="success"), InlineKeyboardButton(text="➖ Вывести", callback_data="withdraw", style="danger")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_profile", style="primary")]
    ])
    
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data == "deposit")
async def deposit_start(call: types.CallbackQuery, state: FSMContext):
    text = (
        "<blockquote>➕ Введите сумму пополнения в USDT\n\n"
        "Минимум: 1 USDT\n"
        "Комиссия: 6% — списывается при зачислении\n\n"
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
            await message.answer("<blockquote>❌ Минимальная сумма пополнения: 1 USDT</blockquote>", parse_mode="HTML")
            return
    except ValueError:
        await message.answer("<blockquote>❌ Введите число. Пример: 5 или 50</blockquote>", parse_mode="HTML")
        return
    
    amount_with_fee = amount * 1.06
    
    invoice_url, invoice_id = await create_invoice(amount_with_fee, user_id)
    
    if not invoice_url:
        await message.answer("<blockquote>❌ Ошибка создания счета. Попробуйте позже.</blockquote>", parse_mode="HTML")
        await state.clear()
        return
    
    text = (
        f"<blockquote>💳 Счёт создан\n\n"
        f"💲 К оплате: {amount_with_fee:.2f} USDT\n"
        f"💸 Будет зачислено: {amount:.2f} USDT\n"
        f"⌛ Счёт действует 5 минут\n\n"
        f"Нажмите кнопку ниже и оплатите через CryptoBot.</blockquote>"
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
        f"<blockquote>➖ Введите сумму вывода в USDT\n\n"
        f"Минимум: 1 USDT\n"
        f"Комиссия: 0%\n\n"
        f"Доступно для вывода: {balance:.2f} USDT</blockquote>"
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
            await message.answer("<blockquote>❌ Минимальная сумма вывода: 1 USDT</blockquote>", parse_mode="HTML")
            return
        if amount > balance:
            await message.answer(f"<blockquote>❌ Недостаточно средств. Доступно: {balance:.2f} USDT</blockquote>", parse_mode="HTML")
            return
    except ValueError:
        await message.answer("<blockquote>❌ Введите число. Пример: 5 или 50</blockquote>", parse_mode="HTML")
        return
    
    check_url = await create_check(amount)
    
    if not check_url:
        await message.answer("<blockquote>❌ Ошибка создания чека. Попробуйте позже.</blockquote>", parse_mode="HTML")
        await state.clear()
        return
    
    conn = await get_conn()
    await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id = $2", amount, user_id)
    await conn.close()
    
    text = (
        f"<blockquote>✅ Чек создан\n\n"
        f"💸 Сумма: {amount:.2f} USDT\n"
        f"🔗 <a href='{check_url}'>Ссылка на чек</a>\n\n"
        f"Комиссия: 0%\n"
        f"Средства списаны с баланса.</blockquote>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="wallet", style="primary")]
    ])
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    await state.clear()

@dp.callback_query(lambda call: call.data == "back_to_profile")
async def back_to_profile(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    user_id = call.from_user.id
    username = call.from_user.username or str(user_id)
    user = await get_or_create_user(user_id, username)
    text = format_profile(user)
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=True))
    await call.answer()

@dp.callback_query(lambda call: call.data == "autogarant")
async def autogarant(call: types.CallbackQuery):
    await call.answer("🔐 АвтоГарант — в разработке", show_alert=True)

@dp.callback_query(lambda call: call.data == "back_to_menu")
async def back_to_menu(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    text = (
        "<blockquote>🛡 SHIFT | РЕПУТАЦИЯ — система репутации и доверия.\n\n"
        "Окунись в мир безопасности. Проверяйте пользователей и проводите сделки.</blockquote>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_main_keyboard())
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("rep_action_"))
async def rep_action(call: types.CallbackQuery, state: FSMContext):
    target_user_id = int(call.data.split("_")[2])
    user = await get_user_by_id(target_user_id)
    username = user["username"] if user else str(target_user_id)
    
    await state.update_data(target_user_id=target_user_id, target_username=username)
    
    text = f"<blockquote>📄 Какую репутацию @{username} вы хотите посмотреть?</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все", callback_data=f"rep_type_all_{target_user_id}", style="primary")],
        [InlineKeyboardButton(text="Положительные", callback_data=f"rep_type_positive_{target_user_id}", style="success"), InlineKeyboardButton(text="Отрицательные", callback_data=f"rep_type_negative_{target_user_id}", style="danger")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data=f"back_to_user_profile_{target_user_id}", style="primary")]
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
        await call.answer("📭 Репутация отсутствует", show_alert=True)
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
        keyboard_buttons.append([InlineKeyboardButton(text=f"📝 Отзыв #{r['id']}", callback_data=f"review_{r['id']}")])
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"rep_page_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(text=f"{page+1}/{((total-1)//limit)+1}", callback_data="ignore"))
    if (page + 1) * limit < total:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"rep_page_{page+1}"))
    
    keyboard_buttons.append(nav_buttons)
    keyboard_buttons.append([InlineKeyboardButton(text="◀️ Вернуться", callback_data=f"rep_action_{target_user_id}", style="primary")])
    
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
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_reviews")]
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
    user = await get_user_by_id(target_user_id)
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
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
