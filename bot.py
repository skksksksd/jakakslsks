import asyncio
import os
import logging
import asyncpg
import random
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

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class SearchStates(StatesGroup):
    waiting_search = State()

async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            virtual_id INT UNIQUE,
            username TEXT,
            reputation_positive INT DEFAULT 0,
            reputation_negative INT DEFAULT 0,
            deposit DECIMAL DEFAULT 0,
            deals_count INT DEFAULT 0,
            deals_sum DECIMAL DEFAULT 0,
            about TEXT DEFAULT '',
            registered_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
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
            "INSERT INTO users (user_id, virtual_id, username) VALUES ($1, $2, $3)",
            user_id, virtual_id, username
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

def format_profile(user, is_own_profile=True):
    user_id = user["user_id"]
    username = user["username"] or str(user_id)
    virtual_id = user["virtual_id"]
    
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

def get_profile_keyboard(is_own_profile=True):
    if is_own_profile:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Кошелек", callback_data="wallet", style="primary")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_menu", style="primary")]
        ])
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡️ Репутация", callback_data="rep_action", style="danger")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_search")]
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
    text = format_profile(user, is_own_profile=True)
    
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
    
    text = format_profile(user, is_own_profile=False)
    await message.answer(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=False))
    await state.clear()

@dp.callback_query(lambda call: call.data == "back_to_search")
async def back_to_search(call: types.CallbackQuery, state: FSMContext):
    text = (
        "<blockquote>🔎 Введите @юзернейм или ID пользователя для поиска.</blockquote>"
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Назад", callback_data="back_to_menu", style="primary")]
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await state.set_state(SearchStates.waiting_search)
    await call.answer()

@dp.callback_query(lambda call: call.data == "rep_action")
async def rep_action(call: types.CallbackQuery):
    await call.answer("⚡️ Репутация — в разработке", show_alert=True)

@dp.callback_query(lambda call: call.data == "autogarant")
async def autogarant(call: types.CallbackQuery):
    await call.answer("🔐 АвтоГарант — в разработке", show_alert=True)

@dp.callback_query(lambda call: call.data == "wallet")
async def wallet(call: types.CallbackQuery):
    await call.answer("💳 Кошелек — в разработке", show_alert=True)

@dp.callback_query(lambda call: call.data == "back_to_menu")
async def back_to_menu(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    text = (
        "<blockquote>🛡 SHIFT | РЕПУТАЦИЯ — система репутации и доверия.\n\n"
        "Окунись в мир безопасности. Проверяйте пользователей и проводите сделки.</blockquote>"
    )
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_main_keyboard())
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("rep_"))
async def reputation_click(call: types.CallbackQuery):
    user_id = int(call.data.split("_")[1])
    conn = await get_conn()
    user = await conn.fetchrow("SELECT username FROM users WHERE user_id = $1", user_id)
    await conn.close()
    username = user["username"] if user else str(user_id)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все", callback_data=f"rep_all_{user_id}")],
        [InlineKeyboardButton(text="Положительные", callback_data=f"rep_positive_{user_id}")],
        [InlineKeyboardButton(text="Отрицательные", callback_data=f"rep_negative_{user_id}")],
        [InlineKeyboardButton(text="Назад", callback_data="profile")]
    ])
    
    await call.message.edit_text(
        f"📄 Какую репутацию @{username} вы хотите посмотреть?",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("rep_all_"))
async def rep_all(call: types.CallbackQuery):
    await call.answer("📋 Все отзывы — в разработке", show_alert=True)

@dp.callback_query(lambda call: call.data.startswith("rep_positive_"))
async def rep_positive(call: types.CallbackQuery):
    await call.answer("👍 Положительные отзывы — в разработке", show_alert=True)

@dp.callback_query(lambda call: call.data.startswith("rep_negative_"))
async def rep_negative(call: types.CallbackQuery):
    await call.answer("👎 Отрицательные отзывы — в разработке", show_alert=True)

async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
