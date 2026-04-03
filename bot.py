import asyncio
import os
import logging
import asyncpg
import random
import re
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

class ReputationStates(StatesGroup):
    waiting_type = State()
    waiting_review_photo = State()

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
    
    try:
        await conn.execute("ALTER TABLE users ADD COLUMN virtual_id INT UNIQUE")
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

async def get_user_by_id(user_id: int):
    conn = await get_conn()
    user = await conn.fetchrow("SELECT username FROM users WHERE user_id = $1", user_id)
    await conn.close()
    return user

async def add_review(from_user_id: int, to_user_id: int, review_type: str, review_text: str, photo_id: str = None):
    conn = await get_conn()
    
    await conn.execute(
        "INSERT INTO reviews (from_user_id, to_user_id, review_type, review_text, photo_id) VALUES ($1, $2, $3, $4, $5)",
        from_user_id, to_user_id, review_type, review_text, photo_id
    )
    
    if review_type == "positive":
        await conn.execute("UPDATE users SET reputation_positive = reputation_positive + 1 WHERE user_id = $1", to_user_id)
    else:
        await conn.execute("UPDATE users SET reputation_negative = reputation_negative + 1 WHERE user_id = $1", to_user_id)
    
    await conn.close()

def parse_review_command(text: str):
    text_lower = text.lower()
    
    patterns = [
        r'(\+реп|\-реп)\s+@?(\w+)\s+(.+)',
        r'@?(\w+)\s+(\+реп|\-реп)\s+(.+)',
        r'(\+реп|\-реп)\s+(\d+)\s+(.+)',
        r'(\d+)\s+(\+реп|\-реп)\s+(.+)',
        r'(\+rep|\-rep)\s+@?(\w+)\s+(.+)',
        r'@?(\w+)\s+(\+rep|\-rep)\s+(.+)',
        r'(\+rep|\-rep)\s+(\d+)\s+(.+)',
        r'(\d+)\s+(\+rep|\-rep)\s+(.+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text_lower, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) == 3:
                if groups[0] in ['+реп', '+rep', '-реп', '-rep']:
                    review_type = 'positive' if groups[0] in ['+реп', '+rep'] else 'negative'
                    target = groups[1]
                    review_text = groups[2]
                else:
                    review_type = 'positive' if groups[1] in ['+реп', '+rep'] else 'negative'
                    target = groups[0]
                    review_text = groups[2]
                
                return {
                    'type': review_type,
                    'target': target,
                    'text': review_text.strip()
                }
    
    return None

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

def get_profile_keyboard(is_own_profile=True):
    if is_own_profile:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Кошелек", callback_data="wallet", style="primary")],
            [InlineKeyboardButton(text="Назад", callback_data="back_to_menu", style="primary")]
        ])
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⚡️ Репутация", callback_data="rep_action", style="danger")],
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
    await message.answer(text, parse_mode="HTML", reply_markup=get_profile_keyboard(is_own_profile=is_own))
    await state.clear()

@dp.callback_query(lambda call: call.data == "rep_action")
async def rep_action(call: types.CallbackQuery, state: FSMContext):
    text = call.message.text
    match = re.search(r'ID:\s*(\d+)', text)
    if not match:
        await call.answer("Ошибка: не удалось определить пользователя", show_alert=True)
        return
    
    user_id = int(match.group(1))
    conn = await get_conn()
    user = await conn.fetchrow("SELECT username FROM users WHERE user_id = $1", user_id)
    await conn.close()
    username = user["username"] if user else str(user_id)
    
    await state.update_data(target_user_id=user_id)
    
    text = f"<blockquote>📄 Какую репутацию @{username} вы хотите посмотреть?</blockquote>"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все", callback_data="rep_type_all", style="primary")],
        [InlineKeyboardButton(text="Положительные", callback_data="rep_type_positive", style="success")],
        [InlineKeyboardButton(text="Отрицательные", callback_data="rep_type_negative", style="danger")],
        [InlineKeyboardButton(text="Назад", callback_data="back_to_profile", style="primary")]
    ])
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await call.answer()

@dp.callback_query(lambda call: call.data.startswith("rep_type_"))
async def rep_type(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    target_user_id = data.get("target_user_id")
    
    review_type = call.data.split("_")[2]
    if review_type == "all":
        review_type = None
    
    reviews, total = await get_reviews(target_user_id, review_type)
    
    if total == 0:
        await call.answer("📭 Репутация отсутствует", show_alert=True)
        return
    
    user = await get_user_by_id(target_user_id)
    username = user["username"] if user else str(target_user_id)
    
    type_name = "Все" if review_type is None else ("Положительные" if review_type == "positive" else "Отрицательные")
    
    await state.update_data(current_page=0, total_reviews=total, current_type=review_type, target_user_id=target_user_id)
    
    await show_reviews_page(call, state, target_user_id, review_type, 0, username, type_name)

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
    keyboard_buttons.append([InlineKeyboardButton(text="◀️ Вернуться", callback_data="rep_action", style="primary")])
    
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

@dp.callback_query(lambda call: call.data == "ignore")
async def ignore(call: types.CallbackQuery):
    await call.answer()

@dp.message()
async def handle_review_command(message: types.Message, state: FSMContext):
    if message.text and ('+реп' in message.text.lower() or '-реп' in message.text.lower() or '+rep' in message.text.lower() or '-rep' in message.text.lower()):
        parsed = parse_review_command(message.text)
        
        if not parsed:
            await message.answer("<blockquote>❌ Неверный формат. Примеры:\n+реп @username текст отзыва\n-реп 123456 текст отзыва</blockquote>", parse_mode="HTML")
            return
        
        target = parsed['target']
        review_type = parsed['type']
        review_text = parsed['text']
        
        from_user_id = message.from_user.id
        
        if target.isdigit():
            target_user_id = int(target)
            target_user = await get_user_by_id(target_user_id)
            target_username = target_user["username"] if target_user else str(target_user_id)
        else:
            target_user = await find_user_by_query(f"@{target}")
            if not target_user:
                await message.answer("<blockquote>❌ Пользователь не найден</blockquote>", parse_mode="HTML")
                return
            target_user_id = target_user["user_id"]
            target_username = target_user["username"]
        
        if from_user_id == target_user_id:
            await message.answer("<blockquote>❌ Нельзя оставить отзыв самому себе</blockquote>", parse_mode="HTML")
            return
        
        await state.update_data(
            review_type=review_type,
            review_text=review_text,
            target_user_id=target_user_id,
            target_username=target_username
        )
        
        await message.answer("<blockquote>📸 Отправьте фото к отзыву</blockquote>", parse_mode="HTML")
        await state.set_state(ReputationStates.waiting_review_photo)

@dp.message(ReputationStates.waiting_review_photo)
async def handle_review_photo(message: types.Message, state: FSMContext):
    if not message.photo:
        await message.answer("<blockquote>❌ Вы должны отправить фото</blockquote>", parse_mode="HTML")
        return
    
    photo_id = message.photo[-1].file_id
    data = await state.get_data()
    
    review_type = data.get('review_type')
    review_text = data.get('review_text')
    target_user_id = data.get('target_user_id')
    target_username = data.get('target_username')
    from_user_id = message.from_user.id
    
    await add_review(from_user_id, target_user_id, review_type, review_text, photo_id)
    
    await message.answer(f"<blockquote>✅ Отзыв для @{target_username} оставлен!</blockquote>", parse_mode="HTML")
    await state.clear()

async def main():
    await init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
