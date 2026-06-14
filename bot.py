#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎬 KINO BOT - PostgreSQL versiya (Railway uchun)
Versiya: 4.4 - Video + Caption birga
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

import asyncpg
from aiogram import Bot, Dispatcher, F, Router, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest
from dotenv import load_dotenv

load_dotenv()

# ==================== KONFIGURATSIYA ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "BOT_TOKENINGIZNI_BU_YERGA_YOZING")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "123456789").split(",") if x.strip()]
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/kino_bot")
DATABASE_URL_SSL = os.getenv("DATABASE_URL", DATABASE_URL)
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "@dwadaaadfdgdth")
CHANNEL_INVITE_LINK = os.getenv("CHANNEL_INVITE_LINK", "https://t.me/dwadaaadfdgdth")
RATE_LIMIT_SECONDS = int(os.getenv("RATE_LIMIT_SECONDS", "2"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ==================== DATABASE ====================
class Database:
    def __init__(self, db_url: str = DATABASE_URL_SSL):
        self.db_url = db_url
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(
                self.db_url, min_size=3, max_size=15, command_timeout=60
            )
            await self.init_db()
            logger.info("✅ PostgreSQL bazaga ulandi!")
        except Exception as e:
            logger.error(f"❌ PostgreSQL ulanish xatosi: {e}")
            raise

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def init_db(self):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS movies (
                    id SERIAL PRIMARY KEY, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
                    year INTEGER DEFAULT 0, genre TEXT DEFAULT 'Noma''lum',
                    description TEXT DEFAULT '', file_id TEXT NOT NULL,
                    poster_id TEXT, views INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, added_by BIGINT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS favorites (
                    user_id BIGINT, movie_id INTEGER,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_id, movie_id),
                    FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS requests (
                    id SERIAL PRIMARY KEY, user_id BIGINT, username TEXT,
                    movie_name TEXT, status TEXT DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    user_id BIGINT PRIMARY KEY, added_by BIGINT,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id SERIAL PRIMARY KEY, admin_id BIGINT, action TEXT,
                    movie_code TEXT, details TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    id SERIAL PRIMARY KEY, channel_username TEXT UNIQUE,
                    invite_link TEXT, required INTEGER DEFAULT 1
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS subscription_links (
                    id SERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            for admin_id in ADMIN_IDS:
                await conn.execute("""
                    INSERT INTO admins (user_id, added_by) VALUES ($1, $2)
                    ON CONFLICT (user_id) DO NOTHING
                """, admin_id, admin_id)
            await conn.execute("""
                INSERT INTO channels (channel_username, invite_link, required)
                VALUES ($1, $2, 1)
                ON CONFLICT (channel_username) DO UPDATE SET
                    invite_link = EXCLUDED.invite_link, required = EXCLUDED.required
            """, CHANNEL_USERNAME, CHANNEL_INVITE_LINK)

    async def add_user(self, user_id: int, username: str, first_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (user_id, username, first_name) VALUES ($1, $2, $3)
                ON CONFLICT (user_id) DO UPDATE SET
                    username = EXCLUDED.username, first_name = EXCLUDED.first_name,
                    last_active = CURRENT_TIMESTAMP
            """, user_id, username, first_name)

    async def get_user_count(self) -> int:
        async with self.pool.acquire() as conn:
            return (await conn.fetchrow("SELECT COUNT(*) as cnt FROM users"))['cnt']

    async def get_today_users(self) -> int:
        async with self.pool.acquire() as conn:
            return (await conn.fetchrow("""
                SELECT COUNT(*) as cnt FROM users WHERE DATE(last_active) = CURRENT_DATE
            """))['cnt']

    async def get_all_users(self) -> List[int]:
        async with self.pool.acquire() as conn:
            return [r['user_id'] for r in await conn.fetch("SELECT user_id FROM users")]

    async def add_movie(self, code: str, name: str, year: int, genre: str,
                        description: str, file_id: str, poster_id: Optional[str],
                        added_by: int) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO movies (code, name, year, genre, description, file_id, poster_id, added_by)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """, code, name, year, genre, description, file_id, poster_id, added_by)
                return True
        except asyncpg.UniqueViolationError:
            return False
        except Exception as e:
            logger.error(f"add_movie xato: {e}")
            return False

    async def get_movie_by_code(self, code: str) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM movies WHERE code = $1", code)
            return dict(row) if row else None

    async def get_movie_by_id(self, movie_id: int) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM movies WHERE id = $1", movie_id)
            return dict(row) if row else None

    async def increment_views(self, movie_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE movies SET views = views + 1 WHERE id = $1", movie_id)

    async def get_latest_movies(self, limit: int = 10) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch(
                "SELECT * FROM movies ORDER BY created_at DESC LIMIT $1", limit)]

    async def get_popular_movies(self, limit: int = 10) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch(
                "SELECT * FROM movies ORDER BY views DESC LIMIT $1", limit)]

    async def get_random_movie(self) -> Optional[Dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM movies ORDER BY RANDOM() LIMIT 1")
            return dict(row) if row else None

    async def get_movie_count(self) -> int:
        async with self.pool.acquire() as conn:
            return (await conn.fetchrow("SELECT COUNT(*) as cnt FROM movies"))['cnt']

    async def search_movies(self, query: str, limit: int = 5) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("""
                SELECT * FROM movies WHERE code ILIKE $1 OR name ILIKE $2
                ORDER BY views DESC LIMIT $3
            """, f"%{query}%", f"%{query}%", limit)]

    async def update_movie(self, movie_id: int, field: str, value: Any) -> bool:
        allowed_fields = ['code', 'name', 'year', 'genre', 'description', 'file_id', 'poster_id']
        if field not in allowed_fields:
            return False
        async with self.pool.acquire() as conn:
            await conn.execute(f"UPDATE movies SET {field} = $1 WHERE id = $2", value, movie_id)
            return True

    async def delete_movie(self, movie_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM favorites WHERE movie_id = $1", movie_id)
            await conn.execute("DELETE FROM movies WHERE id = $1", movie_id)

    async def get_all_movies(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("SELECT * FROM movies ORDER BY id")]

    async def add_favorite(self, user_id: int, movie_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO favorites (user_id, movie_id) VALUES ($1, $2) ON CONFLICT DO NOTHING
            """, user_id, movie_id)

    async def remove_favorite(self, user_id: int, movie_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM favorites WHERE user_id = $1 AND movie_id = $2", user_id, movie_id)

    async def is_favorite(self, user_id: int, movie_id: int) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("""
                SELECT 1 FROM favorites WHERE user_id = $1 AND movie_id = $2
            """, user_id, movie_id) is not None

    async def get_favorites(self, user_id: int) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("""
                SELECT m.* FROM movies m JOIN favorites f ON m.id = f.movie_id
                WHERE f.user_id = $1 ORDER BY f.added_at DESC
            """, user_id)]

    async def add_request(self, user_id: int, username: str, movie_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO requests (user_id, username, movie_name) VALUES ($1, $2, $3)
            """, user_id, username, movie_name)

    async def get_requests(self, status: str = 'pending') -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("""
                SELECT * FROM requests WHERE status = $1 ORDER BY created_at DESC
            """, status)]

    async def update_request_status(self, request_id: int, status: str):
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE requests SET status = $1 WHERE id = $2", status, request_id)

    async def is_admin(self, user_id: int) -> bool:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow("SELECT 1 FROM admins WHERE user_id = $1", user_id) is not None

    async def add_admin(self, user_id: int, added_by: int):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO admins (user_id, added_by) VALUES ($1, $2) ON CONFLICT DO NOTHING
            """, user_id, added_by)

    async def remove_admin(self, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)

    async def get_admins(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("SELECT * FROM admins")]

    async def add_log(self, admin_id: int, action: str, movie_code: str = "", details: str = ""):
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO admin_logs (admin_id, action, movie_code, details) VALUES ($1, $2, $3, $4)
            """, admin_id, action, movie_code, details)

    async def get_logs(self, limit: int = 50) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("""
                SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT $1
            """, limit)]

    async def get_required_channels(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("SELECT * FROM channels WHERE required = 1")]

    async def get_all_channels(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("SELECT * FROM channels ORDER BY id")]

    async def add_channel(self, username: str, invite_link: str) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO channels (channel_username, invite_link, required)
                    VALUES ($1, $2, 1)
                    ON CONFLICT (channel_username) DO UPDATE SET
                        invite_link = EXCLUDED.invite_link, required = 1
                """, username, invite_link)
                return True
        except Exception as e:
            logger.error(f"add_channel xato: {e}")
            return False

    async def delete_channel(self, channel_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM channels WHERE id = $1", channel_id)

    # --- Subscription links (Instagram, YouTube va boshqalar) ---
    async def get_subscription_links(self) -> List[Dict]:
        async with self.pool.acquire() as conn:
            return [dict(r) for r in await conn.fetch("SELECT * FROM subscription_links ORDER BY id")]

    async def add_subscription_link(self, title: str, url: str) -> bool:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO subscription_links (title, url) VALUES ($1, $2)
                """, title, url)
                return True
        except Exception as e:
            logger.error(f"add_subscription_link xato: {e}")
            return False

    async def delete_subscription_link(self, link_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM subscription_links WHERE id = $1", link_id)


db = Database()


# ==================== FSM HOLATLAR ====================
class AdminStates(StatesGroup):
    menu = State()
    add_code = State()
    add_name = State()
    add_description = State()
    add_video = State()
    edit_select = State()
    edit_field = State()
    edit_value = State()
    delete_confirm = State()
    broadcast = State()
    broadcast_caption = State()
    broadcast_select_targets = State()
    add_admin = State()
    remove_admin = State()
    # Serial (ko'p qismli) qo'shish
    series_count = State()
    series_codes = State()
    series_name = State()
    series_descriptions = State()
    series_videos = State()
    # Obuna sozlamalari
    add_channel_username = State()
    add_channel_link = State()
    add_sub_link_title = State()
    add_sub_link_url = State()


class UserStates(StatesGroup):
    request_movie = State()


# ==================== KLAVIATURALAR ====================
def main_menu_kb(is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🎲 Tasodifiy kino", callback_data="random"),
         InlineKeyboardButton(text="❤️ Sevimlilar", callback_data="favorites")],
        [InlineKeyboardButton(text="📝 Kino so'rash", callback_data="request"),
         InlineKeyboardButton(text="📢 Kanalga o'tish", url=CHANNEL_INVITE_LINK)],
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(text="👨‍💻 Admin panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Kino qo'shish", callback_data="admin_add"),
         InlineKeyboardButton(text="✏️ Kino tahrirlash", callback_data="admin_edit")],
        [InlineKeyboardButton(text="🎞 Serial qo'shish", callback_data="admin_add_series")],
        [InlineKeyboardButton(text="📃 Kinolar ro'yxati", callback_data="admin_movielist_0")],
        [InlineKeyboardButton(text="❌ Kino o'chirish", callback_data="admin_delete"),
         InlineKeyboardButton(text="📊 Statistika", callback_data="admin_stats")],
        [InlineKeyboardButton(text="📨 Reklama yuborish", callback_data="admin_broadcast"),
         InlineKeyboardButton(text="🎬 So'rovlar", callback_data="admin_requests")],
        [InlineKeyboardButton(text="👮 Adminlar", callback_data="admin_admins"),
         InlineKeyboardButton(text="📋 Loglar", callback_data="admin_logs")],
        [InlineKeyboardButton(text="📢 Obuna sozlamalari", callback_data="admin_subscriptions")],
        [InlineKeyboardButton(text="🔙 Asosiy menyu", callback_data="main_menu")]
    ])


def movie_action_kb(movie_id: int, is_fav: bool = False, invite_link: str = None) -> InlineKeyboardMarkup:
    fav_text = "💔 O'chirish" if is_fav else "❤️ Saqlash"
    buttons = [
        [InlineKeyboardButton(text=fav_text, callback_data=f"fav_{movie_id}"),
         InlineKeyboardButton(text="🎲 Boshqa tasodifiy", callback_data="random")],
        [InlineKeyboardButton(text="👥 Odam qo'shish", url=invite_link or CHANNEL_INVITE_LINK)],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def confirm_delete_kb(movie_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Ha, o'chirish", callback_data=f"del_confirm_{movie_id}"),
         InlineKeyboardButton(text="❌ Bekor qilish", callback_data="admin_delete")]
    ])


def edit_field_kb(movie_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔢 Kod", callback_data=f"edit_code_{movie_id}"),
         InlineKeyboardButton(text="🎬 Nomi", callback_data=f"edit_name_{movie_id}")],
        [InlineKeyboardButton(text="📝 Tavsif", callback_data=f"edit_desc_{movie_id}"),
         InlineKeyboardButton(text="🎥 Video", callback_data=f"edit_video_{movie_id}")],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_edit")]
    ])


def back_admin_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Admin menyuga", callback_data="admin_panel")]
    ])


# ==================== MIDDLEWARE ====================
class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit: int = RATE_LIMIT_SECONDS):
        self.limit = limit
        self.users = {}
        super().__init__()

    async def __call__(self, handler, event, data):
        user_id = None
        if isinstance(event, Message):
            user_id = event.from_user.id
        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id

        if user_id:
            now = datetime.now()
            if user_id in self.users:
                diff = (now - self.users[user_id]).total_seconds()
                if diff < self.limit:
                    if isinstance(event, Message):
                        await event.answer("⏱ <b>Tezlik cheklovi!</b> Iltimos, biroz kuting...",
                                           parse_mode=ParseMode.HTML)
                    return None
            self.users[user_id] = now
        return await handler(event, data)


class SubscriptionMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        bot: Bot = data.get("bot")
        user_id = None

        if isinstance(event, Message):
            user_id = event.from_user.id
            text = event.text or ""
            if text.startswith("/admin") or await db.is_admin(user_id):
                pass
            else:
                if not await self.check_subscription(bot, user_id):
                    await self.send_subscribe_msg(event, bot)
                    return None

        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id
            if not await db.is_admin(user_id):
                if not await self.check_subscription(bot, user_id):
                    await event.answer("❌ Avval kanalga obuna bo'ling!", show_alert=True)
                    return None

        return await handler(event, data)

    async def check_subscription(self, bot: Bot, user_id: int) -> bool:
        channels = await db.get_required_channels()
        if not channels:
            return True
        for ch in channels:
            try:
                member = await bot.get_chat_member(ch['channel_username'], user_id)
                if member.status in ['left', 'kicked']:
                    return False
            except:
                return False
        return True

    async def send_subscribe_msg(self, event: Message, bot: Bot):
        channels = await db.get_required_channels()
        extra_links = await db.get_subscription_links()
        if not channels and not extra_links:
            return

        buttons = []
        text = "🔒 <b>Botdan foydalanish uchun quyidagilarga obuna bo'ling!</b>\n\n"

        for ch in channels:
            link = ch['invite_link'] or f"https://t.me/{ch['channel_username'].replace('@', '')}"
            buttons.append([InlineKeyboardButton(text=f"📢 {ch['channel_username']}", url=link)])
            text += f"• {ch['channel_username']}\n"

        for lnk in extra_links:
            buttons.append([InlineKeyboardButton(text=f"🔗 {lnk['title']}", url=lnk['url'])])
            text += f"• {lnk['title']}\n"

        text += "\nObuna bo'lgach, <b>✅ Obunani tekshirish</b> tugmasini bosing."
        buttons.append([InlineKeyboardButton(text="✅ Obunani tekshirish", callback_data="check_sub")])

        kb = InlineKeyboardMarkup(inline_keyboard=buttons)
        await event.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)


# ==================== YORDAMCHI FUNKSIYALAR ====================
async def send_movie(bot: Bot, chat_id: int, movie: Dict, user_id: int):
    """Kino ma'lumotlarini chiroyli ko'rinishda yuborish
    
    USLUB: Video fayl + caption (tavsif matni + knopkalar) BIR xabarda
    """
    await db.increment_views(movie['id'])
    is_fav = await db.is_favorite(user_id, movie['id'])
    # Kanal invite linkini olish
    channels = await db.get_required_channels()
    invite_link = channels[0]['invite_link'] if channels else CHANNEL_INVITE_LINK
    kb = movie_action_kb(movie['id'], is_fav, invite_link)

    # Tavsif matni - video caption sifatida
    caption = (
        f"🎬 <b>{movie['name']}</b>\n\n"
        f"🔢 <b>Kod:</b> <code>{movie['code']}</code>\n"
        f"👁 <b>Ko'rishlar:</b> {movie['views'] + 1}\n\n"
        f"📝 <b>Tavsif:</b>\n"
        f"<i>{movie['description'] or 'Tavsif mavjud emas.'}</i>"
    )

    try:
        # Video faylni caption bilan BIRGA yuborish
        if movie.get('file_id'):
            await bot.send_video(
                chat_id=chat_id,
                video=movie['file_id'],
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )
        else:
            # Video yo'q bo'lsa, faqat matn yuborish
            await bot.send_message(
                chat_id=chat_id,
                text=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=kb
            )
    except Exception as e:
        logger.error(f"Kino yuborishda xato: {e}")
        await bot.send_message(
            chat_id=chat_id,
            text="❌ Kino yuborishda xatolik yuz berdi. Admin bilan bog'laning.",
            reply_markup=main_menu_kb(await db.is_admin(user_id))
        )


def get_movie_list_text(movies: List[Dict], title: str) -> str:
    if not movies:
        return f"😕 {title}\n\nHozircha kinolar mavjud emas."
    text = f"🎬 <b>{title}</b>\n\n"
    for i, m in enumerate(movies, 1):
        text += f"{i}. <b>{m['name']}</b> (<code>{m['code']}</code>) | 👁 {m['views']}\n"
    text += "\n<i>Kino kodini yuborib, to'liq ma'lumot olishingiz mumkin.</i>"
    return text


# ==================== HANDLERLAR ====================
router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    username = message.from_user.username or ""
    first_name = message.from_user.first_name or ""
    await db.add_user(user_id, username, first_name)
    is_admin = await db.is_admin(user_id)

    if is_admin:
        await message.answer(
            "👨‍💻 <b>Admin panelga xush kelibsiz!</b>\n\n"
            "Quyidagi tugmalar orqali boshqaruvni amalga oshiring.",
            parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb()
        )
        return

    await message.answer(
        f"🎬 <b>Kino Botiga xush kelibsiz, {first_name}!</b>\n\n"
        f"🔢 Kino kodini yuboring va to'liq ma'lumot oling.\n"
        f"Yoki quyidagi tugmalardan foydalaning:",
        parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(is_admin)
    )


# FAQAT FOYDALANUVCHI PANELI - StateFilter(None) bilan
@router.message(F.text.regexp(r"^\d+$"), StateFilter(None))
async def search_by_code(message: Message, state: FSMContext):
    """Faqat default state'da ishlaydi - admin holatlarida EMAS"""
    await state.clear()
    code = message.text.strip()
    movie = await db.get_movie_by_code(code)

    if not movie:
        similar = await db.search_movies(code, limit=3)
        text = f"😕 <b>Kino topilmadi!</b>\n\nKod: <code>{code}</code>\n\n"
        if similar:
            text += "🎯 <b>Sizga tavsiya etamiz:</b>\n"
            for m in similar:
                text += f"• {m['name']} (<code>{m['code']}</code>)\n"
        else:
            text += "📝 Kino so'rash tugmasini bosib, adminga murojaat qiling."
        await message.answer(text, parse_mode=ParseMode.HTML,
                             reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                 [InlineKeyboardButton(text="📝 Kino so'rash", callback_data="request")],
                                 [InlineKeyboardButton(text="🔙 Menyu", callback_data="main_menu")]
                             ]))
        return

    try:
        await message.delete()
    except:
        pass
    await send_movie(message.bot, message.chat.id, movie, message.from_user.id)


# ---------- CALLBACK HANDLERLAR ----------


async def safe_edit_text(callback: CallbackQuery, text: str, **kwargs):
    """Photo/video xabarlarida edit_text ishlaganda xavfsiz ishlaydi."""
    try:
        await callback.message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if "there is no text in the message to edit" in msg:
            try:
                await callback.message.delete()
            except Exception:
                pass
            await callback.message.answer(text, **kwargs)
            return
        raise


@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    is_admin = await db.is_admin(callback.from_user.id)
    await safe_edit_text(
        callback,
        "🎬 <b>Asosiy menyu</b>\n\n"
        "🔢 Kino kodini yuboring yoki tugmalardan foydalaning:",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(is_admin)
    )


@router.callback_query(F.data == "check_sub")
async def cb_check_sub(callback: CallbackQuery):
    middleware = SubscriptionMiddleware()
    is_sub = await middleware.check_subscription(callback.bot, callback.from_user.id)
    if is_sub:
        await callback.message.edit_text(
            "✅ <b>Obuna tasdiqlandi!</b>\n\nBotdan foydalanishingiz mumkin.",
            parse_mode=ParseMode.HTML,
            reply_markup=main_menu_kb(await db.is_admin(callback.from_user.id))
        )
    else:
        await callback.answer("❌ Hali obuna bo'lmagansiz!", show_alert=True)


@router.callback_query(F.data == "random")
async def cb_random(callback: CallbackQuery):
    movie = await db.get_random_movie()
    if not movie:
        await callback.answer("😕 Hozircha kinolar mavjud emas!", show_alert=True)
        return
    await callback.message.delete()
    await send_movie(callback.bot, callback.message.chat.id, movie, callback.from_user.id)


@router.callback_query(F.data == "favorites")
async def cb_favorites(callback: CallbackQuery):
    user_id = callback.from_user.id
    movies = await db.get_favorites(user_id)
    if not movies:
        await callback.message.edit_text(
            "❤️ <b>Sevimli kinolar</b>\n\n"
            "Hozircha sevimli kinolar yo'q.\n"
            "Kino kartochkasidagi ❤️ tugmasini bosing.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Orqaga", callback_data="main_menu")]
            ]))
        return
    text = get_movie_list_text(movies, "Sevimli kinolar")
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                         [InlineKeyboardButton(text="🔙 Orqaga", callback_data="main_menu")]
                                     ]))


@router.callback_query(F.data == "request")
async def cb_request(callback: CallbackQuery, state: FSMContext):
    await state.set_state(UserStates.request_movie)
    await callback.message.edit_text(
        "📝 <b>Kino so'rash</b>\n\n"
        "Kerakli kino nomini yuboring:\n"
        "<i>Admin ko'rib chiqib, bazaga qo'shadi.</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="main_menu")]
        ]))


@router.message(UserStates.request_movie)
async def process_request(message: Message, state: FSMContext):
    movie_name = message.text.strip()
    await db.add_request(message.from_user.id, message.from_user.username or "", movie_name)
    await state.clear()
    admins = await db.get_admins()
    for admin in admins:
        try:
            await message.bot.send_message(
                admin['user_id'],
                f"🆕 <b>Yangi kino so'rovi!</b>\n\n"
                f"👤 Foydalanuvchi: {message.from_user.first_name} (@{message.from_user.username or 'yoq'})\n"
                f"🆔 ID: <code>{message.from_user.id}</code>\n"
                f"🎬 Kino: <b>{movie_name}</b>",
                parse_mode=ParseMode.HTML
            )
        except:
            pass
    await message.answer(
        "✅ <b>So'rovingiz qabul qilindi!</b>\n\n"
        "Admin ko'rib chiqib, bazaga qo'shadi.",
        parse_mode=ParseMode.HTML,
        reply_markup=main_menu_kb(await db.is_admin(message.from_user.id))
    )


@router.callback_query(F.data.startswith("fav_"))
async def cb_favorite(callback: CallbackQuery):
    movie_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    if await db.is_favorite(user_id, movie_id):
        await db.remove_favorite(user_id, movie_id)
        await callback.answer("💔 Sevimlilardan o'chirildi", show_alert=False)
    else:
        await db.add_favorite(user_id, movie_id)
        await callback.answer("❤️ Sevimlilarga qo'shildi", show_alert=False)
    is_fav = await db.is_favorite(user_id, movie_id)
    kb = movie_action_kb(movie_id, is_fav)
    try:
        await callback.message.edit_reply_markup(reply_markup=kb)
    except:
        pass


# ==================== ADMIN PANEL ====================
@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        await callback.answer("❌ Ruxsat yo'q!", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        "👨‍💻 <b>Admin panel</b>\n\n"
        "Quyidagi tugmalar orqali boshqaruvni amalga oshiring:",
        parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb()
    )


# ---------- KINO QO'SHISH (SODDALASHTIRILGAN) ----------
@router.callback_query(F.data == "admin_add")
async def cb_admin_add(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.add_code)
    await callback.message.edit_text(
        "➕ <b>Kino qo'shish</b>\n\n"
        "1️⃣ Kino kodini kiriting (faqat raqam, masalan: 4587):\n\n"
        "<i>Kerakli ma'lumotlar: kod, nom, tavsif, video</i>",
        parse_mode=ParseMode.HTML, reply_markup=back_admin_kb()
    )


@router.message(AdminStates.add_code)
async def process_add_code(message: Message, state: FSMContext):
    code = message.text.strip()
    if not code.isdigit():
        await message.answer("❌ Kod faqat raqam bo'lishi kerak! Qayta kiriting:")
        return
    existing = await db.get_movie_by_code(code)
    if existing:
        await message.answer("❌ Bu kod allaqachon mavjud! Boshqa kod kiriting:")
        return
    await state.update_data(code=code)
    await state.set_state(AdminStates.add_name)
    await message.answer("🎬 Kino nomini kiriting:")


@router.message(AdminStates.add_name)
async def process_add_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AdminStates.add_description)
    await message.answer("📝 Kino tavsifini kiriting:")


@router.message(AdminStates.add_description)
async def process_add_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await state.set_state(AdminStates.add_video)
    await message.answer(
        "🎥 Kino video faylini yuboring:\n\n"
        "<i>Video faylni to'g'ridan-to'g'ri yuboring.</i>"
    )


@router.message(AdminStates.add_video, F.video)
async def process_add_video(message: Message, state: FSMContext):
    file_id = message.video.file_id
    await state.update_data(file_id=file_id)
    await finish_add_movie(message, state)


@router.message(AdminStates.add_video)
async def process_add_video_invalid(message: Message):
    await message.answer("❌ Iltimos, video fayl yuboring!")


async def finish_add_movie(message: Message, state: FSMContext):
    data = await state.get_data()
    admin_id = message.from_user.id

    success = await db.add_movie(
        code=data['code'],
        name=data['name'],
        year=0,
        genre="Noma'lum",
        description=data['description'],
        file_id=data['file_id'],
        poster_id=None,
        added_by=admin_id
    )

    if success:
        await db.add_log(admin_id, "ADD_MOVIE", data['code'], f"Added: {data['name']}")
        await message.answer(
            f"✅ <b>Kino muvaffaqiyatli qo'shildi!</b>\n\n"
            f"🔢 Kod: <code>{data['code']}</code>\n"
            f"🎬 Nomi: {data['name']}\n"
            f"📝 Tavsif:\n {data['description'][:50]}...",
            parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb()
        )
    else:
        await message.answer("❌ Xatolik! Kino qo'shilmadi.", reply_markup=admin_menu_kb())
    await state.clear()


# ---------- SERIAL (KO'P QISMLI) QO'SHISH ----------
@router.callback_query(F.data == "admin_add_series")
async def cb_admin_add_series(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    await state.clear()
    await state.set_state(AdminStates.series_count)
    await callback.message.edit_text(
        "🎞 <b>Serial (ko'p qismli kino) qo'shish</b>\n\n"
        "Nechta qismi bor? Sonini kiriting (masalan: 20):",
        parse_mode=ParseMode.HTML, reply_markup=back_admin_kb()
    )


@router.message(AdminStates.series_count)
async def process_series_count(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1 or int(text) > 100:
        await message.answer("❌ 1 dan 100 gacha bo'lgan raqam kiriting:")
        return
    count = int(text)
    await state.update_data(
        series_count=count,
        series_codes=[], series_names=[], series_descs=[], series_videos=[],
        series_idx=0
    )
    await state.set_state(AdminStates.series_codes)
    await message.answer(
        f"🔢 <b>1/{count}-qism kodini kiriting</b> (faqat raqam):",
        parse_mode=ParseMode.HTML
    )


@router.message(AdminStates.series_codes)
async def process_series_codes(message: Message, state: FSMContext):
    code = message.text.strip()
    if not code.isdigit():
        await message.answer("❌ Kod faqat raqam bo'lishi kerak! Qayta kiriting:")
        return
    existing = await db.get_movie_by_code(code)
    if existing:
        await message.answer(f"❌ <code>{code}</code> kodi allaqachon mavjud! Boshqa kod kiriting:",
                              parse_mode=ParseMode.HTML)
        return

    data = await state.get_data()
    codes = data['series_codes']
    if code in codes:
        await message.answer(f"❌ <code>{code}</code> kodi allaqachon shu seriyada ishlatilgan! Boshqa kod kiriting:",
                              parse_mode=ParseMode.HTML)
        return

    codes.append(code)
    count = data['series_count']
    idx = len(codes)
    await state.update_data(series_codes=codes)

    if idx < count:
        await message.answer(f"🔢 <b>{idx + 1}/{count}-qism kodini kiriting</b> (faqat raqam):",
                              parse_mode=ParseMode.HTML)
    else:
        await state.set_state(AdminStates.series_name)
        await message.answer(
            "🎬 <b>Serial nomini kiriting</b>\n\n"
            "<i>Bu nom barcha qismlar uchun umumiy bo'ladi (masalan: \"Naruto\"). "
            "Qism raqami avtomatik qo'shiladi.</i>",
            parse_mode=ParseMode.HTML
        )


@router.message(AdminStates.series_name)
async def process_series_name(message: Message, state: FSMContext):
    base_name = message.text.strip()
    data = await state.get_data()
    count = data['series_count']
    await state.update_data(series_base_name=base_name, series_descs=[])
    await state.set_state(AdminStates.series_descriptions)
    await message.answer(
        f"📝 <b>1/{count}-qism tavsifini kiriting</b>:",
        parse_mode=ParseMode.HTML
    )


@router.message(AdminStates.series_descriptions)
async def process_series_descriptions(message: Message, state: FSMContext):
    desc = message.text.strip()
    data = await state.get_data()
    descs = data['series_descs']
    descs.append(desc)
    count = data['series_count']
    idx = len(descs)
    await state.update_data(series_descs=descs)

    if idx < count:
        await message.answer(f"📝 <b>{idx + 1}/{count}-qism tavsifini kiriting</b>:",
                              parse_mode=ParseMode.HTML)
    else:
        await state.update_data(series_videos=[])
        await state.set_state(AdminStates.series_videos)
        await message.answer(
            "🎥 <b>1/" + str(count) + "-qism video faylini yuboring:</b>",
            parse_mode=ParseMode.HTML
        )


@router.message(AdminStates.series_videos, F.video)
async def process_series_videos(message: Message, state: FSMContext):
    data = await state.get_data()
    videos = data['series_videos']
    videos.append(message.video.file_id)
    count = data['series_count']
    idx = len(videos)
    await state.update_data(series_videos=videos)

    if idx < count:
        await message.answer(f"🎥 <b>{idx + 1}/{count}-qism video faylini yuboring:</b>",
                              parse_mode=ParseMode.HTML)
    else:
        await finish_add_series(message, state)


@router.message(AdminStates.series_videos)
async def process_series_videos_invalid(message: Message):
    await message.answer("❌ Iltimos, video fayl yuboring!")


async def finish_add_series(message: Message, state: FSMContext):
    data = await state.get_data()
    admin_id = message.from_user.id
    count = data['series_count']
    codes = data['series_codes']
    base_name = data['series_base_name']
    descs = data['series_descs']
    videos = data['series_videos']

    added = 0
    failed_codes = []
    for i in range(count):
        part_name = base_name
        success = await db.add_movie(
            code=codes[i],
            name=part_name,
            year=0,
            genre="Serial",
            description=descs[i],
            file_id=videos[i],
            poster_id=None,
            added_by=admin_id
        )
        if success:
            added += 1
            await db.add_log(admin_id, "ADD_MOVIE", codes[i], f"Added (series): {part_name}")
        else:
            failed_codes.append(codes[i])

    result_text = (
        f"✅ <b>Serial qo'shildi!</b>\n\n"
        f"🎬 Nomi: {base_name}\n"
        f"📦 Jami qismlar: {count}\n"
        f"✅ Muvaffaqiyatli qo'shildi: {added}\n"
    )
    if failed_codes:
        result_text += f"❌ Xatolik (kodlar): {', '.join(failed_codes)}\n"

    result_text += "\n🔢 <b>Kodlar:</b>\n"
    for i, code in enumerate(codes, 1):
        result_text += f"  {i}-qism: <code>{code}</code>\n"

    await message.answer(result_text, parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())
    await state.clear()


# ---------- KINO TAHRIRLASH ----------
@router.callback_query(F.data == "admin_edit")
async def cb_admin_edit(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    movies = await db.get_all_movies()
    if not movies:
        await callback.answer("😕 Kinolar mavjud emas!", show_alert=True)
        return
    text = "✏️ <b>Tahrirlash uchun kino tanlang:</b>\n\n"
    buttons = []
    for m in movies:
        buttons.append([InlineKeyboardButton(
            text=f"{m['name']} ({m['code']})",
            callback_data=f"edit_select_{m['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_panel")])
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("edit_select_"))
async def cb_edit_select(callback: CallbackQuery, state: FSMContext):
    movie_id = int(callback.data.split("_")[2])
    movie = await db.get_movie_by_id(movie_id)
    if not movie:
        await callback.answer("Kino topilmadi!", show_alert=True)
        return
    await state.update_data(edit_movie_id=movie_id)
    await callback.message.edit_text(
        f"✏️ <b>{movie['name']}</b> ni tahrirlash\n\n"
        f"Qaysi maydonni o'zgartirmoqchisiz?",
        parse_mode=ParseMode.HTML, reply_markup=edit_field_kb(movie_id)
    )


@router.callback_query(F.data.startswith("edit_"))
async def cb_edit_field(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split("_")
    if len(parts) < 3:
        return
    field = parts[1]
    movie_id = int(parts[2])

    field_names = {
        'code': '🔢 Kod',
        'name': '🎬 Nomi',
        'desc': '📝 Tavsif',
        'video': '🎥 Video'
    }
    if field not in field_names:
        return

    await state.update_data(edit_field=field, edit_movie_id=movie_id)
    await state.set_state(AdminStates.edit_value)

    if field == 'video':
        await callback.message.edit_text("🎥 Yangi video faylni yuboring:",
                                         reply_markup=back_admin_kb())
    elif field == 'desc':
        await callback.message.edit_text(f"📝 Yangi {field_names[field]} ni kiriting:",
                                         reply_markup=back_admin_kb())
    else:
        await callback.message.edit_text(f"{field_names[field]} ni kiriting:",
                                         reply_markup=back_admin_kb())


@router.message(AdminStates.edit_value)
async def process_edit_value(message: Message, state: FSMContext):
    data = await state.get_data()
    field = data.get('edit_field')
    movie_id = data.get('edit_movie_id')

    if not field or not movie_id:
        await state.clear()
        return

    db_field = field if field != 'desc' else 'description'

    if field == 'video' and message.video:
        value = message.video.file_id
    elif message.text:
        value = message.text.strip()
    else:
        await message.answer("❌ Noto'g'ri format!")
        return

    success = await db.update_movie(movie_id, db_field, value)
    movie = await db.get_movie_by_id(movie_id)

    if success and movie:
        await db.add_log(message.from_user.id, "EDIT_MOVIE", movie['code'],
                         f"Edited field: {db_field}")
        await message.answer("✅ <b>Kino muvaffaqiyatli tahrirlandi!</b>",
                             parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())
    else:
        await message.answer("❌ Xatolik yuz berdi!", reply_markup=admin_menu_kb())
    await state.clear()


# ---------- KINO O'CHIRISH ----------
@router.callback_query(F.data == "admin_delete")
async def cb_admin_delete(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    movies = await db.get_all_movies()
    if not movies:
        await callback.answer("😕 Kinolar mavjud emas!", show_alert=True)
        return
    text = "❌ <b>O'chirish uchun kino tanlang:</b>\n\n"
    buttons = []
    for m in movies:
        buttons.append([InlineKeyboardButton(
            text=f"🗑 {m['name']} ({m['code']})",
            callback_data=f"del_select_{m['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_panel")])
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("del_select_"))
async def cb_del_select(callback: CallbackQuery, state: FSMContext):
    movie_id = int(callback.data.split("_")[2])
    movie = await db.get_movie_by_id(movie_id)
    if not movie:
        await callback.answer("Kino topilmadi!", show_alert=True)
        return
    await callback.message.edit_text(
        f"🗑 <b>{movie['name']}</b> ni o'chirmoqchimisiz?\n\n"
        f"⚠️ Bu amal qaytarib bo'lmaydi!",
        parse_mode=ParseMode.HTML, reply_markup=confirm_delete_kb(movie_id)
    )


@router.callback_query(F.data.startswith("del_confirm_"))
async def cb_del_confirm(callback: CallbackQuery):
    movie_id = int(callback.data.split("_")[2])
    movie = await db.get_movie_by_id(movie_id)
    if movie:
        await db.delete_movie(movie_id)
        await db.add_log(callback.from_user.id, "DELETE_MOVIE", movie['code'],
                         f"Deleted: {movie['name']}")
        await callback.message.edit_text("✅ <b>Kino o'chirildi!</b>",
                                         reply_markup=admin_menu_kb())
    else:
        await callback.answer("Kino topilmadi!", show_alert=True)


# ---------- STATISTIKA ----------
@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    total_users = await db.get_user_count()
    today_users = await db.get_today_users()
    total_movies = await db.get_movie_count()
    popular = await db.get_popular_movies(5)

    text = (
        "📊 <b>Bot statistikasi</b>\n\n"
        f"👥 <b>Jami foydalanuvchilar:</b> {total_users}\n"
        f"📈 <b>Bugungi foydalanuvchilar:</b> {today_users}\n"
        f"🎬 <b>Jami kinolar:</b> {total_movies}\n\n"
        f"🔥 <b>Eng mashhur 5 kino:</b>\n"
    )
    for i, m in enumerate(popular, 1):
        text += f"{i}. {m['name']} - {m['views']} ko'rish\n"

    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=back_admin_kb())


# ---------- REKLAMA YUBORISH ----------

def broadcast_type_kb() -> InlineKeyboardMarkup:
    """Broadcast turi tanlash knopkalari"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Matn", callback_data="bc_type_text")],
        [InlineKeyboardButton(text="🖼 Rasm", callback_data="bc_type_photo")],
        [InlineKeyboardButton(text="🎥 Video", callback_data="bc_type_video")],
        [InlineKeyboardButton(text="🔙 Admin menyuga", callback_data="admin_panel")]
    ])


def broadcast_targets_kb(channels: list, selected: list) -> InlineKeyboardMarkup:
    """Kanal va guruhlarni tanlash knopkalari"""
    buttons = []
    # @KinoMarkettt kanali har doim birinchi bo'lishi kerak
    kino_market = "@KinoMarkettt"
    kino_in_db = any(ch['channel_username'] == kino_market for ch in channels)
    all_targets = list(channels)
    if not kino_in_db:
        # Uni ro'yxatga qo'shamiz virtual tarzda
        all_targets.insert(0, {
            'channel_username': kino_market,
            'invite_link': 'https://t.me/KinoMarkettt',
            'id': 'kino_market'
        })

    for ch in all_targets:
        ch_id = str(ch['id'])
        tick = "✅ " if ch_id in selected else "☑️ "
        buttons.append([InlineKeyboardButton(
            text=f"{tick}{ch['channel_username']}",
            callback_data=f"bc_target_{ch_id}"
        )])

    # "Barchasi" tugmasi
    all_ids = [str(ch['id']) for ch in all_targets]
    all_selected = all(i in selected for i in all_ids)
    buttons.append([InlineKeyboardButton(
        text="✅ Barchasini tanlash" if not all_selected else "❌ Barchasini olib tashlash",
        callback_data="bc_target_all"
    )])
    buttons.append([InlineKeyboardButton(text="📤 Yuborish", callback_data="bc_send_confirm")])
    buttons.append([InlineKeyboardButton(text="🔙 Bekor qilish", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.edit_text(
        "📨 <b>Reklama yuborish</b>\n\n"
        "Qanday turdagi xabar yubormoqchisiz?\n"
        "Birini tanlang:",
        parse_mode=ParseMode.HTML,
        reply_markup=broadcast_type_kb()
    )


@router.callback_query(F.data.in_({"bc_type_text", "bc_type_photo", "bc_type_video"}))
async def cb_broadcast_type(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    bc_type = callback.data.replace("bc_type_", "")
    await state.update_data(bc_type=bc_type)
    await state.set_state(AdminStates.broadcast)

    hints = {
        "text": "📝 <b>Matn xabar yuboring</b>\n\nXabar matnini yozing:\n\n<i>Bekor qilish: /cancel</i>",
        "photo": "🖼 <b>Rasm yuboring</b>\n\nRasmni yuboring (caption ixtiyoriy):\n\n<i>Bekor qilish: /cancel</i>",
        "video": "🎥 <b>Video yuboring</b>\n\nVideoni yuboring (caption ixtiyoriy):\n\n<i>Bekor qilish: /cancel</i>",
    }
    await callback.message.edit_text(
        hints[bc_type],
        parse_mode=ParseMode.HTML,
        reply_markup=back_admin_kb()
    )


@router.message(AdminStates.broadcast, Command("cancel"))
async def cancel_broadcast(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Bekor qilindi.", reply_markup=admin_menu_kb())


@router.message(AdminStates.broadcast)
async def process_broadcast_content(message: Message, state: FSMContext):
    """Xabar kontentini qabul qilib, caption so'rash yoki to'g'ridan kanal tanlashga o'tish"""
    data = await state.get_data()
    bc_type = data.get("bc_type", "text")

    # Tekshiruv: to'g'ri content yuborilganmi?
    if bc_type == "text" and not message.text:
        await message.answer("❌ Iltimos, matn yuboring!")
        return
    if bc_type == "photo" and not message.photo:
        await message.answer("❌ Iltimos, rasm yuboring!")
        return
    if bc_type == "video" and not message.video:
        await message.answer("❌ Iltimos, video yuboring!")
        return

    # Kontentni saqlash
    if bc_type == "text":
        await state.update_data(bc_text=message.text, bc_file_id=None, bc_caption=None)
        # Matn uchun caption kerak emas, to'g'ridan target tanlashga o'tamiz
        await _show_target_selection(message, state)
    elif bc_type == "photo":
        await state.update_data(bc_file_id=message.photo[-1].file_id, bc_text=None)
        # Caption so'rash
        await state.set_state(AdminStates.broadcast_caption)
        await message.answer(
            "✍️ <b>Caption (tavsif) kiriting</b>\n\n"
            "Rasm ostiga yoziladigan matnni kiriting.\n"
            "Agar caption kerak bo'lmasa, <b>-</b> yuboring:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏭ Caption yo'q", callback_data="bc_no_caption")]
            ])
        )
    elif bc_type == "video":
        await state.update_data(bc_file_id=message.video.file_id, bc_text=None)
        # Caption so'rash
        await state.set_state(AdminStates.broadcast_caption)
        await message.answer(
            "✍️ <b>Caption (tavsif) kiriting</b>\n\n"
            "Video ostiga yoziladigan matnni kiriting.\n"
            "Agar caption kerak bo'lmasa, <b>-</b> yuboring:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⏭ Caption yo'q", callback_data="bc_no_caption")]
            ])
        )


@router.callback_query(F.data == "bc_no_caption")
async def cb_bc_no_caption(callback: CallbackQuery, state: FSMContext):
    await state.update_data(bc_caption=None)
    await _show_target_selection_cb(callback, state)


@router.message(AdminStates.broadcast_caption)
async def process_broadcast_caption(message: Message, state: FSMContext):
    caption = None if message.text.strip() == "-" else message.text.strip()
    await state.update_data(bc_caption=caption)
    await _show_target_selection(message, state)


async def _show_target_selection(message: Message, state: FSMContext):
    """Kanal/guruh tanlash ekranini ko'rsatish (message orqali)"""
    await state.update_data(bc_selected_targets=[])
    await state.set_state(AdminStates.broadcast_select_targets)
    channels = await db.get_all_channels()
    # @KinoMarkettt ni qo'shish agar yo'q bo'lsa
    kino_market = "@KinoMarkettt"
    if not any(ch['channel_username'] == kino_market for ch in channels):
        channels.insert(0, {
            'channel_username': kino_market,
            'invite_link': 'https://t.me/KinoMarkettt',
            'id': 'kino_market'
        })
    await state.update_data(bc_channels=channels)
    await message.answer(
        "📢 <b>Qaysi kanal/guruhlarga yuborilsin?</b>\n\n"
        "Tanlang (bir nechta bo'lishi mumkin).\n"
        "Barcha foydalanuvchilarga har doim yuboriladi.",
        parse_mode=ParseMode.HTML,
        reply_markup=broadcast_targets_kb(channels, [])
    )


async def _show_target_selection_cb(callback: CallbackQuery, state: FSMContext):
    """Kanal/guruh tanlash ekranini ko'rsatish (callback orqali)"""
    await state.update_data(bc_selected_targets=[])
    await state.set_state(AdminStates.broadcast_select_targets)
    channels = await db.get_all_channels()
    kino_market = "@KinoMarkettt"
    if not any(ch['channel_username'] == kino_market for ch in channels):
        channels.insert(0, {
            'channel_username': kino_market,
            'invite_link': 'https://t.me/KinoMarkettt',
            'id': 'kino_market'
        })
    await state.update_data(bc_channels=channels)
    await callback.message.edit_text(
        "📢 <b>Qaysi kanal/guruhlarga yuborilsin?</b>\n\n"
        "Tanlang (bir nechta bo'lishi mumkin).\n"
        "Barcha foydalanuvchilarga har doim yuboriladi.",
        parse_mode=ParseMode.HTML,
        reply_markup=broadcast_targets_kb(channels, [])
    )


@router.callback_query(F.data.startswith("bc_target_"), AdminStates.broadcast_select_targets)
async def cb_bc_target_toggle(callback: CallbackQuery, state: FSMContext):
    """Kanal/guruh tanlash/bekor qilish"""
    data = await state.get_data()
    channels = data.get("bc_channels", [])
    selected = data.get("bc_selected_targets", [])

    target_id = callback.data.replace("bc_target_", "")

    if target_id == "all":
        all_ids = [str(ch['id']) for ch in channels]
        if all(i in selected for i in all_ids):
            selected = []
        else:
            selected = all_ids
    else:
        if target_id in selected:
            selected.remove(target_id)
        else:
            selected.append(target_id)

    await state.update_data(bc_selected_targets=selected)
    await callback.message.edit_reply_markup(
        reply_markup=broadcast_targets_kb(channels, selected)
    )
    await callback.answer()


@router.callback_query(F.data == "bc_send_confirm", AdminStates.broadcast_select_targets)
async def cb_bc_send_confirm(callback: CallbackQuery, state: FSMContext):
    """Yuborishni tasdiqlash va amalga oshirish"""
    if not await db.is_admin(callback.from_user.id):
        return

    data = await state.get_data()
    await state.clear()

    bc_type = data.get("bc_type", "text")
    bc_text = data.get("bc_text")
    bc_file_id = data.get("bc_file_id")
    bc_caption = data.get("bc_caption")
    selected_targets = data.get("bc_selected_targets", [])
    channels = data.get("bc_channels", [])

    # Tanlangan kanallarni olish
    target_channels = [ch for ch in channels if str(ch['id']) in selected_targets]

    await callback.message.edit_text("📤 <b>Yuborilmoqda...</b>", parse_mode=ParseMode.HTML)

    bot = callback.bot
    sent_users = 0
    blocked_users = 0
    sent_channels = 0

    # Kanalga yuborish uchun "Odam qo'shish" inline knopkasi
    def make_invite_kb_for_channel(ch_list):
        buttons = []
        for ch in ch_list:
            link = ch.get('invite_link') or f"https://t.me/{ch['channel_username'].replace('@', '')}"
            buttons.append([InlineKeyboardButton(text=f"👥 {ch['channel_username']}ga qo'shiling", url=link)])
        return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

    channel_invite_kb = make_invite_kb_for_channel(target_channels) if target_channels else None

    # 1. Tanlangan kanallarga yuborish
    for ch in target_channels:
        try:
            ch_username = ch['channel_username']
            if bc_type == "text" and bc_text:
                await bot.send_message(ch_username, bc_text, parse_mode=ParseMode.HTML,
                                       reply_markup=channel_invite_kb)
            elif bc_type == "photo" and bc_file_id:
                await bot.send_photo(ch_username, bc_file_id, caption=bc_caption,
                                     parse_mode=ParseMode.HTML, reply_markup=channel_invite_kb)
            elif bc_type == "video" and bc_file_id:
                await bot.send_video(ch_username, bc_file_id, caption=bc_caption,
                                     parse_mode=ParseMode.HTML, reply_markup=channel_invite_kb)
            sent_channels += 1
        except Exception as e:
            logger.error(f"Kanalga yuborishda xato ({ch['channel_username']}): {e}")

    # 2. Barcha foydalanuvchilarga yuborish (har doim)
    users = await db.get_all_users()
    user_invite_kb = channel_invite_kb  # Foydalanuvchilarga ham odam qo'shish knopkasi

    for user_id in users:
        try:
            if bc_type == "text" and bc_text:
                await bot.send_message(user_id, bc_text, parse_mode=ParseMode.HTML,
                                       reply_markup=user_invite_kb)
            elif bc_type == "photo" and bc_file_id:
                await bot.send_photo(user_id, bc_file_id, caption=bc_caption,
                                     parse_mode=ParseMode.HTML, reply_markup=user_invite_kb)
            elif bc_type == "video" and bc_file_id:
                await bot.send_video(user_id, bc_file_id, caption=bc_caption,
                                     parse_mode=ParseMode.HTML, reply_markup=user_invite_kb)
            sent_users += 1
        except TelegramForbiddenError:
            blocked_users += 1
        except Exception:
            pass

    channel_names = ", ".join(ch['channel_username'] for ch in target_channels) or "Yo'q"
    await callback.message.edit_text(
        f"✅ <b>Yuborildi!</b>\n\n"
        f"📢 Kanallar: {channel_names}\n"
        f"📤 Foydalanuvchilar: {sent_users} ta\n"
        f"🚫 Bloklangan: {blocked_users} ta\n"
        f"👥 Jami: {len(users)} ta",
        parse_mode=ParseMode.HTML,
        reply_markup=admin_menu_kb()
    )


# ---------- SO'ROVLAR ----------
@router.callback_query(F.data == "admin_requests")
async def cb_admin_requests(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    requests = await db.get_requests('pending')
    if not requests:
        await callback.answer("😕 Yangi so'rovlar yo'q!", show_alert=True)
        return

    text = "🎬 <b>Kino so'rovlari</b>\n\n"
    buttons = []
    for req in requests:
        text += (
            f"🆔 <b>So'rov #{req['id']}</b>\n"
            f"👤 {req['username'] or "Noma'lum"} (ID: {req['user_id']})\n"
            f"🎬 {req['movie_name']}\n"
            f"📅 {req['created_at']}\n\n"
        )
        buttons.append([InlineKeyboardButton(
            text=f"✅ #{req['id']} ni ko'rildi qilish",
            callback_data=f"req_done_{req['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_panel")])

    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data.startswith("req_done_"))
async def cb_req_done(callback: CallbackQuery):
    req_id = int(callback.data.split("_")[2])
    await db.update_request_status(req_id, 'done')
    await callback.answer("✅ So'rov ko'rildi deb belgilandi!", show_alert=False)
    await cb_admin_requests(callback)


# ---------- ADMINLAR BOSHQARUVI ----------
@router.callback_query(F.data == "admin_admins")
async def cb_admin_admins(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    admins = await db.get_admins()
    text = "👮 <b>Adminlar ro'yxati:</b>\n\n"
    for a in admins:
        text += f"🆔 <code>{a['user_id']}</code> (Qo'shilgan: {a['added_at']})\n"

    buttons = [
        [InlineKeyboardButton(text="➕ Admin qo'shish", callback_data="admin_add_admin"),
         InlineKeyboardButton(text="➖ Admin o'chirish", callback_data="admin_remove_admin")],
        [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_panel")]
    ]
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.callback_query(F.data == "admin_add_admin")
async def cb_add_admin(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.add_admin)
    await callback.message.edit_text(
        "➕ <b>Admin qo'shish</b>\n\n"
        "Yangi adminning Telegram ID sini kiriting:",
        parse_mode=ParseMode.HTML, reply_markup=back_admin_kb()
    )


@router.message(AdminStates.add_admin)
async def process_add_admin(message: Message, state: FSMContext):
    try:
        new_admin_id = int(message.text.strip())
        await db.add_admin(new_admin_id, message.from_user.id)
        await db.add_log(message.from_user.id, "ADD_ADMIN",
                         details=f"Added admin: {new_admin_id}")
        await message.answer("✅ <b>Admin muvaffaqiyatli qo'shildi!</b>",
                             parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())
    except ValueError:
        await message.answer("❌ ID faqat raqam bo'lishi kerak! Qayta kiriting:")
        return
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}", reply_markup=admin_menu_kb())
    await state.clear()


@router.callback_query(F.data == "admin_remove_admin")
async def cb_remove_admin(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.remove_admin)
    await callback.message.edit_text(
        "➖ <b>Admin o'chirish</b>\n\n"
        "O'chiriladigan adminning ID sini kiriting:\n"
        "<i>O'zingizni o'chira olmaysiz!</i>",
        parse_mode=ParseMode.HTML, reply_markup=back_admin_kb()
    )


@router.message(AdminStates.remove_admin)
async def process_remove_admin(message: Message, state: FSMContext):
    try:
        admin_id = int(message.text.strip())
        if admin_id == message.from_user.id:
            await message.answer("❌ O'zingizni o'chira olmaysiz!",
                                 reply_markup=admin_menu_kb())
            await state.clear()
            return
        await db.remove_admin(admin_id)
        await db.add_log(message.from_user.id, "REMOVE_ADMIN",
                         details=f"Removed admin: {admin_id}")
        await message.answer("✅ <b>Admin o'chirildi!</b>",
                             parse_mode=ParseMode.HTML, reply_markup=admin_menu_kb())
    except ValueError:
        await message.answer("❌ ID faqat raqam bo'lishi kerak! Qayta kiriting:")
        return
    except Exception as e:
        await message.answer(f"❌ Xatolik: {e}", reply_markup=admin_menu_kb())
    await state.clear()


# ---------- KINOLAR RO'YXATI ----------
MOVIES_PER_PAGE = 30


@router.callback_query(F.data.startswith("admin_movielist_"))
async def cb_admin_movielist(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    page = int(callback.data.replace("admin_movielist_", ""))
    movies = await db.get_all_movies()

    if not movies:
        await callback.answer("😕 Kinolar mavjud emas!", show_alert=True)
        return

    total = len(movies)
    total_pages = (total + MOVIES_PER_PAGE - 1) // MOVIES_PER_PAGE
    page = max(0, min(page, total_pages - 1))
    start = page * MOVIES_PER_PAGE
    end = start + MOVIES_PER_PAGE
    page_movies = movies[start:end]

    text = f"📃 <b>Kinolar ro'yxati</b> (jami: {total})\n\n"
    for m in page_movies:
        text += f"🔢 <code>{m['code']}</code> — {m['name']}\n"

    text += f"\n📄 Sahifa {page + 1}/{total_pages}"

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Oldingi", callback_data=f"admin_movielist_{page - 1}"))
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="Keyingi ➡️", callback_data=f"admin_movielist_{page + 1}"))

    buttons = []
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton(text="🔙 Admin menyuga", callback_data="admin_panel")])

    try:
        await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                         reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except TelegramBadRequest:
        await callback.answer()


# ---------- LOGLAR ----------
@router.callback_query(F.data == "admin_logs")
async def cb_admin_logs(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    logs = await db.get_logs(20)
    if not logs:
        await callback.answer("😕 Loglar bo'sh!", show_alert=True)
        return

    text = "📋 <b>So'nggi admin loglari:</b>\n\n"
    for log in logs:
        text += (
            f"🕐 {log['created_at']}\n"
            f"👤 Admin: <code>{log['admin_id']}</code>\n"
            f"⚡ {log['action']} | 🎬 {log['movie_code'] or '-'}\n"
            f"📝 {log['details'] or '-'}\n\n"
        )
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=back_admin_kb())


# ==================== OBUNA SOZLAMALARI ====================

@router.callback_query(F.data == "admin_subscriptions")
async def cb_admin_subscriptions(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    channels = await db.get_all_channels()
    extra_links = await db.get_subscription_links()

    text = "📢 <b>Obuna sozlamalari</b>\n\n"

    text += "📌 <b>Telegram kanallar/guruhlar:</b>\n"
    if channels:
        for ch in channels:
            text += f"  • {ch['channel_username']} (ID: {ch['id']})\n"
    else:
        text += "  <i>Hozircha yo'q</i>\n"

    text += "\n🔗 <b>Boshqa linklar (Instagram va h.k.):</b>\n"
    if extra_links:
        for lnk in extra_links:
            text += f"  • {lnk['title']} (ID: {lnk['id']})\n    {lnk['url']}\n"
    else:
        text += "  <i>Hozircha yo'q</i>\n"

    buttons = [
        [InlineKeyboardButton(text="➕ Kanal/Guruh qo'shish", callback_data="sub_add_channel")],
        [InlineKeyboardButton(text="➖ Kanal/Guruh o'chirish", callback_data="sub_del_channel")],
        [InlineKeyboardButton(text="➕ Link qo'shish (Instagram va h.k.)", callback_data="sub_add_link")],
        [InlineKeyboardButton(text="➖ Link o'chirish", callback_data="sub_del_link")],
        [InlineKeyboardButton(text="🔙 Admin menyuga", callback_data="admin_panel")]
    ]
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


# --- Kanal qo'shish ---
@router.callback_query(F.data == "sub_add_channel")
async def cb_sub_add_channel(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.add_channel_username)
    await callback.message.edit_text(
        "➕ <b>Kanal/Guruh qo'shish</b>\n\n"
        "Kanal yoki guruh username'ini kiriting:\n"
        "<i>Masalan: @mening_kanalim</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_subscriptions")]
        ])
    )


@router.message(AdminStates.add_channel_username)
async def process_add_channel_username(message: Message, state: FSMContext):
    username = message.text.strip()
    if not username.startswith("@"):
        await message.answer("❌ Username @ bilan boshlanishi kerak! Qayta kiriting:")
        return
    await state.update_data(ch_username=username)
    await state.set_state(AdminStates.add_channel_link)
    await message.answer(
        "🔗 Kanal/guruhga invite link kiriting:\n"
        "<i>Masalan: https://t.me/mening_kanalim</i>",
        parse_mode=ParseMode.HTML
    )


@router.message(AdminStates.add_channel_link)
async def process_add_channel_link(message: Message, state: FSMContext):
    invite_link = message.text.strip()
    data = await state.get_data()
    username = data['ch_username']
    await state.clear()

    # Bot kanalga kira oladimi tekshirish
    try:
        test_member = await message.bot.get_chat_member(username, message.bot.id)
        bot_status = test_member.status
        is_admin_str = "✅ Ha" if bot_status in ['administrator', 'creator'] else "⚠️ Yo'q (botni admin qiling!)"
    except Exception as e:
        bot_status = "noma'lum"
        is_admin_str = f"❌ Kirish xatosi: {e}"

    success = await db.add_channel(username, invite_link)
    if success:
        await db.add_log(message.from_user.id, "ADD_CHANNEL", details=f"{username}")
        await message.answer(
            f"✅ <b>Kanal qo'shildi!</b>\n\n"
            f"📢 Kanal: {username}\n"
            f"🔗 Havola: {invite_link}\n"
            f"🤖 Bot kanalda admin: {is_admin_str}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Obuna sozlamalari", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="🔙 Admin menyu", callback_data="admin_panel")]
            ])
        )
    else:
        await message.answer("❌ Qo'shishda xatolik yuz berdi!", reply_markup=admin_menu_kb())


# --- Kanal o'chirish ---
@router.callback_query(F.data == "sub_del_channel")
async def cb_sub_del_channel(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    channels = await db.get_all_channels()
    if not channels:
        await callback.answer("😕 O'chiriladigan kanal yo'q!", show_alert=True)
        return
    buttons = []
    for ch in channels:
        buttons.append([InlineKeyboardButton(
            text=f"🗑 {ch['channel_username']}",
            callback_data=f"sub_delch_{ch['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_subscriptions")])
    await callback.message.edit_text(
        "➖ <b>O'chiriladigan kanalni tanlang:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("sub_delch_"))
async def cb_sub_delch_confirm(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    channel_id = int(callback.data.split("_")[2])
    await db.delete_channel(channel_id)
    await db.add_log(callback.from_user.id, "DELETE_CHANNEL", details=f"ID: {channel_id}")
    await callback.answer("✅ Kanal o'chirildi!", show_alert=True)
    # Ro'yxatni qayta ko'rsatish
    channels = await db.get_all_channels()
    extra_links = await db.get_subscription_links()
    text = "📢 <b>Obuna sozlamalari</b>\n\n📌 <b>Telegram kanallar/guruhlar:</b>\n"
    if channels:
        for ch in channels:
            text += f"  • {ch['channel_username']} (ID: {ch['id']})\n"
    else:
        text += "  <i>Hozircha yo'q</i>\n"
    text += "\n🔗 <b>Boshqa linklar:</b>\n"
    if extra_links:
        for lnk in extra_links:
            text += f"  • {lnk['title']} (ID: {lnk['id']})\n"
    else:
        text += "  <i>Hozircha yo'q</i>\n"
    buttons = [
        [InlineKeyboardButton(text="➕ Kanal/Guruh qo'shish", callback_data="sub_add_channel")],
        [InlineKeyboardButton(text="➖ Kanal/Guruh o'chirish", callback_data="sub_del_channel")],
        [InlineKeyboardButton(text="➕ Link qo'shish (Instagram va h.k.)", callback_data="sub_add_link")],
        [InlineKeyboardButton(text="➖ Link o'chirish", callback_data="sub_del_link")],
        [InlineKeyboardButton(text="🔙 Admin menyuga", callback_data="admin_panel")]
    ]
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


# --- Link qo'shish (Instagram, YouTube va boshqalar) ---
@router.callback_query(F.data == "sub_add_link")
async def cb_sub_add_link(callback: CallbackQuery, state: FSMContext):
    if not await db.is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.add_sub_link_title)
    await callback.message.edit_text(
        "➕ <b>Link qo'shish</b>\n\n"
        "Link nomini kiriting:\n"
        "<i>Masalan: Instagram sahifamiz, YouTube kanal, Rasmiy sayt</i>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_subscriptions")]
        ])
    )


@router.message(AdminStates.add_sub_link_title)
async def process_add_sub_link_title(message: Message, state: FSMContext):
    await state.update_data(link_title=message.text.strip())
    await state.set_state(AdminStates.add_sub_link_url)
    await message.answer(
        "🔗 Link URL manzilini kiriting:\n"
        "<i>Masalan: https://instagram.com/mening_sahifam</i>",
        parse_mode=ParseMode.HTML
    )


@router.message(AdminStates.add_sub_link_url)
async def process_add_sub_link_url(message: Message, state: FSMContext):
    url = message.text.strip()
    if not (url.startswith("http://") or url.startswith("https://")):
        await message.answer("❌ URL http:// yoki https:// bilan boshlanishi kerak! Qayta kiriting:")
        return
    data = await state.get_data()
    title = data['link_title']
    await state.clear()

    success = await db.add_subscription_link(title, url)
    if success:
        await db.add_log(message.from_user.id, "ADD_SUB_LINK", details=f"{title}: {url}")
        await message.answer(
            f"✅ <b>Link qo'shildi!</b>\n\n"
            f"📌 Nomi: {title}\n"
            f"🔗 URL: {url}",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Obuna sozlamalari", callback_data="admin_subscriptions")],
                [InlineKeyboardButton(text="🔙 Admin menyu", callback_data="admin_panel")]
            ])
        )
    else:
        await message.answer("❌ Qo'shishda xatolik yuz berdi!", reply_markup=admin_menu_kb())


# --- Link o'chirish ---
@router.callback_query(F.data == "sub_del_link")
async def cb_sub_del_link(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    links = await db.get_subscription_links()
    if not links:
        await callback.answer("😕 O'chiriladigan link yo'q!", show_alert=True)
        return
    buttons = []
    for lnk in links:
        buttons.append([InlineKeyboardButton(
            text=f"🗑 {lnk['title']}",
            callback_data=f"sub_dellnk_{lnk['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="🔙 Orqaga", callback_data="admin_subscriptions")])
    await callback.message.edit_text(
        "➖ <b>O'chiriladigan linkni tanlang:</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@router.callback_query(F.data.startswith("sub_dellnk_"))
async def cb_sub_dellnk_confirm(callback: CallbackQuery):
    if not await db.is_admin(callback.from_user.id):
        return
    link_id = int(callback.data.split("_")[2])
    await db.delete_subscription_link(link_id)
    await db.add_log(callback.from_user.id, "DELETE_SUB_LINK", details=f"ID: {link_id}")
    await callback.answer("✅ Link o'chirildi!", show_alert=True)
    # Ro'yxatni qayta ko'rsatish
    channels = await db.get_all_channels()
    extra_links = await db.get_subscription_links()
    text = "📢 <b>Obuna sozlamalari</b>\n\n📌 <b>Telegram kanallar/guruhlar:</b>\n"
    if channels:
        for ch in channels:
            text += f"  • {ch['channel_username']} (ID: {ch['id']})\n"
    else:
        text += "  <i>Hozircha yo'q</i>\n"
    text += "\n🔗 <b>Boshqa linklar:</b>\n"
    if extra_links:
        for lnk in extra_links:
            text += f"  • {lnk['title']} (ID: {lnk['id']})\n"
    else:
        text += "  <i>Hozircha yo'q</i>\n"
    buttons = [
        [InlineKeyboardButton(text="➕ Kanal/Guruh qo'shish", callback_data="sub_add_channel")],
        [InlineKeyboardButton(text="➖ Kanal/Guruh o'chirish", callback_data="sub_del_channel")],
        [InlineKeyboardButton(text="➕ Link qo'shish (Instagram va h.k.)", callback_data="sub_add_link")],
        [InlineKeyboardButton(text="➖ Link o'chirish", callback_data="sub_del_link")],
        [InlineKeyboardButton(text="🔙 Admin menyuga", callback_data="admin_panel")]
    ]
    await callback.message.edit_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


# ---------- KANAL SOZLASH ----------
@router.message(Command("setchannel"))
async def cmd_set_channel(message: Message):
    if not await db.is_admin(message.from_user.id):
        await message.answer("❌ Ruxsat yo'q!")
        return
    args = message.text.split()
    if len(args) < 3:
        await message.answer(
            "📢 <b>Kanal sozlash</b>\n\n"
            "Foydalanish: <code>/setchannel @username https://t.me/username</code>\n\n"
            "Yoki avvalgi kanalni o'chirish uchun: <code>/setchannel clear</code>",
            parse_mode=ParseMode.HTML
        )
        return
    if args[1].lower() == "clear":
        async with db.pool.acquire() as conn:
            await conn.execute("DELETE FROM channels")
        await message.answer("✅ Barcha majburiy kanallar o'chirildi.")
        return
    channel_username = args[1]
    invite_link = args[2]
    if not channel_username.startswith("@"):
        await message.answer("❌ Kanal username @ bilan boshlanishi kerak!")
        return
    try:
        test_member = await message.bot.get_chat_member(channel_username, message.bot.id)
        is_admin = "✅ Ha" if test_member.status in ['administrator', 'creator'] else "❌ Yo'q"
        await message.answer(
            f"✅ <b>Kanal sozlandi!</b>\n\n"
            f"📢 Kanal: {channel_username}\n"
            f"🔗 Havola: {invite_link}\n"
            f"🤖 Bot status: {test_member.status}\n\n"
            f"<i>Bot kanalda admin: {is_admin}</i>",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await message.answer(
            f"⚠️ <b>Kanal sozlandi, lekin bot kanalga kira olmayapti!</b>\n\n"
            f"📢 Kanal: {channel_username}\n"
            f"❌ Xato: {e}\n\n"
            f"<b>Botni kanalga admin qiling!</b>",
            parse_mode=ParseMode.HTML
        )
    async with db.pool.acquire() as conn:
        await conn.execute("UPDATE channels SET required = 0")
        await conn.execute("""
            INSERT INTO channels (channel_username, invite_link, required)
            VALUES ($1, $2, 1)
            ON CONFLICT (channel_username) DO UPDATE SET
                invite_link = EXCLUDED.invite_link, required = 1
        """, channel_username, invite_link)


@router.message(Command("channels"))
async def cmd_list_channels(message: Message):
    if not await db.is_admin(message.from_user.id):
        await message.answer("❌ Ruxsat yo'q!")
        return
    channels = await db.get_required_channels()
    if not channels:
        await message.answer("📭 Majburiy kanallar sozlanmagan.")
        return
    text = "📢 <b>Majburiy kanallar:</b>\n\n"
    for ch in channels:
        text += f"• {ch['channel_username']}\n  Havola: {ch['invite_link']}\n\n"
    await message.answer(text, parse_mode=ParseMode.HTML)


# ==================== ASOSIY FUNKSIYA ====================
async def main():
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.middleware(RateLimitMiddleware())
    dp.callback_query.middleware(RateLimitMiddleware())
    dp.message.middleware(SubscriptionMiddleware())
    dp.callback_query.middleware(SubscriptionMiddleware())
    dp.include_router(router)

    await db.connect()
    logger.info("🚀 Bot ishga tushdi (PostgreSQL + Railway)...")
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())