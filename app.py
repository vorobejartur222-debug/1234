import asyncio
import fcntl
import hashlib
import html
import io
import json
import os
import random
import re
import signal
import sys
import threading
import time
import uuid
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import feedparser
import requests
from flask import Flask
from groq import Groq
from PIL import Image, ImageDraw, ImageFont
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, BotCommand
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from werkzeug.serving import make_server


# ============================================================
# Настройки
# ============================================================

def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задана переменная окружения {name}")
    return value


TELEGRAM_TOKEN = required_env("TELEGRAM_TOKEN")
CHANNEL_ID = required_env("CHANNEL_ID")
ADMIN_ID = int(required_env("ADMIN_ID"))

SUPABASE_URL = required_env("SUPABASE_URL")
SUPABASE_KEY = required_env("SUPABASE_KEY")

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.environ.get("GROQ_MODEL", "").strip() or "openai/gpt-oss-20b"

print(f"🔧 GROQ_MODEL = '{GROQ_MODEL}'")

TIMEZONE_NAME = "Europe/Moscow"
try:
    LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    LOCAL_TZ = ZoneInfo("UTC")

MAX_ATTEMPTS = 3
FIND_TOTAL_TIMEOUT = 60
GROQ_TIMEOUT = 20

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# Умная ротация RSS-источников
# ============================================================

FEED_CATEGORIES = {
    "science": [
        "https://nplus1.ru/rss",
        "https://naked-science.ru/feed",
        "https://www.ixbt.com/export/news.rss",
        "https://hi-news.ru/feed/",
        "https://tproger.ru/feed/",
    ],
    "cinema": [
        "https://www.kinopoisk.ru/news.rss",
    ],
    "games": [
        "https://www.playground.ru/rss/news",
    ],
    "culture": [
        "https://kulturomania.ru/rss/",
        "https://aif.ru/rss/culture.php",
    ],
    "belarus": [
        "https://charter97.link/ru/page/rss/",
        "https://krupki.gov.by/ru/novosti-regiona?format=feed&Itemid=274",
        "https://freenewsapi.ai/v1/rss?host=news.by&size=50",
    ]
}

# Порядок очереди
CATEGORY_ORDER = ["science", "cinema", "games", "culture", "belarus"]


# ============================================================
# Supabase-хранилище
# ============================================================

DEFAULT_DATA = {
    "items": [],
    "pending": {},
    "last_skipped": None,
    "last_category_index": -1, # Для ротации
    "stats": {
        "published": 0, 
        "skipped_by_admin": 0, 
        "skipped_by_ai": 0, 
        "sources": {}
    },
    "last_auto_publish_date": "",
}

_data = dict(DEFAULT_DATA)
_data_lock = asyncio.Lock()
_find_lock = asyncio.Lock()
_seen_set = set()
pending_queue = _data["pending"]


def load_data_sync():
    global _data, pending_queue, _seen_set
    for attempt in range(1, 6):
        try:
            response = supabase.table("bot_data").select("data").eq("id", 1).execute()
            if not response.data:
                _data = dict(DEFAULT_DATA)
            else:
                loaded = response.data[0].get("data") or {}
                if isinstance(loaded, str):
                    loaded = json.loads(loaded)
                _data = dict(DEFAULT_DATA)
                _data.update(loaded)
                _data.setdefault("items", [])
                _data.setdefault("pending", {})
                _data.setdefault("last_skipped", None)
                _data.setdefault("last_category_index", -1)
                
                stats = _data.setdefault("stats", {})
                if "skipped" in stats and "skipped_by_admin" not in stats:
                    stats["skipped_by_admin"] = stats.pop("skipped")
                stats.setdefault("skipped_by_admin", 0)
                stats.setdefault("skipped_by_ai", 0)
                stats.setdefault("published", 0)
                stats.setdefault("sources", {})
                _data.setdefault("last_auto_publish_date", "")
            break
        except Exception as exc:
            print(f"⚠️ Попытка {attempt}/5 загрузки из Supabase: {type(exc).__name__}")
            if attempt == 5:
                _data = dict(DEFAULT_DATA)
            else:
                time.sleep(3)

    pending_queue = _data["pending"]
    _seen_set = set(_data.get("items", []))
    print(f"💾 Загружено из Supabase: {len(_data['items'])} items, {len(pending_queue)} pending")
    return _data


def save_data_sync():
    try:
        _data["pending"] = pending_queue
        supabase.table("bot_data").upsert(
            {"id": 1, "data": _data, "updated_at": datetime.utcnow().isoformat()}
        ).execute()
        return True
    except Exception as exc:
        print(f"❌ Ошибка сохранения в Supabase: {type(exc).__name__}")
        return False


async def load_data():
    async with _data_lock:
        return await asyncio.to_thread(load_data_sync)


async def save_data():
    async with _data_lock:
        try:
            return await asyncio.wait_for(asyncio.to_thread(save_data_sync), timeout=15)
        except asyncio.TimeoutError:
            print("⚠️ Тайм-аут сохранения в Supabase.")
            return False


def is_seen(key: str) -> bool:
    return key in _seen_set

def mark_seen(key: str):
    if key in _seen_set:
        return
    _seen_set.add(key)
    items = _data.setdefault("items", [])
    items.append(key)
    if len(items) > 2000:
        removed = items[:-2000]
        _data["items"] = items[-2000:]
        for old_key in removed:
            _seen_set.discard(old_key)

def bump_stat(stat_name: str):
    stats = _data.setdefault("stats", DEFAULT_DATA["stats"])
    stats[stat_name] = stats.get(stat_name, 0) + 1

def bump_source(feed_url: str):
    stats = _data.setdefault("stats", DEFAULT_DATA["stats"])
    sources = stats.setdefault("sources", {})
    sources[feed_url] = sources.get(feed_url, 0) + 1

def reset_seen():
    global _seen_set
    _seen_set.clear()
    _data["items"] = []
    _data["stats"] = dict(DEFAULT_DATA["stats"])
    _data["last_skipped"] = None
    _data["last_category_index"] = -1
    print("🔄 Seen set, статистика и очередь сброшены")


# ============================================================
# Flask health-check
# ============================================================

flask_app = Flask(__name__)

@flask_app.route("/")
@flask_app.route("/health")
def health():
    return "OK", 200


# ============================================================
# StayPresent
# ============================================================

async def stay_present_loop():
    base_url = os.environ.get("RENDER_EXTERNAL_URL", "https://one234-0ioz.onrender.com")
    ping_url = f"{base_url.rstrip('/')}/health"
    while True:
        try:
            await asyncio.to_thread(requests.get, ping_url, timeout=30)
        except Exception:
            pass
        await asyncio.sleep(600)


# ============================================================
# Водяной знак + Генерация картинок
# ============================================================

def add_watermark_sync(image_url: str, watermark_text: str = "+VIBE NEWS") -> io.BytesIO:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(image_url, headers=headers, timeout=15)
        response.raise_for_status()

        img = Image.open(io.BytesIO(response.content)).convert("RGBA")
        img.thumbnail((1280, 1280), Image.Resampling.LANCZOS)

        txt_layer = Image.new("RGBA", img.size, (255, 255, 255, 0))
        draw = ImageDraw.Draw(txt_layer)
        font_size = max(12, int(img.width * 0.025))
        
        try:
            font = ImageFont.truetype("arial.ttf", size=font_size)
        except IOError:
            try:
                font = ImageFont.load_default(size=font_size)
            except TypeError:
                font = ImageFont.load_default()

        if hasattr(draw, 'textbbox'):
            bbox = draw.textbbox((0, 0), watermark_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
        else:
            text_width, text_height = draw.textsize(watermark_text, font=font)

        margin = 20
        x = img.width - text_width - margin
        y = img.height - text_height - margin

        draw.text((x, y), watermark_text, font=font, fill=(255, 255, 255, 120))

        out = Image.alpha_composite(img, txt_layer).convert("RGB")
        output = io.BytesIO()
        out.save(output, format="JPEG", quality=85)
        output.seek(0)
        return output
    except Exception as e:
        print(f"⚠️ Ошибка при наложении водяного знака: {e}")
        return None

def generate_ai_image(title: str) -> str:
    """Генерирует картинку через Pollinations AI с рандомным seed для уникальности."""
    prompt = f"high quality photo, news illustration, {title}"
    encoded_prompt = urllib.parse.quote(prompt)
    random_seed = random.randint(1, 1000000)
    return f"https://image.pollinations.ai/prompt/{encoded_prompt}?width=1024&height=576&nologo=true&seed={random_seed}"


# ============================================================
# RSS: обработка
# ============================================================

def clean_html(raw: str) -> str:
    if not raw: return ""
    text = re.sub(r"<[^>]*>", " ", raw)
    text = html.unescape(text)
    text = re.sub(r"https?://\S+", "", text)
    return re.sub(r"\s+", " ", text).strip()

def get_summary(entry) -> str:
    summary = getattr(entry, "summary", "") or ""
    if not summary:
        contents = getattr(entry, "content", None) or []
        if contents:
            summary = contents[0].get("value", "")
    return clean_html(summary)[:1200]

def get_item_key(entry, feed_url: str, title: str) -> str:
    source_id = getattr(entry, "id", None) or getattr(entry, "link", None) or f"{feed_url}:{title}"
    return hashlib.sha256(str(source_id).encode("utf-8")).hexdigest()

def extract_image(entry, raw_summary: str = ""):
    for field in ("media_content", "media_thumbnail"):
        for media in getattr(entry, field, None) or []:
            if media.get("url"): return media["url"]
    for enclosure in getattr(entry, "enclosures", None) or []:
        if "image" in enclosure.get("type", "") and enclosure.get("href"): return enclosure["href"]
    match = re.search(r"""<img[^>]+src=["']([^"']+)["']""", raw_summary or "", re.IGNORECASE)
    return match.group(1) if match else None

BLOCK_MARKERS = [
    "туториал", "руководство", "как я ", "как сделать", "how to", "tutorial",
    "война", "убит", "погиб", "теракт", "катастрофа", "трагедия", "авария", "смерть", "умер",
    "путин", "зеленск", "трамп", "байден", "нато", "санкц", "кремл", "госдум", "минобороны",
    "москв", "парковк", "подъезд", "жкх", "собянин", "чиновник", "штраф", "запрет", "закон"
]

def is_news_item(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    return not any(marker in text for marker in BLOCK_MARKERS)

def parse_feed_sync(feed_url: str):
    response = requests.get(feed_url, headers={"User-Agent": "VibeNewsBot/1.0"}, timeout=(3, 5))
    response.raise_for_status()
    return feedparser.parse(response.content)

async def fetch_fresh_from_feed(feed_url: str):
    try:
        feed = await asyncio.to_thread(parse_feed_sync, feed_url)
        fresh = []
        for entry in feed.entries[:10]:
            title = clean_html(getattr(entry, "title", "") or "") or "Без заголовка"
            key = get_item_key(entry, feed_url, title)
            if is_seen(key): continue
            summary = get_summary(entry)
            if not is_news_item(title, summary):
                mark_seen(key)
                continue
            fresh.append({"entry": entry, "title": title, "feed_url": feed_url, "key": key})
        return fresh
    except Exception:
        return []

async def find_one_new_entry():
    """Умный поиск по ротации категорий."""
    
    # Определяем следующую категорию
    current_index = _data.get("last_category_index", -1)
    next_index = (current_index + 1) % len(CATEGORY_ORDER)
    category = CATEGORY_ORDER[next_index]
    
    feeds = list(FEED_CATEGORIES[category])
    random.shuffle(feeds)
    
    print(f"🔎 Очередь ротации: ищу в категории '{category.upper()}'")

    for feed_url in feeds:
        fresh = await fetch_fresh_from_feed(feed_url)
        if fresh:
            # Если нашли новость, сдвигаем очередь в Supabase
            _data["last_category_index"] = next_index
            await save_data()
            return random.choice(fresh)
            
    # Если в этой категории пусто, ищем в остальных (запасной вариант)
    print(f"⚠️ В категории '{category.upper()}' пусто. Ищу везде...")
    all_feeds = [f for cat_feeds in FEED_CATEGORIES.values() for f in cat_feeds]
    random.shuffle(all_feeds)
    
    for feed_url in all_feeds:
        fresh = await fetch_fresh_from_feed(feed_url)
        if fresh:
            return random.choice(fresh)

    return None


# ============================================================
# Groq: генерация (Строгий и понятный промпт)
# ============================================================

def rewrite_with_ai(title: str, summary: str):
    if not groq_client: return None

    prompt = f"""Ты — профессиональный редактор Telegram-канала +VIBE NEWS.
Твоя задача: взять факты из источника и написать связный пост на русском языке.

ПРАВИЛА:
1. Если новость про США, Африку или скучный быт — верни ровно слово ОТКАЗ.
2. Пиши строго по фактам. Не выдумывай ничего от себя.
3. Текст должен быть логичным и связным. Никаких рубленых фраз или бреда.
4. Если оригинал на английском — переведи.
5. Никакого кринжа. Если новость про медицину — пиши нейтрально.

ФОРМАТ:
- 3-4 предложения.
- Ровно 2 эмодзи (одно в начале, одно по смыслу в тексте).
- В самом конце добавь 1-2 хэштега (например: #Наука #Кино).
- Без ссылок и лишних заголовков.

Заголовок: {title}
Описание: {summary}

Напиши только текст поста (или ОТКАЗ):"""

    for attempt in range(3):
        try:
            response = groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5, # Снизили температуру для логики
                max_tokens=1000,
                timeout=GROQ_TIMEOUT,
            )
            if not response.choices: continue

            text = (response.choices[0].message.content or "").strip()
            
            if text.upper().startswith("ОТКАЗ") or text == "ОТКАЗ":
                return "ОТКАЗ"

            if getattr(response.choices[0], "finish_reason", None) == "length": continue

            text = re.sub(r"https?://\S+", "", text).strip()
            if not text: continue
                
            if len(text) > 1000: text = text[:997].rsplit(" ", 1)[0].rstrip() + "..."
            return text
        except Exception as exc:
            print(f"❌ Groq: {type(exc).__name__}")
            return None
    return None


# ============================================================
# Telegram: отправка и модерация
# ============================================================

def build_keyboard(review_id: str, show_undo: bool = False):
    buttons = [
        InlineKeyboardButton("🚀 Опубликовать", callback_data=f"publish:{review_id}"),
        InlineKeyboardButton("✏️ Изменить", callback_data=f"edit:{review_id}")
    ]
    if show_undo:
        buttons.append(InlineKeyboardButton("↩️ Вернуть", callback_data="undo:last"))
        
    buttons.append(InlineKeyboardButton("⏭️ Дальше", callback_data=f"skip:{review_id}"))
    return InlineKeyboardMarkup([buttons])

async def safe_edit(query, text: str):
    try:
        await query.edit_message_text(text)
    except Exception:
        try:
            await query.edit_message_caption(caption=text)
        except Exception: pass

async def send_post_for_review(app: Application, review_id: str, item: dict):
    message_text = f"📬 Пост на проверку\n\n{item['post_text']}"
    show_undo = bool(_data.get("last_skipped"))
    keyboard = build_keyboard(review_id, show_undo)
    image_url = item.get("image_url")

    try:
        if image_url and len(item["post_text"]) <= 1024:
            watermarked_image = await asyncio.to_thread(add_watermark_sync, image_url)
            photo_to_send = watermarked_image if watermarked_image else image_url
            await app.bot.send_photo(chat_id=ADMIN_ID, photo=photo_to_send, caption=message_text, reply_markup=keyboard)
        else:
            await app.bot.send_message(chat_id=ADMIN_ID, text=message_text[:4096], reply_markup=keyboard)
    except Exception:
        await app.bot.send_message(chat_id=ADMIN_ID, text=message_text[:4096], reply_markup=keyboard)

async def _find_and_send_next_inner(app: Application, max_attempts: int):
    for attempt in range(1, max_attempts + 1):
        candidate = await find_one_new_entry()

        if not candidate:
            await app.bot.send_message(chat_id=ADMIN_ID, text="😔 Новых подходящих публикаций не нашёл.")
            await save_data()
            return False

        entry = candidate["entry"]
        title = candidate["title"]
        summary = get_summary(entry)
        source_url = getattr(entry, "link", "") or ""
        raw_summary = getattr(entry, "summary", "") or ""
        
        image_url = extract_image(entry, raw_summary)
        if not image_url:
            print("🎨 Генерирую картинку через ИИ...")
            image_url = generate_ai_image(title)

        post_text = await asyncio.to_thread(rewrite_with_ai, title, summary)

        if post_text == "ОТКАЗ":
            print(f"🚫 AI отклонил новость: {title[:80]}")
            mark_seen(candidate["key"])
            bump_stat("skipped_by_ai")
            await save_data()
            continue

        if not post_text:
            print(f"⏭️ AI не справился: {title[:80]}")
            mark_seen(candidate["key"])
            bump_stat("skipped_by_ai")
            await save_data()
            continue

        mark_seen(candidate["key"])
        bump_source(candidate["feed_url"])

        review_id = uuid.uuid4().hex[:8]
        pending_queue[review_id] = {
            "title": title, "post_text": post_text,
            "image_url": image_url, "source_url": source_url,
            "created_at": time.time(),
        }
        await save_data()

        await send_post_for_review(app, review_id, pending_queue[review_id])
        return True

    await app.bot.send_message(chat_id=ADMIN_ID, text="😔 Не получилось подготовить пост из найденных материалов.")
    return False

async def find_and_send_next(app: Application, max_attempts: int = MAX_ATTEMPTS):
    if _find_lock.locked():
        try:
            await app.bot.send_message(chat_id=ADMIN_ID, text="⏳ Бот уже ищет новость, подожди завершения.")
        except Exception:
            pass
        return False

    try:
        async with _find_lock:
            return await asyncio.wait_for(_find_and_send_next_inner(app, max_attempts), timeout=FIND_TOTAL_TIMEOUT)
    except asyncio.TimeoutError:
        try: await app.bot.send_message(chat_id=ADMIN_ID, text="⏱️ Поиск занял слишком много времени. Сбрасываю зависший поиск.")
        except: pass
        return False

async def publish_to_channel(app: Application, item: dict):
    post_text = item["post_text"]
    image_url = item.get("image_url")
    source_url = item.get("source_url")

    reply_markup = None
    if source_url:
        reply_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Читать оригинал", url=source_url)
        ]])

    if image_url and len(post_text) <= 1024:
        try:
            watermarked_image = await asyncio.to_thread(add_watermark_sync, image_url)
            photo_to_send = watermarked_image if watermarked_image else image_url
            return await app.bot.send_photo(
                chat_id=CHANNEL_ID, 
                photo=photo_to_send, 
                caption=post_text,
                reply_markup=reply_markup
            )
        except Exception: pass
            
    return await app.bot.send_message(
        chat_id=CHANNEL_ID, 
        text=post_text[:4096],
        reply_markup=reply_markup
    )


# ============================================================
# Команды
# ============================================================

async def require_admin(update: Update):
    return update.effective_user and update.effective_user.id == ADMIN_ID

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    await update.effective_message.reply_text(
        "Привет! Я готовлю посты для проверки.\n\n"
        "/next — найти пост\n"
        "/undo — вернуть пропущенный пост\n"
        "/edit — изменить последний\n"
        "/clear — очистить очередь постов\n"
        "/reset — сбросить историю\n"
        "/stats — статистика"
    )

async def next_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    await update.effective_message.reply_text("🔎 Ищу материал...")
    asyncio.create_task(find_and_send_next(context.application))

async def undo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    last = _data.get("last_skipped")
    if not last:
        await update.effective_message.reply_text("Нечего возвращать. Нет недавно пропущенных постов.")
        return
        
    review_id = uuid.uuid4().hex[:8]
    pending_queue[review_id] = last
    _data["last_skipped"] = None
    await save_data()
    
    await update.effective_message.reply_text("↩️ Возвращаю предыдущий пост:")
    await send_post_for_review(context.application, review_id, last)

async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    count = len(pending_queue)
    pending_queue.clear()
    await save_data()
    await update.effective_message.reply_text(f"🗑 Очередь очищена. Удалено {count} постов.")

async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    if not pending_queue:
        await update.effective_message.reply_text("😔 Нет постов в очереди.")
        return
    review_id = list(pending_queue.keys())[-1]
    item = pending_queue[review_id]
    context.user_data["editing_review_id"] = review_id
    await update.effective_message.reply_text(f"✏️ Отправь новый текст:\n\n📝 Текущий:\n{item['post_text']}\n\n/cancel — отмена.")

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    if "editing_review_id" in context.user_data:
        del context.user_data["editing_review_id"]
        await update.effective_message.reply_text("❌ Редактирование отменено.")
    else:
        await update.effective_message.reply_text("Нечего отменять.")

async def handle_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID: return
    if "editing_review_id" not in context.user_data: return
    if not update.message or not update.message.text: return

    review_id = context.user_data.pop("editing_review_id")
    item = pending_queue.get(review_id)
    if not item: return

    item["post_text"] = update.message.text.strip()
    await save_data()
    await update.message.reply_text("✅ Текст обновлён.")
    await send_post_for_review(context.application, review_id, item)

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    reset_seen()
    await save_data()
    await update.effective_message.reply_text("🔄 История, статистика и очередь категорий сброшены.")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    stats = _data.get("stats", {})
    
    # Определяем, что пойдет следующим
    current_index = _data.get("last_category_index", -1)
    next_index = (current_index + 1) % len(CATEGORY_ORDER)
    next_cat = CATEGORY_ORDER[next_index].upper()
    
    text = (
        "📊 **Статистика**\n\n"
        f"✅ Опубликовано: {stats.get('published', 0)}\n"
        f"🙈 Отфильтровано ботом: {stats.get('skipped_by_ai', 0)}\n"
        f"⏭️ Пропущено тобой: {stats.get('skipped_by_admin', 0)}\n"
        f"📥 Ждут решения: {len(pending_queue)}\n\n"
        f"🔄 Следующая категория: {next_cat}"
    )
    await update.effective_message.reply_text(text, parse_mode='Markdown')

async def review_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.from_user or query.from_user.id != ADMIN_ID: return

    await query.answer()
    try:
        action, review_id = query.data.split(":", 1)
    except: return

    if action == "undo":
        last = _data.get("last_skipped")
        if not last:
            await safe_edit(query, "Нечего возвращать.")
            return
            
        if review_id in pending_queue:
             pending_queue.pop(review_id, None)
             
        new_review_id = uuid.uuid4().hex[:8]
        pending_queue[new_review_id] = last
        _data["last_skipped"] = None
        await save_data()
        
        await safe_edit(query, "↩️ Возвращаю предыдущий пост...")
        await send_post_for_review(context.application, new_review_id, last)
        return

    item = pending_queue.get(review_id)
    if not item:
        await safe_edit(query, "Эта публикация уже обработана.")
        return

    if action == "skip":
        _data["last_skipped"] = item
        pending_queue.pop(review_id, None)
        bump_stat("skipped_by_admin") 
        await save_data()
        
        await safe_edit(query, "⏭️ Пропущено. Ищу следующий материал...")
        asyncio.create_task(find_and_send_next(context.application))
        return

    if action == "edit":
        context.user_data["editing_review_id"] = review_id
        await safe_edit(query, f"✏️ Отправь новый текст:\n\n{item['post_text']}\n\n/cancel — отменить.")
        return

    if action == "publish":
        try:
            await publish_to_channel(context.application, item)
            pending_queue.pop(review_id, None)
            _data["last_skipped"] = None
            bump_stat("published")
            await save_data()
            await safe_edit(query, f"✅ Опубликовано: {item['title'][:60]}")
        except Exception:
            await safe_edit(query, "❌ Ошибка публикации. Смотри логи.")


# ============================================================
# Авто-постинг в 18:00
# ============================================================

async def auto_publish_loop(app: Application):
    while True:
        try:
            now = datetime.now(LOCAL_TZ)
            today = now.strftime("%Y-%m-%d")
            
            if now.hour == 18 and now.minute == 0 and _data.get("last_auto_publish_date") != today:
                _data["last_auto_publish_date"] = today
                await save_data()
                print("⏰ 18:00 МСК! Авто-публикация...")
                
                if pending_queue:
                    review_id = list(pending_queue.keys())[0]
                    item = pending_queue.pop(review_id)
                    _data["last_skipped"] = None 
                    bump_stat("published")
                    await save_data()
                    await publish_to_channel(app, item)
                    await app.bot.send_message(chat_id=ADMIN_ID, text=f"⏰ Авто-публикация (из очереди):\n{item['title'][:60]}")
                else:
                    candidate = await find_one_new_entry()
                    if candidate:
                        title = candidate["title"]
                        summary = get_summary(candidate["entry"])
                        
                        image_url = extract_image(candidate["entry"], getattr(candidate["entry"], "summary", ""))
                        if not image_url:
                            image_url = generate_ai_image(title)
                            
                        post_text = await asyncio.to_thread(rewrite_with_ai, title, summary)
                        source_url = getattr(candidate["entry"], "link", "") or ""

                        if post_text and post_text != "ОТКАЗ":
                            mark_seen(candidate["key"])
                            bump_stat("published")
                            await save_data()
                            item = {"title": title, "post_text": post_text, "image_url": image_url, "source_url": source_url}
                            await publish_to_channel(app, item)
                            await app.bot.send_message(chat_id=ADMIN_ID, text=f"⏰ Авто-поиск и публикация:\n{title[:60]}")
        except Exception: pass
        await asyncio.sleep(30)


# ============================================================
# Инициализация и запуск
# ============================================================

async def post_init(app: Application):
    try:
        await app.bot.delete_my_commands()
        await app.bot.set_my_commands([
            BotCommand("next", "Найти следующий пост"),
            BotCommand("undo", "Вернуть пропущенный"),
            BotCommand("edit", "Редактировать текст"),
            BotCommand("clear", "Очистить очередь"),
            BotCommand("reset", "Сбросить историю"),
            BotCommand("stats", "Показать статистику"),
            BotCommand("start", "Запустить бота")
        ])
        print("✅ Меню команд Telegram обновлено.")
    except Exception as e:
        print(f"⚠️ Ошибка обновления меню: {e}")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    pass

async def main():
    acquire_lock()
    await load_data()

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .build()
    )

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("next", next_command))
    application.add_handler(CommandHandler("undo", undo_command))
    application.add_handler(CommandHandler("clear", clear_command))
    application.add_handler(CommandHandler("edit", edit_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("reset", reset_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CallbackQueryHandler(review_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_text))
    application.add_error_handler(error_handler)

    port = int(os.environ.get("PORT", "10000"))
    http_server = make_server("0.0.0.0", port, flask_app, threaded=True)
    threading.Thread(target=http_server.serve_forever, name="health-server", daemon=True).start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop_event.set))

    background_tasks = []
    try:
        await application.initialize()
        await application.start()
        if application.updater is None:
            raise RuntimeError("У Telegram Application отсутствует Updater")

        await asyncio.sleep(2)
        await application.updater.start_polling(drop_pending_updates=True)

        background_tasks = [
            asyncio.create_task(auto_publish_loop(application), name="auto_publish"),
            asyncio.create_task(stay_present_loop(), name="stay_present"),
        ]
        print("✅ Бот запущен.")

        await stop_event.wait()
    finally:
        for task in background_tasks: task.cancel()
        if background_tasks: await asyncio.gather(*background_tasks, return_exceptions=True)
        if application.updater and application.updater.running: await application.updater.stop()
        if application.running: await application.stop()
        await application.shutdown()
        http_server.shutdown()
        http_server.server_close()
        release_lock()


if __name__ == "__main__":
    asyncio.run(main())
