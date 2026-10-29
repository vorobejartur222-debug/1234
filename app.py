import asyncio
import fcntl
import hashlib
import html
import json
import os
import random
import re
import signal
import sys
import threading
import time
import uuid
import urllib.request  # <--- ДОБАВЛЕНО для StayPresent
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import feedparser
import requests
from flask import Flask
from groq import Groq
from supabase import create_client, Client
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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
print(f"🔧 SUPABASE_URL = '{SUPABASE_URL}'")
print(f"🔧 SUPABASE_KEY = {'задан' if SUPABASE_KEY else 'НЕ ЗАДАН'}")

TIMEZONE_NAME = os.environ.get("TIMEZONE", "Europe/Minsk").strip()
try:
    LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    LOCAL_TZ = ZoneInfo("UTC")
    print(f"⚠️ Неизвестная временная зона {TIMEZONE_NAME}, использую UTC")

MAX_PARALLEL_FEEDS = 10
FEED_TOTAL_TIMEOUT = 8
MAX_ENTRIES_PER_FEED = 15
MAX_ATTEMPTS = 5
FIND_TOTAL_TIMEOUT = 60
GROQ_TIMEOUT = 20

SCHEDULE_HOURS = {10, 14, 18, 22}

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# Защита от двойного запуска
# ============================================================

LOCK_FILE = "/tmp/bot.lock"
_LOCK_FD = None


def acquire_lock():
    global _LOCK_FD
    _LOCK_FD = open(LOCK_FILE, "a+")
    try:
        fcntl.flock(_LOCK_FD.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        _LOCK_FD.close()
        print("⚠️ Бот уже запущен в этом контейнере.")
        sys.exit(1)
    _LOCK_FD.seek(0)
    _LOCK_FD.truncate()
    _LOCK_FD.write(str(os.getpid()))
    _LOCK_FD.flush()


def release_lock():
    global _LOCK_FD
    if _LOCK_FD:
        try:
            fcntl.flock(_LOCK_FD.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        _LOCK_FD.close()
        _LOCK_FD = None


# ============================================================
# RSS-источники
# ============================================================

RSS_FEEDS = [
    "https://nplus1.ru/rss",
    "https://naked-science.ru/feed",
    "https://www.ixbt.com/export/news.rss",
    "https://hi-news.ru/feed/",
    "https://tproger.ru/feed/",
    "https://kulturomania.ru/rss/",
    "https://www.positive.news/feed/",
    "https://www.optimistdaily.com/feed/",
    "http://www.gazeta.ru/export/rss/autonews.xml",
    "https://aif.ru/rss/automobile.php",
    "https://freenewsapi.ai/v1/rss?host=www.kinopoisk.ru&size=50",
    "https://www.playground.ru/rss/news",
    "https://charter97.link/ru/page/rss/",
    "https://krupki.gov.by/ru/novosti-regiona?format=feed&Itemid=274",
    "https://freenewsapi.ai/v1/rss?host=news.by&size=50",
    "https://aif.ru/rss/sport.php",
    "https://aif.ru/rss/health.php",
]


# ============================================================
# Supabase-хранилище
# ============================================================

DEFAULT_DATA = {
    "items": [],
    "pending": {},
    "stats": {"published": 0, "skipped": 0, "sources": {}},
    "last_schedule_slot": "",
}

_data = dict(DEFAULT_DATA)
_data_lock = asyncio.Lock()
_find_lock = asyncio.Lock()
_seen_set = set()
pending_queue = _data["pending"]


def load_data_sync():
    global _data, pending_queue, _seen_set
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
            _data.setdefault("stats", {"published": 0, "skipped": 0, "sources": {}})
            _data.setdefault("last_schedule_slot", "")
    except Exception as exc:
        print(f"❌ Ошибка загрузки из Supabase: {type(exc).__name__}: {exc}")
        _data = dict(DEFAULT_DATA)

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
        print(f"✅ Сохранено в Supabase: {len(_data['items'])} items, {len(pending_queue)} pending")
        return True
    except Exception as exc:
        print(f"❌ Ошибка сохранения в Supabase: {type(exc).__name__}: {exc}")
        return False


async def load_data():
    async with _data_lock:
        return await asyncio.to_thread(load_data_sync)


async def save_data():
    async with _data_lock:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(save_data_sync), timeout=15
            )
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
    print("🔄 Seen set сброшен")


# ============================================================
# Flask health-check
# ============================================================

flask_app = Flask(__name__)


@flask_app.route("/")
@flask_app.route("/health")
def health():
    return "OK", 200


# ============================================================
# StayPresent: Механизм предотвращения засыпания на Render
# ============================================================

async def stay_present_loop():
    """Фоновый пингер. Обращается к своему же URL, чтобы Render видел трафик."""
    # Render автоматически подставляет переменную RENDER_EXTERNAL_URL
    # Если её нет (локальный запуск), используем ваш URL
    base_url = os.environ.get("RENDER_EXTERNAL_URL", "https://one234-0ioz.onrender.com")
    ping_url = f"{base_url.rstrip('/')}/health"
    
    print(f"🔄 StayPresent активирован. Пингую {ping_url} каждые 10 минут.")
    
    while True:
        try:
            # Делаем асинхронный запрос, чтобы не блокировать работу бота
            await asyncio.to_thread(
                lambda: urllib.request.urlopen(ping_url, timeout=30).read()
            )
            print("📡 StayPresent: успешный пинг, Render не уснет.")
        except Exception as e:
            print(f"⚠️ StayPresent: ошибка пинга - {e}")
            
        # Ждем 10 минут (600 секунд). Этого достаточно, чтобы Render не уснул (лимит 15 мин)
        await asyncio.sleep(600)


# ============================================================
# RSS: обработка
# ============================================================

def clean_html(raw: str) -> str:
    if not raw:
        return ""
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
            if media.get("url"):
                return media["url"]
    for enclosure in getattr(entry, "enclosures", None) or []:
        if "image" in enclosure.get("type", "") and enclosure.get("href"):
            return enclosure["href"]
    match = re.search(r"""<img[^>]+src=["']([^"']+)["']""", raw_summary or "", re.IGNORECASE)
    return match.group(1) if match else None


BLOCK_MARKERS = [
    "туториал", "руководство", "как я ", "как сделать", "how to", "tutorial",
    "война", "убит", "погиб", "теракт", "катастрофа", "трагедия", "авария", "смерть", "умер",
    "путин", "зеленск", "трамп", "байден", "нато", "санкц", "кремл", "госдум", "минобороны",
]


def is_news_item(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    return not any(marker in text for marker in BLOCK_MARKERS)


def parse_feed_sync(feed_url: str):
    response = requests.get(
        feed_url,
        headers={"User-Agent": "VibeNewsBot/1.0"},
        timeout=(3, 5),
    )
    response.raise_for_status()
    return feedparser.parse(response.content)


async def fetch_fresh_from_feed(feed_url: str, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            feed = await asyncio.wait_for(
                asyncio.to_thread(parse_feed_sync, feed_url),
                timeout=FEED_TOTAL_TIMEOUT,
            )
            fresh = []
            for entry in feed.entries[:MAX_ENTRIES_PER_FEED]:
                title = clean_html(getattr(entry, "title", "") or "") or "Без заголовка"
                key = get_item_key(entry, feed_url, title)
                if is_seen(key):
                    continue
                summary = get_summary(entry)
                if not is_news_item(title, summary):
                    mark_seen(key)
                    continue
                fresh.append({
                    "entry": entry, "title": title,
                    "feed_url": feed_url, "key": key,
                })
            return fresh
        except asyncio.TimeoutError:
            print(f"⏱️ Таймаут RSS: {feed_url}")
        except Exception as exc:
            print(f"⚠️ Ошибка RSS {feed_url}: {type(exc).__name__}")
        return []


async def find_one_new_entry():
    feeds = list(RSS_FEEDS)
    random.shuffle(feeds)
    semaphore = asyncio.Semaphore(MAX_PARALLEL_FEEDS)
    tasks = [asyncio.create_task(fetch_fresh_from_feed(url, semaphore)) for url in feeds]

    try:
        while tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    fresh = task.result()
                except Exception:
                    continue
                if fresh:
                    for t in tasks:
                        t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    return random.choice(fresh)
        return None
    except asyncio.CancelledError:
        return None


# ============================================================
# Groq: генерация
# ============================================================

def rewrite_with_ai(title: str, summary: str):
    if not groq_client:
        print("⚠️ GROQ_API_KEY не задан.")
        return None

    prompt = f"""Ты — автор молодёжного Telegram-канала +VIBE NEWS.
Пиши живо, с лёгким юмором. Как будто рассказываешь другу.

ВАЖНО:
- Если заголовок или описание на английском, белорусском — ПЕРЕВЕДИ их на русский.
- Финальный пост — ТОЛЬКО на русском.
- Только одно событие. Используй только факты из источника. Ничего не выдумывай.
- Не пиши «новость», «сообщается», «по данным», «по информации».

СТИЛЬ:
- Живой, энергичный, с лёгким вайбом. Уместен сленг: «вайб», «топчик», «кринж», «кайф».
- В конце — короткий вопрос или лёгкая шутка для подписчиков, если уместно.
- Без кликбейта и громких неподтверждённых заявлений.

ФОРМАТ:
- 3-4 предложения, до 700 символов.
- РОВНО 2 эмодзи — не больше, не меньше.
- Начни с одного яркого эмодзи (🚀, 🔥, 💡, 🎉, 🌟, 🤯, 😎, 💥, 🎮, 🎵, 🧠, 🌍, ✨).
- Остальное эмодзи — по смыслу в тексте.
- Закончи полной фразой с точкой, вопросительным или восклицательным знаком.
- Без ссылок, хэштегов, заголовков.

Заголовок источника:
{title}

Описание источника:
{summary}

Напиши только готовый пост:"""

    for attempt in range(3):
        try:
            response = groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.85,
                max_tokens=1200,
                timeout=GROQ_TIMEOUT,
            )
            if not response.choices:
                print(f"⚠️ Groq: пустой ответ (попытка {attempt + 1})")
                continue

            choice = response.choices[0]
            text = (choice.message.content or "").strip()
            finish_reason = getattr(choice, "finish_reason", None)

            print(f"ℹ️ Groq attempt={attempt + 1}, finish_reason={finish_reason}, len={len(text)}")

            if finish_reason == "length":
                print("⚠️ Groq: обрезано лимитом")
                continue

            text = re.sub(r"https?://\S+", "", text).strip()
            if not text:
                print("⚠️ Groq: пустой текст после очистки")
                continue
            if text[-1] not in ".!?…»”":
                print(f"⚠️ Groq: текст не заканчивается на точку — {text[-50:]!r}")
                continue
            if len(text) > 1000:
                text = text[:997].rsplit(" ", 1)[0].rstrip() + "..."
            if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", text)) < 10:
                print(f"⚠️ Groq: слишком короткий — {text!r}")
                continue

            return text
        except Exception as exc:
            print(f"❌ Groq: {type(exc).__name__}: {exc}")
            return None
    return None


# ============================================================
# Telegram: отправка и модерация
# ============================================================

def build_keyboard(review_id: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🚀 Опубликовать", callback_data=f"publish:{review_id}"),
        InlineKeyboardButton("✏️ Изменить", callback_data=f"edit:{review_id}"),
        InlineKeyboardButton("⏭️ Дальше", callback_data=f"skip:{review_id}"),
    ]])


async def safe_edit(query, text: str):
    try:
        await query.edit_message_text(text)
    except Exception:
        try:
            await query.edit_message_caption(caption=text)
        except Exception:
            pass


async def send_post_for_review(app: Application, review_id: str, item: dict):
    message_text = f"📬 Пост на проверку\n\n{item['post_text']}"
    keyboard = build_keyboard(review_id)
    image_url = item.get("image_url")

    try:
        if image_url and len(item["post_text"]) <= 1024:
            await app.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=image_url,
                caption=message_text,
                reply_markup=keyboard,
            )
        else:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text=message_text[:4096],
                reply_markup=keyboard,
            )
    except Exception as exc:
        print(f"⚠️ Не удалось отправить фото: {exc}")
        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text=message_text[:4096],
            reply_markup=keyboard,
        )


async def _find_and_send_next_inner(app: Application, max_attempts: int):
    for attempt in range(1, max_attempts + 1):
        print(f"🔎 Попытка {attempt}/{max_attempts}")
        candidate = await find_one_new_entry()

        if not candidate:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text="😔 Новых подходящих публикаций не нашёл. Попробуй /reset и затем /next.",
            )
            await save_data()
            return False

        entry = candidate["entry"]
        title = candidate["title"]
        summary = get_summary(entry)
        source_url = getattr(entry, "link", "") or ""
        raw_summary = getattr(entry, "summary", "") or ""
        image_url = extract_image(entry, raw_summary)

        print(f"📝 Найдено: {title[:80]}")
        post_text = await asyncio.to_thread(rewrite_with_ai, title, summary)

        if not post_text:
            print(f"⏭️ AI не справился: {title[:80]}")
            mark_seen(candidate["key"])
            bump_stat("skipped")
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
        print(f"📩 Отправлено админу на проверку: {title}")
        return True

    await app.bot.send_message(
        chat_id=ADMIN_ID,
        text="😔 Не получилось подготовить пост из найденных материалов.",
    )
    return False


async def find_and_send_next(app: Application, max_attempts: int = MAX_ATTEMPTS):
    if _find_lock.locked():
        print("⚠️ Поиск уже идёт, пропускаю вызов.")
        return False

    try:
        async with _find_lock:
            return await asyncio.wait_for(
                _find_and_send_next_inner(app, max_attempts),
                timeout=FIND_TOTAL_TIMEOUT,
            )
    except asyncio.TimeoutError:
        print("⏱️ Общий таймаут поиска.")
        try:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text="⏱️ Поиск занял слишком много времени. Попробуй ещё раз.",
            )
        except Exception:
            pass
        return False


async def publish_to_channel(app: Application, item: dict):
    post_text = item["post_text"]
    image_url = item.get("image_url")

    if image_url and len(post_text) <= 1024:
        try:
            return await app.bot.send_photo(
                chat_id=CHANNEL_ID, photo=image_url, caption=post_text,
            )
        except Exception:
            pass
    return await app.bot.send_message(chat_id=CHANNEL_ID, text=post_text[:4096])


# ============================================================
# Команды
# ============================================================

async def require_admin(update: Update):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return False
    return True


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    await update.effective_message.reply_text(
        "Привет! Я готовлю посты для проверки.\n\n"
        "/next — найти пост\n"
        "/edit — изменить последний пост\n"
        "/reset — сбросить историю\n"
        "/stats — статистика"
    )


async def next_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    await update.effective_message.reply_text("🔎 Ищу материал...")
    asyncio.create_task(find_and_send_next(context.application))


async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return

    if not pending_queue:
        await update.effective_message.reply_text(
            "😔 Нет постов в очереди. Сначала /next."
        )
        return

    review_id = list(pending_queue.keys())[-1]
    item = pending_queue[review_id]

    context.user_data["editing_review_id"] = review_id

    await update.effective_message.reply_text(
        f"✏️ Отправь новый текст для поста:\n\n"
        f"📝 Текущий текст:\n{item['post_text']}\n\n"
        f"Отправь /cancel чтобы отменить."
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    if "editing_review_id" in context.user_data:
        del context.user_data["editing_review_id"]
        await update.effective_message.reply_text("❌ Редактирование отменено.")
    else:
        await update.effective_message.reply_text("Нечего отменять.")


async def handle_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return
    if "editing_review_id" not in context.user_data:
        return
    if not update.message or not update.message.text:
        return

    review_id = context.user_data.pop("editing_review_id")
    item = pending_queue.get(review_id)

    if not item:
        await update.message.reply_text("⚠️ Пост уже обработан. Начни заново с /next.")
        return

    new_text = update.message.text.strip()
    if len(new_text) < 5:
        await update.message.reply_text("⚠️ Слишком короткий текст. Попробуй ещё раз.")
        context.user_data["editing_review_id"] = review_id
        return

    item["post_text"] = new_text
    await save_data()

    await update.message.reply_text("✅ Текст обновлён. Вот новый пост:")
    await send_post_for_review(context.application, review_id, item)
    print(f"✏️ Отредактирован пост: {item['title'][:60]}")


async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    reset_seen()
    await save_data()
    await update.effective_message.reply_text(
        "🔄 История сброшена. Теперь /next найдёт свежие новости."
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    stats = _data.get("stats", {})
    sources = stats.get("sources", {})
    await update.effective_message.reply_text(
        "📊 Статистика\n\n"
        f"Опубликовано: {stats.get('published', 0)}\n"
        f"Пропущено: {stats.get('skipped', 0)}\n"
        f"Ожидают решения: {len(pending_queue)}\n"
        f"В seen set: {len(_seen_set)}\n"
        f"Источников с публикациями: {len(sources)}"
    )


async def review_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.from_user or query.from_user.id != ADMIN_ID:
        if query: await query.answer("Доступно только администратору.", show_alert=True)
        return

    await query.answer()
    try:
        action, review_id = query.data.split(":", 1)
    except (AttributeError, ValueError):
        await safe_edit(query, "Не удалось распознать действие.")
        return

    item = pending_queue.get(review_id)
    if not item:
        await safe_edit(query, "Эта публикация уже обработана.")
        return

    if action == "skip":
        pending_queue.pop(review_id, None)
        bump_stat("skipped")
        await save_data()
        await safe_edit(query, "⏭️ Пропущено. Ищу следующий материал...")
        asyncio.create_task(find_and_send_next(context.application))
        return

    if action == "edit":
        context.user_data["editing_review_id"] = review_id
        await safe_edit(
            query,
            f"✏️ Отправь новый текст для поста.\n\n"
            f"Текущий текст:\n{item['post_text']}\n\n"
            f"/cancel — отменить."
        )
        return

    if action == "publish":
        try:
            await publish_to_channel(context.application, item)
            pending_queue.pop(review_id, None)
            bump_stat("published")
            await save_data()
            await safe_edit(query, f"✅ Опубликовано: {item['title'][:60]}")
            print(f"✅ Опубликовано: {item['title']}")
        except Exception as exc:
            await safe_edit(query, "❌ Ошибка публикации. Смотри логи.")
            print(f"❌ Ошибка публикации: {type(exc).__name__}: {exc}")
    else:
        await safe_edit(query, "Неизвестное действие.")


# ============================================================
# Фоновые задачи и запуск
# ============================================================

async def schedule_loop(app: Application):
    while True:
        try:
            now = datetime.now(LOCAL_TZ)
            if now.hour in SCHEDULE_HOURS:
                slot = now.strftime("%Y-%m-%d-%H")
                if _data.get("last_schedule_slot") != slot:
                    _data["last_schedule_slot"] = slot
                    await save_data()
                    print(f"🕒 Плановый поиск, слот {slot}")
                    await find_and_send_next(app)
        except Exception as exc:
            print(f"❌ Ошибка расписания: {type(exc).__name__}: {exc}")
        await asyncio.sleep(30)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"❌ Ошибка Telegram: {context.error}")


async def main():
    acquire_lock()
    await load_data()

    application = Application.builder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("next", next_command))
    application.add_handler(CommandHandler("edit", edit_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("reset", reset_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CallbackQueryHandler(review_callback))

    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_text)
    )
    application.add_error_handler(error_handler)

    port = int(os.environ.get("PORT", "10000"))
    http_server = make_server("0.0.0.0", port, flask_app, threaded=True)
    http_thread = threading.Thread(
        target=http_server.serve_forever, name="health-server", daemon=True,
    )
    http_thread.start()

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
            asyncio.create_task(schedule_loop(application), name="schedule"),
            asyncio.create_task(stay_present_loop(), name="stay_present"), # <--- ДОБАВЛЕНО СЮДА
        ]
        print("✅ Бот запущен и готов к работе.")

        await stop_event.wait()
    finally:
        print("🛑 Останавливаю приложение...")
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

        if application.updater and application.updater.running:
            await application.updater.stop()
        if application.running:
            await application.stop()
        await application.shutdown()

        http_server.shutdown()
        http_server.server_close()
        http_thread.join(timeout=5)
        release_lock()
        print("✅ Приложение остановлено.")


if __name__ == "__main__":
    asyncio.run(main())
