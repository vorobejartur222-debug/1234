import asyncio
import base64
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
from datetime import datetime, timezone
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import feedparser
import requests
from flask import Flask
from groq import Groq
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
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

GITHUB_TOKEN = required_env("GITHUB_TOKEN")
GITHUB_REPO = required_env("GITHUB_REPO").strip()
GITHUB_DATA_FILE = os.environ.get("GITHUB_DATA_FILE", "seen_news.json").strip()

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile").strip()

TIMEZONE_NAME = os.environ.get("TIMEZONE", "UTC").strip()
try:
    LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError as exc:
    raise RuntimeError(f"Неизвестная временная зона: {TIMEZONE_NAME}") from exc

MAX_PARALLEL_FEEDS = 10
FEED_TOTAL_TIMEOUT = 8
MAX_ENTRIES_PER_FEED = 15
MAX_ATTEMPTS = 5
FIND_TOTAL_TIMEOUT = 60
GROQ_TIMEOUT = 20

SCHEDULE_HOURS = {10, 14, 18, 22}

if "/" not in GITHUB_REPO:
    raise RuntimeError("GITHUB_REPO должен иметь формат owner/repository")

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None

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
    "https://freenewsapi.ai/v1/rss?host=www.kinopoisk.ru&size=50",
]

# ============================================================
# Данные и GitHub-хранилище
# ============================================================

def fresh_default_data():
    return {
        "items": [],
        "pending": {},
        "stats": {"published": 0, "skipped": 0, "sources": {}},
        "last_schedule_slot": "",
    }

_data = fresh_default_data()
_data_sha = None
_data_lock = asyncio.Lock()
_find_lock = asyncio.Lock()
_seen_set = set()
pending_queue = _data["pending"]

GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

_encoded_repo = quote(GITHUB_REPO, safe="/")
_encoded_data_file = quote(GITHUB_DATA_FILE, safe="/")
GITHUB_CONTENTS_URL = f"https://api.github.com/repos/{_encoded_repo}/contents/{_encoded_data_file}"

def _load_sync():
    global _data, _data_sha, pending_queue, _seen_set
    try:
        response = requests.get(GITHUB_CONTENTS_URL, headers=GH_HEADERS, timeout=15)
        if response.status_code == 404:
            print("ℹ️ Файл данных GitHub не найден; будет создан при первом сохранении.")
            _data = fresh_default_data()
            _data_sha = None
        elif response.status_code != 200:
            print(f"⚠️ GitHub GET {response.status_code}: {response.text[:200]}")
            _data = fresh_default_data()
            _data_sha = None
        else:
            payload = response.json()
            file_content = base64.b64decode(payload["content"]).decode("utf-8")
            loaded = json.loads(file_content)

            _data = fresh_default_data()
            _data.update(loaded)

            if not isinstance(_data.get("items"), list): _data["items"] = []
            if not isinstance(_data.get("pending"), dict): _data["pending"] = {}
            if not isinstance(_data.get("stats"), dict): _data["stats"] = fresh_default_data()["stats"]

            _data["stats"].setdefault("published", 0)
            _data["stats"].setdefault("skipped", 0)
            _data["stats"].setdefault("sources", {})
            _data.setdefault("last_schedule_slot", "")
            _data_sha = payload.get("sha")
    except Exception as exc:
        print(f"❌ Ошибка загрузки данных из GitHub: {type(exc).__name__}: {exc}")
        _data = fresh_default_data()
        _data_sha = None

    pending_queue = _data["pending"]
    _seen_set = set(_data.get("items", []))
    print(f"💾 Загружено: {len(_data['items'])} items, {len(pending_queue)} pending")
    return _data

def _save_sync():
    global _data_sha
    _data["pending"] = pending_queue
    for attempt in range(3):
        try:
            content = json.dumps(_data, ensure_ascii=False, indent=2)
            payload = {
                "message": f"Update bot data {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC",
                "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
                "committer": {"name": "News Bot", "email": "bot@example.com"},
            }
            if _data_sha:
                payload["sha"] = _data_sha

            response = requests.put(GITHUB_CONTENTS_URL, headers=GH_HEADERS, json=payload, timeout=20)

            if response.status_code in (200, 201):
                _data_sha = response.json().get("content", {}).get("sha")
                print(f"✅ Данные сохранены в GitHub (sha: {_data_sha[:8]}...)")
                return True

            if response.status_code == 409 and attempt < 2:
                current = requests.get(GITHUB_CONTENTS_URL, headers=GH_HEADERS, timeout=15)
                if current.status_code == 200:
                    _data_sha = current.json().get("sha")
                    continue
                if current.status_code == 404:
                    _data_sha = None
                    continue

            print(f"⚠️ GitHub PUT {response.status_code}: {response.text[:200]}")
            return False
        except Exception as exc:
            print(f"❌ Ошибка сохранения в GitHub: {type(exc).__name__}: {exc}")
            return False
    return False

async def load_data():
    async with _data_lock:
        return await asyncio.to_thread(_load_sync)

async def save_data():
    async with _data_lock:
        try:
            return await asyncio.wait_for(asyncio.to_thread(_save_sync), timeout=15)
        except asyncio.TimeoutError:
            print("⚠️ Тайм-аут сохранения данных в GitHub.")
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
    stats = _data.setdefault("stats", fresh_default_data()["stats"])
    stats[stat_name] = stats.get(stat_name, 0) + 1

def bump_source(feed_url: str):
    stats = _data.setdefault("stats", fresh_default_data()["stats"])
    sources = stats.setdefault("sources", {})
    sources[feed_url] = sources.get(feed_url, 0) + 1

def reset_seen():
    global _seen_set
    _seen_set.clear()
    _data["items"] = []
    _data["stats"] = fresh_default_data()["stats"]
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
# RSS: обработка и фильтр
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
    # политика (чтобы не забивать канал)
    "путин", "зеленск", "трамп", "байден", "нато", "санкц", "кремл", "госдум", "минобороны",
]

def is_news_item(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    return not any(marker in text for marker in BLOCK_MARKERS)

def parse_feed_sync(feed_url: str):
    response = requests.get(
        feed_url,
        headers={"User-Agent": "VibeNewsBot/1.0"},
        timeout=(3, 5)
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
                fresh.append({"entry": entry, "title": title, "feed_url": feed_url, "key": key})
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

    prompt = f"""Ты — редактор молодёжного Telegram-канала +VIBE NEWS.
Напиши короткий, связный и содержательный пост на русском языке.

ВАЖНО:
- Если заголовок или описание на английском — ПЕРЕВЕДИ их на русский и напиши пост на русском.
- Рассказывай только об одном событии.
- Используй только факты из материалов ниже. Ничего не выдумывай.

Стиль:
- Дружелюбный и спокойный, немного серьёзнее развлекательного.
- Без навязчивого сленга и кликбейта.
- Простые слова, естественные фразы.

Формат:
- 2–3 законченных предложения.
- Не более 500 символов.
- Начни с одного уместного эмодзи.
- Закончи полной фразой с точкой.
- Без ссылок, хэштегов, заголовков.

Заголовок источника:
{title}

Описание источника:
{summary}

Напиши только готовый пост:"""

    for _ in range(2):
        try:
            response = groq_client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
                max_tokens=500,
                timeout=GROQ_TIMEOUT,
            )
            if not response.choices:
                continue

            choice = response.choices[0]
            text = (choice.message.content or "").strip()

            if getattr(choice, "finish_reason", None) == "length":
                continue

            text = re.sub(r"https?://\S+", "", text).strip()
            if not text or text[-1] not in ".!?…»”":
                continue
            if len(text) > 700:
                text = text[:697].rsplit(" ", 1)[0].rstrip() + "..."
            if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", text)) < 10:
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
            "image_url": image_url, "source_url": source_url, "created_at": time.time(),
        }
        await save_data()

        message_text = f"📬 Пост на проверку\n\n{post_text}"
        keyboard = build_keyboard(review_id)

        try:
            if image_url and len(post_text) <= 1024:
                await app.bot.send_photo(
                    chat_id=ADMIN_ID, photo=image_url,
                    caption=message_text, reply_markup=keyboard,
                )
            else:
                await app.bot.send_message(
                    chat_id=ADMIN_ID, text=message_text[:4096],
                    reply_markup=keyboard,
                )
        except Exception as exc:
            print(f"⚠️ Не удалось отправить фото: {exc}")
            await app.bot.send_message(
                chat_id=ADMIN_ID, text=message_text[:4096],
                reply_markup=keyboard,
            )

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
        "/now — то же, что /next\n"
        "/reset — сбросить историю\n"
        "/stats — статистика\n"
        "/help — список команд"
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    await update.effective_message.reply_text(
        "/next — найти следующую публикацию\n"
        "/now — то же, что /next\n"
        "/reset — сбросить seen set\n"
        "/stats — показать статистику\n"
        "Публикуйте или пропускайте пост кнопками под сообщением."
    )

async def next_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    await update.effective_message.reply_text("🔎 Ищу материал...")
    asyncio.create_task(find_and_send_next(context.application))

async def now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await next_command(update, context)

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_admin(update): return
    reset_seen()
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
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("next", next_command))
    application.add_handler(CommandHandler("now", now_command))
    application.add_handler(CommandHandler("reset", reset_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CallbackQueryHandler(review_callback))
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
