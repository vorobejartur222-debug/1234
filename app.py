import os
import re
import html
import json
import base64
import random
import asyncio
import threading
import hashlib
import time
import uuid
import sys
import fcntl
import subprocess
from datetime import datetime

import requests
import feedparser
from flask import Flask
from groq import Groq
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)


# ---------- Защита от двойного запуска ----------

LOCK_FILE = "/tmp/bot.lock"


def acquire_lock():
    try:
        lock_fd = open(LOCK_FILE, "w")
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_fd.write(str(os.getpid()))
        lock_fd.flush()
        return lock_fd
    except (IOError, OSError):
        print("⚠️ Другой экземпляр уже запущен. Выхожу.")
        sys.exit(0)


# ---------- Настройки ----------

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
CHANNEL_ID = os.environ["CHANNEL_ID"]
ADMIN_ID = int(os.environ["ADMIN_ID"])
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.1-8b-instant")

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO = os.environ["GITHUB_REPO"].strip()
GITHUB_DATA_FILE = os.environ.get("GITHUB_DATA_FILE", "seen_news.json")

AUTO_PUBLISH_MINUTES = 60
SCHEDULE_HOURS = [10, 14, 18, 22]

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# ---------- RSS-ленты ----------

RSS_FEEDS = [
    "https://nplus1.ru/rss",
    "https://naked-science.ru/feed",
    "https://www.ixbt.com/export/news.rss",
    "https://hi-news.ru/feed/",
    "https://tproger.ru/feed/",
    "https://the-flow.ru/rss/",
    "https://www.sports.ru/rss/all.xml",
    "https://kulturomania.ru/rss/",
    "https://positime.ru/rss/",
    "https://www.positive.news/feed/",
    "https://www.optimistdaily.com/feed/",
]


# ---------- GitHub-хранилище ----------

GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

DEFAULT_DATA = {
    "items": [],
    "pending": {},
    "stats": {"published": 0, "skipped": 0, "sources": {}},
    "last_digest": "",
}

_data = dict(DEFAULT_DATA)
_data_sha = None
_data_lock = asyncio.Lock()
pending_queue = _data["pending"]


def _gh_url():
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_DATA_FILE}"


def _load_sync():
    global _data, _data_sha, pending_queue
    try:
        r = requests.get(_gh_url(), headers=GH_HEADERS, timeout=10)
        if r.status_code != 200:
            print(f"⚠️ GitHub GET {r.status_code}: {r.text[:200]}")
            _data = dict(DEFAULT_DATA)
            _data_sha = None
        else:
            payload = r.json()
            loaded = json.loads(base64.b64decode(payload["content"]).decode())
            loaded.setdefault("items", [])
            loaded.setdefault("pending", {})
            loaded.setdefault("stats", {"published": 0, "skipped": 0, "sources": {}})
            loaded.setdefault("last_digest", "")
            _data = loaded
            _data_sha = payload["sha"]
    except Exception as e:
        print(f"❌ load: {e}")
        _data = dict(DEFAULT_DATA)
        _data_sha = None

    pending_queue = _data["pending"]
    print(f"💾 Загружено: {len(_data['items'])} items, {len(pending_queue)} pending")
    return _data


def _save_sync(attempt=1):
    global _data_sha
    if attempt > 3:
        return False

    _data["pending"] = pending_queue
    try:
        content = json.dumps(_data, ensure_ascii=False, indent=2)
        payload = {
            "message": f"Update ({time.strftime('%Y-%m-%d %H:%M')})",
            "content": base64.b64encode(content.encode()).decode(),
            "committer": {"name": "bot", "email": "bot@vibe.news"},
        }
        if _data_sha:
            payload["sha"] = _data_sha

        r = requests.put(_gh_url(), headers=GH_HEADERS, json=payload, timeout=10)

        if r.status_code == 409:
            _load_sync()
            return _save_sync(attempt + 1)

        if r.status_code not in (200, 201):
            print(f"⚠️ GitHub PUT {r.status_code}: {r.text[:200]}")
            return False

        _data_sha = r.json()["content"]["sha"]
        return True
    except Exception as e:
        print(f"❌ save: {e}")
        return False


async def load_data():
    async with _data_lock:
        return await asyncio.to_thread(_load_sync)


async def save_data():
    try:
        async with _data_lock:
            return await asyncio.wait_for(
                asyncio.to_thread(_save_sync), timeout=30
            )
    except asyncio.TimeoutError:
        return False


def is_seen(key):
    return key in _data.get("items", [])


def mark_seen(key):
    items = _data.setdefault("items", [])
    if key not in items:
        items.append(key)
    if len(items) > 2000:
        _data["items"] = items[-2000:]


def bump_stat(key):
    stats = _data.setdefault("stats", {"published": 0, "skipped": 0, "sources": {}})
    stats[key] = stats.get(key, 0) + 1


def bump_source(feed_url):
    stats = _data.setdefault("stats", {"published": 0, "skipped": 0, "sources": {}})
    sources = stats.setdefault("sources", {})
    sources[feed_url] = sources.get(feed_url, 0) + 1


# ---------- Flask ----------

flask_app = Flask(__name__)


@flask_app.route("/")
@flask_app.route("/health")
def health():
    return "OK", 200


def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


# ---------- Обработка RSS ----------

def clean_html(raw):
    if not raw:
        return ""
    text = re.sub(r"<[^>]*>", " ", raw)
    text = html.unescape(text)
    text = re.sub(r"https?://\S+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def get_summary(entry):
    summary = getattr(entry, "summary", "") or ""
    if not summary:
        contents = getattr(entry, "content", None) or []
        if contents:
            summary = contents[0].get("value", "")
    return clean_html(summary)[:1200]


def get_item_key(entry, feed_url, title):
    source_id = getattr(entry, "id", None) or getattr(entry, "link", None) or f"{feed_url}:{title}"
    return hashlib.sha256(source_id.encode()).hexdigest()


def extract_image(entry, raw_summary=""):
    for field in ("media_content", "media_thumbnail"):
        for media in getattr(entry, field, None) or []:
            if media.get("url"):
                return media["url"]

    for enc in getattr(entry, "enclosures", None) or []:
        if "image" in enc.get("type", "") and enc.get("href"):
            return enc["href"]

    match = re.search(r"""<img[^>]+src=["']([^"']+)["']""", raw_summary or "", re.IGNORECASE)
    return match.group(1) if match else None


# ---------- Жёсткий фильтр ----------

BLOCK_MARKERS = [
    "туториал", "руководство", "как я ", "как сделать",
    "how to", "tutorial",
    "война", "убит", "погиб", "теракт", "катастрофа",
    "трагедия", "авария", "смерть", "умер",
]


def is_news_item(title, summary):
    text = f"{title} {summary}".lower()
    return not any(marker in text for marker in BLOCK_MARKERS)


# ---------- AI ----------

def rewrite_with_ai(title, summary):
    if not groq_client:
        return None

    prompt = f"""Ты — автор позитивного молодёжного Telegram-канала +VIBE NEWS.
Напиши короткий пост на русском по заголовку и описанию.
Пиши живо, неформально, с лёгким юмором.

СТИЛЬ:
- Сленг уместен: «вайб», «топчик», «жиза», «кринж», «пушка», «кайф».
- Задай вопрос подписчикам в конце, если уместно.
- Не пиши «новость», «сообщается», «по данным».

ПРАВИЛА:
- Начни с яркого эмодзи по смыслу (🚀, 🔥, 💡, 🎉, 🌟, 🤯, 😎, 💥, 🎮, 🎵, 🧠, 🌍, ✨).
- 6-10 эмодзи по ходу текста.
- 3-4 предложения, до 800 символов.
- Без ссылок, не выдумывай факты.

Заголовок: {title}
Описание: {summary}

Текст поста:"""

    try:
        r = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.9,
            max_tokens=300,
        )
        if not r.choices:
            return None
        result = r.choices[0].message.content
        if not result:
            return None
        result = result.strip()
        result = re.sub(r"https?://\S+", "", result).strip()
        if len(result) > 1000:
            result = result[:997].rstrip() + "..."
        if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", result)) < 10:
            return None
        return result
    except Exception as e:
        print(f"❌ Groq: {type(e).__name__}: {e}")
        return None


# ---------- Поиск ----------

_find_lock = asyncio.Lock()


async def find_one_new_entry():
    async with _find_lock:
        feeds = list(RSS_FEEDS)
        random.shuffle(feeds)

        for feed_url in feeds:
            try:
                feed = await asyncio.to_thread(feedparser.parse, feed_url)
                if not feed.entries:
                    continue

                fresh = []
                for entry in feed.entries:
                    title = clean_html(getattr(entry, "title", "") or "") or "Без заголовка"
                    key = get_item_key(entry, feed_url, title)
                    if is_seen(key):
                        continue

                    summary = get_summary(entry)
                    if not is_news_item(title, summary):
                        mark_seen(key)
                        continue

                    fresh.append({
                        "entry": entry,
                        "title": title,
                        "feed_url": feed_url,
                        "key": key,
                    })

                if fresh:
                    return random.choice(fresh)

            except Exception as e:
                print(f"❌ RSS {feed_url}: {type(e).__name__}: {e}")

        return None


# ---------- Утилиты ----------

async def safe_edit(query, text):
    try:
        await query.edit_message_text(text)
    except Exception:
        try:
            await query.edit_message_caption(caption=text)
        except Exception as e:
            print(f"⚠️ edit: {e}")


def build_keyboard(review_id):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🚀 Опубликовать", callback_data=f"publish:{review_id}"),
        InlineKeyboardButton("⏭️ Дальше", callback_data=f"skip:{review_id}"),
    ]])


# ---------- Отправка ----------

async def find_and_send_next(app, depth=0):
    if depth > 5:
        await app.bot.send_message(chat_id=ADMIN_ID, text="😔 Не нашёл подходящих новостей.")
        return

    candidate = await find_one_new_entry()

    if not candidate:
        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text="😔 Больше новых новостей нет.",
        )
        return

    entry = candidate["entry"]
    title = candidate["title"]
    summary = get_summary(entry)
    source_url = getattr(entry, "link", "") or ""
    raw_summary = getattr(entry, "summary", "") or ""
    image_url = extract_image(entry, raw_summary)

    post_text = await asyncio.to_thread(rewrite_with_ai, title, summary)

    if not post_text:
        print(f"⏭ AI не справился: {title[:60]}")
        mark_seen(candidate["key"])
        bump_stat("skipped")
        await save_data()
        return await find_and_send_next(app, depth + 1)

    mark_seen(candidate["key"])
    bump_source(candidate["feed_url"])
    review_id = uuid.uuid4().hex[:8]

    pending_queue[review_id] = {
        "title": title,
        "post_text": post_text,
        "image_url": image_url,
        "source_url": source_url,
        "created_at": time.time(),
    }
    await save_data()

    message_text = f"📬 Новость на проверку\n\n{post_text}"
    keyboard = build_keyboard(review_id)

    try:
        if image_url:
            await app.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=image_url,
                caption=message_text[:1024],
                reply_markup=keyboard,
            )
        else:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text=message_text[:4096],
                reply_markup=keyboard,
            )
    except Exception as e:
        print(f"⚠️ send: {e}")
        await app.bot.send_message(
            chat_id=ADMIN_ID,
            text=message_text[:4096],
            reply_markup=keyboard,
        )

    print(f"📩 Отправлено: {title}")


# ---------- Публикация ----------

async def publish_to_channel(app, item):
    post_text = item["post_text"]
    image_url = item.get("image_url")

    if image_url and len(post_text) <= 1024:
        try:
            msg = await app.bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=image_url,
                caption=post_text,
            )
        except Exception:
            msg = await app.bot.send_message(chat_id=CHANNEL_ID, text=post_text[:4096])
    else:
        msg = await app.bot.send_message(chat_id=CHANNEL_ID, text=post_text[:4096])

    try:
        await app.bot.set_message_reaction(
            chat_id=CHANNEL_ID,
            message_id=msg.message_id,
            reaction=["🔥", "❤️", "😎"],
        )
    except Exception:
        pass

    return msg


# ---------- Кнопки ----------

async def review_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or q.from_user.id != ADMIN_ID:
        return

    await q.answer()
    action, review_id = q.data.split(":", 1)
    item = pending_queue.get(review_id)

    if not item:
        await safe_edit(q, "Эта новость уже обработана.")
        return

    if action == "skip":
        del pending_queue[review_id]
        bump_stat("skipped")
        await save_data()
        await safe_edit(q, "⏭️ Пропущено. Ищу следующую...")
        await find_and_send_next(ctx.application)
        return

    try:
        await publish_to_channel(ctx.application, item)
        del pending_queue[review_id]
        bump_stat("published")
        await save_data()
        await safe_edit(q, f"✅ Опубликовано: {item['title'][:60]}")
        print(f"✅ Опубликовано: {item['title']}")
    except Exception as e:
        await safe_edit(q, "❌ Ошибка публикации. Смотри логи.")
        print(f"❌ publish: {e}")


# ---------- Авто-публикация через час ----------

async def auto_publish_loop(app):
    while True:
        try:
            now = time.time()
            to_remove = []
            for rid, item in list(pending_queue.items()):
                created = item.get("created_at", now)
                if now - created > AUTO_PUBLISH_MINUTES * 60:
                    try:
                        await publish_to_channel(app, item)
                        bump_stat("published")
                        to_remove.append(rid)
                        await app.bot.send_message(
                            chat_id=ADMIN_ID,
                            text=f"⏰ Авто-публикация: {item['title'][:60]}",
                        )
                    except Exception as e:
                        print(f"❌ auto-publish: {e}")
            if to_remove:
                for rid in to_remove:
                    pending_queue.pop(rid, None)
                await save_data()
            await asyncio.sleep(300)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"❌ auto_publish_loop: {e}")
            await asyncio.sleep(60)


# ---------- Планировщик по часам ----------

async def scheduler(app):
    sent_hours = set()
    while True:
        try:
            now = datetime.now()
            hour = now.hour

            if hour in SCHEDULE_HOURS and hour not in sent_hours and not pending_queue:
                sent_hours.add(hour)
                try:
                    await find_and_send_next(app)
                except Exception as e:
                    print(f"❌ scheduler: {e}")

            if hour not in SCHEDULE_HOURS:
                sent_hours.discard(hour)

            await asyncio.sleep(60)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"❌ scheduler: {e}")
            await asyncio.sleep(60)


# ---------- Дайджест ----------

async def digest_loop(app):
    while True:
        try:
            now = datetime.now()
            today = now.strftime("%Y-%m-%d")
            last_digest = _data.get("last_digest", "")

            if now.hour == 22 and last_digest != today:
                stats = _data.get("stats", {})
                text = (
                    f"📊 Дайджест за {today}\n\n"
                    f"✅ Опубликовано: {stats.get('published', 0)}\n"
                    f"⏭️ Пропущено: {stats.get('skipped', 0)}\n"
                    f"📋 В очереди: {len(pending_queue)}\n"
                )
                await app.bot.send_message(chat_id=ADMIN_ID, text=text)
                _data["last_digest"] = today
                await save_data()

            await asyncio.sleep(600)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"❌ digest: {e}")
            await asyncio.sleep(60)


# ---------- Команды ----------

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text(
        "Бот подключён.\n\n"
        "/now — прислать новость\n"
        "/pending — очередь\n"
        "/reset — очистить\n"
        "/stats — статистика"
    )


async def cmd_now(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if pending_queue:
        pending_queue.clear()
        await save_data()
        await update.message.reply_text("🗑 Очередь очищена. Ищу новую...")
    else:
        await update.message.reply_text("🔍 Ищу...")

    try:
        await find_and_send_next(ctx.application)
    except Exception as e:
        print(f"❌ /now: {e}")


async def cmd_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    if not pending_queue:
        await update.message.reply_text("✅ Очередь пуста.")
        return

    text = f"📋 В очереди: {len(pending_queue)}\n\n"
    for rid, item in pending_queue.items():
        text += f"• {rid}: {item['title'][:60]}\n"
    await update.message.reply_text(text[:4096])


async def cmd_reset(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    count = len(pending_queue)
    pending_queue.clear()
    await save_data()
    await update.message.reply_text(f"🗑 Очищено ({count}).")


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return

    stats = _data.get("stats", {})
    sources = stats.get("sources", {})
    top_sources = sorted(sources.items(), key=lambda x: -x[1])[:5]

    text = (
        f"📊 Статистика\n\n"
        f"✅ Опубликовано: {stats.get('published', 0)}\n"
        f"⏭️ Пропущено: {stats.get('skipped', 0)}\n"
        f"📋 В очереди: {len(pending_queue)}\n"
        f"👁 Просмотрено записей: {len(_data.get('items', []))}\n\n"
        f"🏆 Топ источников:\n"
    )
    for url, count in top_sources:
        short = url.split("//")[1].split("/")[0]
        text += f"• {short}: {count}\n"

    await update.message.reply_text(text[:4096])


# ---------- Запуск ----------

async def post_init(app: Application):
    await load_data()
    app.bot_data["scheduler"] = asyncio.create_task(scheduler(app))
    app.bot_data["auto_publish"] = asyncio.create_task(auto_publish_loop(app))
    app.bot_data["digest"] = asyncio.create_task(digest_loop(app))
    await app.bot.set_my_commands([
        ("start", "Запустить"),
        ("now", "Новость сейчас"),
        ("pending", "Очередь"),
        ("reset", "Очистить"),
        ("stats", "Статистика"),
    ])
    print("✅ Бот готов")


async def post_shutdown(app: Application):
    for key in ("scheduler", "auto_publish", "digest"):
        task = app.bot_data.get(key)
        if task:
            task.cancel()
    await asyncio.gather(
        *[app.bot_data.get(k) for k in ("scheduler", "auto_publish", "digest") if app.bot_data.get(k)],
        return_exceptions=True,
    )


def main():
    try:
        result = subprocess.run(
            ["ps", "-ef"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        python_procs = [
            line for line in result.stdout.split("\n")
            if "python" in line and "app.py" in line
        ]
        print(f"🔍 Процессов python app.py: {len(python_procs)}")
        for p in python_procs:
            print(f"   {p}")
    except Exception as e:
        print(f"⚠️ ps failed: {e}")

    acquire_lock()
    print(f"🔒 PID = {os.getpid()}")

    threading.Thread(target=run_flask, daemon=True).start()

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("now", cmd_now))
    app.add_handler(CommandHandler("pending", cmd_pending))
    app.add_handler(CommandHandler("reset", cmd_reset))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CallbackQueryHandler(review_callback, pattern=r"^(publish|skip):[a-f0-9]+$"))

    app.run_polling(
        drop_pending_updates=True,
        poll_interval=1.0,
        timeout=1,
    )


if __name__ == "__main__":
    main()
