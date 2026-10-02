import os
import re
import html
import json
import base64
import random
import asyncio
import threading
import hashlib
import calendar
import time

import requests
import feedparser
from flask import Flask
from groq import Groq
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)


# ---------- Настройки ----------

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
ADMIN_ID_RAW = os.environ.get("ADMIN_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
GITHUB_REPO = os.environ.get("GITHUB_REPO")  # e.g. "user/repo"
GITHUB_DATA_FILE = os.environ.get("GITHUB_DATA_FILE", "seen_news.json")

if not TELEGRAM_TOKEN:
    raise RuntimeError("В Render не задана переменная TELEGRAM_TOKEN")
if not CHANNEL_ID:
    raise RuntimeError("В Render не задана переменная CHANNEL_ID")
if not ADMIN_ID_RAW:
    raise RuntimeError("В Render не задана переменная ADMIN_ID")
if not GITHUB_TOKEN:
    raise RuntimeError("В Render не задана переменная GITHUB_TOKEN")
if not GITHUB_REPO:
    raise RuntimeError("В Render не задана переменная GITHUB_REPO")

try:
    ADMIN_ID = int(ADMIN_ID_RAW)
except ValueError:
    raise RuntimeError("ADMIN_ID должен быть числовым Telegram ID")

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# ---------- RSS-ленты ----------

RSS_FEEDS = [
    "https://nplus1.ru/rss",
    "https://naked-science.ru/feed",
    "https://www.ixbt.com/export/news.rss",
    "https://tproger.ru/feed/",
    "https://the-flow.ru/rss/",
    "https://www.sports.ru/rss/all.xml",
    "https://kulturomania.ru/rss/",
    "https://hi-news.ru/feed/",
    "https://russian.rt.com/rss/",
    "https://lenta.ru/rss/",
    "https://news.mail.ru/rss/",
]


# ---------- GitHub-хранилище (JSON) ----------

GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

_data_cache = None
_data_sha = None
_data_lock = asyncio.Lock()


def _gh_url(path=""):
    return f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"


def _load_data_sync():
    """Читает JSON из GitHub. Возвращает (data, sha)."""
    global _data_cache, _data_sha

    response = requests.get(
        _gh_url(GITHUB_DATA_FILE),
        headers=GH_HEADERS,
        timeout=15,
    )

    if response.status_code == 404:
        # Файла нет — создаём пустую структуру
        default = {"items": [], "last_feed_index": 0}
        return default, None

    response.raise_for_status()
    payload = response.json()

    content = base64.b64decode(payload["content"]).decode("utf-8")
    data = json.loads(content)
    sha = payload["sha"]

    _data_cache = data
    _data_sha = sha
    return data, sha


def _save_data_sync(data, sha=None):
    """Сохраняет JSON в GitHub."""
    global _data_cache, _data_sha

    content = json.dumps(data, ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    payload = {
        "message": f"Update seen_news ({time.strftime('%Y-%m-%d %H:%M')})",
        "content": encoded,
        "committer": {
            "name": "vibe-news-bot",
            "email": "bot@vibe.news",
        },
    }

    if sha:
        payload["sha"] = sha

    response = requests.put(
        _gh_url(GITHUB_DATA_FILE),
        headers=GH_HEADERS,
        json=payload,
        timeout=15,
    )

    if response.status_code == 409:
        # Конфликт: кто-то другой обновил файл — перечитываем и пробуем снова
        print("⚠️ Конфликт при сохранении в GitHub, перечитываю...")
        data, new_sha = _load_data_sync()
        return _save_data_sync(data, new_sha)

    response.raise_for_status()
    result = response.json()

    _data_cache = data
    _data_sha = result["content"]["sha"]
    return True


async def load_data():
    """Асинхронно читает данные."""
    async with _data_lock:
        return await asyncio.to_thread(_load_data_sync)


async def save_data(data):
    """Асинхронно сохраняет данные."""
    global _data_sha
    async with _data_lock:
        return await asyncio.to_thread(_save_data_sync, data, _data_sha)


def is_seen(item_key):
    if _data_cache is None:
        return False
    return item_key in _data_cache.get("items", [])


def mark_seen(item_key):
    if _data_cache is None:
        return
    items = _data_cache.setdefault("items", [])
    if item_key not in items:
        items.append(item_key)


def get_last_feed_index():
    if _data_cache is None:
        return 0
    return int(_data_cache.get("last_feed_index", 0) or 0)


def set_last_feed_index(index):
    if _data_cache is None:
        return
    _data_cache["last_feed_index"] = index


# ---------- Flask для Render ----------

flask_app = Flask(__name__)


@flask_app.route("/")
def home():
    return "Bot is running", 200


@flask_app.route("/health")
def health():
    return "OK", 200


def run_flask():
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port)


# ---------- Обработка RSS ----------

def clean_html(raw_text):
    if not raw_text:
        return ""
    text = re.sub(r"<[^>]*>", " ", raw_text)
    text = html.unescape(text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def get_summary(entry):
    summary = getattr(entry, "summary", "") or ""
    if not summary:
        contents = getattr(entry, "content", None) or []
        if contents:
            summary = contents[0].get("value", "")
    return clean_html(summary)[:1200]


def get_item_key(entry, feed_url, title):
    source_id = (
        getattr(entry, "id", None)
        or getattr(entry, "link", None)
        or f"{feed_url}:{title}"
    )
    return hashlib.sha256(source_id.encode("utf-8")).hexdigest()


def get_title_hash(title):
    normalized = re.sub(r"\W+", "", title.lower())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def extract_image(entry, summary_html=""):
    for field in ("media_content", "media_thumbnail"):
        for media in getattr(entry, field, None) or []:
            image_url = media.get("url")
            if image_url:
                return image_url

    for enclosure in getattr(entry, "enclosures", None) or []:
        if "image" in enclosure.get("type", ""):
            image_url = enclosure.get("href")
            if image_url:
                return image_url

    match = re.search(
        r"""<img[^>]+src=["']([^"']+)["']""",
        summary_html or "",
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else None


# ---------- Фильтр не-новостей ----------

NON_NEWS_MARKERS = [
    "туториал", "руководство", "как я ", "как сделать",
    "мнение", "колонка", "перевод", "личный опыт",
    "урок", "гайд", "how to", "tutorial", "заметки",
    "мой проект", "я решил", "я сделал",
]


def is_news_item(title, summary):
    text = f"{title} {summary}".lower()
    for marker in NON_NEWS_MARKERS:
        if marker in text:
            return False
    return True


# ---------- AI-текст ----------

def rewrite_with_ai(title, summary):
    if not groq_client:
        print("⚠️ GROQ_API_KEY не задан — AI недоступен")
        return None

    prompt = f"""Ты — автор позитивного молодёжного Telegram-канала.
Напиши самостоятельный короткий пост на русском языке по заголовку и описанию.
Пиши живо, неформально, с лёгким юмором.

ПРАВИЛА ОФОРМЛЕНИЯ:
- Начни пост с одного яркого эмодзи, который подходит по смыслу новости (например: 🚀, 🔥, 💡, 🎉, 🌟, 🤯, 😎, 💥, 🎮, 🎵, 🧠, 🌍, ✨).
- Используй ещё 3-5 эмодзи по ходу текста, чтобы оживить его. Эмодзи должны подходить по смыслу, не ставь их наугад.
- Не более 3-4 предложений.
- Длина итогового текста — не более 800 символов.
- Не добавляй ссылки и не придумывай факты.
- Раскрой смысл новости, а не просто повтори заголовок.

Заголовок: {title}
Описание: {summary}

Текст поста:"""

    try:
        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.9,
            max_tokens=300,
        )
        if not response.choices:
            return None
        result = response.choices[0].message.content
        if not result:
            return None
        result = re.sub(r"https?://\S+", "", result.strip()).strip()
        if len(result) > 1000:
            result = result[:997].rstrip() + "..."
        if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", result)) < 10:
            return None
        return result
    except Exception as error:
        print(f"❌ Ошибка Groq: {type(error).__name__}: {error}")
        return None


def make_post_text(title, summary):
    ai_text = rewrite_with_ai(title, summary)
    if ai_text:
        return ai_text
    if summary:
        print(f"⚠️ Использую описание RSS вместо AI: {title}")
        fallback = f"🔥 {title}\n\n{summary} ✨"
        return fallback[:1000]
    return None


# ---------- Поиск одной новой записи (случайная лента) ----------

_find_lock = asyncio.Lock()


async def find_one_new_entry():
    async with _find_lock:
        total_feeds = len(RSS_FEEDS)
        feed_indices = list(range(total_feeds))
        random.shuffle(feed_indices)

        for feed_index in feed_indices:
            feed_url = RSS_FEEDS[feed_index]

            try:
                feed = await asyncio.to_thread(feedparser.parse, feed_url)
                if not feed.entries:
                    continue

                fresh_entries = []
                for entry in feed.entries:
                    title = clean_html(
                        getattr(entry, "title", "") or ""
                    ) or "Без заголовка"

                    item_key = get_item_key(entry, feed_url, title)
                    if is_seen(item_key):
                        continue

                    summary = get_summary(entry)
                    if not is_news_item(title, summary):
                        print(f"⏭ Пропускаю не-новость: {title[:80]}")
                        mark_seen(item_key)
                        continue

                    fresh_entries.append({
                        "entry": entry,
                        "title": title,
                        "feed_url": feed_url,
                        "item_key": item_key,
                    })

                if not fresh_entries:
                    continue

                chosen = random.choice(fresh_entries)
                set_last_feed_index((feed_index + 1) % total_feeds)
                await save_data(_data_cache)
                print(f"📡 Источник: {feed_url}")
                return chosen

            except Exception as error:
                print(
                    f"❌ Ошибка чтения RSS {feed_url}: "
                    f"{type(error).__name__}: {error}"
                )

        return None


# ---------- Отправка новости админу ----------

async def send_for_review(application, item):
    item_id = item["id"]
    message_text = (
        f"📬 Новость на проверку\n\n"
        f"{item['post_text']}\n\n"
        f"Источник: {item['source_url'] or 'ссылка не указана'}"
    )

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Опубликовать",
                callback_data=f"publish:{item_id}",
            ),
            InlineKeyboardButton(
                "⏭ Пропустить",
                callback_data=f"skip:{item_id}",
            ),
        ]
    ])

    image_url = item["image_url"]
    if image_url:
        try:
            await application.bot.send_photo(
                chat_id=ADMIN_ID,
                photo=image_url,
                caption=message_text[:1024],
                reply_markup=keyboard,
            )
        except Exception as photo_error:
            print(f"⚠️ Фото не отправилось: {photo_error}")
            await application.bot.send_message(
                chat_id=ADMIN_ID,
                text=message_text[:4096],
                reply_markup=keyboard,
            )
    else:
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text=message_text[:4096],
            reply_markup=keyboard,
        )

    print(f"📩 Новость отправлена админу: {item['title']}")


# ---------- Вспомогательная функция ----------

async def find_and_send_next(application):
    candidate = await find_one_new_entry()

    if not candidate:
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text="😔 Больше новых новостей нет. Пришлю, когда появятся.",
        )
        return

    entry = candidate["entry"]
    title = candidate["title"]
    summary = get_summary(entry)
    source_url = getattr(entry, "link", "") or ""
    raw_summary = getattr(entry, "summary", "") or ""
    image_url = extract_image(entry, raw_summary)

    post_text = await asyncio.to_thread(make_post_text, title, summary)

    if not post_text:
        await application.bot.send_message(
            chat_id=ADMIN_ID,
            text="😔 Не удалось сгенерировать текст. Нажми /now.",
        )
        return

    # Помечаем как просмотренное в GitHub JSON
    mark_seen(candidate["item_key"])
    await save_data(_data_cache)

    # Отправляем админу напрямую (без SQLite)
    await application.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"📬 Новость на проверку\n\n{post_text}\n\n"
             f"Источник: {source_url or 'ссылка не указана'}",
    )

    # Сохраняем во временную переменную для кнопок
    # (можно хранить в bot_data)
    application.bot_data["last_candidate"] = {
        "title": title,
        "post_text": post_text,
        "image_url": image_url,
        "source_url": source_url,
    }


# ---------- Проверка кнопок ----------

async def review_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    if not query:
        return

    if query.from_user.id != ADMIN_ID:
        await query.answer("Только для админа.", show_alert=True)
        return

    await query.answer()

    try:
        action, _ = query.data.split(":", 1)
    except (ValueError, AttributeError):
        return

    candidate = context.bot_data.get("last_candidate")

    if action == "skip":
        await query.edit_message_text("⏭ Пропущено.")
        context.bot_data["last_candidate"] = None
        try:
            await find_and_send_next(context.application)
        except Exception as error:
            print(f"❌ Ошибка после skip: {error}")
        return

    if action == "publish":
        if not candidate:
            await query.edit_message_text("⚠️ Нет данных для публикации.")
            return

        try:
            post_text = candidate["post_text"]
            image_url = candidate["image_url"]

            if image_url and len(post_text) <= 1024:
                try:
                    await context.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=image_url,
                        caption=post_text,
                    )
                except Exception:
                    await context.bot.send_message(
                        chat_id=CHANNEL_ID,
                        text=post_text[:4096],
                    )
            else:
                await context.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=post_text[:4096],
                )

            await query.edit_message_text(
                f"✅ Опубликовано: {candidate['title'][:60]}"
            )
            print(f"✅ Опубликовано: {candidate['title']}")
            context.bot_data["last_candidate"] = None

        except Exception as error:
            await query.edit_message_text(
                "❌ Не удалось опубликовать. Смотри логи Render."
            )
            print(f"❌ Ошибка публикации: {error}")


# ---------- Планировщик ----------

async def news_scheduler(application: Application):
    while True:
        try:
            try:
                await find_and_send_next(application)
            except Exception as error:
                print(f"❌ Ошибка планировщика: {error}")

            # Раз в 2 часа
            await asyncio.sleep(7200)

        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(f"❌ Ошибка планировщика: {error}")
            await asyncio.sleep(60)


# ---------- Команды ----------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Нет доступа.")
        return
    await update.message.reply_text(
        "Бот подключён. Новости приходят сюда.\n\n"
        "/now — прислать новость сейчас.\n"
        "/reset — сбросить последнюю."
    )


async def force_fetch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    if update.effective_user.id != ADMIN_ID:
        return

    await update.message.reply_text("🔍 Ищу свежую новость...")
    try:
        await find_and_send_next(context.application)
    except Exception as error:
        print(f"❌ Ошибка в /now: {error}")


async def reset_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    if update.effective_user.id != ADMIN_ID:
        return
    context.bot_data["last_candidate"] = None
    await update.message.reply_text("🗑 Сброшено. Нажми /now.")


# ---------- Запуск ----------

async def post_init(application: Application):
    # Загружаем данные из GitHub при старте
    await load_data()
    print(f"✅ Загружено {len(_data_cache.get('items', []))} просмотренных новостей")

    application.bot_data["scheduler_task"] = asyncio.create_task(
        news_scheduler(application)
    )
    await application.bot.set_my_commands([
        ("start", "Запустить бота"),
        ("now", "Прислать новость сейчас"),
        ("reset", "Сбросить последнюю"),
    ])


async def post_shutdown(application: Application):
    task = application.bot_data.get("scheduler_task")
    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def main():
    threading.Thread(target=run_flask, daemon=True).start()

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("now", force_fetch))
    application.add_handler(CommandHandler("reset", reset_pending))
    application.add_handler(
        CallbackQueryHandler(review_callback, pattern=r"^(publish|skip):\d+$")
    )

    application.run_polling()


if __name__ == "__main__":
    main()
