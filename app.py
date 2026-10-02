import os
import re
import html
import asyncio
import threading
import sqlite3
import hashlib
import calendar

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
GROQ_MODEL = os.environ.get(
    "GROQ_MODEL",
    "llama-3.3-70b-versatile",
)
DB_PATH = os.environ.get("DB_PATH", "news_bot.sqlite3")

if not TELEGRAM_TOKEN:
    raise RuntimeError("В Render не задана переменная TELEGRAM_TOKEN")

if not CHANNEL_ID:
    raise RuntimeError("В Render не задана переменная CHANNEL_ID")

if not ADMIN_ID_RAW:
    raise RuntimeError("В Render не задана переменная ADMIN_ID")

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
]


# ---------- База данных ----------

def get_db():
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def init_db():
    with get_db() as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_key TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                post_text TEXT NOT NULL,
                image_url TEXT,
                source_url TEXT,
                status TEXT NOT NULL,
                notified INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        db.execute("""
            UPDATE news
            SET status = 'pending'
            WHERE status = 'publishing'
        """)


def has_seen_item(item_key):
    with get_db() as db:
        row = db.execute(
            "SELECT 1 FROM news WHERE item_key = ?",
            (item_key,),
        ).fetchone()
        return row is not None


def mark_as_seen(item_key, title):
    with get_db() as db:
        db.execute(
            """
            INSERT OR IGNORE INTO news
                (item_key, title, post_text, status)
            VALUES (?, ?, '', 'skipped')
            """,
            (item_key, title),
        )


def create_pending_item(item_key, title, post_text, image_url, source_url):
    with get_db() as db:
        cursor = db.execute(
            """
            INSERT OR IGNORE INTO news
                (item_key, title, post_text, image_url, source_url, status)
            VALUES (?, ?, ?, ?, ?, 'pending')
            """,
            (item_key, title, post_text, image_url, source_url),
        )

        if cursor.rowcount == 0:
            row = db.execute(
                "SELECT id FROM news WHERE item_key = ?",
                (item_key,),
            ).fetchone()
            return row["id"] if row else None

        return cursor.lastrowid


def get_pending_item():
    with get_db() as db:
        return db.execute(
            """
            SELECT *
            FROM news
            WHERE status = 'pending'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()


def set_notified(item_id):
    with get_db() as db:
        db.execute(
            "UPDATE news SET notified = 1 WHERE id = ?",
            (item_id,),
        )


def set_item_status(item_id, status):
    with get_db() as db:
        db.execute(
            "UPDATE news SET status = ? WHERE id = ?",
            (status, item_id),
        )


def item_is_pending(item_id):
    with get_db() as db:
        row = db.execute(
            "SELECT status FROM news WHERE id = ?",
            (item_id,),
        ).fetchone()
        return bool(row and row["status"] == "pending")


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


def get_timestamp(entry):
    parsed_date = (
        getattr(entry, "published_parsed", None)
        or getattr(entry, "updated_parsed", None)
    )

    if parsed_date:
        try:
            return calendar.timegm(parsed_date)
        except (TypeError, ValueError, OverflowError):
            pass

    return 0


def get_item_key(entry, feed_url, title):
    source_id = (
        getattr(entry, "id", None)
        or getattr(entry, "link", None)
        or f"{feed_url}:{title}"
    )
    return hashlib.sha256(source_id.encode("utf-8")).hexdigest()


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
            print("⚠️ Groq вернул пустой ответ")
            return None

        result = response.choices[0].message.content

        if not result:
            print("⚠️ Groq вернул пустой текст")
            return None

        result = re.sub(r"https?://\S+", "", result.strip()).strip()

        if len(result) > 1000:
            print(f"⚠️ AI вернул слишком длинный текст ({len(result)}), обрезаю")
            result = result[:997].rstrip() + "..."

        if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", result)) < 10:
            print(f"⚠️ Ответ Groq слишком короткий: {result!r}")
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


# ---------- Поиск одной новой записи ----------

_last_feed_index = 0


async def find_one_new_entry():
    global _last_feed_index

    total_feeds = len(RSS_FEEDS)
    ordered_indices = [
        (_last_feed_index + i) % total_feeds
        for i in range(total_feeds)
    ]

    for feed_index in ordered_indices:
        feed_url = RSS_FEEDS[feed_index]

        try:
            feed = await asyncio.to_thread(feedparser.parse, feed_url)

            for entry in feed.entries:
                title = clean_html(
                    getattr(entry, "title", "") or ""
                ) or "Без заголовка"

                item_key = get_item_key(entry, feed_url, title)

                if has_seen_item(item_key):
                    continue

                summary = get_summary(entry)

                if not is_news_item(title, summary):
                    print(f"⏭ Пропускаю не-новость: {title[:80]}")
                    mark_as_seen(item_key, title)
                    continue

                _last_feed_index = (feed_index + 1) % total_feeds
                print(f"📡 Источник: {feed_url}")

                return {
                    "entry": entry,
                    "title": title,
                    "feed_url": feed_url,
                    "item_key": item_key,
                }

        except Exception as error:
            print(
                f"❌ Ошибка чтения RSS {feed_url}: "
                f"{type(error).__name__}: {error}"
            )

    return None


# ---------- Отправка новости админу на проверку ----------

async def send_for_review(application, item):
    item_id = item["id"]

    if not item_is_pending(item_id):
        return

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
            print(
                f"⚠️ Не удалось отправить превью с фото: "
                f"{type(photo_error).__name__}: {photo_error}"
            )
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

    set_notified(item_id)
    print(f"📩 Новость отправлена админу на проверку: {item['title']}")


# ---------- Проверка кнопок ----------

async def review_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    if not query:
        return

    if query.from_user.id != ADMIN_ID:
        await query.answer(
            "Эти кнопки доступны только администратору.",
            show_alert=True,
        )
        return

    await query.answer()

    try:
        action, item_id_raw = query.data.split(":", 1)
        item_id = int(item_id_raw)
    except (ValueError, AttributeError):
        await query.answer("Некорректная команда.", show_alert=True)
        return

    with get_db() as db:
        item = db.execute(
            "SELECT * FROM news WHERE id = ?",
            (item_id,),
        ).fetchone()

    if not item or item["status"] != "pending":
        await query.edit_message_text("Эта новость уже обработана.")
        return

    if action == "skip":
        set_item_status(item_id, "skipped")
        await query.edit_message_text(
            f"⏭ Новость пропущена.\n\n{item['title']}"
        )
        print(f"⏭ Администратор пропустил: {item['title']}")
        return

    if action != "publish":
        await query.edit_message_text("Неизвестное действие.")
        return

    set_item_status(item_id, "publishing")

    try:
        post_text = item["post_text"]
        image_url = item["image_url"]

        if image_url and len(post_text) <= 1024:
            try:
                await context.bot.send_photo(
                    chat_id=CHANNEL_ID,
                    photo=image_url,
                    caption=post_text,
                )
            except Exception as photo_error:
                print(
                    f"⚠️ Фото не отправилось, пробую текстом: "
                    f"{photo_error}"
                )
                await context.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=post_text[:4096],
                )
        else:
            await context.bot.send_message(
                chat_id=CHANNEL_ID,
                text=post_text[:4096],
            )

        set_item_status(item_id, "published")
        await query.edit_message_text(
            f"✅ Опубликовано в канал.\n\n{item['title']}"
        )
        print(f"✅ Опубликовано по одобрению админа: {item['title']}")

    except Exception as error:
        set_item_status(item_id, "pending")
        await query.edit_message_text(
            "❌ Не удалось опубликовать пост. "
            "Проверь права бота в канале и логи Render.\n\n"
            f"{item['title']}"
        )
        print(
            f"❌ Ошибка публикации в канал: "
            f"{type(error).__name__}: {error}"
        )


# ---------- Планировщик ----------

async def news_scheduler(application: Application):
    while True:
        try:
            pending = get_pending_item()

            if pending:
                if not pending["notified"]:
                    try:
                        await send_for_review(application, pending)
                    except Exception as error:
                        print(
                            f"❌ Не удалось отправить новость админу: "
                            f"{type(error).__name__}: {error}"
                        )

                await asyncio.sleep(60)
                continue

            candidate = await find_one_new_entry()

            if candidate:
                entry = candidate["entry"]
                title = candidate["title"]
                summary = get_summary(entry)
                source_url = getattr(entry, "link", "") or ""
                raw_summary = getattr(entry, "summary", "") or ""
                image_url = extract_image(entry, raw_summary)

                post_text = await asyncio.to_thread(
                    make_post_text,
                    title,
                    summary,
                )

                if not post_text:
                    print(
                        f"⏭ Запись пропущена: нет описания "
                        f"и AI не создал текст — {title}"
                    )
                else:
                    item_id = create_pending_item(
                        item_key=candidate["item_key"],
                        title=title,
                        post_text=post_text,
                        image_url=image_url,
                        source_url=source_url,
                    )

                    if item_id:
                        pending = get_pending_item()
                        if pending:
                            try:
                                await send_for_review(application, pending)
                            except Exception as error:
                                print(
                                    f"❌ Не удалось отправить новость админу: "
                                    f"{type(error).__name__}: {error}"
                                )

            # Раз в 2 часа
            await asyncio.sleep(7200)

        except asyncio.CancelledError:
            raise

        except Exception as error:
            print(
                f"❌ Ошибка планировщика: "
                f"{type(error).__name__}: {error}"
            )
            await asyncio.sleep(60)


# ---------- Команды бота ----------

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.effective_user or not update.message:
        return

    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text(
            "У тебя нет доступа к управлению публикациями."
        )
        return

    await update.message.reply_text(
        "Бот подключён. Новости будут приходить сюда на проверку.\n\n"
        "Команды:\n"
        "/now — прислать новость прямо сейчас, не дожидаясь таймера."
    )


async def force_fetch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/now — принудительно ищет и присылает новость."""
    if not update.effective_user or not update.message:
        return

    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("Нет доступа.")
        return

    pending = get_pending_item()
    if pending:
        await update.message.reply_text(
            "⚠️ У тебя уже есть новость на проверке. "
            "Сначала опубликуй или пропусти её."
        )
        return

    await update.message.reply_text("🔍 Ищу свежую новость...")

    try:
        candidate = await find_one_new_entry()

        if not candidate:
            await update.message.reply_text(
                "😔 Не нашёл новых подходящих новостей. "
                "Попробуй позже."
            )
            return

        entry = candidate["entry"]
        title = candidate["title"]
        summary = get_summary(entry)
        source_url = getattr(entry, "link", "") or ""
        raw_summary = getattr(entry, "summary", "") or ""
        image_url = extract_image(entry, raw_summary)

        post_text = await asyncio.to_thread(
            make_post_text,
            title,
            summary,
        )

        if not post_text:
            await update.message.reply_text(
                "😔 Не удалось сгенерировать текст. Попробуй ещё раз."
            )
            return

        item_id = create_pending_item(
            item_key=candidate["item_key"],
            title=title,
            post_text=post_text,
            image_url=image_url,
            source_url=source_url,
        )

        if not item_id:
            await update.message.reply_text(
                "⚠️ Эта новость уже была в базе."
            )
            return

        pending = get_pending_item()
        if pending:
            await send_for_review(context.application, pending)

    except Exception as error:
        print(
            f"❌ Ошибка в /now: "
            f"{type(error).__name__}: {error}"
        )
        await update.message.reply_text(
            "❌ Что-то пошло не так. Смотри логи Render."
        )


# ---------- Запуск ----------

async def post_init(application: Application):
    application.bot_data["scheduler_task"] = asyncio.create_task(
        news_scheduler(application)
    )

    await application.bot.set_my_commands([
        ("start", "Запустить бота"),
        ("now", "Прислать новость сейчас"),
    ])


async def post_shutdown(application: Application):
    task = application.bot_data.get("scheduler_task")

    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def main():
    init_db()

    threading.Thread(
        target=run_flask,
        daemon=True,
    ).start()

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("now", force_fetch))
    application.add_handler(
        CallbackQueryHandler(
            review_callback,
            pattern=r"^(publish|skip):\d+$",
        )
    )

    application.run_polling()


if __name__ == "__main__":
    main()
