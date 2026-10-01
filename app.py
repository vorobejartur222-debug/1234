import os
import re
import html
import asyncio
import threading
import calendar

import feedparser
from flask import Flask
from groq import Groq
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)


# --- Переменные окружения Render ---

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
GROQ_MODEL = os.environ.get(
    "GROQ_MODEL",
    "llama-3.3-70b-versatile",
)

if not TELEGRAM_TOKEN:
    raise RuntimeError("В Render не задана переменная TELEGRAM_TOKEN")

if not CHANNEL_ID:
    raise RuntimeError("В Render не задана переменная CHANNEL_ID")

# Если GROQ_API_KEY отсутствует, бот попробует использовать описание из RSS.
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# --- RSS-ленты ---

RSS_FEEDS = [
    "https://habr.com/ru/rss/all/all/",
    "https://tproger.ru/feed/",
    "https://the-flow.ru/rss/",
    "https://ixbt.games/rss/",
    "https://www.sports.ru/rss/all.xml",
]


# Записи, уже опубликованные с момента запуска процесса.
published_items = set()


# --- Flask-сервер для Render ---

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


# --- Очистка текста RSS ---

def clean_html(raw_text):
    if not raw_text:
        return ""

    text = re.sub(r"<[^>]*>", " ", raw_text)
    text = html.unescape(text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def get_entry_summary(entry):
    summary = getattr(entry, "summary", "") or ""

    if not summary:
        contents = getattr(entry, "content", None) or []
        if contents:
            summary = contents[0].get("value", "")

    return clean_html(summary)[:1200]


def get_entry_timestamp(entry):
    """Возвращает время записи для сравнения RSS-новостей."""
    parsed_date = (
        getattr(entry, "published_parsed", None)
        or getattr(entry, "updated_parsed", None)
    )

    if parsed_date:
        try:
            return calendar.timegm(parsed_date)
        except (TypeError, ValueError, OverflowError):
            pass

    # Если у RSS нет даты — такая запись будет иметь самый низкий приоритет.
    return 0


def get_entry_id(entry, feed_url, title):
    return (
        getattr(entry, "id", None)
        or getattr(entry, "link", None)
        or f"{feed_url}:{title}"
    )


# --- Создание текста через Groq ---

def rewrite_with_ai(title, summary):
    if not groq_client:
        print("⚠️ GROQ_API_KEY не задан — использую описание RSS")
        return None

    prompt = f"""Ты — автор позитивного молодёжного Telegram-канала.
Создай самостоятельный короткий пост на русском языке на основе заголовка и описания.
Пиши неформально, энергично, с лёгким юмором и добавь 2-3 подходящих эмодзи.
Пост должен раскрывать смысл новости, а не состоять только из заголовка.
Не добавляй ссылки и не выдумывай факты.
Ограничься 3-4 предложениями.

Заголовок: {title}
Описание: {summary}

Текст поста:"""

    try:
        response = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            temperature=0.8,
            max_tokens=250,
        )

        if not response.choices:
            print("⚠️ Groq вернул пустой ответ")
            return None

        result = response.choices[0].message.content

        if not result:
            print("⚠️ Groq вернул пустой текст")
            return None

        result = result.strip()
        result = re.sub(r"https?://\S+", "", result).strip()

        # Не используем ответ, если он слишком короткий или пустой по смыслу.
        meaningful_chars = re.sub(r"[^\wа-яА-ЯёЁ]+", "", result)
        if len(meaningful_chars) < 10:
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
        return f"⚡ {title}\n\n{summary}"

    print(f"⏭️ Запись пропущена: нет описания и AI не создал текст — {title}")
    return None


# --- Поиск картинки в RSS ---

def extract_image(entry, summary_html=""):
    for field in ("media_content", "media_thumbnail"):
        media_items = getattr(entry, field, None) or []

        for media in media_items:
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


# --- Поиск одной самой свежей неопубликованной записи ---

async def find_one_new_entry():
    best_candidate = None
    best_score = None

    for feed_index, feed_url in enumerate(RSS_FEEDS):
        try:
            feed = await asyncio.to_thread(feedparser.parse, feed_url)

            if not feed.entries:
                print(f"⚠️ В RSS нет записей: {feed_url}")
                continue

            for entry_index, entry in enumerate(feed.entries):
                title = clean_html(
                    getattr(entry, "title", "") or ""
                ) or "Без заголовка"

                item_id = get_entry_id(entry, feed_url, title)

                if item_id in published_items:
                    continue

                timestamp = get_entry_timestamp(entry)

                # При одинаковой дате сохраняем порядок RSS-лент.
                score = (
                    timestamp,
                    -feed_index,
                    -entry_index,
                )

                if best_candidate is None or score > best_score:
                    best_candidate = {
                        "entry": entry,
                        "feed_url": feed_url,
                        "title": title,
                        "item_id": item_id,
                    }
                    best_score = score

                # Обычно RSS отсортирован от новых записей к старым.
                # Проверяем следующую ленту после первой неопубликованной записи.
                break

        except Exception as error:
            print(
                f"❌ Ошибка чтения {feed_url}: "
                f"{type(error).__name__}: {error}"
            )

    return best_candidate


# --- Публикация одной записи ---

async def fetch_and_post_one_news(application: Application):
    candidate = await find_one_new_entry()

    if not candidate:
        print("ℹ️ Новых неопубликованных записей не найдено")
        return

    entry = candidate["entry"]
    title = candidate["title"]
    item_id = candidate["item_id"]

    raw_summary = getattr(entry, "summary", "") or ""
    summary = get_entry_summary(entry)
    image_url = extract_image(entry, raw_summary)

    post_text = await asyncio.to_thread(
        make_post_text,
        title,
        summary,
    )

    if not post_text:
        return

    try:
        if image_url:
            # У подписи к фото есть ограничение Telegram по длине.
            await application.bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=image_url,
                caption=post_text[:1024],
            )
            print(f"✅ Опубликована одна новость с фото: {title}")

        else:
            await application.bot.send_message(
                chat_id=CHANNEL_ID,
                text=post_text[:4096],
            )
            print(f"✅ Опубликована одна новость без фото: {title}")

        # Запись считается опубликованной только после успешной отправки.
        published_items.add(item_id)

    except Exception as error:
        print(f"❌ Не удалось отправить пост: {type(error).__name__}: {error}")

        # Если отправка фото не удалась, пробуем отправить только текст.
        if image_url:
            try:
                await application.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=post_text[:4096],
                )
                published_items.add(item_id)
                print(f"✅ Текст новости отправлен без фото: {title}")
            except Exception as text_error:
                print(
                    f"❌ Не удалось отправить и текст: "
                    f"{type(text_error).__name__}: {text_error}"
                )


# --- Планировщик: одна новость за проход, раз в час ---

async def news_scheduler(application: Application):
    while True:
        try:
            await fetch_and_post_one_news(application)
            await asyncio.sleep(3600)

        except asyncio.CancelledError:
            raise

        except Exception as error:
            print(
                f"❌ Ошибка планировщика: "
                f"{type(error).__name__}: {error}"
            )
            await asyncio.sleep(60)


# --- Команда /start ---

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.message:
        await update.message.reply_text(
            "Бот запущен. Он публикует не больше одной новости за проход."
        )


# --- Запуск и остановка фонового планировщика ---

async def post_init(application: Application):
    application.bot_data["scheduler_task"] = asyncio.create_task(
        news_scheduler(application)
    )


async def post_shutdown(application: Application):
    task = application.bot_data.get("scheduler_task")

    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# --- Запуск приложения ---

def main():
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
    application.run_polling()


if __name__ == "__main__":
    main()
