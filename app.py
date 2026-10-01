import os
import re
import html
import asyncio
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import feedparser
from flask import Flask
from groq import Groq
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)


# --- Настройки из Environment на Render ---

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")

# По умолчанию время московское.
TIMEZONE_NAME = os.environ.get("TIMEZONE", "Europe/Moscow")

try:
    TIMEZONE = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError:
    raise RuntimeError(
        f"Неизвестный часовой пояс TIMEZONE={TIMEZONE_NAME!r}"
    )

if not TELEGRAM_TOKEN:
    raise RuntimeError("В Render не задана переменная TELEGRAM_TOKEN")

if not CHANNEL_ID:
    raise RuntimeError("В Render не задана переменная CHANNEL_ID")

# Если ключа Groq нет, бот сможет использовать описание из RSS,
# но генерировать текст через AI не сможет.
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# --- RSS-ленты ---

RSS_FEEDS = [
    "https://habr.com/ru/rss/all/all/",
    "https://tproger.ru/feed/",
    "https://the-flow.ru/rss/",
    "https://ixbt.games/rss/",
    "https://www.sports.ru/rss/all.xml",
]


# --- Flask для Render ---

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


# --- Проверка времени публикации ---

def is_posting_time():
    """Разрешённое окно: с 10:00 включительно до 22:00 не включительно."""
    now = datetime.now(TIMEZONE)
    return 10 <= now.hour < 22


def seconds_until_10am():
    """Сколько секунд ждать до ближайших 10:00 по заданному часовому поясу."""
    now = datetime.now(TIMEZONE)

    if now.hour < 10:
        next_start = now.replace(
            hour=10, minute=0, second=0, microsecond=0
        )
    else:
        next_start = (now + timedelta(days=1)).replace(
            hour=10, minute=0, second=0, microsecond=0
        )

    return max(1, int((next_start - now).total_seconds()))


# --- Обработка текста из RSS ---

def clean_html(raw_text):
    """Удаляет HTML-разметку и ссылки из текста RSS."""
    if not raw_text:
        return ""

    text = re.sub(r"<[^>]*>", " ", raw_text)
    text = html.unescape(text)
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def get_entry_summary(entry):
    """Пробует взять описание из обычного summary или content RSS."""
    summary = getattr(entry, "summary", "") or ""

    if not summary:
        contents = getattr(entry, "content", None) or []
        if contents:
            summary = contents[0].get("value", "")

    return clean_html(summary)[:1200]


# --- Генерация поста через Groq ---

def rewrite_with_ai(title, summary):
    """Создаёт короткий пост. При ошибке возвращает None."""
    if not groq_client:
        print("⚠️ GROQ_API_KEY не задан — AI-генерация недоступна")
        return None

    prompt = f"""Ты — автор позитивного молодёжного Telegram-канала.
Создай самостоятельный короткий пост на русском языке на основе заголовка и описания.
Пиши неформально, энергично, с лёгким юмором и добавь 2-3 подходящих эмодзи.
Пост должен содержать смысл новости, а не только заголовок.
Не добавляй ссылки и не пиши фразы «вот новость» или «читайте далее».
Не выдумывай факты, которых нет в исходном тексте.
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
            print("⚠️ Groq вернул пустой список вариантов")
            return None

        result = response.choices[0].message.content
        if not result:
            print("⚠️ Groq вернул пустой текст")
            return None

        result = result.strip()
        result = re.sub(r"https?://\S+", "", result).strip()

        # Не принимаем ответ, если в нём фактически только ссылка или символы.
        if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", result)) < 10:
            print(f"⚠️ Groq вернул слишком короткий текст: {result!r}")
            return None

        return result

    except Exception as e:
        print(f"❌ Ошибка Groq: {type(e).__name__}: {e}")
        return None


# --- Поиск изображения в записи RSS ---

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


# --- Подготовка текста поста ---

def make_post_text(title, summary):
    """Сначала использует AI, при сбое — текст из RSS, но не ссылку."""
    ai_text = rewrite_with_ai(title, summary)

    if ai_text:
        return ai_text

    if summary:
        print(f"⚠️ Использую описание RSS вместо AI: {title}")
        return f"⚡ {title}\n\n{summary}"

    print(f"⏭️ Пропускаю запись: нет текста для публикации — {title}")
    return None


# --- Получение и публикация записей ---

async def fetch_and_post_news(application: Application):
    for feed_url in RSS_FEEDS:
        if not is_posting_time():
            print("⏰ Окно публикаций закончилось. Остановил текущий проход.")
            break

        try:
            # feedparser и Groq — синхронные, поэтому запускаем их
            # в отдельном потоке, чтобы не блокировать Telegram-бота.
            feed = await asyncio.to_thread(feedparser.parse, feed_url)

            if not feed.entries:
                print(f"⚠️ В RSS нет записей: {feed_url}")
                continue

            entry = feed.entries[0]
            title = clean_html(getattr(entry, "title", "")) or "Без заголовка"
            raw_summary = getattr(entry, "summary", "") or ""
            summary = get_entry_summary(entry)
            image_url = extract_image(entry, raw_summary)

            post_text = await asyncio.to_thread(
                make_post_text,
                title,
                summary,
            )

            # Если AI не ответил и в RSS нет описания — ничего не публикуем.
            if not post_text:
                continue

            # Время могло стать 22:00, пока обрабатывался RSS или AI.
            if not is_posting_time():
                print("⏰ Сейчас уже вне окна публикаций. Пост пропущен.")
                break

            # Подпись к фото в Telegram ограничена по длине.
            try:
                if image_url and len(post_text) <= 1024:
                    await application.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=image_url,
                        caption=post_text,
                    )
                    print(f"✅ Опубликовано с фото: {title}")

                elif image_url:
                    await application.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=image_url,
                    )
                    await application.bot.send_message(
                        chat_id=CHANNEL_ID,
                        text=post_text[:4096],
                    )
                    print(f"✅ Опубликовано фото и текст: {title}")

                else:
                    await application.bot.send_message(
                        chat_id=CHANNEL_ID,
                        text=post_text[:4096],
                    )
                    print(f"✅ Опубликовано без фото: {title}")

            except Exception as send_error:
                print(f"⚠️ Не удалось отправить фото: {send_error}")

                # Если фото не отправилось, пробуем отправить сам текст.
                await application.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=post_text[:4096],
                )
                print(f"✅ Текст отправлен без фото: {title}")

            await asyncio.sleep(5)

        except Exception as e:
            print(f"❌ Ошибка при обработке {feed_url}: {type(e).__name__}: {e}")


# --- Расписание ---

async def news_scheduler(application: Application):
    while True:
        try:
            if not is_posting_time():
                wait_time = seconds_until_10am()
                print(
                    f"⏰ Сейчас вне окна публикаций. "
                    f"Следующая проверка через {wait_time} секунд."
                )
                await asyncio.sleep(wait_time)
                continue

            await fetch_and_post_news(application)

            # Проверяем ленты примерно раз в час.
            if is_posting_time():
                await asyncio.sleep(3600)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"❌ Ошибка планировщика: {type(e).__name__}: {e}")
            await asyncio.sleep(60)


# --- Команда /start ---

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.message:
        await update.message.reply_text(
            "Бот запущен. Публикации выходят с 10:00 до 22:00."
        )


# --- Запуск и остановка приложения ---

async def post_init(application: Application):
    application.bot_data["scheduler_task"] = asyncio.create_task(
        news_scheduler(application)
    )


async def post_shutdown(application: Application):
    task = application.bot_data.get("scheduler_task")

    if task:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


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
