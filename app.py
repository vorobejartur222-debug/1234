import os
import re
import html
import asyncio
import threading

import feedparser
from flask import Flask
from groq import Groq
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


# --- Конфигурация ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

if not TELEGRAM_TOKEN:
    raise RuntimeError("Не задана переменная окружения TELEGRAM_TOKEN")
if not CHANNEL_ID:
    raise RuntimeError("Не задана переменная окружения CHANNEL_ID")
if not GROQ_API_KEY:
    raise RuntimeError("Не задана переменная окружения GROQ_API_KEY")

RSS_FEEDS = [
    "https://habr.com/ru/rss/all/all/",
    "https://tproger.ru/feed/",
    "https://the-flow.ru/rss/",
    "https://ixbt.games/rss/",
    "https://www.sports.ru/rss/all.xml",
]

flask_app = Flask(__name__)
groq_client = Groq(api_key=GROQ_API_KEY)


# --- Веб-сервер Render ---
@flask_app.route("/")
def home():
    return "Bot is running", 200


@flask_app.route("/health")
def health():
    return "OK", 200


def run_flask():
    port = int(os.environ.get("PORT", 5000))
    flask_app.run(host="0.0.0.0", port=port)


# --- Переписывание текста через Groq ---
def rewrite_with_ai(title, summary=""):
    prompt = f"""Ты — автор позитивного молодёжного Telegram-канала о новостях.
Перепиши эту новость в короткий, энергичный пост.
Добавь 2-3 подходящих эмодзи. Пиши на русском, неформально, с лёгким юмором.
Не используй слово «новость» и не пиши «вот новость».
Сделай текст не длиннее 3-4 предложений.
Заголовок: {title}
Краткое описание: {summary}

Твой пост:"""

    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            max_tokens=250,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Ошибка AI: {e}")
        return None


# --- Поиск картинки в RSS ---
def extract_image(entry, summary_html=""):
    for field in ("media_content", "media_thumbnail"):
        media_items = getattr(entry, field, None) or []
        for media in media_items:
            url = media.get("url")
            if url:
                return url

    for enclosure in getattr(entry, "enclosures", None) or []:
        if "image" in enclosure.get("type", ""):
            url = enclosure.get("href")
            if url:
                return url

    match = re.search(
        r"""<img[^>]+src=["']([^"']+)["']""",
        summary_html,
        flags=re.IGNORECASE,
    )
    return match.group(1) if match else None


# --- Публикация новостей ---
async def fetch_and_post_news(application: Application):
    for feed_url in RSS_FEEDS:
        try:
            # feedparser и Groq синхронные — выполняем их вне event loop.
            feed = await asyncio.to_thread(feedparser.parse, feed_url)

            if not feed.entries:
                print(f"Нет записей в RSS: {feed_url}")
                continue

            entry = feed.entries[0]
            title = getattr(entry, "title", "Без заголовка")
            link = getattr(entry, "link", "")
            raw_summary = getattr(entry, "summary", "")

            clean_summary = re.sub(r"<[^>]+>", " ", raw_summary)
            clean_summary = html.unescape(clean_summary)
            clean_summary = re.sub(r"\s+", " ", clean_summary).strip()[:400]

            image_url = extract_image(entry, raw_summary)
            ai_text = await asyncio.to_thread(
                rewrite_with_ai, title, clean_summary
            )

            if not ai_text:
                ai_text = f"✨ {title}"
                if link:
                    ai_text += f"\n\nЧитать далее: {link}"

            try:
                if image_url and len(ai_text) <= 1024:
                    await application.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=image_url,
                        caption=ai_text,
                    )
                    print(f"✅ Опубликовано с фото: {title}")
                elif image_url:
                    await application.bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=image_url,
                    )
                    await application.bot.send_message(
                        chat_id=CHANNEL_ID,
                        text=ai_text[:4096],
                    )
                    print(f"✅ Опубликовано с фото и отдельным текстом: {title}")
                else:
                    await application.bot.send_message(
                        chat_id=CHANNEL_ID,
                        text=ai_text[:4096],
                    )
                    print(f"✅ Опубликовано без фото: {title}")

            except Exception as photo_error:
                print(f"⚠️ Не удалось отправить фото/пост: {photo_error}")
                await application.bot.send_message(
                    chat_id=CHANNEL_ID,
                    text=ai_text[:4096],
                )

            await asyncio.sleep(5)

        except Exception as e:
            print(f"❌ Ошибка при обработке {feed_url}: {e}")


async def news_scheduler(application: Application):
    while True:
        await fetch_and_post_news(application)
        await asyncio.sleep(3600)


# --- Команда /start ---
async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if update.message:
        await update.message.reply_text(
            "Бот-новостник с AI и фото запущен!"
        )


# --- Запуск и остановка фонового расписания ---
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
    threading.Thread(target=run_flask, daemon=True).start()

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
