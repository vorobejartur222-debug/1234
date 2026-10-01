import os
import threading
import time
import requests
import feedparser
from flask import Flask
from telegram import Bot
from telegram.ext import Updater, CommandHandler

# --- Конфигурация ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")

RSS_FEEDS = [
    "https://habr.com/ru/rss/all/all/",
    "https://tproger.ru/feed/",
    "https://the-flow.ru/rss/",
    "https://ixbt.games/rss/",
    "https://www.sports.ru/rss/all.xml"
]

# --- Инициализация ---
bot = Bot(token=TELEGRAM_TOKEN)
app = Flask(__name__)

# --- Веб-сервер для keep-alive ---
@app.route('/')
def home():
    return "Bot is running", 200

@app.route('/health')
def health():
    return "OK", 200

def run_flask():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# --- Логика бота ---
def fetch_and_post_news():
    """Функция для сбора и публикации новостей из списка RSS-лент."""
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if feed.entries:
                latest_entry = feed.entries[0]
                title = latest_entry.title
                link = latest_entry.link
                message = f"✨ *{title}*\n\n[Читать далее]({link})"
                bot.send_message(chat_id=CHANNEL_ID, text=message, parse_mode='Markdown')
                print(f"Опубликовано из {feed_url}: {title}")
                time.sleep(2)
        except Exception as e:
            print(f"Ошибка при обработке {feed_url}: {e}")

def news_scheduler():
    """Планировщик для периодического запуска сбора новостей."""
    while True:
        fetch_and_post_news()
        time.sleep(3600)  # раз в час

def main():
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()

    news_thread = threading.Thread(target=news_scheduler)
    news_thread.daemon = True
    news_thread.start()

    updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher

    def start(update, context):
        update.message.reply_text('Бот-новостник запущен!')
    dp.add_handler(CommandHandler("start", start))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
