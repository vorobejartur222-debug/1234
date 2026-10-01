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
CHANNEL_ID = os.environ.get("CHANNEL_ID")  # Например, "@your_channel_name"
RSS_FEED_URL = "https://example.com/rss"  # Замените на нужный RSS-источник

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
    # Render предоставляет порт через переменную окружения PORT
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

# --- Логика бота ---
def fetch_and_post_news():
    """Функция для сбора и публикации новостей."""
    feed = feedparser.parse(RSS_FEED_URL)
    if feed.entries:
        latest_entry = feed.entries[0]
        title = latest_entry.title
        link = latest_entry.link
        # Простейшее форматирование. Можно улучшить с помощью AI.
        message = f"✨ *{title}*\n\n[Читать далее]({link})"
        try:
            bot.send_message(chat_id=CHANNEL_ID, text=message, parse_mode='Markdown')
            print(f"Опубликовано: {title}")
        except Exception as e:
            print(f"Ошибка при публикации: {e}")

def news_scheduler():
    """Планировщик для периодического запуска сбора новостей."""
    while True:
        fetch_and_post_news()
        # Публикуем раз в час (3600 секунд). Можно изменить.
        time.sleep(3600)

def main():
    # Запускаем Flask-сервер в отдельном потоке
    flask_thread = threading.Thread(target=run_flask)
    flask_thread.daemon = True
    flask_thread.start()
    
    # Запускаем планировщик новостей в отдельном потоке
    news_thread = threading.Thread(target=news_scheduler)
    news_thread.daemon = True
    news_thread.start()

    # Запускаем Telegram-бота (поллинг)
    # Polling будет работать в основном потоке
    updater = Updater(token=TELEGRAM_TOKEN, use_context=True)
    dp = updater.dispatcher
    
    # Пример команды (необязательно)
    def start(update, context):
        update.message.reply_text('Бот-новостник запущен!')
    dp.add_handler(CommandHandler("start", start))
    
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()