import os
import threading
import time
import re
import feedparser
from flask import Flask
from telegram import Bot
from telegram.ext import Updater, CommandHandler
from groq import Groq

# --- Конфигурация ---
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

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
groq_client = Groq(api_key=GROQ_API_KEY)

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

# --- Функция переписывания текста через AI ---
def rewrite_with_ai(title, summary=""):
    """Переписывает новость в стиле молодёжного канала."""
    prompt = f"""Ты — автор позитивного молодёжного Telegram-канала о новостях.
Перепиши эту новость в короткий, энергичный пост.
Добавь 2-3 подходящих эмодзи. Пиши на русском, неформально, с легким юмором.
Не используй слово "новость" и не пиши "вот новость".
Сделай текст не длиннее 3-4 предложений.
Вот заголовок: {title}
Краткое описание: {summary}

Твой пост:"""

    try:
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            max_tokens=250
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"Ошибка AI: {e}")
        return None

# --- Функция поиска картинки в RSS ---
def extract_image(entry, summary_html=""):
    """Пытается вытащить картинку из записи RSS."""
    # Вариант А: media_content
    if hasattr(entry, 'media_content') and entry.media_content:
        for media in entry.media_content:
            url = media.get('url')
            if url:
                return url

    # Вариант Б: media_thumbnail
    if hasattr(entry, 'media_thumbnail') and entry.media_thumbnail:
        for media in entry.media_thumbnail:
            url = media.get('url')
            if url:
                return url

    # Вариант В: enclosures
    if hasattr(entry, 'enclosures') and entry.enclosures:
        for enc in entry.enclosures:
            if 'image' in enc.get('type', ''):
                return enc.get('href')

    # Вариант Г: ищем <img> в HTML описания
    if summary_html:
        img_match = re.search(r'<img[^>]+src="([^"]+)"', summary_html)
        if img_match:
            return img_match.group(1)

    return None

# --- Основная логика ---
def fetch_and_post_news():
    for feed_url in RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            if not feed.entries:
                continue

            latest_entry = feed.entries[0]
            title = latest_entry.title
            link = latest_entry.link

            # Достаём HTML описания и чистим для AI
            raw_summary = getattr(latest_entry, 'summary', '')
            clean_summary = re.sub('<[^<]+?>', '', raw_summary)
            clean_summary = clean_summary[:400]

            # Картинка — из RSS
            image_url = extract_image(latest_entry, raw_summary)

            # Текст — через AI
            ai_text = rewrite_with_ai(title, clean_summary)
            if not ai_text:
                ai_text = f"✨ *{title}*\n\n🔗 [Читать далее]({link})"

            # Публикация
            if image_url:
                try:
                    bot.send_photo(
                        chat_id=CHANNEL_ID,
                        photo=image_url,
                        caption=ai_text,
                        parse_mode='Markdown'
                    )
                    print(f"✅ Опубликовано с фото: {title}")
                except Exception as e:
                    print(f"⚠️ Фото не загрузилось ({e}), отправляю текстом")
                    bot.send_message(chat_id=CHANNEL_ID, text=ai_text, parse_mode='Markdown')
            else:
                bot.send_message(chat_id=CHANNEL_ID, text=ai_text, parse_mode='Markdown')
                print(f"✅ Опубликовано без фото: {title}")

            time.sleep(5)

        except Exception as e:
            print(f"❌ Ошибка при обработке {feed_url}: {e}")

def news_scheduler():
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
        update.message.reply_text('Бот-новостник с AI и фото запущен!')
    dp.add_handler(CommandHandler("start", start))

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
