import os
import re
import html
import asyncio
import threading

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

# Без ключа Groq бот сможет отправлять текст из RSS,
# но не сможет переписывать его с помощью AI.
groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# --- RSS-ленты ---

RSS_FEEDS = [
    "https://habr.com/ru/rss/all/all/",
    "https://tproger.ru/feed/",
    "https://the-flow.ru/rss/",
    "https://ixbt.games/rss/",
    "https://www.sports.ru/rss/all.xml",
]


# Не публиковать одну и ту же запись повторно,
# пока процесс бота работает.
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

        if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", result)) < 10:
            print(f"⚠️ Ответ Groq слишком короткий: {result!r}")
            return None

        return result

    except Exception as error:
        print(f"❌ Ошибка Groq: {type(error).__name__}: {error}")
        return None


# --- Поиск изображения в RSS ---

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
    ai_text = rewrite_with_ai(title, summary)

    if ai_text:
        return ai_text

    if summary:
        print(f"⚠️ Использую описание RSS вместо AI: {title}")
        return
