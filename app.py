import asyncio
import base64
import fcntl
import hashlib
import html
import json
import os
import random
import re
import signal
import sys
import threading
import time
import uuid
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import feedparser
import requests
from flask import Flask
from groq import Groq
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)
from werkzeug.serving import make_server


# =========================
# Настройки и переменные окружения
# =========================

def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Не задана переменная окружения {name}")
    return value


TELEGRAM_TOKEN = required_env("TELEGRAM_TOKEN")
CHANNEL_ID = required_env("CHANNEL_ID")
ADMIN_ID = int(required_env("ADMIN_ID"))

GITHUB_TOKEN = required_env("GITHUB_TOKEN")
GITHUB_REPO = required_env("GITHUB_REPO")  # формат: owner/repository
GITHUB_DATA_FILE = os.environ.get("GITHUB_DATA_FILE", "seen_news.json").strip()

GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.environ.get("GROQ_MODEL", "openai/gpt-oss-20b").strip()

TIMEZONE_NAME = os.environ.get("TIMEZONE", "UTC").strip()
try:
    LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)
except ZoneInfoNotFoundError as exc:
    raise RuntimeError(f"Неизвестная временная зона TIMEZONE={TIMEZONE_NAME}") from exc

AUTO_PUBLISH_MINUTES = int(os.environ.get("AUTO_PUBLISH_MINUTES", "60"))
SCHEDULE_HOURS = {10, 14, 18, 22}
FEED_TIMEOUT_SECONDS = 15

if "/" not in GITHUB_REPO:
    raise RuntimeError("GITHUB_REPO должен иметь формат owner/repository")

groq_client = Groq(api_key=GROQ_API_KEY) if GROQ_API_KEY else None


# =========================
# Защита от повторного запуска в одном экземпляре
# =========================

LOCK_FILE = "/tmp/bot.lock"
_LOCK_FD = None


def acquire_lock():
    """Не позволяет запустить два процесса в одном контейнере."""
    global _LOCK_FD

    lock_fd = open(LOCK_FILE, "a+")
    try:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        lock_fd.close()
        print("⚠️ Другой процесс бота уже запущен в этом контейнере.")
        sys.exit(1)

    lock_fd.seek(0)
    lock_fd.truncate()
    lock_fd.write(str(os.getpid()))
    lock_fd.flush()

    # Файл нельзя закрывать до остановки процесса: вместе с ним снимется lock.
    _LOCK_FD = lock_fd


# =========================
# RSS-источники
# =========================

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


# =========================
# GitHub-хранилище
# =========================

DEFAULT_DATA = {
    "items": [],
    "pending": {},
    "stats": {
        "published": 0,
        "skipped": 0,
        "sources": {},
    },
    "last_schedule_slot": "",
}

_data = {
    "items": [],
    "pending": {},
    "stats": {
        "published": 0,
        "skipped": 0,
        "sources": {},
    },
    "last_schedule_slot": "",
}
_data_sha = None
_data_lock = asyncio.Lock()
pending_queue = _data["pending"]

GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}

_encoded_repo = quote(GITHUB_REPO, safe="/")
_encoded_data_file = quote(GITHUB_DATA_FILE, safe="/")
GITHUB_CONTENTS_URL = (
    f"https://api.github.com/repos/{_encoded_repo}"
    f"/contents/{_encoded_data_file}"
)


def fresh_default_data():
    """Новая копия структуры, чтобы не разделять вложенные словари."""
    return {
        "items": [],
        "pending": {},
        "stats": {
            "published": 0,
            "skipped": 0,
            "sources": {},
        },
        "last_schedule_slot": "",
    }


def _load_sync():
    global _data, _data_sha, pending_queue

    try:
        response = requests.get(
            GITHUB_CONTENTS_URL,
            headers=GH_HEADERS,
            timeout=15,
        )

        if response.status_code == 404:
            # Файл ещё может не существовать: первый PUT создаст его.
            print("ℹ️ Файл данных в GitHub пока не найден; будет создан при сохранении.")
            _data = fresh_default_data()
            _data_sha = None

        elif response.status_code != 200:
            print(
                f"⚠️ GitHub GET {response.status_code}: "
                f"{response.text[:300]}"
            )
            _data = fresh_default_data()
            _data_sha = None

        else:
            payload = response.json()
            content = base64.b64decode(payload["content"]).decode("utf-8")
            loaded = json.loads(content)

            _data = fresh_default_data()
            _data.update(loaded)

            # Защита от некорректной структуры файла.
            if not isinstance(_data.get("items"), list):
                _data["items"] = []
            if not isinstance(_data.get("pending"), dict):
                _data["pending"] = {}
            if not isinstance(_data.get("stats"), dict):
                _data["stats"] = fresh_default_data()["stats"]

            _data["stats"].setdefault("published", 0)
            _data["stats"].setdefault("skipped", 0)
            _data["stats"].setdefault("sources", {})
            _data.setdefault("last_schedule_slot", "")
            _data_sha = payload["sha"]

    except Exception as exc:
        print(f"❌ Ошибка загрузки данных из GitHub: {type(exc).__name__}: {exc}")
        _data = fresh_default_data()
        _data_sha = None

    pending_queue = _data["pending"]
    print(
        f"💾 Загружено: {len(_data['items'])} items, "
        f"{len(pending_queue)} pending"
    )
    return _data


def _save_sync():
    """Сохраняет данные в GitHub Contents API."""
    global _data_sha

    _data["pending"] = pending_queue

    for attempt in range(3):
        try:
            content = json.dumps(
                _data,
                ensure_ascii=False,
                indent=2,
            )
            payload = {
                "message": f"Update bot data {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC",
                "content": base64.b64encode(
                    content.encode("utf-8")
                ).decode("ascii"),
                "committer": {
                    "name": "News Bot",
                    "email": "bot@example.com",
                },
            }

            if _data_sha:
                payload["sha"] = _data_sha

            response = requests.put(
                GITHUB_CONTENTS_URL,
                headers=GH_HEADERS,
                json=payload,
                timeout=20,
            )

            if response.status_code in (200, 201):
                result = response.json()
                _data_sha = result.get("content", {}).get("sha")
                return True

            if response.status_code == 409 and attempt < 2:
                # SHA изменился. Получаем актуальную версию и повторяем запись.
                current = requests.get(
                    GITHUB_CONTENTS_URL,
                    headers=GH_HEADERS,
                    timeout=15,
                )
                if current.status_code == 200:
                    _data_sha = current.json().get("sha")
                    continue
                if current.status_code == 404:
                    _data_sha = None
                    continue

            print(
                f"⚠️ GitHub PUT {response.status_code}: "
                f"{response.text[:300]}"
            )
            return False

        except Exception as exc:
            print(f"❌ Ошибка сохранения в GitHub: {type(exc).__name__}: {exc}")
            return False

    return False


async def load_data():
    async with _data_lock:
        return await asyncio.to_thread(_load_sync)


async def save_data():
    async with _data_lock:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_save_sync),
                timeout=30,
            )
        except asyncio.TimeoutError:
            print("⚠️ Тайм-аут сохранения данных в GitHub.")
            return False


def is_seen(key: str) -> bool:
    return key in _data.get("items", [])


def mark_seen(key: str):
    items = _data.setdefault("items", [])
    if key not in items:
        items.append(key)

    # Не даём JSON-файлу бесконечно расти.
    if len(items) > 2000:
        _data["items"] = items[-2000:]


def bump_stat(stat_name: str):
    stats = _data.setdefault("stats", fresh_default_data()["stats"])
    stats[stat_name] = stats.get(stat_name, 0) + 1


def bump_source(feed_url: str):
    stats = _data.setdefault("stats", fresh_default_data()["stats"])
    sources = stats.setdefault("sources", {})
    sources[feed_url] = sources.get(feed_url, 0) + 1


# =========================
# Flask health-check
# =========================

flask_app = Flask(__name__)


@flask_app.route("/")
@flask_app.route("/health")
def health():
    return "OK", 200


# =========================
# Очистка и обработка RSS
# =========================

def clean_html(raw: str) -> str:
    if not raw:
        return ""

    text = re.sub(r"<[^>]*>", " ", raw)
    text = html.unescape(text)
    text = re.sub(r"https?://\S+", "", text)
    return re.sub(r"\s+", " ", text).strip()


def get_summary(entry) -> str:
    summary = getattr(entry, "summary", "") or ""

    if not summary:
        contents = getattr(entry, "content", None) or []
        if contents:
            summary = contents[0].get("value", "")

    return clean_html(summary)[:1200]


def get_item_key(entry, feed_url: str, title: str) -> str:
    source_id = (
        getattr(entry, "id", None)
        or getattr(entry, "link", None)
        or f"{feed_url}:{title}"
    )
    return hashlib.sha256(str(source_id).encode("utf-8")).hexdigest()


def extract_image(entry, raw_summary: str = ""):
    for field in ("media_content", "media_thumbnail"):
        for media in getattr(entry, field, None) or []:
            if media.get("url"):
                return media["url"]

    for enclosure in getattr(entry, "enclosures", None) or []:
        if "image" in enclosure.get("type", "") and enclosure.get("href"):
            return enclosure["href"]

    match = re.search(
        r"""<img[^>]+src=["']([^"']+)["']""",
        raw_summary or "",
        re.IGNORECASE,
    )
    return match.group(1) if match else None


BLOCK_MARKERS = [
    "туториал",
    "руководство",
    "как я ",
    "как сделать",
    "how to",
    "tutorial",
    "война",
    "убит",
    "погиб",
    "теракт",
    "катастрофа",
    "трагедия",
    "авария",
    "смерть",
    "умер",
]


def is_news_item(title: str, summary: str) -> bool:
    text = f"{title} {summary}".lower()
    return not any(marker in text for marker in BLOCK_MARKERS)


def parse_feed_sync(feed_url: str):
    """Загружает RSS с тайм-аутом, затем передаёт содержимое feedparser."""
    response = requests.get(
        feed_url,
        headers={"User-Agent": "VibeNewsBot/1.0"},
        timeout=FEED_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return feedparser.parse(response.content)


async def find_one_new_entry():
    feeds = list(RSS_FEEDS)
    random.shuffle(feeds)

    for feed_url in feeds:
        try:
            feed = await asyncio.to_thread(parse_feed_sync, feed_url)

            if not feed.entries:
                continue

            fresh = []

            for entry in feed.entries:
                title = (
                    clean_html(getattr(entry, "title", "") or "")
                    or "Без заголовка"
                )
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

        except Exception as exc:
            print(
                f"❌ RSS {feed_url}: "
                f"{type(exc).__name__}: {exc}"
            )

    return None


# =========================
# Groq: генерация поста
# =========================

def rewrite_with_ai(title: str, summary: str):
    if not groq_client:
        print("⚠️ GROQ_API_KEY не задан, генерация поста недоступна.")
        return None

    prompt = f"""Ты — автор позитивного молодёжного Telegram-канала +VIBE NEWS.
Напиши короткий пост на русском по заголовку и описанию.
Пиши живо, неформально, с лёгким юмором.

СТИЛЬ:
- Сленг уместен: «вайб», «топчик».
- Задай вопрос подписчикам в конце, если уместно.

ПРАВИЛА:
- Начни с подходящего эмодзи.
- Используй несколько эмодзи, но не перегружай текст.
- 3–4 предложения, до 800 символов.
- Не добавляй ссылки и не выдумывай факты.

Заголовок: {title}
Описание: {summary}

Текст поста:"""

    try:
        result = groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            max_tokens=300,
        )

        if not result.choices:
            return None

        text = result.choices[0].message.content
        if not text:
            return None

        text = text.strip()
        text = re.sub(r"https?://\S+", "", text).strip()

        if len(text) > 1000:
            text = text[:997].rstrip() + "..."

        if len(re.sub(r"[^\wа-яА-ЯёЁ]+", "", text)) < 10:
            return None

        return text

    except Exception as exc:
        print(f"❌ Groq: {type(exc).__name__}: {exc}")
        return None


# =========================
# Telegram: сообщения и кнопки
# =========================

def build_keyboard(review_id: str):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "🚀 Опубликовать",
            callback_data=f"publish:{review_id}",
        ),
        InlineKeyboardButton(
            "⏭️ Дальше",
            callback_data=f"skip:{review_id}",
        ),
    ]])


async def safe_edit(query, text: str):
    try:
        await query.edit_message_text(text)
    except Exception:
        try:
            await query.edit_message_caption(caption=text)
        except Exception as exc:
            print(f"⚠️ Не удалось обновить сообщение: {exc}")


async def find_and_send_next(app: Application, max_attempts: int = 6):
    for _ in range(max_attempts):
        candidate = await find_one_new_entry()

        if not candidate:
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text="😔 Новых подходящих публикаций не нашёл.",
            )
            await save_data()
            return False

        entry = candidate["entry"]
        title = candidate["title"]
        summary = get_summary(entry)
        source_url = getattr(entry, "link", "") or ""
        raw_summary = getattr(entry, "summary", "") or ""
        image_url = extract_image(entry, raw_summary)

        post_text = await asyncio.to_thread(
            rewrite_with_ai,
            title,
            summary,
        )

        if not post_text:
            print(f"⏭️ AI не подготовил пост: {title[:80]}")
            mark_seen(candidate["key"])
            bump_stat("skipped")
            await save_data()
            continue

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

        message_text = f"📬 Пост на проверку\n\n{post_text}"
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
        except Exception as exc:
            print(f"⚠️ Не удалось отправить фото, отправляю текст: {exc}")
            await app.bot.send_message(
                chat_id=ADMIN_ID,
                text=message_text[:4096],
                reply_markup=keyboard,
            )

        print(f"📩 Отправлено админу на проверку: {title}")
        return True

    await app.bot.send_message(
        chat_id=ADMIN_ID,
        text="😔 Не получилось подготовить пост из найденных материалов.",
    )
    return False


async def publish_to_channel(app: Application, item: dict):
    post_text = item["post_text"]
    image_url = item.get("image_url")

    if image_url and len(post_text) <= 1024:
        try:
            message = await app.bot.send_photo(
                chat_id=CHANNEL_ID,
                photo=image_url,
                caption=post_text,
            )
        except Exception as exc:
            print(f"⚠️ Фото не отправлено, публикую текст: {exc}")
            message = await app.bot.send_message(
                chat_id=CHANNEL_ID,
                text=post_text[:4096],
            )
    else:
        message = await app.bot.send_message(
            chat_id=CHANNEL_ID,
            text=post_text[:4096],
        )

    try:
        await app.bot.set_message_reaction(
            chat_id=CHANNEL_ID,
            message_id=message.message_id,
            reaction=["🔥", "❤️", "😎"],
        )
    except Exception:
        # Реакции могут быть недоступны для канала или конкретного сообщения.
        pass

    return message


# =========================
# Команды и модерация
# =========================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return

    await update.effective_message.reply_text(
        "Привет! Я готовлю новости для проверки.\n\n"
        "/next — найти пост\n"
        "/stats — статистика\n"
        "/help — список команд"
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return

    await update.effective_message.reply_text(
        "/next — найти следующую публикацию\n"
        "/stats — показать статистику\n"
        "/help — помощь\n\n"
        "Посты можно опубликовать или пропустить кнопками под сообщением."
    )


async def next_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return

    await update.effective_message.reply_text("🔎 Ищу материал...")
    await find_and_send_next(context.application)


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or update.effective_user.id != ADMIN_ID:
        return

    stats = _data.get("stats", {})
    sources = stats.get("sources", {})

    text = (
        "📊 Статистика\n\n"
        f"Опубликовано: {stats.get('published', 0)}\n"
        f"Пропущено: {stats.get('skipped', 0)}\n"
        f"Ожидают решения: {len(pending_queue)}\n"
        f"Источников с публикациями: {len(sources)}"
    )

    await update.effective_message.reply_text(text)


async def review_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    if not query:
        return

    if not query.from_user or query.from_user.id != ADMIN_ID:
        await query.answer("Эта кнопка доступна только администратору.", show_alert=True)
        return

    await query.answer()

    try:
        action, review_id = query.data.split(":", 1)
    except (AttributeError, ValueError):
        await safe_edit(query, "Не удалось распознать действие.")
        return

    item = pending_queue.get(review_id)

    if not item:
        await safe_edit(query, "Эта публикация уже обработана.")
        return

    if action == "skip":
        pending_queue.pop(review_id, None)
        bump_stat("skipped")
        await save_data()
        await safe_edit(query, "⏭️ Пропущено. Ищу следующий материал...")
        await find_and_send_next(context.application)
        return

    if action != "publish":
        await safe_edit(query, "Неизвестное действие.")
        return

    try:
        await publish_to_channel(context.application, item)
        pending_queue.pop(review_id, None)
        bump_stat("published")
        await save_data()
        await safe_edit(query, f"✅ Опубликовано: {item['title'][:60]}")
        print(f"✅ Опубликовано: {item['title']}")

    except Exception as exc:
        await safe_edit(query, "❌ Ошибка публикации. Подробности — в логах.")
        print(f"❌ Ошибка публикации: {type(exc).__name__}: {exc}")


# =========================
# Фоновые задачи
# =========================

async def auto_publish_loop(app: Application):
    """Автоматически публикует посты, ожидающие решения дольше заданного срока."""
    while True:
        try:
            now = time.time()

            for review_id, item in list(pending_queue.items()):
                created_at = item.get("created_at", now)

                if now - created_at < AUTO_PUBLISH_MINUTES * 60:
                    continue

                try:
                    await publish_to_channel(app, item)
                    pending_queue.pop(review_id, None)
                    bump_stat("published")
                    await save_data()

                    await app.bot.send_message(
                        chat_id=ADMIN_ID,
                        text=f"⏰ Автопубликация выполнена: {item.get('title', '')[:100]}",
                    )
                    print(f"⏰ Автопубликация: {item.get('title', '')}")

                except Exception as exc:
                    print(
                        f"❌ Ошибка автопубликации: "
                        f"{type(exc).__name__}: {exc}"
                    )

        except Exception as exc:
            print(
                f"❌ Ошибка фоновой проверки: "
                f"{type(exc).__name__}: {exc}"
            )

        await asyncio.sleep(30)


async def schedule_loop(app: Application):
    """Ищет один новый материал в заданные часы по TIMEZONE."""
    while True:
        try:
            now = datetime.now(LOCAL_TZ)

            if now.hour in SCHEDULE_HOURS:
                slot = now.strftime("%Y-%m-%d-%H")

                if _data.get("last_schedule_slot") != slot:
                    # Сохраняем слот до поиска, чтобы не повторять его каждые 30 секунд.
                    _data["last_schedule_slot"] = slot
                    await save_data()

                    print(f"🕒 Плановый поиск, слот {slot}")
                    await find_and_send_next(app)

        except Exception as exc:
            print(
                f"❌ Ошибка расписания: "
                f"{type(exc).__name__}: {exc}"
            )

        await asyncio.sleep(30)


# =========================
# Запуск приложения
# =========================

async def main():
    acquire_lock()
    await load_data()

    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .build()
    )

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("next", next_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CallbackQueryHandler(review_callback))

    port = int(os.environ.get("PORT", "10000"))
    http_server = make_server(
        "0.0.0.0",
        port,
        flask_app,
        threaded=True,
    )
    http_thread = threading.Thread(
        target=http_server.serve_forever,
        name="health-server",
        daemon=True,
    )
    http_thread.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Запасной вариант для окружений без add_signal_handler.
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop_event.set))

    background_tasks = []

    try:
        await application.initialize()
        await application.start()

        if application.updater is None:
            raise RuntimeError("У Telegram Application отсутствует Updater")

        await application.updater.start_polling()

        background_tasks = [
            asyncio.create_task(
                auto_publish_loop(application),
                name="auto-publish",
            ),
            asyncio.create_task(
                schedule_loop(application),
                name="schedule",
            ),
        ]

        print("✅ Бот запущен и готов к работе.")
        await stop_event.wait()

    finally:
        print("🛑 Останавливаю приложение...")

        for task in background_tasks:
            task.cancel()

        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)

        if application.updater and application.updater.running:
            await application.updater.stop()

        if application.running:
            await application.stop()

        await application.shutdown()

        http_server.shutdown()
        http_server.server_close()
        http_thread.join(timeout=5)

        print("✅ Приложение остановлено.")


if __name__ == "__main__":
    asyncio.run(main())
