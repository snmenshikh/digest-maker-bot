import asyncio
import os
import io
import re
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict, Any, Optional

import requests
import pandas as pd
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

from docx import Document
from docx.shared import Pt

import nltk
from nltk.tokenize import sent_tokenize

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(name)s: %(message)s"
)
logger = logging.getLogger("digest_maker_bot")

# ---------- NLTK bootstrap (safe) ----------
def ensure_nltk():
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt")

# ---------- Secure token retrieval ----------
def get_bot_token() -> str:
    """
    Securely fetch BOT_TOKEN from environment (Portainer -> Stack -> Environment).
    Never hardcode tokens in code or in the image.
    """
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "BOT_TOKEN is not set. Configure it via environment variables in Portainer."
        )
    return token

# ---------- FSM States ----------
class DigestStates(StatesGroup):
    WAITING_FOR_EXCEL = State()
    WAITING_FOR_INTERVAL = State()
    WAITING_FOR_KEYWORDS = State()
    PROCESSING = State()

# ---------- Excel validation ----------
TG_URL_RE = re.compile(r"^https://t\.me/([A-Za-z0-9_]+)/?$")

def read_channels_from_excel(file_bytes: bytes) -> List[Tuple[str, str, str]]:
    """
    Reads an Excel file (xls/xlsx). Expects:
      - Col A: Channel Name
      - Col B: Channel URL (https://t.me/<slug>)
    Returns list of tuples: (channel_name, channel_url, slug)
    Raises ValueError with human-friendly messages on problems.
    """
    try:
        # Try to read as Excel (pandas auto-detects xls/xlsx by content)
        df = pd.read_excel(io.BytesIO(file_bytes), header=None)
    except Exception as e:
        raise ValueError(
            f"Не удалось прочитать Excel. Проверь формат файла (*.xls или *.xlsx). Детали: {e}"
        )

    if df.shape[1] < 2:
        raise ValueError("В таблице должно быть минимум 2 столбца: A — имя канала, B — ссылка https://t.me/<slug>.")

    channels: List[Tuple[str, str, str]] = []
    for idx, row in df.iterrows():
        name = str(row.iloc[0]).strip()
        url = str(row.iloc[1]).strip()
        if not name or not url:
            logger.warning(f"Строка {idx+1}: пропущено из-за пустых значений.")
            continue

        m = TG_URL_RE.match(url)
        if not m:
            raise ValueError(
                f"Строка {idx+1}: некорректная ссылка '{url}'. Ожидается формат https://t.me/<slug>"
            )
        slug = m.group(1)
        channels.append((name, url, slug))

    if not channels:
        raise ValueError("В файле не найдено ни одной корректной строки с каналом.")

    return channels

# ---------- Interval helpers ----------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def interval_to_timedelta(interval_key: str) -> timedelta:
    """
    'day' -> 1 day, 'week' -> 7 days, 'month' -> 30 days
    """
    mapping = {
        "day": timedelta(days=1),
        "week": timedelta(days=7),
        "month": timedelta(days=30),
    }
    return mapping[interval_key]

def build_interval_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Сутки", callback_data="interval:day")],
        [InlineKeyboardButton(text="Неделя", callback_data="interval:week")],
        [InlineKeyboardButton(text="Месяц", callback_data="interval:month")],
    ])

# ---------- Scraping ----------
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36"
}

def make_s_url(slug: str) -> str:
    return f"https://t.me/s/{slug}"

def parse_telegram_s_page(slug: str) -> List[Dict[str, Any]]:
    """
    Scrape https://t.me/s/<slug> and return a list of messages:
    Each message dict: { "text": str, "dt": datetime (UTC if possible) }
    Notes:
      - No JS → we get only visible batch (recent posts).
      - Time is taken from <time datetime="..."> if present; otherwise None.
    """
    url = make_s_url(slug)
    r = requests.get(url, headers=HEADERS, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"Не удалось загрузить {url} (HTTP {r.status_code})")

    soup = BeautifulSoup(r.text, "lxml")

    messages = []
    # Telegram web structure usually wraps messages in 'tgme_widget_message' containers
    for msg in soup.select(".tgme_widget_message_wrap, .tgme_widget_message"):
        # Text
        text_el = msg.select_one(".tgme_widget_message_text")
        text = text_el.get_text(separator=" ", strip=True) if text_el else ""

        # Datetime
        dt_value: Optional[datetime] = None
        time_el = msg.find("time")
        if time_el and time_el.has_attr("datetime"):
            try:
                dt_value = dtparser.isoparse(time_el["datetime"])
                if dt_value.tzinfo is None:
                    dt_value = dt_value.replace(tzinfo=timezone.utc)
                else:
                    dt_value = dt_value.astimezone(timezone.utc)
            except Exception:
                dt_value = None

        if text:
            messages.append({"text": text, "dt": dt_value})

    return messages

# ---------- Keyword filtering & summarization ----------
def filter_messages_by_time_and_keywords(
    messages: List[Dict[str, Any]],
    since_dt: datetime,
    keywords: List[str]
) -> List[Dict[str, Any]]:
    """
    Keep messages newer than since_dt and matching keywords (case-insensitive).
    If keywords list is empty, keep all.
    """
    kws = [k.lower() for k in keywords if k.strip()]
    filtered = []
    for m in messages:
        dt_ok = (m["dt"] is None) or (m["dt"] >= since_dt)  # if no dt, we keep it cautiously
        if not dt_ok:
            continue
        if not kws:
            filtered.append(m)
            continue
        text_low = m["text"].lower()
        if any(kw in text_low for kw in kws):
            filtered.append(m)
    return filtered

def summarize_text_extractively(text: str, keywords: List[str], max_sentences: int = 3) -> str:
    """
    Simple extractive summary:
      1) Split into sentences (NLTK Punkt).
      2) Rank sentences: +2 if contains keyword, +1 per unique non-stopword token frequency (lightweight).
      3) Return top-N in original order.
    """
    if not text:
        return ""
    sentences = [s.strip() for s in sent_tokenize(text) if s.strip()]
    if len(sentences) <= max_sentences:
        return " ".join(sentences)

    kws = set(k.lower() for k in keywords if k.strip())

    # crude tokenization
    def tokens(s: str) -> List[str]:
        return re.findall(r"[A-Za-zА-Яа-яЁё0-9_]+", s.lower())

    # frequency map
    freq: Dict[str, int] = {}
    for s in sentences:
        for t in set(tokens(s)):
            freq[t] = freq.get(t, 0) + 1

    # sentence scoring
    scored = []
    for i, s in enumerate(sentences):
        ts = tokens(s)
        score = 0
        if kws and any(k in s.lower() for k in kws):
            score += 2
        score += sum(freq.get(t, 0) for t in set(ts))
        scored.append((i, s, score))

    # pick top-K by score, keep original order
    top = sorted(scored, key=lambda x: x[2], reverse=True)[:max_sentences]
    top_sorted = sorted(top, key=lambda x: x[0])
    return " ".join(s for _, s, _ in top_sorted)

# ---------- DOCX generation ----------
def build_docx_digest(
    user_id: int,
    interval_label: str,
    keywords: List[str],
    results: Dict[str, Dict[str, Any]]
) -> str:
    """
    results: {
      channel_name: {
        "url": "...",
        "items": [ { "dt": datetime|None, "original": str, "summary": str } ]
      }, ...
    }
    Returns path to saved .docx
    """
    doc = Document()

    # Title
    title = doc.add_paragraph()
    run = title.add_run("Краткий дайджест по Telegram-каналам")
    run.font.bold = True
    run.font.size = Pt(16)

    # Meta
    meta = doc.add_paragraph()
    meta.add_run(f"Сформирован: ").bold = True
    meta.add_run(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    meta.add_run("\nИнтервал: ").bold = True
    meta.add_run(interval_label)
    meta.add_run("\nКлючевые слова: ").bold = True
    meta.add_run(", ".join(keywords) if keywords else "—")

    # Body
    for ch_name, data in results.items():
        url = data["url"]
        items = data["items"]

        p = doc.add_paragraph()
        hdr = p.add_run(f"\n{ch_name}")
        hdr.font.bold = True
        hdr.font.size = Pt(13)

        doc.add_paragraph(f"Источник: {ch_name} ({url})")

        if not items:
            doc.add_paragraph("Нет подходящих публикаций за выбранный интервал.")
            continue

        for it in items:
            dt_str = it["dt"].astimezone().strftime("%Y-%m-%d %H:%M") if it["dt"] else "дата не распознана"
            doc.add_paragraph(f"Дата публикации: {dt_str}")
            if it["summary"]:
                doc.add_paragraph(it["summary"])
            else:
                # fallback — оригинальный текст (усечённый)
                txt = it["original"]
                if len(txt) > 800:
                    txt = txt[:800] + "..."
                doc.add_paragraph(txt)
            doc.add_paragraph("---")

    fname = f"digest_{user_id}_{int(datetime.now().timestamp())}.docx"
    path = os.path.join(os.getcwd(), fname)
    doc.save(path)
    return path

# ---------- Bot setup ----------
async def on_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "Привет! Я подготовлю дайджест по Telegram-каналам.\n"
        "Пришлите Excel-файл со списком каналов:\n"
        "• столбец A — имя канала\n"
        "• столбец B — ссылка вида https://t.me/<slug>\n\n"
        "Поддерживаются *.xls и *.xlsx."
    )
    await state.set_state(DigestStates.WAITING_FOR_EXCEL)

async def on_excel(message: Message, state: FSMContext):
    if not message.document:
        await message.answer("Это не файл. Пожалуйста, пришлите Excel (*.xls или *.xlsx).")
        return

    file_name = message.document.file_name or ""
    if not (file_name.endswith(".xls") or file_name.endswith(".xlsx")):
        await message.answer("Нужен Excel-файл (*.xls или *.xlsx). Пришлите правильный файл.")
        return

    try:
        # download file bytes
        file = await message.bot.get_file(message.document.file_id)
        file_bytes = await message.bot.download_file(file.file_path)
        content = file_bytes.read()

        # validate & parse
        channels = read_channels_from_excel(content)
        await state.update_data(channels=channels)

        await message.answer(
            "Файл принят и проверен ✅\nВыберите интервал:",
            reply_markup=build_interval_keyboard()
        )
        await state.set_state(DigestStates.WAITING_FOR_INTERVAL)

    except ValueError as ve:
        logger.exception("Excel validation failed")
        await message.answer(f"Ошибка проверки Excel: {ve}")
    except Exception as e:
        logger.exception("Excel processing unexpected error")
        await message.answer(f"Не удалось обработать файл. Подробности: {e}")

async def on_interval(callback: CallbackQuery, state: FSMContext):
    if not callback.data or not callback.data.startswith("interval:"):
        await callback.answer()
        return
    key = callback.data.split(":", 1)[1]  # 'day' | 'week' | 'month'
    if key not in ("day", "week", "month"):
        await callback.answer("Неизвестный интервал")
        return

    await state.update_data(interval_key=key)
    await callback.message.answer(
        "Введите ключевые слова через запятую (например: ИИ, Python, вакансии).\n"
        "Можно оставить пустым — тогда я соберу всё подряд и кратко резюмирую."
    )
    await state.set_state(DigestStates.WAITING_FOR_KEYWORDS)
    await callback.answer()

async def on_keywords(message: Message, state: FSMContext):
    await message.answer("Принял. Формирую дайджест, это может занять немного времени…")
    await state.set_state(DigestStates.PROCESSING)

    data = await state.get_data()
    channels: List[Tuple[str, str, str]] = data.get("channels", [])
    interval_key: str = data.get("interval_key", "week")

    # parse keywords
    raw = (message.text or "").strip()
    keywords = [w.strip() for w in re.split(r"[,\n;]+", raw) if w.strip()]

    # Ensure NLTK
    ensure_nltk()

    try:
        # Time window
        since = now_utc() - interval_to_timedelta(interval_key)
        interval_label = {"day": "Сутки", "week": "Неделя", "month": "Месяц"}[interval_key]

        results: Dict[str, Dict[str, Any]] = {}

        for ch_name, ch_url, slug in channels:
            try:
                msgs = parse_telegram_s_page(slug)
                msgs = filter_messages_by_time_and_keywords(msgs, since, keywords)

                items = []
                for m in msgs:
                    summary = summarize_text_extractively(m["text"], keywords, max_sentences=3)
                    items.append({
                        "dt": m["dt"],
                        "original": m["text"],
                        "summary": summary
                    })

                results[ch_name] = {"url": ch_url, "items": items}

            except Exception as e:
                logger.exception(f"Ошибка парсинга канала {ch_name} ({ch_url})")
                results[ch_name] = {
                    "url": ch_url,
                    "items": [],
                }
                # Добавим "сообщение об ошибке" как элемент, чтобы пользователь видел, что канал не обработан
                results[ch_name]["items"].append({
                    "dt": None,
                    "original": f"Не удалось получить данные с {make_s_url(slug)}: {e}",
                    "summary": ""
                })

        # Build DOCX
        out_path = build_docx_digest(
            user_id=message.from_user.id,
            interval_label=interval_label,
            keywords=keywords,
            results=results
        )

        await message.answer_document(FSInputFile(out_path), caption="Готово. Ваш дайджест 📄")
        await state.clear()

    except Exception as e:
        logger.exception("Unexpected processing error")
        await message.answer(f"Произошла ошибка при формировании дайджеста: {e}")
        await state.clear()

# ---------- Entrypoint ----------
async def main():
    token = get_bot_token()
    bot = Bot(token=token)
    dp = Dispatcher(storage=MemoryStorage())

    dp.message.register(on_start, CommandStart())
    dp.message.register(on_excel, DigestStates.WAITING_FOR_EXCEL)
    dp.callback_query.register(on_interval, F.data.startswith("interval:"), DigestStates.WAITING_FOR_INTERVAL)
    dp.message.register(on_keywords, DigestStates.WAITING_FOR_KEYWORDS)

    logger.info("Bot started. Waiting for updates...")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")