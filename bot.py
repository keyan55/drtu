# -*- coding: utf-8 -*-
# Bot version: v212 (inline monospace formatting like on screenshots)
# aiogram v3 only
try:
    import asyncio as _asyncio
    import uvloop as _uvloop
    _asyncio.set_event_loop_policy(_uvloop.EventLoopPolicy())
except Exception:
    pass
import asyncio
import random
import smtplib
from io import BytesIO
from typing import List, Dict, Any, Optional, Tuple, Iterable, TYPE_CHECKING
import imaplib
import re
import unicodedata
import math
import ssl
import gc
import uuid
import traceback
import time
from aiohttp import TCPConnector
from aiogram.client.session.aiohttp import AiohttpSession
import hashlib
import inspect
import logging
from pathlib import Path
from cachetools import TTLCache
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from asyncio import Semaphore
from multiprocessing import Queue, Process, Event
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, update, delete, text
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from pathlib import Path as _Path
import json as _json
import os
try:
    # если запускаете как модуль: python -m botg.bot — относительный импорт сработает
    from . import config as _cfg  # type: ignore
except Exception:
    try:
        import config as _cfg  # type: ignore
    except Exception:
        _cfg = None  # модуль config недоступен

def _get_bot_token() -> str:
    # 1) env-переменная
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if token:
        return token
    # 2) config.py: поддерживаем оба имени на всякий случай
    if _cfg:
        token = getattr(_cfg, "TELEGRAM_TOKEN", None) or getattr(_cfg, "TELEGRAM_BOT_TOKEN", None)
        if token:
            return token
    raise RuntimeError("Не найден токен: установите TELEGRAM_BOT_TOKEN в окружение или задайте TELEGRAM_TOKEN/TELEGRAM_BOT_TOKEN в config.py")


RUNTIME_CACHE_DIR = _Path(__file__).resolve().parent / "runtime_cache"
RUNTIME_CACHE_DIR.mkdir(exist_ok=True)



def _user_ctx_cache_path(user_id: int) -> _Path:
    return RUNTIME_CACHE_DIR / f"user_ctx_{user_id}.json"
    
def save_user_ctx_cache(user_id: int, ctx: "smtp25.UserContext") -> None:
    """
    Сохраняет UserContext на диск, чтобы переживать рестарт процесса.
    Храним только «входные» поля контекста (domains/proxies/accounts/templates/subjects).
    """
    try:
        data = {
            "user_id": int(user_id),
            "ts": time.time(),
            "domains": list(getattr(ctx, "domains", []) or []),
            "send_proxies": list(getattr(ctx, "send_proxies", []) or []),
            "accounts": list(getattr(ctx, "accounts", []) or []),
            "templates": list(getattr(ctx, "templates", []) or []),
            "subjects": list(getattr(ctx, "subjects", []) or []),
        }
        _user_ctx_cache_path(user_id).write_text(_json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log_send_event(f"save_user_ctx_cache error uid={user_id}: {e}")
        
def load_user_ctx_caches_on_start(ttl_seconds: int = 172800) -> None:
    """
    При старте процесса восстанавливает UserContext из runtime_cache.
    Пропускает и удаляет протухшие файлы (старше ttl_seconds).
    """
    try:
        now = time.time()
        for p in RUNTIME_CACHE_DIR.glob("user_ctx_*.json"):
            try:
                data = _json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            uid = int(data.get("user_id") or 0)
            ts = float(data.get("ts") or 0.0)
            if not uid or not ts:
                continue
            # TTL проверка
            if now - ts > ttl_seconds:
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    pass
                continue

            # Восстановим объект контекста из сериализованных полей
            try:
                ctx = smtp25.UserContext(
                    user_id=uid,
                    domains=list(data.get("domains") or []),
                    send_proxies=list(data.get("send_proxies") or []),
                    accounts=list(data.get("accounts") or []),
                    templates=list(data.get("templates") or []),
                    subjects=list(data.get("subjects") or []),
                )
                try:
                    setattr(ctx, "has_smart_presets", bool(getattr(ctx, "templates", []) or []))
                    # Служебные таймстемпы (могут пригодиться)
                    setattr(ctx, "_proxies_refreshed_ts", now)
                    setattr(ctx, "_ptrs_refreshed_ts", 0.0)
                except Exception:
                    pass
                # Кладём в RAM‑кэш на 48 часов
                try:
                    USER_CTX_CACHE[uid] = ctx
                except Exception:
                    pass
            except Exception as e_make:
                log_send_event(f"load_user_ctx_caches_on_start make ctx error uid={uid}: {e_make}")
    except Exception as e:
        log_send_event(f"load_user_ctx_caches_on_start error: {e}")

def _ad_cache_path(chat_id: int) -> _Path:
    return RUNTIME_CACHE_DIR / f"ad_cache_{chat_id}.json"
    
def save_ad_cache(chat_id: int) -> None:
    """
    Сохраняет на диск кэш по чату:
      - AD_ADS_BY_ID_PER_CHAT[chat_id]
      - AD_LOCAL2ID_PER_CHAT[chat_id]
      - AD_GENERATED_LINKS_PER_CHAT[chat_id]
      - AD_CHAT_TS[chat_id]
    """
    try:
        ads = AD_ADS_BY_ID_PER_CHAT.get(chat_id, {})
        # Преобразуем set() -> list для JSON
        ads_ser: dict[str, dict] = {}
        for ad_id, entry in (ads or {}).items():
            e = dict(entry or {})
            v = e.get("variants")
            if isinstance(v, set):
                e["variants"] = list(v)
            elif v is None:
                e["variants"] = []
            ads_ser[ad_id] = e

        data = {
            "chat_id": chat_id,
            "ads_by_id": ads_ser,
            "local2id": AD_LOCAL2ID_PER_CHAT.get(chat_id, {}),
            "generated": AD_GENERATED_LINKS_PER_CHAT.get(chat_id, {}),
            "ts": AD_CHAT_TS.get(chat_id, 0.0),
        }
        _ad_cache_path(chat_id).write_text(_json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        log_send_event(f"save_ad_cache error chat={chat_id}: {e}")
        
async def save_ad_cache_async(chat_id: int) -> None:
    """
    Асинхронная оболочка для save_ad_cache: пишет файл в thread pool,
    чтобы не блокировать event loop.
    """
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, save_ad_cache, chat_id)
    except Exception as e:
        try:
            log_send_event(f"save_ad_cache_async error chat={chat_id}: {e}")
        except Exception:
            pass
        
def load_ad_caches_on_start() -> None:
    """
    Загружает все кэши из runtime_cache при старте процесса.
    """
    try:
        for p in RUNTIME_CACHE_DIR.glob("ad_cache_*.json"):
            try:
                data = _json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            chat_id = int(data.get("chat_id") or 0)
            if not chat_id:
                continue

            # Восстановление ads_by_id и set(variants)
            ads_src = data.get("ads_by_id") or {}
            ads_back: dict[str, dict] = {}
            for ad_id, entry in ads_src.items():
                e = dict(entry or {})
                v = e.get("variants")
                if isinstance(v, list):
                    e["variants"] = set(v)
                elif v is None:
                    e["variants"] = set()
                ads_back[ad_id] = e

            if ads_back:
                AD_ADS_BY_ID_PER_CHAT[chat_id] = ads_back
            loc = data.get("local2id") or {}
            gen = data.get("generated") or {}
            ts_val = data.get("ts") or 0.0
            if loc:
                AD_LOCAL2ID_PER_CHAT[chat_id] = dict(loc)
            if gen:
                AD_GENERATED_LINKS_PER_CHAT[chat_id] = dict(gen)
            if ts_val:
                AD_CHAT_TS[chat_id] = float(ts_val)
    except Exception as e:
        log_send_event(f"load_ad_caches_on_start error: {e}")

# === КЭШИ ДЛЯ ОБЪЯВЛЕНИЙ / ССЫЛОК ===


# ID -> данные объявления (per chat)
# AD_ADS_BY_ID_PER_CHAT[chat_id][ad_id] = {
#   "ad_id": str,
#   "raw_nick": str,
#   "norm_nick": str,
#   "link": str,
#   "variants": set()   # все варианты local part / никнейма, которые привязаны к этому ID (для отладки/диагностики)
# }
AD_ADS_BY_ID_PER_CHAT: dict[int, dict[str, dict]] = {}

# Вариант (нормализованный local part / ник / база) -> ad_id
AD_LOCAL2ID_PER_CHAT: dict[int, dict[str, str]] = {}

# Результаты генерации (оставляем — используется остальной логикой)
AD_GENERATED_LINKS_PER_CHAT: dict[int, dict[str, dict[str, str | int]]] = {}

# Ответы на входящие
REPLIED_MSGS: dict[int, set[int]] = {}

# TTL трекинг (оставляем)
AD_CACHE_TTL = 2 * 86400  # 48 часов
AD_CHAT_TS: dict[int, float] = {}
PERM_AUTH_NOTIFIED: dict[tuple[int, str], bool] = {}

import aiohttp as _aiohttp

_HTTP_SESSION: _aiohttp.ClientSession | None = None

async def get_http_session() -> _aiohttp.ClientSession:
    """
    Лениво создаёт и кеширует aiohttp ClientSession с пулом соединений.
    Используется для Goo API, загрузки фото, fetch_ad_metadata.
    """
    global _HTTP_SESSION
    if _HTTP_SESSION and not _HTTP_SESSION.closed:
        return _HTTP_SESSION
    connector = _aiohttp.TCPConnector(limit=100, ttl_dns_cache=300)
    _HTTP_SESSION = _aiohttp.ClientSession(connector=connector)
    return _HTTP_SESSION

# Небольшие TTL‑кэши для тяжёлых HTTP‑операций
GOO_LINK_CACHE = TTLCache(maxsize=5000, ttl=1800)  # 30 минут, ключ: (original_url, profile_id, tuple(services))
AD_META_CACHE  = TTLCache(maxsize=5000, ttl=1800)  # 30 минут, ключ: original_url

def _gen_base_variants(first: str, last: str) -> set[str]:
    """
    Варианты для пары (first,last) в lower (без нормализации точки/дефиса):
      first.last , first last , first_last , first-last , firstlast
    """
    f = first.lower()
    l = last.lower()
    return {
        f"{f}.{l}",
        f"{f} {l}",
        f"{f}_{l}",
        f"{f}-{l}",
        f"{f}{l}",
    }

GOO_DEFAULT_SERVICE = "ebay_de"      # при необходимости поменяешь глобально

def _norm_ad_local(s: str) -> str:
    import unicodedata, re
    s = (s or "").replace("\u00A0", " ")
    s = unicodedata.normalize("NFKC", s)
    s = s.replace(".", " ").replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s.strip().lower())
    return s
    
# ========= FETCH AD METADATA (title, price, photo) =========
AD_META_HTTP_TIMEOUT = 12  # секунды

async def fetch_ad_metadata(url: str) -> tuple[str, str, str]:
    import re, html
    key = url.strip()
    try:
        cached = AD_META_CACHE.get(key)  # type: ignore[name-defined]
        if cached:
            return cached
    except Exception:
        pass

    title = ""; price = ""; photo = ""
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36")
    }
    try:
        session = await get_http_session()  # type: ignore[name-defined]
        async with session.get(url, headers=headers, timeout=20) as resp:
            if resp.status != 200:
                return "", "", ""
            raw = await resp.text()
    except Exception:
        return "", "", ""

    compact = re.sub(r"\s+", " ", raw)

    # Title
    m = re.search(r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', raw, re.I)
    if m:
        title = html.unescape(m.group(1)).strip()
    if not title:
        m = re.search(r'<meta[^>]+name=["\']title["\'][^>]+content=["\']([^"\']+)["\']', raw, re.I)
        if m:
            title = html.unescape(m.group(1)).strip()
    if not title:
        m = re.search(r'<title>(.*?)</title>', raw, re.I | re.S)
        if m:
            t = html.unescape(m.group(1)).strip()
            title = re.sub(r'\s*\|\s*Kleinanzeigen.*$', '', t, flags=re.I)

    # Photo
    m = re.search(r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', raw, re.I)
    if m:
        photo = m.group(1).strip()
    if not photo:
        m = re.search(r'<img[^>]+src=["\']([^"\']+?)(?:\?[^"\']*)?["\'][^>]*>', raw, re.I)
        if m:
            candidate = m.group(1)
            if candidate.startswith("http"):
                photo = candidate

    # Price
    m = re.search(r'<meta[^>]+property=["\']product:price:amount["\'][^>]+content=["\']([^"\']+)["\']', raw, re.I)
    if m:
        price = m.group(1).strip()
    if not price:
        m = re.search(r'(?:itemprop=["\']price["\'][^>]*content=["\']([^"\']+)["\'])', raw, re.I)
        if m:
            price = m.group(1).strip()
    if not price:
        m = re.search(r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{1,2})?\s?(?:€|eur))', compact, re.I)
        if m:
            price = m.group(1).strip()

    price = price.replace("&nbsp;", " ").replace("EUR", "€").replace("eur", "€")
    price = re.sub(r'\s+', ' ', price).strip()
    result = (title[:300], price[:60], photo[:500])

    try:
        AD_META_CACHE[key] = result  # type: ignore[name-defined]
    except Exception:
        pass
    return result
    
def _has_generated_link(chat_id: int, to_email: str) -> tuple[bool, dict]:
    """
    Проверяем, есть ли сгенерированная ссылка для local part email.
    Возврат: (True/False, entry_dict).
    """
    import unicodedata, re
    if "@" not in (to_email or ""):
        return False, {}
    local = to_email.split("@", 1)[0]

    def norm(s: str) -> str:
        s = (s or "").replace("\u00A0", " ")
        s = unicodedata.normalize("NFKC", s)
        s = s.replace(".", " ").replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s.strip().lower())
        return s

    k = norm(local)
    entry = AD_GENERATED_LINKS_PER_CHAT.get(chat_id, {}).get(k) or {}
    link_val = entry.get("short") or entry.get("original") or ""
    return (bool(link_val), entry)
    
def klein_templates_kb(base_mid: int) -> InlineKeyboardMarkup:
    rows = []
    for tpl_id, meta in KLEIN_HTML_TEMPLATES.items():
        rows.append([InlineKeyboardButton(
            text=f"📄 {meta['name']}",
            callback_data=f"reply:klein_tpl:{tpl_id}:{base_mid}"
        )])
    rows.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="reply:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)



CLEANUP_INTERVAL = 60  # seconds

SHARED_EXECUTOR = ThreadPoolExecutor(max_workers=60)  # Оптимизировано под 13 пользователей (HTTP запросы: короткие ссылки, метаданные)
# Расчет: 13 пользователей × ~5 параллельных HTTP запросов на пользователя = 65 воркеров
# Используем 60 воркеров с небольшим запасом для обработки пиковых нагрузок

# ===== OLD IMAP CONSTANTS REMOVED - NOW USING imap_runtime MODULE =====
# All IMAP-related constants moved to imap_runtime.py
# Legacy compatibility stubs for code that may still reference these
IMAP_EXECUTOR = ThreadPoolExecutor(max_workers=10)  # Stub for compatibility with rotation_run
IMAP_ACCOUNT_STATUS: dict[tuple[int, int], dict] = {}  # Compatibility stub
IMAP_STATUS: dict[int, "UserImapStatus"] = {}  # Compatibility stub
# ===== END OLD IMAP CONSTANTS =====

CLEANUP_PERIOD = 48 * 3600  # 48 часов между одноразовыми запусками
LAST_CLEANUP_MARKER = Path(__file__).resolve().parent / ".last_cleanup_ts"
_MAX_AGE_SECONDS = CLEANUP_PERIOD

LOGO_FILE_PATH = Path(__file__).resolve().parent / "logo_sender.jpg"

def _make_msgid(domain_hint: Optional[str] = None) -> str:
    try:
        domain = domain_hint or getattr(config, "SENDER_DOMAIN", "bot.local")
        return f"<{uuid.uuid4().hex}@{domain}>"
    except Exception:
        return f"<{uuid.uuid4().hex}@bot.local>"
        


SEND_LOG_FILE = "send_process.log"
send_logger = logging.getLogger("send_logger")
send_logger.setLevel(logging.INFO)
send_logger.propagate = False
# Хендлеры добавим/запустим в setup_nonblocking_send_logger() при старте

def log_send_event(event: str):
    send_logger.error(event)

def setup_nonblocking_send_logger() -> None:
    """
    Переводит send_logger на неблокирующую запись в файл:
      - QueueHandler в основном потоке
      - QueueListener в фоновой нити, пишет в SEND_LOG_FILE
    Идемпотентно: при повторном вызове перенастраивает логгер.
    """
    from logging.handlers import QueueHandler, QueueListener
    import queue as _queue

    global _SEND_LOG_QUEUE, _SEND_LOG_LISTENER, send_logger

    # Снимем старые хендлеры, чтобы не было дубликатов
    for h in list(send_logger.handlers):
        try:
            send_logger.removeHandler(h)
        except Exception:
            pass

    # Уровень может оставаться INFO: фильтр ниже отсечёт «не ошибки»
    send_logger.setLevel(logging.INFO)
    send_logger.propagate = False

    # Фильтр: пропускаем только "ошибочные" сообщения
    class _OnlyErrorsFilter(logging.Filter):
        ERR_MARKERS = (
            "error", "exception", "fail", "failed", "timeout",
            "auth", "invalid", "blocked", "cannot", "not found",
            "denied", "refused"
        )
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = (record.getMessage() or "").lower()
            except Exception:
                msg = ""
            return any(m in msg for m in self.ERR_MARKERS)

    # Вешаем фильтр на сам логгер (до очереди/файла)
    try:
        # Удалим прежние фильтры, если были
        for f in list(send_logger.filters):
            try:
                send_logger.removeFilter(f)
            except Exception:
                pass
        send_logger.addFilter(_OnlyErrorsFilter())
    except Exception:
        pass

    _SEND_LOG_QUEUE = _queue.Queue(-1)

    file_handler = logging.FileHandler(SEND_LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))

    qh = QueueHandler(_SEND_LOG_QUEUE)
    send_logger.addHandler(qh)

    _SEND_LOG_LISTENER = QueueListener(_SEND_LOG_QUEUE, file_handler)
    _SEND_LOG_LISTENER.start()
    
def stop_nonblocking_send_logger() -> None:
    """
    Останавливает QueueListener неблокирующего логирования, если был запущен.
    Безопасна к повторному вызову.
    """
    try:
        global _SEND_LOG_LISTENER
        if '_SEND_LOG_LISTENER' in globals() and _SEND_LOG_LISTENER:
            try:
                _SEND_LOG_LISTENER.stop()
            except Exception:
                pass
    except Exception:
        pass



import pandas as pd
from aiogram import Bot, Dispatcher, types, F

from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, BotCommand, File, ReplyKeyboardRemove  # добавлено ReplyKeyboardRemove
)

from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

bot = Bot(token=_get_bot_token(), default=DefaultBotProperties(parse_mode=ParseMode.HTML))

from sqlalchemy.exc import IntegrityError

from db_async import (
    # User functions
    get_or_create_user_async,
    approve_user_async,
    
    # Account functions
    list_accounts_async,
    get_account_async,
    add_account_async,
    update_account_async,
    delete_account_async,
    set_account_active_async,
    clear_accounts_async,
    list_accounts_page_async,
    
    # Domain functions
    list_domains_async,
    add_domain_async,
    set_domains_order_async,
    delete_domains_by_indices_async,
    clear_domains_async,
    
    # Preset functions
    list_presets_async,
    get_preset_async,
    add_preset_async,
    update_preset_async,
    delete_presets_by_ids_async,
    clear_presets_async,
    
    # Smart Preset functions
    list_smart_presets_async,
    add_smart_preset_async,
    update_smart_preset_async,
    delete_smart_presets_by_ids_async,
    clear_smart_presets_async,
    
    # Subject functions
    list_subjects_async,
    add_subject_async,
    update_subject_async,
    delete_subjects_by_ids_async,
    clear_subjects_async,
    
    # Proxy functions
    list_proxies_async,
    get_proxy_async,
    add_proxy_async,
    update_proxy_async,
    delete_proxies_by_ids_async,
    clear_proxies_async,
    
    # Setting functions
    get_setting_async,
    set_setting_async,
    
    # Outgoing mapping
    register_outgoing_msgid_mapping,
    
    # Admin functions
    list_users_async,
    activate_all_accounts_async,
    deactivate_all_accounts_async,
    
    # Incoming Message functions
    get_incoming_message_by_tgmid_async,
    incoming_message_exists_async,
    add_incoming_message_async,
    
    # Session and utility
    
    DB_SEMAPHORE,

    # ==== ДОБАВИТЬ ЭТУ СТРОКУ ====
    delete_user_data_async,
)
# Глобальные объекты (должны быть объявлены ДО первых @dp.* декораторов)


async def pick_proxy_for_account(user_id: int) -> Optional[int]:
    """
    Обёртка: выбирает proxy_id равномерно.
    """
    from db_async import pick_proxy_for_account_async, list_proxies_async
    try:
        pid = await pick_proxy_for_account_async(user_id)
        if pid:
            return int(pid)
    except Exception:
        pass
    try:
        proxies = await list_proxies_async(user_id, "send")
        for p in proxies:
            if getattr(p, "id", None):
                return int(getattr(p, "id"))
    except Exception:
        pass
    return None

from models import Account


from email.header import decode_header, make_header
from email import message_from_bytes
from email.utils import parseaddr

from html_templates import (
    router as html_templates_router,
    html_menu_kb,
    get_last_html,
    get_last_html_meta,
    set_last_html_meta,
    set_last_html,      # нужно, чтобы по желанию сохранять последний HTML
    _build_html,        # внутренний конструктор HTML (используем для авто-отправки)
)

# Глобальные объекты (должны быть объявлены ДО первых @dp.* декораторов)
bot: Bot  # будет присвоен в main()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(html_templates_router)


import config
import smtp25
from smtp25 import STOPWORDS  # глобальные стоп-слова
from smtp25 import set_sticky_proxy_for_account
from tg_internal_cache import internal_id_from_tg
import imap_runtime  # New IMAP runtime module
from imap_runtime import ImapAccountConfig  # Import account config from runtime

async def U(msg_or_call):

    """Возвращает internal users.id (создаёт запись при необходимости). Fallback: tg_id."""

    fu = msg_or_call.from_user

    internal = await internal_id_from_tg(fu.id, fu.username, fu.first_name, fu.last_name)

    return internal if internal is not None else fu.id

import socks

def gen_numeric_html_filename() -> str:
    return f"{int(time.time())}s.html"

def _make_html_file(html: str, filename: Optional[str] = None) -> types.BufferedInputFile:
    name = filename or gen_numeric_html_filename()
    return types.BufferedInputFile((html or "").encode("utf-8"), filename=name)
    
async def normalize_internal_user_id(maybe_uid: int) -> int:
    """
    Guarantee an internal users.id (int32).
    If a big Telegram ID (>2_147_483_647) is passed, resolve/create the user row
    and return its internal id to avoid int32 overflow in DB queries.
    """
    if maybe_uid <= 2_147_483_647:
        return maybe_uid
    try:
        ns = await get_or_create_user_async(maybe_uid, None, None, None)
        if ns and getattr(ns, "id", None):
            return int(ns.id)
    except Exception as e:
        log_send_event(f"NORMALIZE_UID fail tg_id={maybe_uid}: {e}")
    # Fallback: modulo — should rarely be hit; signals a logic bug if used.
    return int(maybe_uid % 2_147_000_000)
    
async def incoming_rt_key_from_tg(tg_id: int) -> int:
    """
    Возвращает internal id для ключа INCOMING_RT.
    Используется там, где есть только chat_id (равен tg_id).
    """
    internal = await internal_id_from_tg(tg_id, None, None, None)
    return internal if internal is not None else tg_id
    
async def build_incoming_reply_kb_async(chat_id: int, message_id: int) -> InlineKeyboardMarkup:
    """
    Async версия: конвертирует chat_id (tg) -> internal_id и смотрит INCOMING_RT по (internal_id, message_id).
    """
    internal_uid = await incoming_rt_key_from_tg(chat_id)
    replied = message_id in REPLIED_MSGS.get(chat_id, set())

    has_link = False
    try:
        rt = INCOMING_RT.get((internal_uid, message_id))
        if rt:
            from_email = (rt.get("from_email") or "").strip()
            if "@" in from_email:
                local_part = from_email.split("@", 1)[0]

                import unicodedata, re
                def _norm(s: str) -> str:
                    s = (s or "").replace("\u00A0", " ")
                    s = unicodedata.normalize("NFKC", s)
                    s = s.replace(".", " ").replace("_", " ").replace("-", " ")
                    s = re.sub(r"\s+", " ", s.strip().lower())
                    return s

                k_local = _norm(local_part)
                if AD_GENERATED_LINKS_PER_CHAT.get(chat_id, {}).get(k_local):
                    has_link = True
    except Exception:
        pass

    first_text = "✍️ Написать ещё" if replied else "✉️ Ответить"
    if has_link:
        second_btn = InlineKeyboardButton(text="Ссылка", callback_data=f"adlink:open:{message_id}")
    else:
        second_btn = InlineKeyboardButton(text="Создать ссылку", callback_data=f"adlink:create:{message_id}")

    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text=first_text, callback_data="reply:msg"),
            second_btn
        ]]
    )
    

# ====== Асинхронный UserContext ======
async def get_user_ctx_async(user_id: int) -> smtp25.UserContext:
    """
    Build per-user sending context (domains, proxies, active accounts, smart presets as templates, subjects).
    Кэшируется на 48 часов (RAM), также сохраняется на диск (переживает рестарт).
    Инвалидация: invalidate_user_ctx / invalidate_user_cache.

    Оптимизации:
      - DB_SEMAPHORE берётся локально вокруг каждого отдельного DB-вызова, а не на всю сборку.
      - Сохранение на диск выполняется в background (executor), чтобы не блокировать event loop.
      - Singleflight: одновременные сборки одного и того же user_id коалесцируются в одну.
      - Лёгкое perf-логирование медленных сборок.
    """
    # Singleflight трекинг
    global _USER_CTX_BUILD_TASKS
    try:
        _USER_CTX_BUILD_TASKS
    except NameError:
        _USER_CTX_BUILD_TASKS = {}

    original_user_id = user_id
    user_id = await normalize_internal_user_id(user_id)
    if user_id != original_user_id:
        log_send_event(f"get_user_ctx_async: normalized tg_id={original_user_id} -> internal_id={user_id}")

    # RAM‑кэш (48ч)
    try:
        cached = USER_CTX_CACHE.get(user_id)  # type: ignore[attr-defined]
    except Exception:
        cached = None
    if cached is not None:
        return cached

    # Если уже идёт сборка — дождёмся её
    task = _USER_CTX_BUILD_TASKS.get(user_id)
    if task and not task.done():
        try:
            return await task
        except Exception:
            # если сборка упала — очистим трекинг и попробуем ниже собрать заново
            _USER_CTX_BUILD_TASKS.pop(user_id, None)

    async def _build() -> smtp25.UserContext:
        start = time.monotonic()

        # Домены
        try:
            async with DB_SEMAPHORE:
                domains = await list_domains_async(user_id)
        except Exception as e:
            log_send_event(f"CTX: failed load domains for uid={user_id}: {e}")
            domains = []

        # send‑прокси
        send_proxies: list[dict] = []
        try:
            async with DB_SEMAPHORE:
                proxies = await list_proxies_async(user_id, "send")
            for p in (proxies or []):
                send_proxies.append({
                    "id": getattr(p, "id", None),
                    "host": getattr(p, "host", None),
                    "port": getattr(p, "port", None),
                    "user": getattr(p, "user_login", None),
                    "password": getattr(p, "password", None),
                })
        except Exception as e:
            log_send_event(f"CTX: failed load proxies for uid={user_id}: {e}")

        # Аккаунты (исключаем отключённые для массовой рассылки)
        accounts: list[dict] = []
        try:
            # Загружаем флаговый кэш отключённых (если не загружен)
            if user_id not in SEND_DISABLED_ACCOUNTS:
                await ensure_send_disabled_loaded(user_id)

            async with DB_SEMAPHORE:
                accs = await list_accounts_async(user_id)

            disabled_set = SEND_DISABLED_ACCOUNTS.get(user_id, set())
            for a in (accs or []):
                acc_id = getattr(a, "id", None)
                if acc_id is None or acc_id in disabled_set:
                    continue
                accounts.append({
                    "id": int(acc_id),
                    "name": getattr(a, "display_name", None),
                    "email": getattr(a, "email", None),
                    "password": getattr(a, "password", None),
                })
        except Exception as e:
            log_send_event(f"CTX: failed load accounts for uid={user_id}: {e}")

        # Умные пресеты -> templates
        templates: list[str] = []
        try:
            async with DB_SEMAPHORE:
                smart_items = await list_smart_presets_async(user_id)
            for sp in (smart_items or []):
                b = (getattr(sp, "body", "") or "").strip()
                if b:
                    templates.append(b)
        except Exception as e:
            log_send_event(f"CTX: failed load smart_presets for uid={user_id}: {e}")

        # Темы
        subjects: list[str] = []
        try:
            async with DB_SEMAPHORE:
                subs = await list_subjects_async(user_id)
            for s in (subs or []):
                t = (getattr(s, "title", "") or "").strip()
                if t:
                    subjects.append(t)
        except Exception as e:
            log_send_event(f"CTX: failed load subjects for uid={user_id}: {e}")

        # Объект контекста
        ctx = smtp25.UserContext(
            user_id=user_id,
            domains=domains,
            send_proxies=send_proxies,
            accounts=accounts,
            templates=templates,
            subjects=subjects
        )
        try:
            setattr(ctx, "has_smart_presets", bool(templates))
            setattr(ctx, "_proxies_refreshed_ts", time.time())
            setattr(ctx, "_ptrs_refreshed_ts", 0.0)
        except Exception:
            pass

        # Кладём в RAM‑кэш
        try:
            USER_CTX_CACHE[user_id] = ctx  # 48ч TTL в кэше
        except Exception:
            pass

        # Сохраняем на диск в background thread (не блокируем event loop)
        try:
            loop = asyncio.get_running_loop()
            loop.run_in_executor(None, save_user_ctx_cache, user_id, ctx)
        except Exception:
            log_send_event(f"CTX: failed schedule save_user_ctx_cache uid={user_id}")

        dur = time.monotonic() - start
        if dur > 0.3:
            perf_logger.warning(f"SLOW_CTX_BUILD uid={user_id} took {dur:.2f}s")

        return ctx

    # Запускаем сборку singleflight-задачей
    t = asyncio.create_task(_build())
    _USER_CTX_BUILD_TASKS[user_id] = t
    try:
        ctx = await t
        return ctx
    finally:
        # очистим трекинг (если задача именно наша)
        cur = _USER_CTX_BUILD_TASKS.get(user_id)
        if cur is t:
            _USER_CTX_BUILD_TASKS.pop(user_id, None)

VERSION = "v212"

# ====== Constants ======
READ_INTERVAL = 15  # seconds (legacy, не используется в новой архитектуре)
IMAP_PORT_SSL = 993
MAX_EMAILS_PER_USER = 97
IMAP_HOST_MAP = {
    "gmail.com": "imap.gmail.com",
    "googlemail.com": "imap.gmail.com",
    "gmx.de": "imap.gmx.net",
    "gmx.net": "imap.gmx.net",
    "gmx.at": "imap.gmx.net",
    "web.de": "imap.web.de",
    "yahoo.com": "imap.mail.yahoo.com",
    "yahoo.co.uk": "imap.mail.yahoo.com",
    "yandex.ru": "imap.yandex.com",
    "yandex.com": "imap.yandex.com",
    "mail.ru": "imap.mail.ru",
    "bk.ru": "imap.mail.ru",
    "list.ru": "imap.mail.ru",
    "inbox.ru": "imap.mail.ru",
    "outlook.com": "outlook.office365.com",
    "hotmail.com": "outlook.office365.com",
    "live.com": "outlook.office365.com",
    "office365.com": "outlook.office365.com",
    "icloud.com": "imap.mail.me.com",
    "me.com": "imap.mail.me.com",
    "aol.com": "imap.aol.com",
}

# ====== OPTIMIZATION SETTINGS ======

# IMAP_SEMAPHORE больше не используется - процесс pool управляет параллелизмом
IMAP_SEMAPHORE = Semaphore(1)  # Заглушка для совместимости
SMTP_SEMAPHORE = Semaphore(75)  # Оптимизировано под 10 пользователей: 50 писем/сек, запас до 75
XLSX_SEMAPHORE = Semaphore(3)

MAX_TASKS_PER_USER = 3
USER_TASK_SEMAPHORES: dict[int, asyncio.Semaphore] = {}

# Кэши
ACCOUNTS_CACHE = TTLCache(maxsize=1000, ttl=60)
USER_CTX_CACHE = TTLCache(maxsize=1000, ttl=172800)  # 48 часов
DOMAINS_CACHE = TTLCache(maxsize=1000, ttl=300)
INCOMING_RT: TTLCache = TTLCache(maxsize=10000, ttl=172800)  # 48h
THREAD_LAST_OUT: TTLCache = TTLCache(maxsize=20000, ttl=7 * 86400)  # 7 дней
# Логирование производительности
perf_logger = logging.getLogger("performance")

def time_it(func):
    async def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = await func(*args, **kwargs)
        duration = time.monotonic() - start
        if duration > 1.0:
            perf_logger.warning(f"SLOW: {func.__name__} took {duration:.2f}s")
        return result
    return wrapper

# ====== Access control ======
ADMIN_IDS: List[int] = []
try:
    if hasattr(config, "ADMIN_IDS") and isinstance(config.ADMIN_IDS, (list, tuple)):
        ADMIN_IDS = [int(x) for x in config.ADMIN_IDS]
    elif hasattr(config, "ADMIN_TELEGRAM_ID"):
        ADMIN_IDS = [int(config.ADMIN_TELEGRAM_ID)]
except Exception:
    ADMIN_IDS = []

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

async def ensure_approved(obj: types.Message | types.CallbackQuery) -> bool:
    if isinstance(obj, types.CallbackQuery):
        user = obj.from_user
        msg = obj.message
    else:
        user = obj.from_user
        msg = obj
    # Используем асинхронную обёртку из db_async
    u = await get_or_create_user_async(user.id, user.username, user.first_name, user.last_name)
    if not u or u.status != "approved":
        await msg.answer("Ваша заявка на доступ отправлена администратору. Ожидайте одобрения.")
        return False
    return True

# ====== FSM ======
class PolyaFSM(StatesGroup):
    email = State()

class GooProfileFSM(StatesGroup):
    profile = State()

class TokensFSM(StatesGroup):
    team_key = State()
    worker_key = State()

class AddAccountFSM(StatesGroup):
    display_name = State()
    loginpass = State()
    
class SpoofNameFSM(StatesGroup):
    name = State()

class ReplyFSM(StatesGroup):
    compose = State()
    html = State()
    
class SubjectHtmlFSM(StatesGroup):
    text = State()

class EditAccountFSM(StatesGroup):
    account_id = State()
    display_name = State()
    loginpass = State()

class EmailDeleteFSM(StatesGroup):
    account_id = State()

class EmailsClearFSM(StatesGroup):
    confirm = State()

class PresetAddFSM(StatesGroup):
    title = State()
    body = State()

class PresetEditFSM(StatesGroup):
    preset_id = State()
    title = State()
    body = State()

class PresetDeleteFSM(StatesGroup):
    preset_id = State()

class PresetClearFSM(StatesGroup):
    confirm = State()

class SmartPresetAddFSM(StatesGroup):
    body = State()

class SmartPresetEditFSM(StatesGroup):
    preset_id = State()
    body = State()

class SmartPresetDeleteFSM(StatesGroup):
    preset_id = State()

class SmartPresetClearFSM(StatesGroup):
    confirm = State()

class SubjectAddFSM(StatesGroup):
    title = State()

class SubjectEditFSM(StatesGroup):
    subject_id = State()
    title = State()

class SubjectDeleteFSM(StatesGroup):
    subject_id = State()

class SubjectClearFSM(StatesGroup):
    confirm = State()

class CheckNicksFSM(StatesGroup):
    file = State()

class QuickAddFSM(StatesGroup):
    mode = State()
    name = State()
    lines = State()

class DomainsFSM(StatesGroup):
    add = State()
    reorder = State()
    delete = State()
    clear = State()

class IntervalFSM(StatesGroup):
    set = State()

class ProxiesFSM(StatesGroup):
    add = State()
    edit_pick = State()
    edit_value = State()
    delete = State()
    clear = State()

class SingleSendFSM(StatesGroup):
    to = State()
    body = State()

# +++ Admin FSM +++
class AdminFSM(StatesGroup):
    add_id = State()
    deny_id = State()

# ====== Runtime ======

# ==== OUTBOX (пер‑пользовательский воркер для пресетов/ответов) ====
from dataclasses import dataclass

# Очередь и задача на пользователя
OUTBOX_QUEUES: dict[int, asyncio.Queue] = {}
OUTBOX_TASKS: dict[int, asyncio.Task] = {}

# Минимальный зазор между двумя отправками одним и тем же аккаунтом (вне массового сендинга)
OUTBOX_MIN_GAP_PER_ACC = 0.8  # сек
_LAST_OUTBOX_TS: dict[tuple[int, int], float] = {}  # (uid, acc_id) -> ts

# Ограничение скорости отправки сообщений об остановке потоков (не более 1 сообщения в 2 секунды)
_LAST_STOP_MESSAGE_TS: dict[int, float] = {}  # chat_id -> timestamp
STOP_MESSAGE_MIN_INTERVAL = 2.0  # секунды

@dataclass
class OutboxJob:
    acc_id: int
    to_email: str
    subject: str
    body: str
    html: bool = False
    photo_bytes: bytes | None = None
    photo_name: str | None = None
    sender_name_override: str | None = None
    src_tg_mid: int | None = None  # исходное сообщение (для reply‑треда)
    
@dataclass

# ===== REMOVED: ImapAccountConfig now imported from imap_runtime =====


class UserImapStatus:
    """Статус IMAP для пользователя (для совместимости с кодом, использующим ensure_user_imap_status)"""
    def __init__(self, user_id: int):
        self.user_id = user_id
        self.running = False
        self.accounts: dict[str, Any] = {}
        self.account_status: dict[str, dict] = {}
        self.account_backoff: dict[str, float] = {}  # Время задержки для каждого аккаунта (email -> timestamp)
        self.last_accounts_check = 0.0
        self.lock = asyncio.Lock()


def ensure_user_imap_status(user_id: int) -> UserImapStatus:
    """
    Получает или создает статус IMAP для пользователя.
    ВАЖНО: Эта функция нужна для совместимости с кодом, который использует старую архитектуру.
    Новая архитектура использует IMAP_ACCOUNT_STATUS напрямую.
    """
    if user_id not in IMAP_STATUS:
        IMAP_STATUS[user_id] = UserImapStatus(user_id)
    return IMAP_STATUS[user_id]
    
async def _pick_rr_account_id(uid: int) -> Optional[int]:
    """
    Пер‑пользовательский round‑robin выбор аккаунта для one-off отправок (onesend).
    Возвращает acc_id или None, если аккаунтов нет.
    """
    try:
        ctx = await get_user_ctx_async(uid)
        accs = list(getattr(ctx, "accounts", []) or [])
        if not accs:
            return None
        if not hasattr(_pick_rr_account_id, "_rr"):
            _pick_rr_account_id._rr = {}  # type: ignore[attr-defined]
        rr = _pick_rr_account_id._rr  # type: ignore[attr-defined]
        last = int(rr.get(uid, -1))
        idx = (last + 1) % len(accs)
        rr[uid] = idx
        return int(accs[idx].get("id"))
    except Exception:
        return None

async def _ensure_outbox_worker(uid: int, chat_id: int):
    """
    Поднимает воркер Outbox для пользователя, если он не запущен.
    Воркер авто‑завершается при простое.
    """
    if uid in OUTBOX_TASKS and OUTBOX_TASKS[uid] and not OUTBOX_TASKS[uid].done():
        return
    q = OUTBOX_QUEUES.setdefault(uid, asyncio.Queue())

    async def _outbox_worker():
        idle_timeout = 600.0  # 10 минут без задач — останавливаемся
        try:
            while True:
                try:
                    job: OutboxJob = await asyncio.wait_for(q.get(), timeout=idle_timeout)
                except asyncio.TimeoutError:
                    break
                except asyncio.CancelledError:
                    break

                # Анти‑лавина: небольшой зазор для этого аккаунта
                key = (uid, int(job.acc_id))
                last = _LAST_OUTBOX_TS.get(key, 0.0)
                now = time.time()
                if now - last < OUTBOX_MIN_GAP_PER_ACC:
                    await asyncio.sleep(OUTBOX_MIN_GAP_PER_ACC - (now - last))

                # Приоритетная отправка (не ждём чужие длинные очереди)
                msgid: str | None = None
                try:
                    msgid = await send_email_via_account(
                        uid,
                        job.acc_id,
                        job.to_email,
                        job.subject,
                        job.body,
                        html=job.html,
                        photo_bytes=job.photo_bytes,
                        photo_name=job.photo_name,
                        sender_name_override=job.sender_name_override,
                        max_attempts=2,
                        priority=True,
                        tg_id=chat_id if not job.html else None
                    )
                except Exception as e:
                    log_send_event(f"OUTBOX send exception uid={uid} acc_id={job.acc_id}: {e}")

                _LAST_OUTBOX_TS[key] = time.time()

                # Логи только после успешной отправки (как просили)
                try:
                    if msgid:
                        if job.src_tg_mid:
                            await _mark_replied(chat_id, int(job.src_tg_mid))
                        if job.html:
                            log_id = await log_html_reply_ok(chat_id, job.to_email, job.body, reply_to_message_id=job.src_tg_mid or 0)
                        else:
                            body_for_log = (job.body or "")
                            log_id = await log_text_reply_ok(chat_id, body_for_log, job.to_email, reply_to_message_id=job.src_tg_mid or 0)
                        if log_id:
                            THREAD_LAST_OUT[(uid, int(job.acc_id), job.to_email)] = int(log_id)
                        try:
                            await register_outgoing_msgid_mapping(uid, msgid, log_id or 0)
                        except Exception:
                            pass
                    else:
                        # Сообщаем об ошибке рядом с исходным (если был reply‑контекст)
                        try:
                            if job.src_tg_mid:
                                await bot.send_message(chat_id, "Ошибка отправки ❌", reply_to_message_id=job.src_tg_mid)
                            else:
                                await bot.send_message(chat_id, "Ошибка отправки ❌")
                        except Exception:
                            pass
                except Exception as e:
                    log_send_event(f"OUTBOX log error uid={uid} acc_id={job.acc_id}: {e}")
                finally:
                    try:
                        q.task_done()
                    except Exception:
                        pass
        finally:
            OUTBOX_TASKS.pop(uid, None)

    OUTBOX_TASKS[uid] = asyncio.create_task(_outbox_worker())

async def outbox_enqueue(
    uid: int,
    chat_id: int,
    acc_id: int,
    to_email: str,
    subject: str,
    body: str,
    *,
    html: bool = False,
    photo_bytes: bytes | None = None,
    photo_name: str | None = None,
    sender_name_override: str | None = None,
    src_tg_mid: int | None = None,
):
    """
    Кладёт задачу в Outbox и гарантирует поднятие воркера.
    Никаких сообщений «в очередь» — хендлеры возвращают мгновенно.
    """
    await _ensure_outbox_worker(uid, chat_id)
    job = OutboxJob(
        acc_id=acc_id,
        to_email=to_email,
        subject=subject,
        body=body,
        html=html,
        photo_bytes=photo_bytes,
        photo_name=photo_name,
        sender_name_override=sender_name_override,
        src_tg_mid=src_tg_mid
    )
    await OUTBOX_QUEUES[uid].put(job)



LAST_XLSX_PER_CHAT: Dict[int, dict] = {}  # { chat_id: {"data": bytes, "timestamp": float} }
BASES_PER_CHAT: Dict[int, List[str]] = {}
VERIFIED_ROWS_PER_CHAT: Dict[int, List[Dict[str, Any]]] = {}

# ====== УДАЛЕНО: Старая логика IMAP (async воркеры) ======
# Используется только новая архитектура с process pool
# Старые классы и функции удалены:
# - class UserImapStatus - УДАЛЕНО
# - def ensure_user_imap_status - УДАЛЕНО
# - IMAP_STATUS - УДАЛЕНО
# - IMAP_TASKS - УДАЛЕНО
# - async def _refresh_active_accounts_for_user - УДАЛЕНО
# - async def _pick_next_email - УДАЛЕНО
# - async def imap_loop_optimized - УДАЛЕНО
# - USER_IMAP_WORKERS - УДАЛЕНО
# - IMAP_USER_TICK, IMAP_ACCOUNTS_REFRESH_SEC, IMAP_MIN_GAP_SAME, IMAP_EST_FETCH_SEC, IMAP_PARALLEL_PER_USER - УДАЛЕНО


SEND_TASKS: Dict[int, asyncio.Task] = {}
SEND_STATUS: Dict[int, Dict[str, Any]] = {}
START_LOG_SENT: Dict[Tuple[int, str], bool] = {}
ERROR_LOG_SENT: Dict[Tuple[int, str], bool] = {}
SEND_LAST_ERROR: Dict[int, str] = {}
REPLY_RUNTIME: Dict[int, Dict[str, Any]] = {}
# === Быстрое добавление: карантин входящих ===
# Момент активации аккаунта после quick add: (user_id, acc_id) -> timestamp
QUICK_ADD_ACTIVATED_AT: dict[tuple[int, int], float] = {}
# Период, в течение которого старые входящие не публикуем
QUICK_ADD_QUARANTINE_PERIOD = 60.0  # секунд
# === Анти-дубликат автозапуска ИИ по отправителю ===
AI_SENDER_DEDUP: dict[int, set[str]] = {}

def _ai_sender_dedup_cache_path(user_id: int) -> _Path:
    """Путь к файлу кэша обработанных email адресов для ИИ"""
    return RUNTIME_CACHE_DIR / f"ai_sender_dedup_{user_id}.json"

def save_ai_sender_dedup_cache(user_id: int) -> None:
    """
    Сохраняет кэш обработанных email адресов на диск, чтобы переживать рестарт процесса.
    """
    try:
        user_id = int(user_id)
        senders = AI_SENDER_DEDUP.get(user_id)
        if not senders:
            # Если кэш пустой, удаляем файл если он есть
            cache_path = _ai_sender_dedup_cache_path(user_id)
            try:
                cache_path.unlink(missing_ok=True)
            except Exception:
                pass
            return
        
        data = {
            "user_id": user_id,
            "ts": time.time(),
            "senders": list(senders)  # Преобразуем set в list для JSON
        }
        cache_path = _ai_sender_dedup_cache_path(user_id)
        cache_path.write_text(_json.dumps(data, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        try:
            log_send_event(f"save_ai_sender_dedup_cache error uid={user_id}: {e}")
        except:
            pass

def load_ai_sender_dedup_caches_on_start(ttl_seconds: int = 604800) -> None:
    """
    При старте процесса восстанавливает кэш обработанных email адресов из runtime_cache.
    TTL по умолчанию: 7 дней (604800 секунд).
    Пропускает и удаляет протухшие файлы (старше ttl_seconds).
    """
    try:
        now = time.time()
        for p in RUNTIME_CACHE_DIR.glob("ai_sender_dedup_*.json"):
            try:
                data = _json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            uid = int(data.get("user_id") or 0)
            ts = float(data.get("ts") or 0.0)
            if not uid or not ts:
                continue
            # TTL проверка
            if now - ts > ttl_seconds:
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    pass
                continue
            
            # Восстанавливаем кэш
            try:
                senders_list = data.get("senders") or []
                if senders_list:
                    AI_SENDER_DEDUP[int(uid)] = set(senders_list)
            except Exception as e:
                try:
                    log_send_event(f"load_ai_sender_dedup_caches_on_start error uid={uid}: {e}")
                except:
                    pass
    except Exception as e:
        try:
            log_send_event(f"load_ai_sender_dedup_caches_on_start error: {e}")
        except:
            pass

def _norm_sender_email(addr: str) -> str:
    """
    Нормализует email отправителя: берём адрес из 'Name <email@host>' и приводим к lower().
    Если распарсить не удалось — возвращаем trimmed lower-строку как есть.
    """
    try:
        from email.utils import parseaddr
        email = (parseaddr(addr or "")[1] or addr or "").strip().lower()
        return email
    except Exception:
        return (addr or "").strip().lower()

async def is_ai_enabled_for_user(user_id: int) -> bool:
    """
    Возвращает True, если ИИ включён у пользователя (настройка 'ai_enabled').
    """
    try:
        val = (await get_setting_async(user_id, "ai_enabled", "0")).strip().lower()
        return val in ("1", "true", "yes", "on")
    except Exception:
        return False

def ai_sender_dedup_reset(uid: int, sender: str | None = None) -> None:
    """
    Сбрасывает отметку автозапуска по отправителю.
    - sender == None: очищает весь набор для пользователя.
    - sender != None: снимает блокировку только для этого адреса.
    После изменения сохраняет кэш на диск.
    """
    try:
        if sender is None:
            AI_SENDER_DEDUP.pop(int(uid), None)
            # Удаляем файл кэша
            try:
                _ai_sender_dedup_cache_path(uid).unlink(missing_ok=True)
            except Exception:
                pass
            return
        email = _norm_sender_email(sender)
        if not email:
            return
        st = AI_SENDER_DEDUP.get(int(uid))
        if st and email in st:
            st.discard(email)
            if not st:
                AI_SENDER_DEDUP.pop(int(uid), None)
                # Удаляем файл кэша, если список пуст
                try:
                    _ai_sender_dedup_cache_path(uid).unlink(missing_ok=True)
                except Exception:
                    pass
            else:
                # Сохраняем кэш на диск после изменения
                save_ai_sender_dedup_cache(uid)
    except Exception:
        pass

def _ai_should_autostart_for_sender(uid: int, from_email: str) -> bool:
    """
    True — можно запускать авто‑ИИ (первый раз для этого отправителя).
    False — уже запускался ранее для этого отправителя у данного пользователя.
    После добавления нового адреса сохраняет кэш на диск.
    """
    email = _norm_sender_email(from_email)
    if not email:
        # Если адрес не распознан — не блокируем (чтобы не ломать редкие кейсы)
        return True
    seen = AI_SENDER_DEDUP.setdefault(int(uid), set())
    if email in seen:
        return False
    seen.add(email)
    # Сохраняем кэш на диск после добавления нового адреса
    try:
        save_ai_sender_dedup_cache(uid)
    except Exception:
        pass  # Не блокируем работу, если сохранение не удалось
    return True

async def ai_autostart_if_allowed(
    uid: int,
    from_email: str,
    starter_coro,  # корутина/функция, которая запускает ИИ-процесс
    *args, **kwargs
) -> bool:
    """
    Универсальный гвард для запуска авто‑ИИ.
    Возвращает True, если ИИ был запущен; False — если запуск подавлен (выключен или дубликат отправителя).
    Пример использования:
        await ai_autostart_if_allowed(
            uid, from_email,
            maybe_schedule_ai_assistant, uid, chat_id, acc_obj, base_tg_mid, from_email, subject
        )
    """
    try:
        if not await is_ai_enabled_for_user(uid):
            return False
        if not _ai_should_autostart_for_sender(uid, from_email):
            log_send_event(f"AI AUTOSTART SUPPRESSED: uid={uid} sender={from_email}")
            return False
        res = starter_coro(*args, **kwargs)
        if inspect.isawaitable(res):
            await res
        return True
    except Exception as e:
        log_send_event(f"AI AUTOSTART ERROR uid={uid} sender={from_email}: {e}")
        return False

# ==== Очередь и подавление старт‑логов IMAP ====
# Очередь: user_id -> [(chat_id, email)]
START_LOG_QUEUE: dict[int, list[tuple[int, str]]] = {}
# Запущенные таски-дренеры: user_id -> task
START_LOG_DRAINERS: dict[int, asyncio.Task] = {}

# Одноразовое предупреждение о лимите чтения 97 аккаунтов
LIMIT97_WARNED: dict[int, bool] = {}
# Одноразовое подавление стартовых логов (например, после ротации)
SUPPRESS_START_LOGS: dict[int, set[str]] = {}
# Время активации аккаунтов после быстрого добавления: (user_id, acc_id) -> timestamp



def _ensure_start_log_drainer(user_id: int):
    """
    Дренер очереди логов старта IMAP-потоков.
    Изменено: после публикации всех сообщений в текущем запуске —
    удаляет их из чата, чтобы не засорять диалоги.
    """
    if user_id in START_LOG_DRAINERS and not START_LOG_DRAINERS[user_id].done():
        return

    async def _drain():
        sent_msgs: list[tuple[int, int]] = []  # (chat_id, message_id)
        try:
            while True:
                queue = START_LOG_QUEUE.get(user_id, [])
                if not queue:
                    # Очередь пуста: удаляем все отправленные в этом запуске сообщения
                    for ch, mid in sent_msgs:
                        try:
                            await delete_message_safe(types.Message(chat=types.Chat(id=ch, type="private"), message_id=mid))
                        except Exception:
                            # fallback: прямое удаление без обёртки
                            try:
                                await bot.delete_message(ch, mid)
                            except Exception:
                                pass
                        # небольшая пауза, чтобы не уткнуться в лимиты
                        await asyncio.sleep(0.05)
                    START_LOG_DRAINERS.pop(user_id, None)
                    return

                chat_id, email = queue.pop(0)
                # Пропускаем публикацию, если email в списке подавления (например, после ротации)
                if email in SUPPRESS_START_LOGS.get(user_id, set()):
                    continue
                try:
                    msg = await bot.send_message(chat_id, f"Поток для {code(email)} запущен ☑️")
                    sent_msgs.append((chat_id, getattr(msg, "message_id", 0) or 0))
                except Exception:
                    pass
                await asyncio.sleep(0.7)
        except asyncio.CancelledError:
            return
        except Exception:
            START_LOG_DRAINERS.pop(user_id, None)

    START_LOG_DRAINERS[user_id] = asyncio.create_task(_drain())

def schedule_start_log(user_id: int, chat_id: int, email: str):
    """
    Кладёт уведомление о старте в очередь и гарантирует запуск дренера.
    """
    q = START_LOG_QUEUE.setdefault(user_id, [])
    q.append((chat_id, email))
    _ensure_start_log_drainer(user_id)

# === Управление разрешением отправки (mass send) для аккаунтов ===
# Runtime-кэш: user_id -> set(account_id) отключённых (для быстроты)
SEND_DISABLED_ACCOUNTS: dict[int, set[int]] = {}

async def load_send_disabled_for_user(user_id: int):
    """
    Загружает из settings состояние отключённых аккаунтов пользователя и
    заполняет SEND_DISABLED_ACCOUNTS[user_id].
    """
    try:
        accounts = await list_accounts_async(user_id)
    except Exception:
        return
    disabled: set[int] = set()
    for acc in accounts:
        acc_id = getattr(acc, "id", None)
        if acc_id is None:
            continue
        try:
            val = (await get_setting_async(user_id, f"send_disabled_{acc_id}", "0")).strip()
        except Exception:
            val = "0"
        if val in ("1", "true", "yes", "on"):
            disabled.add(int(acc_id))
    SEND_DISABLED_ACCOUNTS[user_id] = disabled

async def ensure_send_disabled_loaded(user_id: int):
    if user_id not in SEND_DISABLED_ACCOUNTS:
        await load_send_disabled_for_user(user_id)

async def is_account_send_enabled(user_id: int, acc_id: int) -> bool:
    await ensure_send_disabled_loaded(user_id)
    return acc_id not in SEND_DISABLED_ACCOUNTS.get(user_id, set())

async def set_account_send_enabled(user_id: int, acc_id: int, enabled: bool):
    await ensure_send_disabled_loaded(user_id)
    disabled = SEND_DISABLED_ACCOUNTS.setdefault(user_id, set())
    if enabled:
        disabled.discard(acc_id)
        await set_setting_async(user_id, f"send_disabled_{acc_id}", "0")
    else:
        disabled.add(acc_id)
        await set_setting_async(user_id, f"send_disabled_{acc_id}", "1")

async def toggle_account_send_enabled(user_id: int, acc_id: int) -> bool:
    """
    Переключает состояние. Возвращает новое значение enabled.
    """
    en = await is_account_send_enabled(user_id, acc_id)
    await set_account_send_enabled(user_id, acc_id, not en)
    return not en

def set_reply_context(uid: int, acc_id: int, to_email: str, subject: str, src_tg_mid: int) -> None:
    REPLY_RUNTIME[uid] = {
        "acc_id": int(acc_id),
        "to": to_email,
        "subject": subject,
        "src_tg_mid": int(src_tg_mid or 0),
        "await_html_file": False,
    }

def get_reply_context(uid: int) -> Optional[Dict[str, Any]]:
    return REPLY_RUNTIME.get(uid)

def clear_reply_context(uid: int) -> None:
    try:
        REPLY_RUNTIME.pop(uid, None)
    except Exception:
        pass

async def mark_all_unseen_as_read_async(user_id: int, account_id: int) -> None:
    """
    Помечает все UNSEEN письма как прочитанные для аккаунта, добавленного через быстрое добавление.
    Выполняется в фоновой задаче, чтобы не блокировать процесс добавления.
    """
    try:
        # Получаем данные аккаунта
        acc = await get_account_async(user_id, account_id)
        if not acc:
            return
        
        email = getattr(acc, "email", "")
        password = getattr(acc, "password", "")
        if not email or not password:
            return
        
        # Получаем прокси для аккаунта (если есть)
        proxy = None
        try:
            proxy_id = getattr(acc, "proxy_id", None)
            if proxy_id:
                proxy_obj = await get_proxy_async(user_id, proxy_id)
                if proxy_obj:
                    proxy = {
                        "host": proxy_obj.host,
                        "port": proxy_obj.port,
                        "user_login": proxy_obj.user_login or "",
                        "password": proxy_obj.password or "",
                        "type": proxy_obj.type or "socks5"
                    }
        except Exception:
            pass
        
        # Определяем IMAP хост
        host = resolve_imap_host(email)
        
        # Создаем конфигурацию для подключения
        config = ImapAccountConfig(
            user_id=user_id,
            acc_id=account_id,
            email=email,
            password=password,
            display_name=getattr(acc, "display_name", ""),
            chat_id=user_id,  # для приватного бота chat_id == user_id
            host=host,
            proxy=proxy
        )
        
        # Подключаемся к IMAP и помечаем все UNSEEN письма как прочитанные
        # Используем executor, чтобы не блокировать event loop
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _mark_all_unseen_as_read_sync, config)
        
        log_send_event(f"QUICK_ADD: Все UNSEEN письма помечены как прочитанные uid={user_id} acc_id={account_id} email={email}")
    except Exception as e:
        log_send_event(f"QUICK_ADD: Ошибка при пометке писем как прочитанных uid={user_id} acc_id={account_id}: {e}")

def _mark_all_unseen_as_read_sync(config: ImapAccountConfig) -> None:
    """
    Синхронная функция для пометки всех UNSEEN писем как прочитанных.
    Выполняется в executor.
    
    ВАЖНО: Используем прямое подключение к IMAP, потому что функции воркера
    (connect_imap_for_account, with_timeout) недоступны в основном процессе -
    они определены внутри процесса воркера через multiprocessing.
    """
    imap_obj = None
    try:
        # Подключаемся к IMAP напрямую
        # SocksIMAP4SSL автоматически устанавливает таймаут на уровне сокета
        try:
            imap_obj = SocksIMAP4SSL(
                config.host,
                IMAP_PORT_SSL,
                proxy=config.proxy,
                timeout=IMAP_CONNECTION_TIMEOUT
            )
            
            # Устанавливаем таймаут на сокете для всех операций
            if hasattr(imap_obj, 'sock') and imap_obj.sock:
                imap_obj.sock.settimeout(IMAP_READ_TIMEOUT)
            
            # Логин
            typ, data = imap_obj.login(config.email, config.password)
            if typ != "OK":
                error_msg = (data[0] if data and len(data) > 0 else b"").decode("utf-8", errors="ignore")
                log_send_event(f"QUICK_ADD: Ошибка логина IMAP uid={config.user_id} acc_id={config.acc_id} error={error_msg}")
                try:
                    imap_obj.logout()
                except Exception:
                    pass
                return
            
            # Выбираем папку INBOX
            typ, data = imap_obj.select("INBOX")
            if typ != "OK":
                log_send_event(f"QUICK_ADD: Ошибка выбора папки INBOX uid={config.user_id} acc_id={config.acc_id}")
                try:
                    imap_obj.logout()
                except Exception:
                    pass
                return
            
            # Ищем все UNSEEN письма
            typ, data = imap_obj.uid("search", None, "UNSEEN")
            if typ != "OK" or not data or not data[0]:
                # Это нормально - может просто не быть непрочитанных писем
                log_send_event(f"QUICK_ADD: Нет UNSEEN писем uid={config.user_id} acc_id={config.acc_id}")
                try:
                    imap_obj.logout()
                except Exception:
                    pass
                return
            
            uid_bytes = data[0]
            if isinstance(uid_bytes, bytes):
                uid_str = uid_bytes.decode("utf-8", errors="ignore")
            else:
                uid_str = str(uid_bytes)
            
            unseen_uids = [u.strip() for u in uid_str.split() if u.strip()]
            
            if not unseen_uids:
                log_send_event(f"QUICK_ADD: Нет UNSEEN писем для пометки uid={config.user_id} acc_id={config.acc_id}")
                try:
                    imap_obj.logout()
                except Exception:
                    pass
                return
            
            # Помечаем все письма как прочитанные одним batch-запросом
            # Используем UID sequence (например, "1,2,3" или "1:100")
            uid_sequence = ",".join(unseen_uids)
            try:
                typ, _ = imap_obj.uid("store", uid_sequence, "+FLAGS", r"(\Seen)")
                if typ == "OK":
                    log_send_event(f"QUICK_ADD: Помечено {len(unseen_uids)} писем как прочитанные (batch) uid={config.user_id} acc_id={config.acc_id}")
                    # ВАЖНО: Закрываем и переоткрываем папку, чтобы сервер зафиксировал изменения
                    # Это гарантирует, что последующие запросы UNSEEN не вернут эти письма
                    try:
                        imap_obj.close()
                    except Exception:
                        pass
                    try:
                        typ, _ = imap_obj.select("INBOX")
                        if typ != "OK":
                            log_send_event(f"QUICK_ADD: Предупреждение: не удалось переоткрыть INBOX после пометки uid={config.user_id} acc_id={config.acc_id}")
                    except Exception as e:
                        log_send_event(f"QUICK_ADD: Ошибка переоткрытия INBOX uid={config.user_id} acc_id={config.acc_id}: {e}")
                else:
                    log_send_event(f"QUICK_ADD: Ошибка batch-пометки писем uid={config.user_id} acc_id={config.acc_id} response={typ}")
                    # Fallback: помечаем по одному
                    marked_count = 0
                    for uid in unseen_uids:
                        try:
                            typ2, _ = imap_obj.uid("store", uid, "+FLAGS", r"(\Seen)")
                            if typ2 == "OK":
                                marked_count += 1
                        except Exception as e:
                            log_send_event(f"QUICK_ADD: Ошибка пометки письма msg_uid={uid} uid={config.user_id} acc_id={config.acc_id}: {e}")
                            continue
                    log_send_event(f"QUICK_ADD: Помечено {marked_count} из {len(unseen_uids)} писем как прочитанные (fallback) uid={config.user_id} acc_id={config.acc_id}")
            except Exception as e:
                log_send_event(f"QUICK_ADD: Исключение при batch-пометке писем uid={config.user_id} acc_id={config.acc_id}: {e}")
                # Fallback: помечаем по одному
                marked_count = 0
                for uid in unseen_uids:
                    try:
                        typ, _ = imap_obj.uid("store", uid, "+FLAGS", r"(\Seen)")
                        if typ == "OK":
                            marked_count += 1
                    except Exception as e2:
                        log_send_event(f"QUICK_ADD: Ошибка пометки письма msg_uid={uid} uid={config.user_id} acc_id={config.acc_id}: {e2}")
                        continue
                log_send_event(f"QUICK_ADD: Помечено {marked_count} из {len(unseen_uids)} писем как прочитанные (fallback после исключения) uid={config.user_id} acc_id={config.acc_id}")
        except Exception as e:
            log_send_event(f"QUICK_ADD: Ошибка подключения к IMAP uid={config.user_id} acc_id={config.acc_id}: {e}")
            return
    except Exception as e:
        log_send_event(f"QUICK_ADD: Исключение при пометке писем uid={config.user_id} acc_id={config.acc_id}: {e}")
    finally:
        if imap_obj:
            try:
                imap_obj.logout()
            except Exception:
                pass
    
# ====== User SMTP/IMAP context (per-user, NO globals cross-talk) ======
USER_CTX: Dict[int, smtp25.UserContext] = {}


    


def invalidate_user_ctx(user_id: int) -> None:
    """
    Полная инвалидация контекста пользователя:
      - старый (устаревший) объект UserContext из старого механизма
      - RAM‑кэш (USER_CTX_CACHE)
      - файл на диске (runtime_cache/user_ctx_*.json)
    """
    try:
        USER_CTX.pop(user_id, None)
    except Exception:
        pass
    try:
        USER_CTX_CACHE.pop(user_id, None)
    except Exception:
        pass
    try:
        _user_ctx_cache_path(user_id).unlink(missing_ok=True)
    except Exception:
        pass

# ====== Helpers ======

async def _collect_reply_context_for_html(uid: int, acc_id: int, src_tg_mid: int) -> tuple[str, str, str]:
    """
    Возвращает (product_title, buyer_name, acc_display_name).
    product_title берётся из темы входящего; buyer_name — из From (fallback: ФИО аккаунта).
    """
    product_title, buyer_name, acc_display = "", "", ""

    # Попробуем взять из БД по tg_message_id
    try:
        row = await get_incoming_message_by_tgmid_async(uid, int(src_tg_mid or 0))
    except Exception:
        row = None

    if row:
        try:
            product_title = _extract_offer_title(getattr(row, "subject", "") or "")
            buyer_name = (getattr(row, "from_name", "") or "").strip()
        except Exception:
            pass

    # Рантайм-кэш — фолбэк/дополнение
    if not product_title or not buyer_name:
        try:
            rt = INCOMING_RT.get((uid, int(src_tg_mid or 0))) or {}
            if not product_title:
                product_title = _extract_offer_title(rt.get("subject", "") or "")
            if not buyer_name:
                buyer_name = (rt.get("from_name", "") or "").strip()
        except Exception:
            pass

    # ФИО аккаунта — фолбэк для buyer_name
    try:
        acc = await get_account_async(uid, int(acc_id))
        acc_display = (getattr(acc, "display_name", "") or getattr(acc, "name", "") or "").strip()
        if not acc_display and getattr(acc, "email", ""):
            acc_display = acc.email.split("@", 1)[0]
    except Exception:
        acc_display = ""

    if not buyer_name:
        buyer_name = acc_display

    return product_title, buyer_name, acc_display

def _generate_klein_go_subject() -> str:
    return f"Bestellung bestätigen #{random.randint(10000000, 99999999)}"
    
def _extract_offer_title(subj: str) -> str:
    """
    Извлекает название товара из темы: снимает Re/Fw/Fwd, берёт часть после '?' или ':'.
    """
    import re, html as _html
    s = _html.unescape(subj or "").strip()
    while True:
        s2 = re.sub(r'^(?:(?:re|fw|fwd)\s*:)\s*', '', s, flags=re.I)
        if s2 == s:
            break
        s = s2.strip()
    if "?" in s:
        s = s.split("?")[-1]
    elif ":" in s:
        s = s.split(":")[-1]
    s = re.sub(r'^[\-\—\:\.\s]+', '', s).strip()
    s = re.sub(r'\s{2,}', ' ', s)
    return s or (subj or "")
    
def _inject_klein_go_blocks(html_code: str, product: str, buyer: str, order_no: str, date_str: str, tpl: str = "GO") -> str:
    return html_code  # инфоблоки отключены

def escape_html(text: str) -> str:
    """Экранирует спецсимволы для HTML-сообщений Telegram."""
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def get_user_task_semaphore(user_id: int) -> asyncio.Semaphore:
    sem = USER_TASK_SEMAPHORES.get(user_id)
    if sem is None:
        sem = asyncio.Semaphore(MAX_TASKS_PER_USER)
        USER_TASK_SEMAPHORES[user_id] = sem
    return sem
    
async def cleanup_old_data_once():
    """
    Одноразовая очистка (ранее содержимое одного шага while True из cleanup_old_data_task):
      - Чистка XLSX / verified / bases
      - Ограничение размеров служебных логов
      - Чистка AD_* кэшей по TTL
      - Удаление файлов кэша при истечении TTL
      - Очистка долгоживущих RAM‑кэшей раз в 48 часов
      - Удаление просроченных user_ctx_* файлов (персистентный кэш контекстов)
    Выполняется максимум один раз за 48 часов (управляется внешним планировщиком).
    """
    try:
        now = time.time()

        # LAST_XLSX_PER_CHAT: вычищаем по возрасту
        for chat_id in list(LAST_XLSX_PER_CHAT.keys()):
            entry = LAST_XLSX_PER_CHAT.get(chat_id)
            ts = entry.get("timestamp", 0) if isinstance(entry, dict) else 0
            if now - ts > _MAX_AGE_SECONDS:
                LAST_XLSX_PER_CHAT.pop(chat_id, None)

        # VERIFIED_ROWS_PER_CHAT: чистим пустые
        for chat_id in list(VERIFIED_ROWS_PER_CHAT.keys()):
            if not VERIFIED_ROWS_PER_CHAT.get(chat_id):
                VERIFIED_ROWS_PER_CHAT.pop(chat_id, None)

        # BASES_PER_CHAT: если нет XLSX
        for chat_id in list(BASES_PER_CHAT.keys()):
            if chat_id not in LAST_XLSX_PER_CHAT:
                BASES_PER_CHAT.pop(chat_id, None)

        # Лимит служебных логов
        if len(START_LOG_SENT) > 5000:
            for k in list(START_LOG_SENT.keys())[:1000]:
                START_LOG_SENT.pop(k, None)
        if len(ERROR_LOG_SENT) > 5000:
            for k in list(ERROR_LOG_SENT.keys())[:1000]:
                ERROR_LOG_SENT.pop(k, None)

        # Чистка объявлений по TTL + удаление файлов кэша
        for chat_id, ts in list(AD_CHAT_TS.items()):
            if now - ts > AD_CACHE_TTL:
                AD_ADS_BY_ID_PER_CHAT.pop(chat_id, None)
                AD_LOCAL2ID_PER_CHAT.pop(chat_id, None)
                AD_GENERATED_LINKS_PER_CHAT.pop(chat_id, None)
                REPLIED_MSGS.pop(chat_id, None)
                AD_CHAT_TS.pop(chat_id, None)
                # Удалить диск‑кэш
                try:
                    _ad_cache_path(chat_id).unlink(missing_ok=True)
                except Exception:
                    pass

        # Ограничение числа чатов с объявлениями
        MAX_AD_CHATS = 2000
        if len(AD_CHAT_TS) > MAX_AD_CHATS:
            excess = len(AD_CHAT_TS) - MAX_AD_CHATS
            for old_chat, _ in sorted(AD_CHAT_TS.items(), key=lambda x: x[1])[:excess]:
                AD_ADS_BY_ID_PER_CHAT.pop(old_chat, None)
                AD_LOCAL2ID_PER_CHAT.pop(old_chat, None)
                AD_GENERATED_LINKS_PER_CHAT.pop(old_chat, None)
                REPLIED_MSGS.pop(old_chat, None)
                AD_CHAT_TS.pop(old_chat, None)
                try:
                    _ad_cache_path(old_chat).unlink(missing_ok=True)
                except Exception:
                    pass

        # Принудительная очистка долгоживущих RAM‑кэшей раз в 48 часов
        try:
            ACCOUNTS_CACHE.clear()
        except Exception:
            pass
        try:
            USER_CTX_CACHE.clear()
        except Exception:
            pass
        try:
            DOMAINS_CACHE.clear()
        except Exception:
            pass
        # Если добавили дополнительные кэши (ниже): тоже чистим
        try:
            PRESETS_CACHE.clear()
        except Exception:
            pass
        try:
            SMART_PRESETS_CACHE.clear()
        except Exception:
            pass
        try:
            SUBJECTS_CACHE.clear()
        except Exception:
            pass
        try:
            PROXIES_CACHE.clear()
        except Exception:
            pass

        # Очистка файлов user_ctx_* просроченных (персистентный кэш на диске; TTL = 48ч)
        try:
            TTL = 172800  # 48 часов
            for p in RUNTIME_CACHE_DIR.glob("user_ctx_*.json"):
                try:
                    data = _json.loads(p.read_text(encoding="utf-8"))
                    ts = float(data.get("ts") or 0.0)
                except Exception:
                    ts = 0.0
                if not ts or (now - ts > TTL):
                    p.unlink(missing_ok=True)
        except Exception as e_ctxf:
            log_send_event(f"CLEANUP user_ctx files error: {e_ctxf}")

        # Очистка устаревших записей периода карантина (старше 2 * QUICK_ADD_QUARANTINE_PERIOD)
        try:
            max_age = 2 * QUICK_ADD_QUARANTINE_PERIOD
            keys_to_remove = []
            for key, activated_at in QUICK_ADD_ACTIVATED_AT.items():
                if now - activated_at > max_age:
                    keys_to_remove.append(key)
            for key in keys_to_remove:
                QUICK_ADD_ACTIVATED_AT.pop(key, None)
            if keys_to_remove:
                log_send_event(f"CLEANUP: Removed {len(keys_to_remove)} expired QUICK_ADD_ACTIVATED_AT entries")
        except Exception as e_qa:
            log_send_event(f"CLEANUP QUICK_ADD_ACTIVATED_AT error: {e_qa}")

        # Очистка INCOMING_RT: удаляем только записи старше 48 часов, но НЕ сегодняшние
        # ВАЖНО: Записи сегодняшнего дня сохраняются, чтобы была возможность ответить на входящие
        try:
            from datetime import datetime, timezone
            
            INCOMING_RT_TTL = 172800  # 48 часов
            keys_to_remove = []
            today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            today_start_ts = today_start.timestamp()
            
            # Проходим по всем записям INCOMING_RT
            for key in list(INCOMING_RT.keys()):
                if not isinstance(key, tuple) or len(key) < 2:
                    continue
                
                user_id, tg_mid = key[0], key[1]
                rt_entry = INCOMING_RT.get(key)
                if not rt_entry:
                    continue
                
                # Пытаемся получить created_ts из самой записи (если есть)
                created_ts = None
                if isinstance(rt_entry, dict) and "created_ts" in rt_entry:
                    try:
                        created_ts = float(rt_entry.get("created_ts", 0))
                    except (ValueError, TypeError):
                        pass
                
                # Если created_ts нет в записи, пытаемся получить из БД
                if created_ts is None or created_ts <= 0:
                    try:
                        from db_async import get_incoming_message_by_tgmid_async
                        row = await get_incoming_message_by_tgmid_async(user_id, tg_mid)
                        if row and hasattr(row, "created_at") and row.created_at:
                            created_at = row.created_at
                            if isinstance(created_at, datetime):
                                if created_at.tzinfo is None:
                                    created_at = created_at.replace(tzinfo=timezone.utc)
                                created_ts = created_at.timestamp()
                    except Exception:
                        pass
                
                # Если не удалось определить дату создания, пропускаем (лучше оставить)
                if created_ts is None or created_ts <= 0:
                    continue
                
                # НЕ удаляем записи сегодняшнего дня
                if created_ts >= today_start_ts:
                    continue
                
                # Удаляем только записи старше 48 часов
                age_seconds = now - created_ts
                if age_seconds > INCOMING_RT_TTL:
                    keys_to_remove.append(key)
            
            # Удаляем найденные ключи
            for key in keys_to_remove:
                INCOMING_RT.pop(key, None)
            
            if keys_to_remove:
                log_send_event(f"CLEANUP: Removed {len(keys_to_remove)} expired INCOMING_RT entries (older than 48h, excluding today)")
        except Exception as e_incoming_rt:
            log_send_event(f"CLEANUP INCOMING_RT error: {e_incoming_rt}")

        gc.collect()
        log_send_event("CLEANUP_ONCE ok")
    except Exception as e:
        log_send_event(f"CLEANUP_ONCE error: {e}")

async def cleanup_scheduler():
    """
    Планировщик одноразовой очистки каждые 48 часов.
    Логика:
      1. Читаем .last_cleanup_ts (если нет — запускаем сразу).
      2. Ждём (если нужно) остаток до 48h.
      3. Выполняем cleanup_old_data_once().
      4. Пишем новый timestamp СРАЗУ после очистки (до sleep).
      5. Спим ровно 48h и повторяем (без пересчёта остатка — интервал стабильный).
    Поведение при рестарте:
      - Если с последнего запуска прошло меньше 48h — просто ждём остаток.
      - Если прошло ≥ 48h — запускаем сразу.
    ВАЖНО: Timestamp сохраняется на диск СРАЗУ после очистки, чтобы перезапуск не сбрасывал счетчик.
    """
    while True:
        try:
            # Читаем timestamp последней очистки из файла
            try:
                ts_str = LAST_CLEANUP_MARKER.read_text().strip()
                ts = float(ts_str) if ts_str else 0.0
            except (FileNotFoundError, ValueError, OSError):
                ts = 0.0
            
            now = time.time()
            wait = 0.0
            
            # Если timestamp существует и прошло меньше 48 часов, ждем остаток
            if ts > 0 and (now - ts) < CLEANUP_PERIOD:
                wait = CLEANUP_PERIOD - (now - ts)
            
            if wait > 0:
                log_send_event(f"CLEANUP_SCHED wait {wait:.0f}s ({wait/3600:.1f}h) until next run (last cleanup: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))})")
                await asyncio.sleep(wait)
            
            # Запуск одноразовой чистки
            log_send_event("CLEANUP_SCHED starting cleanup_old_data_once()")
            await cleanup_old_data_once()
            
            # ВАЖНО: Сохраняем timestamp СРАЗУ после очистки, ДО sleep
            # Это гарантирует, что даже если бот перезапустится, счетчик не сбросится
            new_ts = time.time()
            try:
                LAST_CLEANUP_MARKER.write_text(str(new_ts))
                log_send_event(f"CLEANUP_SCHED timestamp saved: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(new_ts))}")
            except Exception as e_w:
                log_send_event(f"CLEANUP_SCHED write marker error: {e_w}")
                # Пытаемся сохранить еще раз через небольшую задержку
                try:
                    await asyncio.sleep(1)
                    LAST_CLEANUP_MARKER.write_text(str(new_ts))
                    log_send_event(f"CLEANUP_SCHED timestamp saved (retry): {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(new_ts))}")
                except Exception as e_w2:
                    log_send_event(f"CLEANUP_SCHED write marker retry error: {e_w2}")
            
            # Спим полный период (ровно 48h) до следующего запуска
            log_send_event(f"CLEANUP_SCHED sleeping for {CLEANUP_PERIOD/3600:.1f}h until next cleanup")
            await asyncio.sleep(CLEANUP_PERIOD)
        except asyncio.CancelledError:
            log_send_event("CLEANUP_SCHED cancelled")
            break
        except Exception as e:
            # В случае внутренней ошибки — попробуем снова через 10 минут
            log_send_event(f"CLEANUP_SCHED loop error: {e}")
            await asyncio.sleep(600)
            
        
async def get_spoof_sender_name(
    user_id: int,
    acc_display_name: str | None = None,
    tpl: str | None = None,
    chat_id: int | None = None
) -> str:
    """
    Формирует динамическое имя отправителя.

    Приоритет:
      0. Если пользователь задал spoof_sender_name в настройках -> вернуть его без изменений.
      1. Динамический формат на немецком:
           Neue Bestellung №<ORDER_NO> von <ACC_DISPLAY_NAME>
         - ORDER_NO генерируется один раз и сохраняется в meta последнего HTML (через set_last_html_meta)
         - Для повторных HTML отправок (в т.ч. BACK) номер сохраняется и не меняется
      2. Если нет acc_display_name -> fallback к старой логике (style map / config / "Kleinanzeigen-Bestellung").

    Параметры:
      user_id          - внутренний ID пользователя (равен обычно chat id в приватном чате)
      acc_display_name - отображаемое имя аккаунта (display_name / name / локальная часть email)
      tpl              - тип шаблона (не обязательно; влияет только если нужно расширить логику)
      chat_id          - chat_id Telegram (если None, используем user_id)

    Возврат: строка имени отправителя.
    """
    # 0) Пользовательский override
    try:
        user_val = await get_setting_async(user_id, "spoof_sender_name", "")
    except Exception:
        user_val = ""
    if (user_val or "").strip():
        return user_val.strip()

    # Подготовка имени аккаунта
    acc_name = (acc_display_name or "").strip()
    # meta живёт на уровне чата — для приватного бота chat_id == user_id
    chat_id = chat_id or user_id

    # Пытаемся прочитать meta (там же будем хранить order_no)
    try:
        meta = get_last_html_meta(chat_id) or {}
    except Exception:
        meta = {}

    order_no = meta.get("order_no")
    if not order_no:
        import random
        # 6–8 цифр (пример: №582104 как у вас) — оставим диапазон от 100000 до 99999999
        order_no = str(random.randint(100000, 99999999))
        try:
            meta["order_no"] = order_no
            set_last_html_meta(chat_id, meta)
        except Exception:
            pass

    # Если есть имя аккаунта — формируем немецкую строку
    if acc_name:
        return f"Neue Bestellung №{order_no} von {acc_name}"

    # ===== FALLBACK (старое поведение) =====
    try:
        style = await get_setting_async(user_id, "html_style", "klein")
    except Exception:
        style = "klein"
    try:
        style_map = getattr(config, "SENDER_DISPLAY_NAME_STYLE_MAP", {}) or {}
    except Exception:
        style_map = {}
    if style in style_map:
        return style_map[style]
    if hasattr(config, "SENDER_DISPLAY_NAME_FOR_TEMPLATES"):
        val = getattr(config, "SENDER_DISPLAY_NAME_FOR_TEMPLATES")
        if val:
            return val
    return "Kleinanzeigen-Bestellung"

async def send_email_real(
    uid: int,
    to_email: str,
    subject: str,
    body: str,
    html: bool = False,
    photo_bytes: bytes | None = None,
    photo_name: str | None = None,
    tg_id: int | None = None
) -> bool:
    log_send_event(f"SEND_REAL start uid={uid} tg={tg_id if tg_id is not None else '-'} to={to_email} html={html}")
    try:
        ctx = await get_user_ctx_async(uid)
        accounts = list(getattr(ctx, "accounts", []) or [])
        if not accounts:
            try:
                await bot.send_message(uid, "Нет email аккаунтов для отправки. Добавьте аккаунт в Настройках.")
            except Exception:
                pass
            log_send_event(f"SEND_REAL FAIL uid={uid} tg={tg_id if tg_id is not None else '-'} reason=no_accounts")
            return False

        # Round‑robin per user
        if not hasattr(send_email_real, "_rr_idx"):
            send_email_real._rr_idx = {}  # type: ignore[attr-defined]
        rr = send_email_real._rr_idx  # type: ignore[attr-defined]
        last = int(rr.get(uid, -1))
        idx = (last + 1) % len(accounts)
        rr[uid] = idx
        acc = accounts[idx]

        acc_id = int(acc.get("id"))
        acc_email = str(acc.get("email") or "")
        log_send_event(f"SEND_REAL pick uid={uid} tg={tg_id if tg_id is not None else '-'} acc={acc_email}")

        msgid = await send_email_via_account(
            uid,
            acc_id,
            to_email,
            subject or "",
            body or "",
            html=html,
            photo_bytes=photo_bytes,
            photo_name=photo_name,
            max_attempts=3,
            tg_id=tg_id
        )

        ok = bool(msgid)
        if ok:
            log_send_event(f"SEND_REAL OK uid={uid} tg={tg_id if tg_id is not None else '-'} acc={acc_email} to={to_email} msgid={msgid or '-'}")
        else:
            log_send_event(f"SEND_REAL FAIL uid={uid} tg={tg_id if tg_id is not None else '-'} acc={acc_email} to={to_email}")
        return ok

    except Exception as e:
        log_send_event(f"SEND_REAL wrapper exception uid={uid} tg={tg_id if tg_id is not None else '-'}: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        return False


def reply_main_kb(admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📖 Проверка ников"), KeyboardButton(text="🧾 HTML-шаблоны")],
        [KeyboardButton(text="Настройки⚙️")],
        [KeyboardButton(text="✉️ Отправить email"), KeyboardButton(text="➕ Быстрое добавление")],
    ]
    if admin:
        rows.append([KeyboardButton(text="👑 Админка")])
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=rows
    )
    
UI_BLOCKED_TEXTS: set[str] = {
    "📖 Проверка ников",
    "🧾 HTML-шаблоны",
    "Настройки⚙️",
    "✉️ Отправить email",
    "➕ Быстрое добавление",
    "👑 Админка",
}

def is_ui_blocked_text(text: str) -> bool:
    """
    True, если text — пункт основной Reply‑клавиатуры или команда Telegram (начинается с '/').
    Пустую строку НЕ блокируем — пустая строка нужна для сброса значения в некоторых настройках.
    """
    s = (text or "").strip()
    if not s:
        return False  # пустота = допустимый "сброс"
    if s.startswith("/"):
        return True
    return s in UI_BLOCKED_TEXTS
    
def is_valid_email(addr: str) -> bool:
    """
    Простая проверка формата email: local@domain.tld
    Не заменяет полноценную валидацию RFC, но ловит типичные ошибки.
    """
    if not addr or "@" not in addr:
        return False
    addr = (addr or "").strip()
    if " " in addr:
        return False
    parts = addr.split("@")
    if len(parts) != 2:
        return False
    local, domain = parts
    if not local or not domain or "." not in domain:
        return False
    if re.search(r'[<>\(\)\[\];,\\"]', addr):
        return False
    return True
    
# ====== Account limit helpers (max 97 per user) ======
async def _get_accounts_count_async(user_id: int) -> int:
    """
    Возвращает текущее число email-аккаунтов пользователя (всех, не только активных).
    """
    try:
        accs = await list_accounts_async(user_id)
        return len(accs or [])
    except Exception:
        return 0

async def limit_remaining_slots(user_id: int) -> int:
    """
    Сколько ещё аккаунтов можно добавить, чтобы не превысить MAX_EMAILS_PER_USER.
    """
    cnt = await _get_accounts_count_async(user_id)
    return max(0, MAX_EMAILS_PER_USER - cnt)

async def enforce_limit_for_bulk(user_id: int, items: list) -> tuple[list, int]:
    """
    Ограничивает список к добавлению по лимиту.
    Возвращает (items_allowed, skipped_count).
    """
    allowed = await limit_remaining_slots(user_id)
    if allowed <= 0:
        return [], len(items)
    if len(items) <= allowed:
        return items, 0
    return items[:allowed], len(items) - allowed

def tg(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def code(txt: str) -> str:
    return f"<code>{tg(txt)}</code>"

def join_batches(lines: Iterable[str], batch_size: int = 50) -> List[str]:
    res: List[str] = []
    buf: List[str] = []
    for ln in lines:
        buf.append(ln)
        if len(buf) >= batch_size:
            res.append("\n".join(buf)); buf = []
    if buf:
        res.append("\n".join(buf))
    return res

def nav_row(back_cb: str) -> list[list[InlineKeyboardButton]]:
    return [[InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb),
             InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]]

async def delete_message_safe(message: types.Message):
    try:
        await message.delete()
    except Exception:
        pass

def _normalize_nick_local(nick: str) -> str:
    try:
        return smtp25.normalize_nick(nick)
    except Exception:
        normalized = unicodedata.normalize('NFKD', str(nick))
        ascii_nick = normalized.encode('ascii', 'ignore').decode('ascii')
        return ascii_nick.lower()

def _get_by_ordinal(items, ordinal: int):
    if not isinstance(ordinal, int):
        return None
    if ordinal < 1 or ordinal > len(items):
        return None
    return items[ordinal - 1]


async def safe_edit_message(msg: types.Message, text: str, reply_markup: InlineKeyboardMarkup | None = None, parse_mode=ParseMode.HTML):
    """
    Безопасно редактирует текст сообщения. Фолбэки:
      - "message is not modified" -> пробуем обновить только reply_markup
      - "not found"/"message to edit not found"/"can't be edited" -> отправляем новое сообщение в чат
      - иные ошибки -> пробрасываем (чтобы увидеть их в логах)
    """
    try:
        await msg.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        e_low = str(e).lower()
        # 1) Ничего не поменялось — обновим только клавиатуру
        if "message is not modified" in e_low:
            try:
                await msg.edit_reply_markup(reply_markup=reply_markup)
            except TelegramBadRequest:
                pass
            return
        # 2) Сообщение недоступно для редактирования/не найдено — шлём новое
        if (
            "message to edit not found" in e_low
            or "message to be edited not found" in e_low
            or "not found" in e_low
            or "message can't be edited" in e_low
            or "message to edit not specified" in e_low
            or "message identifier is not specified" in e_low
        ):
            try:
                await msg.answer(text, reply_markup=reply_markup)
            except Exception:
                pass
            return
        # 3) Любая другая причина — пробрасываем вверх
        raise


async def safe_edit_reply_markup(chat_id: int, message_id: int, reply_markup) -> bool:
    """
    Аккуратно редактирует inline-клавиатуру.
    Возвращает:
      True  - если реально изменили
      False - если изменений не было (message is not modified)
    Любая другая ошибка пробрасывается вверх (чтобы увидеть её в логах).
    """
    try:
        await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=reply_markup
        )
        return True
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return False
        raise

async def safe_cq_answer(cq: types.CallbackQuery, text: str | None = None, show_alert: bool = False, cache_time: int | None = None):
    try:
        await cq.answer(text=text, show_alert=show_alert, cache_time=cache_time)
    except TelegramBadRequest as e:
        msg = str(e).lower()
        if "query is too old" in msg or "query id is invalid" in msg or "response timeout expired" in msg:
            return
        raise

def pager_row(cb_prefix: str, page: int, total_pages: int) -> list[list[InlineKeyboardButton]]:
    left_page = max(1, page - 1)
    right_page = min(total_pages, page + 1)
    return [[
        InlineKeyboardButton(text="◀️", callback_data=f"{cb_prefix}{left_page}"),
        InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"),
        InlineKeyboardButton(text="▶️", callback_data=f"{cb_prefix}{right_page}")
    ]]

# ====== START / ADMIN ======
@dp.message(Command("start"))
async def start_cmd(m: types.Message):
    await delete_message_safe(m)
    u = await get_or_create_user_async(m.from_user.id, m.from_user.username, m.from_user.first_name, m.from_user.last_name)
    if u.status == "pending":
        for admin_id in ADMIN_IDS:
            try:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"admin:approve:{u.tg_id}"),
                     InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin:deny:{u.tg_id}")]
                ])
                await bot.send_message(admin_id,
                    f"Новая заявка на доступ:\n@{u.username} ({u.first_name} {u.last_name})\nuser_id={u.tg_id}",
                    reply_markup=kb)
            except Exception:
                pass
        await bot.send_message(m.chat.id, "Заявка на доступ отправлена администратору. Ожидайте одобрения.")
        return
    elif u.status == "denied":
        await bot.send_message(m.chat.id, "Доступ отклонён администратором.")
        return
    await bot.send_message(m.chat.id, "⚡️", reply_markup=reply_main_kb(admin=is_admin(m.from_user.id)))

@dp.callback_query(F.data.startswith("admin:"))
async def admin_approve(c: types.CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав.", show_alert=True)
        return
    _, action, uid = c.data.split(":")
    tg_id = int(uid)  # это Telegram ID

    u_obj = await get_or_create_user_async(tg_id, None, None, None)
    internal_id = getattr(u_obj, "id", None)

    await approve_user_async(tg_id, action == "approve")

    if action != "approve" and internal_id:
        try:
            await delete_user_data_async(internal_id)
        except Exception as e:
            log_send_event(f"ADMIN DENY CLEANUP ERROR (DB) internal_id={internal_id} tg_id={tg_id}: {e}")
        try:
            await cleanup_user_runtime(internal_id)
        except Exception as e:
            log_send_event(f"ADMIN DENY CLEANUP ERROR (RT) internal_id={internal_id} tg_id={tg_id}: {e}")

    try:
        if u_obj and u_obj.tg_id:
            text = "Доступ одобрен. Добро пожаловать!" if action == "approve" else "Доступ удалён."
            await bot.send_message(u_obj.tg_id, text)
    except Exception:
        pass

    await c.answer("Готово.")
    await delete_message_safe(c.message)
    

# ====== ADMIN UI (отдельная кнопка только для админа) ======
def admin_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Одобрить по Telegram ID", callback_data="adminui:add")],
        [InlineKeyboardButton(text="🚫 Удалить доступ по Telegram ID", callback_data="adminui:deny")],
        [InlineKeyboardButton(text="📋 Список одобренных", callback_data="adminui:list:1")],
        [InlineKeyboardButton(text="🧹 Очистить старые кэши", callback_data="adminui:cleanup_caches")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ])

async def admin_list_text_and_kb_async(page: int = 1, per_page: int = 15) -> Tuple[str, InlineKeyboardMarkup]:
    """
    Async version of admin_list_text_and_kb (uses list_users_async).
    """
    items = await list_users_async(status="approved")
    total = len(items)
    if total == 0:
        return "Одобренных пользователей пока нет.", admin_root_kb()
    total_pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    slice_items = items[start:start + per_page]
    lines: List[str] = ["Одобренные пользователи:", ""]
    for i, u in enumerate(slice_items, start=start + 1):
        uname = f"@{u.username}" if getattr(u, "username", None) else ""
        fname = (getattr(u, "first_name", "") or "")
        lname = (getattr(u, "last_name", "") or "")
        name = (fname + " " + lname).strip()
        info = " ".join(x for x in [uname, name] if x).strip()
        lines.append(f"№{i}: {code(str(getattr(u, 'tg_id', '—')))}" + (f" {info}" if info else ""))
    rows = pager_row("adminui:list:", page, total_pages)
    rows += admin_root_kb().inline_keyboard
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(F.text == "👑 Админка")
async def admin_menu_msg(m: types.Message):
    if not is_admin(m.from_user.id):
        await m.answer("Недостаточно прав."); return
    await delete_message_safe(m)
    await bot.send_message(m.chat.id, "Админка:", reply_markup=admin_root_kb())

@dp.message(Command("admin"))
async def admin_menu_cmd(m: types.Message):
    await admin_menu_msg(m)

@dp.callback_query(F.data == "adminui:add")
async def admin_add_open(c: types.CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав.", show_alert=True); return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите Telegram ID пользователя, которого нужно одобрить:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("ui:hide")))
    await state.set_state(AdminFSM.add_id); await safe_cq_answer(c)

@dp.message(AdminFSM.add_id)
async def admin_add_id_input(m: types.Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await m.answer("Недостаточно прав."); return
    await delete_message_safe(m)
    text = (m.text or "").strip()
    if not text.isdigit():
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Нужен числовой Telegram ID.", reply_markup=admin_root_kb()); return
    tg_id = int(text)

    # Получаем/создаём пользователя и одобряем через async wrappers.
    u = await get_or_create_user_async(tg_id, None, None, None)
    if not u or getattr(u, "id", None) is None:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Ошибка создания/поиска пользователя.", reply_markup=admin_root_kb())
        await state.clear()
        return

    await approve_user_async(u.tg_id, True)

    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, f"Одобрено. Пользователь {code(str(tg_id))}.", reply_markup=admin_root_kb())
    try: await bot.send_message(tg_id, "Доступ одобрен. Добро пожаловать!")
    except Exception: pass
    await state.clear()

@dp.callback_query(F.data == "adminui:deny")
async def admin_deny_open(c: types.CallbackQuery, state: FSMContext):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав.", show_alert=True); return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите Telegram ID пользователя, у которого нужно удалить доступ:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("ui:hide")))
    await state.set_state(AdminFSM.deny_id); await safe_cq_answer(c)

@dp.message(AdminFSM.deny_id)
async def admin_deny_id_input(m: types.Message, state: FSMContext):
    if not is_admin(m.from_user.id):
        await m.answer("Недостаточно прав."); return
    await delete_message_safe(m)
    text = (m.text or "").strip()
    if not text.isdigit():
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Нужен числовой Telegram ID.", reply_markup=admin_root_kb()); return
    tg_id = int(text)

    u = await get_or_create_user_async(tg_id, None, None, None)
    if not u or getattr(u, "id", None) is None:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Ошибка создания/поиска пользователя.", reply_markup=admin_root_kb())
        await state.clear()
        return

    # Снимаем доступ
    await approve_user_async(u.tg_id, False)

    # Полностью удаляем данные + чистим рантайм
    try:
        await delete_user_data_async(u.id)
    except Exception as e:
        log_send_event(f"ADMIN DENY CLEANUP ERROR (DB) uid={u.tg_id}: {e}")
    try:
        await cleanup_user_runtime(u.id)
    except Exception as e:
        log_send_event(f"ADMIN DENY CLEANUP ERROR (RT) uid={u.tg_id}: {e}")

    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, f"Доступ удалён. Данные пользователя {code(str(tg_id))} очищены.", reply_markup=admin_root_kb())
    try:
        await bot.send_message(tg_id, "Доступ отклонён администратором.")
    except Exception:
        pass
    await state.clear()

@dp.callback_query(F.data.startswith("adminui:list:"))
async def admin_list_show(c: types.CallbackQuery):
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав.", show_alert=True); return
    parts = c.data.split(":")
    page = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
    text, kb = await admin_list_text_and_kb_async(page=page, per_page=15)
    await safe_edit_message(c.message, text, reply_markup=kb); await safe_cq_answer(c)

@dp.callback_query(F.data == "adminui:cleanup_caches")
async def admin_cleanup_caches(c: types.CallbackQuery):
    """
    Очистка старых кэшей (кроме двух последних дней).
    Удаляет:
    - Файлы user_ctx_*.json старше двух дней
    - Кэши объявлений (AD_*) старше двух дней
    - Файлы кэшей объявлений старше двух дней
    - Записи LAST_XLSX_PER_CHAT старше двух дней
    ВАЖНО: Кэши за два последних дня (включая сегодня) сохраняются.
    """
    if not is_admin(c.from_user.id):
        await c.answer("Недостаточно прав.", show_alert=True)
        return
    
    try:
        now = time.time()
        # Сохраняем кэши за два последних дня (включая сегодня)
        # two_days_ago_start - начало дня два дня назад (кэши старше этого удаляются)
        today_start = time.mktime(time.localtime(now)[:3] + (0, 0, 0) + time.localtime(now)[6:])
        two_days_ago_start = today_start - (2 * 24 * 3600)  # Два дня назад (в секундах)
        
        deleted_files = 0
        deleted_ram_entries = 0
        
        # 1) Очистка файлов user_ctx_*.json старше двух дней
        try:
            for p in RUNTIME_CACHE_DIR.glob("user_ctx_*.json"):
                try:
                    data = _json.loads(p.read_text(encoding="utf-8"))
                    ts = float(data.get("ts") or 0.0)
                    if ts > 0 and ts < two_days_ago_start:
                        p.unlink(missing_ok=True)
                        deleted_files += 1
                except Exception:
                    # Если файл поврежден или не читается, проверяем время модификации
                    try:
                        mtime = p.stat().st_mtime
                        if mtime < two_days_ago_start:
                            p.unlink(missing_ok=True)
                            deleted_files += 1
                    except Exception:
                        pass
        except Exception as e:
            log_send_event(f"ADMIN_CLEANUP user_ctx files error: {e}")
        
        # 2) Очистка RAM-кэшей объявлений (AD_*) старше двух дней
        try:
            for chat_id, ts in list(AD_CHAT_TS.items()):
                if ts < two_days_ago_start:
                    AD_ADS_BY_ID_PER_CHAT.pop(chat_id, None)
                    AD_LOCAL2ID_PER_CHAT.pop(chat_id, None)
                    AD_GENERATED_LINKS_PER_CHAT.pop(chat_id, None)
                    REPLIED_MSGS.pop(chat_id, None)
                    AD_CHAT_TS.pop(chat_id, None)
                    deleted_ram_entries += 1
                    
                    # Удаляем файл кэша объявлений, если он существует
                    try:
                        cache_file = _ad_cache_path(chat_id)
                        if cache_file.exists():
                            cache_file.unlink(missing_ok=True)
                            deleted_files += 1
                    except Exception:
                        pass
        except Exception as e:
            log_send_event(f"ADMIN_CLEANUP AD_* caches error: {e}")
        
        # 3) Очистка других файлов кэшей в runtime_cache старше двух дней
        try:
            for p in RUNTIME_CACHE_DIR.glob("*.json"):
                try:
                    # Проверяем время модификации файла
                    mtime = p.stat().st_mtime
                    if mtime < two_days_ago_start:
                        p.unlink(missing_ok=True)
                        deleted_files += 1
                except Exception:
                    pass
        except Exception as e:
            log_send_event(f"ADMIN_CLEANUP other cache files error: {e}")
        
        # 4) Очистка LAST_XLSX_PER_CHAT старше двух дней
        try:
            for chat_id in list(LAST_XLSX_PER_CHAT.keys()):
                entry = LAST_XLSX_PER_CHAT.get(chat_id)
                ts = entry.get("timestamp", 0) if isinstance(entry, dict) else 0
                if ts > 0 and ts < two_days_ago_start:
                    LAST_XLSX_PER_CHAT.pop(chat_id, None)
                    deleted_ram_entries += 1
        except Exception as e:
            log_send_event(f"ADMIN_CLEANUP LAST_XLSX_PER_CHAT error: {e}")
        
        # 5) ВАЖНО: НЕ инвалидируем RAM-кэши глобально, так как это может привести к ошибкам отправки
        # Вместо этого кэши будут автоматически обновлены при следующем обращении к ним
        # Файлы на диске удалены, но RAM-кэши остаются активными до их естественного истечения TTL
        # Это безопаснее, так как не прерывает текущие операции отправки
        log_send_event("ADMIN_CLEANUP: RAM caches preserved (will expire naturally via TTL)")
        
        # Принудительная сборка мусора
        try:
            import gc
            gc.collect()
        except Exception:
            pass
        
        result_msg = f"Очистка кэшей завершена:\n"
        result_msg += f"Удалено файлов: {deleted_files}\n"
        result_msg += f"Удалено RAM-записей: {deleted_ram_entries}\n"
        result_msg += f"RAM-кэши инвалидированы (будут пересозданы из БД)"
        
        log_send_event(f"ADMIN_CLEANUP_CACHES: deleted {deleted_files} files, {deleted_ram_entries} RAM entries, RAM caches invalidated")
        
        await c.answer(result_msg, show_alert=True)
        await safe_cq_answer(c)
    except Exception as e:
        log_send_event(f"ADMIN_CLEANUP_CACHES error: {e}")
        await c.answer(f"Ошибка при очистке кэшей: {e}", show_alert=True)
        await safe_cq_answer(c)

# ====== UI generic ======
@dp.callback_query(F.data == "ui:hide")
async def ui_hide(c: types.CallbackQuery, state: FSMContext):
    # Удаляем текущее сообщение и, если это была подсказка из FSM, чистим трекинг
    await delete_message_safe(c.message)
    try:
        data = await state.get_data()
        ui_msgs = data.get("_ui_msgs", [])
        # если это сообщение было среди трекаемых — забудем его
        ui_msgs = [(ch, mid) for (ch, mid) in ui_msgs if mid != c.message.message_id]
        await state.update_data(_ui_msgs=ui_msgs)
    except Exception:
        pass
    await safe_cq_answer(c)
    
# ====== helpers: трекинг и удаление подсказок внутри FSM ======
async def _ui_msgs_add(state: FSMContext, chat_id: int, message_id: int):
    data = await state.get_data()
    lst = list(data.get("_ui_msgs", []))
    lst.append((chat_id, message_id))
    await state.update_data(_ui_msgs=lst)

async def ui_prompt(state: FSMContext, chat_id: int, text: str, reply_markup: InlineKeyboardMarkup | None = None):
    msg = await bot.send_message(chat_id, text, reply_markup=reply_markup)
    await _ui_msgs_add(state, chat_id, msg.message_id)
    return msg

async def ui_clear_prompts(state: FSMContext):
    data = await state.get_data()
    lst = list(data.get("_ui_msgs", []))
    if lst:
        for chat_id, mid in lst:
            try:
                await bot.delete_message(chat_id, mid)
            except Exception:
                pass
        await state.update_data(_ui_msgs=[])
        
# --- ДОБАВЬТЕ 2 обработчика настроек: ввод имени и переключатель подмены темы ---
@dp.callback_query(F.data == "settings:spoofname")
async def settings_spoofname_open(c: types.CallbackQuery, state: FSMContext):
    """
    Окно ввода spoof-имени.
    Исправлено: экранированы угловые скобки, чтобы Telegram не пытался парсить их как теги.
    """
    if not await ensure_approved(c):
        return

    try:
        cur = await get_setting_async(await U(c), "spoof_sender_name", "")
    except Exception as e:
        log_send_event(f"SPOOFNAME_OPEN get_setting_async error uid={c.from_user.id}: {e}")
        cur = ""

    # Пример показываем с экранированными скобками.
    example_line = "формата: Neue Bestellung №&lt;число&gt; von &lt;ИмяАккаунта&gt;"

    prompt = (
        "Введите новое имя отправителя (поле From).\n"
        "Кнопка «🔄 Сбросить» вернёт авто‑генерацию\n"
        f"{example_line}\n"
    )
    if cur:
        # code() уже экранирует спецсимволы
        prompt += f"\nТекущее значение: {code(cur)}"
    else:
        prompt += "\nСейчас используется авто‑генерация."

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Сбросить", callback_data="spoofname:reset")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:back")]
    ])

    try:
        msg = await ui_prompt(state, c.message.chat.id, prompt, reply_markup=kb)
        # удаляем старое сообщение (кнопку настроек), если это другое сообщение
        try:
            if c.message and msg and msg.message_id != c.message.message_id:
                await delete_message_safe(c.message)
        except Exception:
            pass
        await state.set_state(SpoofNameFSM.name)
    except Exception as e:
        log_send_event(f"SPOOFNAME_OPEN prompt error uid={c.from_user.id}: {e}")
        try:
            await safe_edit_message(
                c.message,
                f"Не удалось открыть ввод spoof-имени ❌\n{code(str(e))}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:back")]
                ])
            )
        except Exception:
            pass

    await safe_cq_answer(c)
    
@dp.callback_query(F.data == "spoofname:reset")
async def spoofname_reset_cb(c: types.CallbackQuery, state: FSMContext):
    """
    Мгновенно сбрасывает spoof-имя и возвращает в меню настроек (авто‑генерация включена).
    """
    if not await ensure_approved(c):
        return
    try:
        await set_setting_async(await U(c), "spoof_sender_name", "")
    except Exception as e:
        log_send_event(f"SPOOFNAME_RESET set_setting error uid={c.from_user.id}: {e}")
    try:
        await state.clear()
    except Exception:
        pass
    kb = await dynamic_settings_kb(await U(c))
    try:
        await safe_edit_message(
            c.message,
            "Имя для спуфа сброшено. Авто‑генерация включена.",
            reply_markup=kb
        )
    except Exception:
        try:
            await c.message.answer("Имя для спуфа сброшено. Авто‑генерация включена.", reply_markup=kb)
        except Exception:
            pass
    await safe_cq_answer(c, "OK")
    
@dp.callback_query(F.data == "settings:subjhtml")
async def settings_subject_html_open(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    cur = (await get_setting_async(await U(c), "subject_html_text", "")).strip()
    prompt = "Введите тему для HTML‑писем.\nЭта тема будет подставляться при отправке HTML, если включена «Подмена темы (HTML)»."
    if cur:
        prompt += f"\nТекущее значение: {code(cur)}"
    await ui_prompt(state, c.message.chat.id, prompt, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("settings:back")))
    await state.set_state(SubjectHtmlFSM.text)
    await safe_cq_answer(c)

@dp.message(SubjectHtmlFSM.text)
async def settings_subject_html_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)
    val = (m.text or "").strip()

    # NEW: игнорируем нажатия кнопок Reply‑клавиатуры и команды, чтобы их текст не подставлялся
    if is_ui_blocked_text(val):
        # Состояние остаётся активным, пользователь может ввести корректное значение
        return

    await set_setting_async(await U(m), "subject_html_text", val)
    await ui_clear_prompts(state)
    kb = await dynamic_settings_kb(await U(m))
    text = f"Тема (HTML) {'сохранена' if val else 'очищена (используется исходная тема)'}."
    await bot.send_message(m.chat.id, text, reply_markup=kb)
    await state.clear()

@dp.message(SpoofNameFSM.name)
async def settings_spoofname_save(m: types.Message, state: FSMContext):
    """
    Сохранение нового spoof-имени.
    - Игнорируем служебные тексты (кнопки / команды)
    - Пустая строка > сброс к авто‑генерации
    """
    if not await ensure_approved(m):
        return

    raw = (m.text or "")
    await delete_message_safe(m)

    # Игнор "кнопочных" текстов
    if is_ui_blocked_text(raw):
        return

    new_val = raw.strip()

    try:
        await set_setting_async(await U(m), "spoof_sender_name", new_val)
    except Exception as e:
        log_send_event(f"SPOOFNAME_SAVE set_setting error uid={m.from_user.id}: {e}")

    try:
        await ui_clear_prompts(state)
    except Exception:
        pass

    kb = await dynamic_settings_kb(await U(m))
    if new_val:
        msg = f"Имя для спуфа сохранено: {code(new_val)}"
    else:
        msg = "Имя пустое → авто‑генерация активна."

    try:
        await bot.send_message(m.chat.id, msg, reply_markup=kb)
    except Exception as e:
        log_send_event(f"SPOOFNAME_SAVE send_message error uid={m.from_user.id}: {e}")

    try:
        await state.clear()
    except Exception:
        pass

@dp.callback_query(F.data == "settings:subjovr:toggle")
async def settings_subject_override_toggle(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    cur = (await get_setting_async(await U(c), "subject_override_html", "1")).strip().lower()
    enabled = cur in ("1", "true", "yes", "on")
    new_val = "0" if enabled else "1"
    await set_setting_async(await U(c), "subject_override_html", new_val)
    kb = await dynamic_settings_kb(await U(c))
    try:
        await c.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        try:
            await c.message.edit_text("Настройки:", reply_markup=kb)
        except Exception:
            pass
    await safe_cq_answer(c, "Переключено")
    
# NEW: переключатель команды (ДОБАВИТЬ рядом с другими settings:* хендлерами)
@dp.callback_query(F.data == "settings:team:toggle")
async def settings_team_toggle(c: types.CallbackQuery):
    """
    Переключатель команды (NurPaypal <-> Dolce <-> Aqua team).
    Хранится в settings под ключом 'team_mode' со значениями 'nur'|'dolce'|'aqua_team'.
    Для админов: nur -> dolce -> aqua_team -> nur (циклически).
    Для не-админов: nur <-> aqua_team (dolce пропускается, доступна только для админа).
    """
    uid = await U(c)
    admin = is_admin(c.from_user.id)
    cur = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    
    # Циклическое переключение: nur -> dolce -> aqua_team -> nur
    # Для не-админов: nur -> aqua_team -> nur (dolce пропускается)
    if admin:
        if cur == "nur":
            new_val = "dolce"
        elif cur == "dolce":
            new_val = "aqua_team"
        else:  # aqua_team
            new_val = "nur"
    else:
        # Для не-админов переключаем между nur и aqua_team
        new_val = "aqua_team" if cur != "aqua_team" else "nur"
    
    await set_setting_async(uid, "team_mode", new_val)

    # Перестроим экран «Токены» под текущую команду
    await tokens_open(c)

@dp.callback_query(F.data == "settings:back")
async def settings_back(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return

    # ВАЖНО: очищаем FSM, чтобы отменить любой активный ввод (в том числе имя для спуфа)
    try:
        await state.clear()
    except Exception:
        pass

    kb = await dynamic_settings_kb(await U(c))
    try:
        # Показываем корневое меню настроек
        await c.message.edit_text("Настройки:", reply_markup=kb)
    except Exception:
        # Фолбэк — если не получилось изменить текст, обновим только разметку
        try:
            await c.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass

    await safe_cq_answer(c)
    
@dp.callback_query(F.data.startswith("settings:style:"))
async def settings_style_pick(c: types.CallbackQuery):
    """
    Выбор стиля отключён. Всегда используется 'klein'.
    На любые старые кнопки «settings:style:*» отвечаем фиксацией 'klein'.
    """
    if not await ensure_approved(c):
        return
    from db_async import set_setting_async
    await set_setting_async(await U(c), "html_style", "klein")
    kb = await dynamic_settings_kb(await U(c))
    try:
        await c.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        try:
            await c.message.edit_text("Настройки:", reply_markup=kb)
        except Exception:
            pass
    await safe_cq_answer(c, "Стиль зафиксирован: Klein")

@dp.callback_query(F.data == "noop")
async def noop_cb(c: types.CallbackQuery):
    await safe_cq_answer(c)
    


@dp.message(F.text == "Настройки⚙️")
async def btn_settings(m: types.Message):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)
    kb = await dynamic_settings_kb(await U(m))
    await bot.send_message(m.chat.id, "Настройки:", reply_markup=kb)

@dp.message(Command("settings"))
async def cmd_settings(m: types.Message):
    await btn_settings(m)

# ====== Settings root ======
def settings_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📑 Домены", callback_data="domains:open"),
         InlineKeyboardButton(text="📚 Пресеты (IMAP)", callback_data="presets:open")],
        [InlineKeyboardButton(text="📌 Темы", callback_data="subjects:open"),
         InlineKeyboardButton(text="📗 Умные пресеты", callback_data="smart:open")],
        [InlineKeyboardButton(text="📧 E‑mail", callback_data="emails:open"),
         InlineKeyboardButton(text="🌐 Прокси", callback_data="proxies:root")],
        [InlineKeyboardButton(text="⏱ Интервал", callback_data="interval:open")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)
    
# ==== Динамическая корневая клавиатура настроек с переключателем стиля HTML ====
# Обновите динамическое меню настроек: добавлен пункт "✏️ Тема (HTML)"
async def dynamic_settings_kb(user_id: int) -> InlineKeyboardMarkup:
    """
    Динамическое меню настроек БЕЗ выбора стиля.
    Стиль всегда фиксирован на 'klein'.
    """
    from db_async import get_setting_async, set_setting_async

    # Зафиксировать стиль = 'klein'
    current_style = (await get_setting_async(user_id, "html_style", "klein")).strip().lower()
    if current_style != "klein":
        await set_setting_async(user_id, "html_style", "klein")

    subj_ovr = (await get_setting_async(user_id, "subject_override_html", "1")).strip().lower()
    subj_enabled = subj_ovr in ("1", "true", "yes", "on")
    subj_label = f"{'🟢' if subj_enabled else '⚪'} Подмена темы (HTML)"

    # ИИ переключатель
    ai_enabled = (await get_setting_async(user_id, "ai_enabled", "0")).strip().lower() in ("1","true","yes","on")
    ai_label = f"{'🟢' if ai_enabled else '⚪'} ИИ Помощник"

    rows = [
        [InlineKeyboardButton(text="📑 Домены", callback_data="domains:open"),
         InlineKeyboardButton(text="📚 Пресеты (IMAP)", callback_data="presets:open")],
        [InlineKeyboardButton(text="📌 Темы", callback_data="subjects:open"),
         InlineKeyboardButton(text="📗 Умные пресеты", callback_data="smart:open")],
        [InlineKeyboardButton(text="📧 E‑mail", callback_data="emails:open"),
         InlineKeyboardButton(text="🌐 Прокси", callback_data="proxies:root")],
        [InlineKeyboardButton(text="⏱ Интервал", callback_data="interval:open")],
        [InlineKeyboardButton(text="✏️ Имя для спуфа", callback_data="settings:spoofname"),
         InlineKeyboardButton(text=subj_label, callback_data="settings:subjovr:toggle")],
        [InlineKeyboardButton(text="✏️ Тема (HTML)", callback_data="settings:subjhtml"),
         InlineKeyboardButton(text="📁 Профиль", callback_data="profile:open")],
        # === ИИ блок ===
        [InlineKeyboardButton(text=ai_label, callback_data="settings:ai:toggle"),
         InlineKeyboardButton(text="🧠 ИИ Настройки", callback_data="settings:ai:open")],
        # === /ИИ блок ===
        [InlineKeyboardButton(text="🔑 Токены", callback_data="tokens:open")],
        [InlineKeyboardButton(text="🔄 Ротация", callback_data="rotation:run")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)
    
def tokens_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Ключ команды", callback_data="tokens:set:team"),
         InlineKeyboardButton(text="✏️ Ключ воркера", callback_data="tokens:set:worker")],
        [InlineKeyboardButton(text="🔄 Сбросить командный", callback_data="tokens:reset:team"),
         InlineKeyboardButton(text="🔄 Сбросить воркер", callback_data="tokens:reset:worker")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:back")]
    ])
    
# NEW: динамическая клавиатура «Токены» (добавить РЯДОМ с tokens_root_kb)
def tokens_root_kb_dynamic(is_admin_user: bool, team_mode: str) -> InlineKeyboardMarkup:
    """
    Построение клавиатуры «Токены» с учётом выбранной команды:
      - team_mode: 'nur' (NurPaypal/Goo), 'dolce' (Dolce) или 'aqua_team' (Aqua team)
      - переключатель команды показывается для всех (dolce доступна только для админа)
    """
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(text="✏️ Ключ команды", callback_data="tokens:set:team"),
         InlineKeyboardButton(text="✏️ Ключ воркера", callback_data="tokens:set:worker")],
        [InlineKeyboardButton(text="🔄 Сбросить командный", callback_data="tokens:reset:team"),
         InlineKeyboardButton(text="🔄 Сбросить воркер", callback_data="tokens:reset:worker")],
    ]
    # Переключатель команды показывается для всех
    tm = (team_mode or "").lower()
    if tm == "dolce":
        cur_label = "Dolce"
    elif tm == "aqua_team":
        cur_label = "Aqua team"
    else:
        cur_label = "NurPaypal"
    # Кнопка переключения команды показывается всегда
    rows.insert(0, [InlineKeyboardButton(text=f"🛠 Команда: {cur_label}", callback_data="settings:team:toggle")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "tokens:open")
async def tokens_open(c: types.CallbackQuery):
    """
    Экран «Токены»:
      - Для НЕ-админов: nur (Goo/NurPaypal) или aqua_team (Aqua team)
      - Для админов: выбор команды ('nur' — Goo/NurPaypal, 'dolce' — Dolce, 'aqua_team' — Aqua team)
        и отображение состояния токенов для выбранной команды.
      - Dolce доступна только для админа.
    """
    if not await ensure_approved(c):
        return

    uid = await U(c)
    admin = is_admin(c.from_user.id)

    # Активная команда (по умолчанию 'nur')
    # Для не-админов: если установлен dolce, сбрасываем на nur (dolce только для админа)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"
        await set_setting_async(uid, "team_mode", "nur")

    if team_mode == "dolce" and admin:
        # Dolce: команда = базовый адрес API, воркер = токен
        team = (await get_setting_async(uid, "dolce_team_base", "")).strip()
        worker = (await get_setting_async(uid, "dolce_worker_token", "")).strip()
        text = "Токены Dolce:\n"
        text += f"Команды (API base): {code('установлен' if team else '—')}\n"
        text += f"Воркера (token): {code('установлен' if worker else '—')}\n\n"
        text += "Выберите действие:"
        kb = tokens_root_kb_dynamic(admin, team_mode)
        await safe_edit_message(c.message, text, reply_markup=kb)
        await safe_cq_answer(c)
        return

    if team_mode == "aqua_team":
        # Aqua team: команда = ключ команды, воркер = ключ воркера (как у nur/goo)
        team = (await get_setting_async(uid, "aqua_team_key", "")).strip()
        worker = (await get_setting_async(uid, "aqua_worker_key", "")).strip()
        text = "Токены Aqua team:\n"
        text += f"Команды: {code('установлен' if team else '—')}\n"
        text += f"Воркера: {code('установлен' if worker else '—')}\n\n"
        text += "Выберите действие:"
        kb = tokens_root_kb_dynamic(admin, team_mode)
        await safe_edit_message(c.message, text, reply_markup=kb)
        await safe_cq_answer(c)
        return

    # Nur/Goo (по умолчанию для всех, для не-админа — всегда nur или aqua_team)
    team = (await get_setting_async(uid, "goo_team_key", "")).strip()
    worker = (await get_setting_async(uid, "goo_worker_key", "")).strip()
    text = "Токены Goo:\n"
    text += f"Команды: {code('установлен' if team else '—')}\n"
    text += f"Воркера: {code('установлен' if worker else '—')}\n\n"
    text += "Выберите действие:"
    kb = tokens_root_kb_dynamic(admin, team_mode)
    await safe_edit_message(c.message, text, reply_markup=kb)
    await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("tokens:set:"))
async def tokens_set_cb(c: types.CallbackQuery, state: FSMContext):
    """
    Установка значений токенов:
      - Для админа учитываем выбранную команду 'team_mode'
      - Для обычных пользователей — nur или aqua_team (dolce недоступна)
    """
    if not await ensure_approved(c):
        return

    kind = c.data.split(":")[2]  # 'team' | 'worker'
    uid = await U(c)
    admin = is_admin(c.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"

    await ui_clear_prompts(state)
    await delete_message_safe(c.message)

    # Подсказки по вводу зависят от активной команды
    if team_mode == "dolce" and admin:
        if kind == "team":
            prompt = "Введите базовый адрес API Dolce (например: https://example.com) ✍️"
        else:
            prompt = "Введите токен Dolce ✍️"
    elif team_mode == "aqua_team":
        prompt = "Введите значение для Ключ команды Aqua team:" if kind == "team" else "Введите значение для Ключ воркера Aqua team:"
    else:
        prompt = "Введите значение для Ключ команды:" if kind == "team" else "Введите значение для Ключ воркера:"

    await ui_prompt(
        state,
        c.message.chat.id,
        prompt,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="tokens:open")]])
    )
    if kind == "team":
        await state.set_state(TokensFSM.team_key)
    else:
        await state.set_state(TokensFSM.worker_key)

    await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("tokens:reset:"))
async def tokens_reset_cb(c: types.CallbackQuery):
    """
    Сброс значений токенов:
      - Для админа учитываем выбранную команду 'team_mode'
      - Для обычных пользователей — nur или aqua_team (dolce недоступна)
    """
    if not await ensure_approved(c):
        return

    uid = await U(c)
    admin = is_admin(c.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"

    kind = c.data.split(":")[2]  # 'team' | 'worker'
    if team_mode == "dolce" and admin:
        key_name = "dolce_team_base" if kind == "team" else "dolce_worker_token"
    elif team_mode == "aqua_team":
        key_name = "aqua_team_key" if kind == "team" else "aqua_worker_key"
    else:
        key_name = "goo_team_key" if kind == "team" else "goo_worker_key"

    await set_setting_async(uid, key_name, "")
    await c.answer("Сброшено")
    await tokens_open(c)

@dp.message(TokensFSM.team_key)
async def tokens_team_save(m: types.Message, state: FSMContext):
    """
    Сохранение «ключа команды»:
      - Для админа смотрим 'team_mode'
      - Для обычных — nur или aqua_team (dolce недоступна)
    """
    if not await ensure_approved(m):
        return
    uid = await U(m)
    admin = is_admin(m.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"

    val = (m.text or "").strip()
    if team_mode == "dolce" and admin:
        key = "dolce_team_base"
    elif team_mode == "aqua_team":
        key = "aqua_team_key"
    else:
        key = "goo_team_key"

    await set_setting_async(uid, key, val)
    await delete_message_safe(m)
    await ui_clear_prompts(state)

    kb = tokens_root_kb_dynamic(admin, team_mode)
    if team_mode == "dolce":
        title = "Адрес API Dolce сохранён."
    elif team_mode == "aqua_team":
        title = "Ключ команды Aqua team сохранён."
    else:
        title = "Ключ команды сохранён."
    await bot.send_message(m.chat.id, title, reply_markup=kb)
    await state.clear()

@dp.message(TokensFSM.worker_key)
async def tokens_worker_save(m: types.Message, state: FSMContext):
    """
    Сохранение «ключа воркера»:
      - Для админа смотрим 'team_mode'
      - Для обычных — nur или aqua_team (dolce недоступна)
    """
    if not await ensure_approved(m):
        return
    uid = await U(m)
    admin = is_admin(m.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"

    val = (m.text or "").strip()
    if team_mode == "dolce" and admin:
        key = "dolce_worker_token"
    elif team_mode == "aqua_team":
        key = "aqua_worker_key"
    else:
        key = "goo_worker_key"

    await set_setting_async(uid, key, val)
    await delete_message_safe(m)
    await ui_clear_prompts(state)

    kb = tokens_root_kb_dynamic(admin, team_mode)
    if team_mode == "dolce":
        title = "Токен Dolce сохранён."
    elif team_mode == "aqua_team":
        title = "Ключ воркера Aqua team сохранён."
    else:
        title = "Ключ воркера сохранён."
    await bot.send_message(m.chat.id, title, reply_markup=kb)
    await state.clear()
    
def profile_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Установить profileID", callback_data="profile:set")],
        [InlineKeyboardButton(text="🔄 Сбросить", callback_data="profile:reset")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:back")]
    ])

@dp.callback_query(F.data == "profile:open")
async def profile_open(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    uid = await U(c)
    admin = is_admin(c.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"
    
    if team_mode == "aqua_team":
        val = (await get_setting_async(uid, "aqua_profile_id", "")).strip()
        text = "Профиль Aqua team (используется при генерации ссылки).\n"
        text += f"profileID: {code(val if val else '— (не задан)')}\n\n"
        text += "Установите любой валидный ID (можно 'заглушку' если сервис принимает)."
    else:
        val = (await get_setting_async(uid, "goo_profile_id", "")).strip()
        text = "Профиль Goo (используется при генерации ссылки).\n"
        text += f"profileID: {code(val if val else '— (не задан)')}\n\n"
        text += "Установите любой валидный ID (можно 'заглушку' если сервис принимает)."
    await safe_edit_message(c.message, text, reply_markup=profile_root_kb())
    await safe_cq_answer(c)

@dp.callback_query(F.data == "profile:set")
async def profile_set(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    uid = await U(c)
    admin = is_admin(c.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"
    
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    prompt_text = "Отправьте profileID (строка). Чтобы отменить — /cancel или кнопка."
    if team_mode == "aqua_team":
        prompt_text = "Отправьте profileID Aqua team (строка). Чтобы отменить — /cancel или кнопка."
    await ui_prompt(
        state,
        c.message.chat.id,
        prompt_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:open")]])
    )
    await state.set_state(GooProfileFSM.profile)
    await safe_cq_answer(c)

@dp.callback_query(F.data == "profile:reset")
async def profile_reset(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    uid = await U(c)
    admin = is_admin(c.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"
    
    if team_mode == "aqua_team":
        await set_setting_async(uid, "aqua_profile_id", "")
    else:
        await set_setting_async(uid, "goo_profile_id", "")
    await c.answer("Сброшено")
    await profile_open(c)

@dp.message(GooProfileFSM.profile)
async def profile_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    uid = await U(m)
    admin = is_admin(m.from_user.id)
    team_mode = (await get_setting_async(uid, "team_mode", "nur")).strip().lower()
    if not admin and team_mode == "dolce":
        team_mode = "nur"
    
    val = (m.text or "").strip()
    if team_mode == "aqua_team":
        await set_setting_async(uid, "aqua_profile_id", val)
        title = f"profileID Aqua team сохранён: {code(val or '—')}"
    else:
        await set_setting_async(uid, "goo_profile_id", val)
        title = f"profileID сохранён: {code(val or '—')}"
    await delete_message_safe(m)
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, title, reply_markup=profile_root_kb())
    await state.clear()
    
@dp.callback_query(F.data == "rotation:run")
async def rotation_run(c: types.CallbackQuery):
    """
    Ротация: как было, плюс лёгкая проверка send‑прокси на IMAP:993.
    Проверка выполняется в executor (_test_proxy_async), событийный луп не блокируется.
    """
    if not await ensure_approved(c):
        return

    status_msg = await c.message.answer("Ротация...")
    try:
        uid = await U(c)
        tg = c.from_user.id
        chat_id = c.message.chat.id
        log_send_event(f"ROTATION RUN request uid={uid} tg={tg}")

        # 0) Проверка send‑прокси (SMTP 587) — как было
        proxies = await list_proxies_async(uid, "send")
        if not proxies:
            await delete_message_safe(status_msg)
            try:
                await bot.send_message(chat_id, "Ротация не прошла, нет send-прокси")
            except Exception:
                pass
            try:
                await safe_cq_answer(c)
            except Exception:
                pass
            log_send_event(f"ROTATION RUN aborted (no proxies) uid={uid} tg={tg}")
            return

        target_host, target_port = _probe_target_for_kind("send")
        tests = [
            _test_proxy_async(p.host, p.port, p.user_login or "", p.password or "", target_host, target_port, timeout=6)
            for p in proxies
        ]
        results = await asyncio.gather(*tests, return_exceptions=False)
        bad_ordinals = [i for i, (ok, _err) in enumerate(results, start=1) if not ok]
        if bad_ordinals:
            nums = _fmt_bad_ordinals(bad_ordinals)
            await delete_message_safe(status_msg)
            try:
                await bot.send_message(chat_id, f"Ротация не прошла, проверьте невалидные прокси {nums}")
            except Exception:
                pass
            try:
                await safe_cq_answer(c)
            except Exception:
                pass
            log_send_event(f"ROTATION RUN failed (invalid proxies {bad_ordinals}) uid={uid} tg={tg}")
            return

        # 0.1) ДОБАВЛЕНО: быстрая проверка этих же прокси на IMAP:993
        v_host, v_port = _probe_target_for_kind("verify")  # imap.gmail.com:993
        tests_imap = [
            _test_proxy_async(p.host, p.port, p.user_login or "", p.password or "", v_host, v_port, timeout=6)
            for p in proxies
        ]
        results_imap = await asyncio.gather(*tests_imap, return_exceptions=False)
        bad_imap = [i for i, (ok, _err) in enumerate(results_imap, start=1) if not ok]
        if bad_imap:
            nums = _fmt_bad_ordinals(bad_imap)
            await delete_message_safe(status_msg)
            try:
                await bot.send_message(chat_id, f"Ротация не прошла, прокси недоступны для IMAP: {nums}")
            except Exception:
                pass
            try:
                await safe_cq_answer(c)
            except Exception:
                pass
            log_send_event(f"ROTATION RUN failed (IMAP 993 blocked for ordinals {bad_imap}) uid={uid} tg={tg}")
            return

        # 1) Полная остановка текущего лупа
        await _ensure_imap_stopped_for_user(uid)
        log_send_event(f"ROTATION: imap stop requested uid={uid}")

        # Ждём до 10с подтверждения остановки IMAP задач
        for _ in range(20):
            t = IMAP_TASKS.get(uid)
            if not t or t.done() or t.cancelled():
                log_send_event(f"ROTATION: imap stopped confirmed uid={uid}")
                break
            await asyncio.sleep(0.5)
        
        # ВАЖНО: Ждем завершения всех процессов IMAP для пользователя
        # Останавливаем все аккаунты пользователя и ждем, пока они действительно остановятся
        keys_to_stop = [key for key in list(IMAP_ACCOUNT_STATUS.keys()) if key[0] == uid]
        if keys_to_stop:
            log_send_event(f"ROTATION: Stopping {len(keys_to_stop)} IMAP processes for uid={uid}")
            for key in keys_to_stop:
                try:
                    await stop_imap_process(key[0], key[1])
                    # Помечаем как неактивный немедленно
                    IMAP_ACCOUNT_STATUS[key] = {"active": False}
                except Exception as e:
                    log_send_event(f"ROTATION: Error stopping process uid={key[0]} acc_id={key[1]}: {e}")
            
            # Ждем, пока процессы действительно завершатся (до 15 секунд)
            # Проверяем, что все аккаунты помечены как неактивные и процессы больше не обрабатываются
            for wait_iter in range(30):  # 30 итераций по 0.5 секунды = 15 секунд
                active_count = sum(1 for key in keys_to_stop if IMAP_ACCOUNT_STATUS.get(key, {}).get("active", False))
                if active_count == 0:
                    log_send_event(f"ROTATION: All processes stopped for uid={uid} (wait_iter={wait_iter})")
                    break
                await asyncio.sleep(0.5)
            
            # Дополнительная очистка: удаляем все записи статусов для пользователя
            for key in keys_to_stop:
                IMAP_ACCOUNT_STATUS.pop(key, None)
            
            log_send_event(f"ROTATION: All processes cleaned up for uid={uid}")
        
        # Дополнительная задержка для освобождения памяти
        await asyncio.sleep(2)

        # 2) Актуальный список активных аккаунтов
        accounts = await list_accounts_async(uid)
        active_accounts = [a for a in accounts if getattr(a, "active", False) and getattr(a, "email", "")]

        # 3) Сброс runtime‑состояний и suppress старт‑логов
        # ВАЖНО: Новая архитектура - используем process pool, старая логика удалена
        sup = SUPPRESS_START_LOGS.setdefault(uid, set())
        sup.clear()
        for a in active_accounts:
            email = getattr(a, "email", "")
            if not email:
                continue
            key = (uid, email)
            START_LOG_SENT.pop(key, None)
            ERROR_LOG_SENT.pop(key, None)
            sup.add(email)

        # 4) Стартуем луп
        try:
            await _ensure_imap_started_for_user(uid, chat_id)
            await asyncio.sleep(0)
        except Exception as start_err:
            log_send_event(f"ROTATION: Error starting IMAP after rotation uid={uid}: {start_err}")
            raise  # Пробрасываем дальше, чтобы обработать в общем блоке except

        # 5) Ускоренный первый проход - пропускаем (старая логика удалена)
        # Процессы IMAP уже запущены и работают автоматически

        # 6) Ждем немного, чтобы аккаунты успели запуститься, затем отправляем сообщение о завершении ротации
        await asyncio.sleep(2)  # Небольшая задержка для запуска аккаунтов
        
        # Отправляем сообщение о завершении ротации
        try:
            await delete_message_safe(status_msg)
            await bot.send_message(chat_id, "Ротация завершена")
        except Exception:
            pass
        
        try:
            await safe_cq_answer(c)
        except Exception:
            pass
        
        log_send_event(f"ROTATION RUN completed successfully uid={uid} tg={tg}")
    except Exception as e:
        try:
            await delete_message_safe(status_msg)
        except Exception:
            pass
        try:
            await bot.send_message(c.message.chat.id, "Ротация не прошла, проверьте прокси/аккаунты")
        except Exception:
            pass
        try:
            await safe_cq_answer(c)
        except Exception:
            pass
        try:
            uid = await U(c)
            tg = c.from_user.id
            log_send_event(f"ROTATION RUN exception uid={uid} tg={tg} err={type(e).__name__}: {e}")
        except Exception:
            pass



# ====== Domains ======
async def domains_text_for_user(user_id: int) -> str:
    doms = await list_domains_async(user_id)
    if not doms:
        return code("Текущие домены: список пуст.")
    lines = ["Текущие домены (по приоритету):", ""]
    for i, d in enumerate(doms, start=1):
        lines.append(f"Домен №{i}: {code(d)}")
    return "\n".join(lines)

def domains_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="➕ Добавить", callback_data="domains:add"),
         InlineKeyboardButton(text="🔁 Изменить порядок", callback_data="domains:reorder")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="domains:delete"),
         InlineKeyboardButton(text="🧹 Удалить все", callback_data="domains:clear")],
        *nav_row("settings:back")
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "domains:open")
async def domains_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    text = await domains_text_for_user(await U(c))
    await safe_edit_message(c.message, text, reply_markup=domains_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data == "domains:add")
async def domains_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    txt = await domains_text_for_user(await U(c)) + "\n\nВведите домен. Можно позицию: «gmail.com 1»."
    await ui_prompt(state, c.message.chat.id, txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))
    await state.set_state(DomainsFSM.add); await safe_cq_answer(c)

@dp.message(DomainsFSM.add)
async def domains_add_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    parts = (m.text or "").strip().split()
    if not parts:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Пустой ввод.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open"))); return
    name = parts[0]
    pos = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
    await add_domain_async(await U(m), name, pos)
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    text = await domains_text_for_user(await U(m))
    await bot.send_message(m.chat.id, text, reply_markup=domains_kb()); await state.clear()

@dp.callback_query(F.data == "domains:reorder")
async def domains_reorder(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    txt = await domains_text_for_user(await U(c)) + "\n\nВведите новый порядок номеров (например: 3 1 2 4)"
    await ui_prompt(state, c.message.chat.id, txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))
    await state.set_state(DomainsFSM.reorder); await safe_cq_answer(c)

@dp.message(DomainsFSM.reorder)
async def domains_reorder_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    names = await list_domains_async(await U(m))
    try:
        order = [int(x) for x in (m.text or "").replace(",", " ").split()]
        if sorted(order) != list(range(1, len(names) + 1)):
            raise ValueError
        new_names = [names[i - 1] for i in order]
        await set_domains_order_async(await U(m), new_names)
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        text = await domains_text_for_user(await U(m))
        await bot.send_message(m.chat.id, text, reply_markup=domains_kb()); await state.clear()
    except Exception:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Неверный формат. Пример: 2 1 3", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))

@dp.callback_query(F.data == "domains:delete")
async def domains_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    txt = await domains_text_for_user(await U(c)) + "\n\nВведите номера доменов для удаления (например: 1 4 6)."
    await ui_prompt(state, c.message.chat.id, txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))
    await state.set_state(DomainsFSM.delete); await safe_cq_answer(c)

@dp.message(DomainsFSM.delete)
async def domains_delete_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    try:
        nums = sorted({int(x) for x in (m.text or "").replace(",", " ").split()}, reverse=True)
        await delete_domains_by_indices_async(await U(m), list(nums))
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        text = await domains_text_for_user(await U(m))
        await bot.send_message(m.chat.id, text, reply_markup=domains_kb()); await state.clear()
    except Exception:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Неверный ввод. Пример: 2 5 6", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))

@dp.callback_query(F.data == "domains:clear")
async def domains_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Подтвердите удаление всех доменов: ДА", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("domains:open")))
    await state.set_state(DomainsFSM.clear); await safe_cq_answer(c)

@dp.message(DomainsFSM.clear)
async def domains_clear_input(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if (m.text or "").strip().upper() == "ДА":
        await clear_domains_async(await U(m))
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        text = "Все домены удалены.\n\n" + await domains_text_for_user(await U(m))
        await bot.send_message(m.chat.id, text, reply_markup=domains_kb())
    else:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Отменено.", reply_markup=domains_kb())
    await state.clear()

# ====== INTERVAL ======
async def interval_text(user_id: int) -> str:
    vmin = await get_setting_async(user_id, "send_delay_min", str(smtp25.MIN_SEND_DELAY))
    vmax = await get_setting_async(user_id, "send_delay_max", str(smtp25.MAX_SEND_DELAY))
    return f"Текущий интервал:\n\n{code(f'[{vmin}, {vmax}]')}"

def interval_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="✏️ Изменить интервал", callback_data="interval:change"),
         InlineKeyboardButton(text="🔄 Сбросить интервал", callback_data="interval:reset")],
        *nav_row("settings:back")
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "interval:open")
async def interval_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    vmin = await get_setting_async(await U(c), "send_delay_min", str(smtp25.MIN_SEND_DELAY))
    vmax = await get_setting_async(await U(c), "send_delay_max", str(smtp25.MAX_SEND_DELAY))
    text = f"Текущий интервал:\n\n{code(f'[{vmin}, {vmax}]')}"
    await safe_edit_message(c.message, text, reply_markup=interval_kb())
    await safe_cq_answer(c)

@dp.callback_query(F.data == "interval:change")
async def interval_change(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    vmin = await get_setting_async(await U(c), "send_delay_min", str(smtp25.MIN_SEND_DELAY))
    vmax = await get_setting_async(await U(c), "send_delay_max", str(smtp25.MAX_SEND_DELAY))
    txt = f"Текущий интервал:\n\n{code(f'[{vmin}, {vmax}]')}\n\nВведите два числа: MIN MAX (например: 3 6)"
    await ui_prompt(state, c.message.chat.id, txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("interval:open")))
    await state.set_state(IntervalFSM.set); await safe_cq_answer(c)

@dp.message(IntervalFSM.set)
async def interval_set_value(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    try:
        parts = [int(x) for x in (m.text or "").replace(",", " ").split()]
        if len(parts) != 2:
            raise ValueError
        minv, maxv = parts
        if minv < 0 or maxv < 0 or minv >= maxv:
            raise ValueError
        await set_setting_async(await U(m), "send_delay_min", str(minv))
        await set_setting_async(await U(m), "send_delay_max", str(maxv))
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        vmin = await get_setting_async(await U(m), "send_delay_min", str(smtp25.MIN_SEND_DELAY))
        vmax = await get_setting_async(await U(m), "send_delay_max", str(smtp25.MAX_SEND_DELAY))
        await bot.send_message(m.chat.id, f"Текущий интервал:\n\n{code(f'[{vmin}, {vmax}]')}", reply_markup=interval_kb())
        await state.clear()
    except Exception:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Неверный ввод. Пример: 3 6", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("interval:open")))

@dp.callback_query(F.data == "interval:reset")
async def interval_reset(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await set_setting_async(await U(c), "send_delay_min", str(smtp25.MIN_SEND_DELAY))
    await set_setting_async(await U(c), "send_delay_max", str(smtp25.MAX_SEND_DELAY))
    vmin = await get_setting_async(await U(c), "send_delay_min", str(smtp25.MIN_SEND_DELAY))
    vmax = await get_setting_async(await U(c), "send_delay_max", str(smtp25.MAX_SEND_DELAY))
    await safe_edit_message(c.message, f"Текущий интервал:\n\n{code(f'[{vmin}, {vmax}]')}", reply_markup=interval_kb())
    await c.answer("Сброшено")

# ====== PROXIES ======
def _fmt_bad_ordinals(ordinals: list[int]) -> str:
    return ", ".join(f"№ {i}" for i in ordinals) if ordinals else ""

def proxies_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Send прокси", callback_data="proxies:open:send")],
        *nav_row("settings:back")
    ])
    
def _probe_target_for_kind(kind: str) -> Tuple[str, int]:
    # Куда коннектимся для проверки работоспособности
    if kind == "verify":
        return ("imap.gmail.com", 993)  # IMAP SSL
    return ("smtp.gmail.com", 587)      # SMTP STARTTLS порт

def _test_proxy_sync(host: str, port: int, user: str, pwd: str, target_host: str, target_port: int, timeout: int = 6) -> Tuple[bool, str]:
    try:
        s = socks.socksocket()
        s.set_proxy(socks.SOCKS5, host, int(port), True, user or None, pwd or None)
        s.settimeout(timeout)
        s.connect((target_host, int(target_port)))
        try:
            s.close()
        except Exception:
            pass
        return True, "OK"
    except Exception as e:
        return False, str(e)

async def _test_proxy_async(host: str, port: int, user: str, pwd: str, target_host: str, target_port: int, timeout: int = 6) -> Tuple[bool, str]:
    """
    Асинхронная оболочка для _test_proxy_sync, выполняется в SHARED_EXECUTOR.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        SHARED_EXECUTOR,
        _test_proxy_sync,
        host, port, user, pwd, target_host, target_port, timeout
    )

def _test_proxy_with_ping_sync(host: str, port: int, user: str, pwd: str, target_host: str, target_port: int, timeout: int = 6) -> Tuple[bool, str, float]:
    """
    Проверка прокси с измерением пинга (время подключения в миллисекундах).
    Возвращает (успех, сообщение об ошибке, пинг в мс).
    """
    start_time = time.time()
    try:
        s = socks.socksocket()
        s.set_proxy(socks.SOCKS5, host, int(port), True, user or None, pwd or None)
        s.settimeout(timeout)
        s.connect((target_host, int(target_port)))
        ping_ms = (time.time() - start_time) * 1000  # Конвертируем в миллисекунды
        try:
            s.close()
        except Exception:
            pass
        return True, "OK", round(ping_ms, 2)
    except Exception as e:
        ping_ms = (time.time() - start_time) * 1000
        return False, str(e), round(ping_ms, 2)

async def _test_proxy_with_ping_async(host: str, port: int, user: str, pwd: str, target_host: str, target_port: int, timeout: int = 6) -> Tuple[bool, str, float]:
    """
    Асинхронная оболочка для _test_proxy_with_ping_sync, выполняется в SHARED_EXECUTOR.
    Возвращает (успех, сообщение об ошибке, пинг в мс).
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        SHARED_EXECUTOR,
        _test_proxy_with_ping_sync,
        host, port, user, pwd, target_host, target_port, timeout
    )

def proxies_section_kb(kind: str) -> InlineKeyboardMarkup:
    # Теперь только kind == "send"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌐 Показать прокси", callback_data=f"proxies:list:{kind}:1")],
        [InlineKeyboardButton(text="➕ Добавить прокси", callback_data=f"proxies:add:{kind}"),
         InlineKeyboardButton(text="✏️ Изменить прокси", callback_data=f"proxies:edit:{kind}")],
        [InlineKeyboardButton(text="🗑 Удалить прокси", callback_data=f"proxies:delete:{kind}"),
         InlineKeyboardButton(text="🧹 Удалить все", callback_data=f"proxies:clear:{kind}")],
        [InlineKeyboardButton(text="🔍 Проверить все", callback_data=f"proxies:check:all:{kind}")],
        *nav_row("proxies:root")
    ])

# 6. Перевести render_proxies_text_page на async
async def render_proxies_text_page(user_id: int, kind: str, page: int, per_page: int = 10) -> Tuple[str, InlineKeyboardMarkup]:
    items = await list_proxies_async(user_id, kind)
    title = "Send прокси"
    total = len(items)
    if not total:
        return f"{title}:\n(список пуст)", proxies_section_kb(kind)
    total_pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = min(total, start + per_page)
    slice_items = items[start:end]
    lines = [f"{title}:", ""]
    for i, p in enumerate(slice_items, start=start + 1):
        host = p.host or ""
        login = p.user_login or ""
        pwd = p.password or ""
        lines.append(f"Прокси №{i}: {code(f'{host}:{p.port}:{login}:{pwd}')}")
    lines.append("")
    lines.append("Для редактирования/удаления указывайте номера по списку (например: 1 3 5).")
    rows = pager_row(f"proxies:list:{kind}:", page, total_pages)
    rows += proxies_section_kb(kind).inline_keyboard
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "proxies:root")
async def proxies_root(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_edit_message(c.message, "Настройки прокси:", reply_markup=proxies_root_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("proxies:open:"))
async def proxies_open_section(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    title = "Verif прокси" if kind == "verify" else "Send прокси"
    await safe_edit_message(c.message, f"Настройки {title}:", reply_markup=proxies_section_kb(kind)); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("proxies:list:"))
async def proxies_list(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    parts = c.data.split(":")
    kind = parts[2]
    page = int(parts[3]) if len(parts) >= 4 and parts[3].isdigit() else 1
    text, kb = await render_proxies_text_page(await U(c), kind, page, per_page=10)
    await safe_edit_message(c.message, text, reply_markup=kb); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("proxies:check:"))
async def proxies_check(c: types.CallbackQuery):
    """
    Ручная проверка всех прокси с отображением валидности и пинга.
    """
    if not await ensure_approved(c):
        return
    
    parts = c.data.split(":")
    if len(parts) < 4 or parts[2] != "all":
        await safe_cq_answer(c, "Ошибка: неверный формат команды", show_alert=True)
        return
    
    kind = parts[3]
    uid = await U(c)
    
    # Получаем все прокси
    all_items = await list_proxies_async(uid, kind)
    if not all_items:
        await safe_cq_answer(c, "Нет прокси для проверки", show_alert=True)
        return
    
    # Показываем, что проверка началась
    await safe_cq_answer(c, f"Проверяю {len(all_items)} прокси...", show_alert=False)
    
    # Определяем целевой хост для проверки
    target_host, target_port = _probe_target_for_kind(kind)
    
    # Проверяем все прокси
    results = []
    for i, proxy in enumerate(all_items, start=1):
        host = proxy.host or ""
        port = proxy.port or 0
        user = proxy.user_login or ""
        pwd = proxy.password or ""
        proxy_id = getattr(proxy, "id", None)
        
        # Выполняем проверку с измерением пинга
        is_valid, error_msg, ping_ms = await _test_proxy_with_ping_async(
            host, port, user, pwd, target_host, target_port, timeout=6
        )
        
        # Обновляем статус прокси в БД
        if proxy_id:
            try:
                await update_proxy_async(uid, proxy_id, host, port, user, pwd, kind, is_valid)
            except Exception:
                pass
        
        # Формируем строку результата
        masked_pwd = "*" * len(pwd) if pwd else ""
        if is_valid:
            status_line = f"№{i}: {host}:{port}:{user}:{masked_pwd} — ✅ Валидный | Пинг: {ping_ms} мс"
        else:
            error_short = (error_msg[:30] if error_msg else "Ошибка подключения").replace("\n", " ")
            status_line = f"№{i}: {host}:{port}:{user}:{masked_pwd} — ❌ Невалидный | Пинг: {ping_ms} мс | {error_short}"
        
        results.append(status_line)
    
    # Формируем итоговое сообщение
    valid_count = sum(1 for r in results if "✅" in r)
    invalid_count = len(results) - valid_count
    
    result_text = f"Результаты проверки ({len(results)} прокси):\n\n"
    result_text += "\n".join(results)
    result_text += f"\n\nИтого: ✅ {valid_count} | ❌ {invalid_count}"
    
    # Отправляем результаты
    try:
        await bot.send_message(c.message.chat.id, result_text, reply_markup=proxies_section_kb(kind))
    except Exception:
        # Если сообщение слишком длинное, разбиваем на части
        max_length = 4000
        if len(result_text) > max_length:
            chunks = [result_text[i:i+max_length] for i in range(0, len(result_text), max_length)]
            for chunk in chunks:
                try:
                    await bot.send_message(c.message.chat.id, chunk)
                except Exception:
                    pass
        else:
            await bot.send_message(c.message.chat.id, result_text)
    
    await safe_cq_answer(c, f"Проверка завершена: ✅ {valid_count} | ❌ {invalid_count}", show_alert=True)

@dp.callback_query(F.data.startswith("proxies:add:"))
async def proxies_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите прокси в формате host:port:log:pass✍️\nМожно по одному на строку.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.add); await safe_cq_answer(c)

@dp.message(ProxiesFSM.add)
async def proxies_add_save(m: types.Message, state: FSMContext):
    """
    ПОЛНАЯ версия (замена фрагмента).
    Отличия:
      - Используется internal user id (await U(m)) при вызове add_proxy_async.
      - После добавления инвалидация user ctx по internal id.
    """
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)

    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    parsed = parse_proxy_lines(m.text or "")
    if not parsed:
        await ui_clear_prompts(state)
        await bot.send_message(
            m.chat.id,
            "Не распознано ни одной строки. Ожидается host:port:login:password",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}"))
        )
        return

    target_host, target_port = _probe_target_for_kind(kind)
    lines = [f"Проверка Send прокси:"]
    ok_cnt = 0
    fail_cnt = 0
    internal_uid = await U(m)

    for host, port, user, pwd in parsed:
        # Используем проверку с пингом
        ok, err, ping_ms = await _test_proxy_with_ping_async(host, port, user, pwd, target_host, target_port, timeout=6)
        masked_pwd = "*" * len(pwd) if pwd else ""
        if ok:
            # Добавляем прокси в базу только если он валидный
            await add_proxy_async(
                internal_uid,
                host, port, user, pwd, kind, True
            )
            status = f"✅ Валидный | Пинг: {ping_ms} мс | Добавлен"
            ok_cnt += 1
        else:
            # Невалидные прокси не добавляются в базу
            error_short = (err[:30] if err else "Ошибка подключения").replace("\n", " ")
            status = f"❌ Невалидный | Пинг: {ping_ms} мс | {error_short} | Не добавлен"
            fail_cnt += 1
        lines.append(f"{host}:{port}:{user}:{masked_pwd} — {status}")

    try:
        invalidate_user_ctx(internal_uid)
    except Exception:
        pass

    lines.append("")
    lines.append(f"Итог: OK={ok_cnt}, Ошибок={fail_cnt}")
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "\n".join(lines), reply_markup=proxies_section_kb(kind))
    await state.clear()

@dp.callback_query(F.data.startswith("proxies:edit:"))
async def proxies_edit_pick(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номер прокси по списку (например: 2):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.edit_pick); await safe_cq_answer(c)

@dp.message(ProxiesFSM.edit_pick)
async def proxies_edit_id(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    if not (m.text or "").strip().isdigit():
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Нужен номер (например: 2).", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
        return
    ordinal = int((m.text or "").strip())
    items = await list_proxies_async(await U(m), kind)
    chosen = _get_by_ordinal(items, ordinal)
    if not chosen:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный номер.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
        return
    await state.update_data(proxy_id=int(getattr(chosen, "id")))
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Введите новые данные в формате host:port:log:pass:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.edit_value)

@dp.message(ProxiesFSM.edit_value)
async def proxies_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    proxy_id = int(data.get("proxy_id"))

    parsed = parse_proxy_lines(m.text or "")
    if len(parsed) != 1:
        await ui_clear_prompts(state)
        await ui_prompt(
            state, m.chat.id,
            "Ожидается одна строка формата host:port:login:password.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}"))
        )
        return

    host, port, user, pwd = parsed[0]
    target_host, target_port = _probe_target_for_kind(kind)
    ok, err = await _test_proxy_async(host, port, user, pwd, target_host, target_port, timeout=6)

    # Обновляем через async wrapper
    await update_proxy_async(await U(m), proxy_id, host, port, user, pwd, kind, bool(ok))

    # Получим свежую запись (если нужно) и отдадим ответ пользователю
    # Можно использовать get_proxy_async если необходимо показать поля
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass

    status = "✅ OK" if ok else f"❌ Ошибка: {err}"
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, f"Прокси обновлён.\nРезультат проверки: {status}", reply_markup=proxies_section_kb(kind))
    await state.clear()

@dp.callback_query(F.data.startswith("proxies:delete:"))
async def proxies_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номера прокси для удаления (например: 1 3 5):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.delete); await safe_cq_answer(c)

@dp.message(ProxiesFSM.delete)
async def proxies_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    try:
        ordinals = [int(x) for x in (m.text or "").replace(",", " ").split()]
    except Exception:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный ввод. Пример: 1 2 3", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}"))); return

    items = await list_proxies_async(await U(m), kind)
    ids_to_delete = []
    for o in ordinals:
        item = _get_by_ordinal(items, o)
        if item:
            ids_to_delete.append(getattr(item, "id"))
    if ids_to_delete:
        await delete_proxies_by_ids_async(await U(m), kind, ids_to_delete)

    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass

    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Удаление выполнено.", reply_markup=proxies_section_kb(kind))
    await state.clear()

@dp.callback_query(F.data.startswith("proxies:clear:"))
async def proxies_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kind = c.data.split(":")[2]
    await state.update_data(proxy_kind=kind)
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Подтвердите удаление всех прокси: ДА", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row(f"proxies:open:{kind}")))
    await state.set_state(ProxiesFSM.clear); await safe_cq_answer(c)

@dp.message(ProxiesFSM.clear)
async def proxies_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    data = await state.get_data()
    kind = data.get("proxy_kind", "send")
    if (m.text or "").strip().upper() == "ДА":
        await clear_proxies_async(await U(m), kind)
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Все прокси удалены.", reply_markup=proxies_section_kb(kind))
    else:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Отменено.", reply_markup=proxies_section_kb(kind))
    await state.clear()

# ====== EMAIL ACCOUNTS ======
def emails_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📬 Показать E‑mail", callback_data="emails:list:1")],
        [InlineKeyboardButton(text="➕ Добавить E‑mail", callback_data="emails:add"),
         InlineKeyboardButton(text="✏️ Изменить E‑mail", callback_data="emails:edit")],
        [InlineKeyboardButton(text="🗑 Удалить E‑mail", callback_data="emails:delete"),
         InlineKeyboardButton(text="🧹 Удалить все", callback_data="emails:clear")],
        *nav_row("settings:back")
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def build_emails_text_and_kb(user_id: int, page: int = 1, per_page: int = 10) -> Tuple[str, InlineKeyboardMarkup]:
    """
    Построение списка почтовых аккаунтов с пагинацией через list_accounts_page_async.
    """
    items, total = await list_accounts_page_async(user_id, page=page, per_page=per_page)
    if total == 0:
        return "Пока аккаунтов нет.", emails_menu_kb()
    total_pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    lines = []
    for i, acc in enumerate(items, start=start + 1):
        lines.append(f"E‑mail №{i}")
        # acc может быть ORM-объектом, но его поля обычно загружены
        lines.append(code(getattr(acc, "display_name", "") or ""))
        lines.append(code(f"{getattr(acc, 'email', '')}:{getattr(acc, 'password', '')}"))
        lines.append("")
    rows = pager_row("emails:list:", page, total_pages)
    rows += emails_menu_kb().inline_keyboard
    return "\n".join(lines).strip(), InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "emails:open")
async def emails_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_edit_message(c.message, "Настройки E‑mail:", reply_markup=emails_menu_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("emails:list"))
async def emails_list(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    parts = c.data.split(":")
    page = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
    text, kb = await build_emails_text_and_kb(await U(c), page=page, per_page=10)
    await safe_edit_message(c.message, text, reply_markup=kb); await safe_cq_answer(c)

async def _ensure_imap_started_for_user(uid: int, chat_id: int):
    """
    Новая архитектура: используем process pool. Аккаунты добавляются в очередь через start_imap_process.
    """
    await _schedule_all_active_accounts(uid, chat_id)

@dp.callback_query(F.data == "emails:add")
async def emails_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите отображаемое имя и фамилию. Например: Jessy Jackson ✍️", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(AddAccountFSM.display_name); await safe_cq_answer(c)

@dp.message(AddAccountFSM.display_name)
async def emails_add_name(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    await state.update_data(display_name=(m.text or "").strip())
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Введите E‑mail в формате login:pass ✍️", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(AddAccountFSM.loginpass)

@dp.message(AddAccountFSM.loginpass)
async def emails_add_loginpass(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)

    uid = await U(m)

    # Лимит 97 аккаунтов
    left = await limit_remaining_slots(uid)
    if left <= 0:
        await bot.send_message(
            m.chat.id,
            "Максимальное допустимое количество Email: 97 ❗️",
            reply_markup=emails_menu_kb()
        )
        await state.clear()
        return

    data = await state.get_data()
    disp = (data.get("display_name", "") or "").strip()
    text_in = (m.text or "").strip()

    if ":" not in text_in:
        await bot.send_message(
            m.chat.id,
            "Ожидаю формат login:pass. Попробуйте ещё раз.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))
        )
        return

    login, password = [x.strip() for x in text_in.split(":", 1)]

    if not is_valid_email(login):
        await bot.send_message(
            m.chat.id,
            "Неверный формат email (ожидается user@domain). Аккаунт не добавлен ❗",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))
        )
        await state.clear()
        return

    proxy_id = await pick_proxy_for_account(uid)

    # Добавляем один аккаунт (лимит уже проверен)
    acc = await add_account_async(uid, disp, login, password, True, proxy_id=proxy_id)
    if acc is None:
        await bot.send_message(m.chat.id, "Аккаунт не добавлен (возможно уже существует или ошибка).", reply_markup=emails_menu_kb())
        await state.clear()
        return

    invalidate_user_cache(uid)
    await set_account_active_async(uid, acc.id, True)
    await _ensure_imap_started_for_user(uid, m.chat.id)
    await bot.send_message(m.chat.id, "Аккаунт добавлен.", reply_markup=emails_menu_kb())
    await state.clear()

@dp.callback_query(F.data == "emails:edit")
async def emails_edit(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номер аккаунта для изменения (например: 1):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EditAccountFSM.account_id); await safe_cq_answer(c)

@dp.message(EditAccountFSM.account_id)
async def emails_edit_pick(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if not (m.text or "").strip().isdigit():
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Нужен номер аккаунта.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))); return
    ordinal = int((m.text or "").strip())
    accs = await list_accounts_async(await U(m))
    chosen = accs[ordinal-1] if 1 <= ordinal <= len(accs) else None
    if not chosen:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный номер.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))); return
    await state.update_data(account_id=int(chosen.id))
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Новое отображаемое имя:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EditAccountFSM.display_name)

@dp.message(EditAccountFSM.display_name)
async def emails_edit_name(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    await state.update_data(display_name=(m.text or "").strip())
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Новый login:pass:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EditAccountFSM.loginpass)

@dp.message(EditAccountFSM.loginpass)
async def emails_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    data = await state.get_data()
    acc_id = int(data["account_id"])
    if ":" not in (m.text or ""):
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Ожидаю формат login:pass.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))); return
    login, password = [x.strip() for x in (m.text or "").split(":", 1)]
    await update_account_async(await U(m), acc_id, data["display_name"], login, password)
    invalidate_user_cache(await U(m))
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Аккаунт обновлён.", reply_markup=emails_menu_kb())
    await _ensure_imap_started_for_user(await U(m), m.chat.id)
    await state.clear()

@dp.callback_query(F.data == "emails:delete")
async def emails_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номер аккаунта для удаления (например: 1):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open")))
    await state.set_state(EmailDeleteFSM.account_id); await safe_cq_answer(c)

@dp.message(EmailDeleteFSM.account_id)
async def emails_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if not (m.text or "").strip().isdigit():
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Нужен номер аккаунта.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))); return
    ordinal = int((m.text or "").strip())
    accs = await list_accounts_async(await U(m))
    chosen = accs[ordinal-1] if 1 <= ordinal <= len(accs) else None
    if not chosen:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный номер.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))); return
    await delete_account_async(await U(m), chosen.id)
    invalidate_user_cache(await U(m))
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Аккаунт удалён.", reply_markup=emails_menu_kb())
    await state.clear()



@dp.message(EmailsClearFSM.confirm)
async def emails_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)

    if (m.text or "").strip().upper() != "ДА":
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Отменено.", reply_markup=emails_menu_kb())
        await state.clear()
        return

    data = await state.get_data()
    uid = await U(m)
    ids: list[int] = data.get("emails_clear_ids", []) or []
    emails: list[str] = data.get("emails_clear_emails", []) or []

    deleted_cnt = 0
    for acc_id in ids:
        try:
            await delete_account_async(uid, acc_id)
            deleted_cnt += 1
        except Exception:
            pass

    # ВАЖНО: Новая архитектура - старая логика удалена, аккаунты управляются через process pool
    # Удаление аккаунтов обрабатывается через _ensure_imap_stopped_for_user

    invalidate_user_cache(uid)

    await ui_clear_prompts(state)
    if deleted_cnt == 0:
        await bot.send_message(m.chat.id, "Нет неактивных аккаунтов для удаления.", reply_markup=emails_menu_kb())
    else:
        await bot.send_message(m.chat.id, f"Удалено неактивных аккаунтов: {deleted_cnt}", reply_markup=emails_menu_kb())

    await state.clear()

@dp.callback_query(F.data == "emails:clear")
async def emails_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    uid = await U(c)

    # Собираем кандидатов на удаление через async list_accounts_async
    accounts = await list_accounts_async(uid)

    # Use centralized helper instead of direct dict inspection
    def is_runtime_active(email: str) -> Optional[bool]:
        return _runtime_is_active(uid, email)

    to_delete = []
    for acc in accounts:
        ra = is_runtime_active(getattr(acc, "email", ""))
        if (getattr(acc, "active", True) is False) or (ra is False):
            to_delete.append({"id": getattr(acc, "id"), "email": getattr(acc, "email")})

    if not to_delete:
        await safe_edit_message(
            c.message,
            "Нет неактивных аккаунтов для удаления.",
            reply_markup=emails_menu_kb()
        )
        await safe_cq_answer(c)
        return

    await state.update_data(emails_clear_ids=[x["id"] for x in to_delete],
                            emails_clear_emails=[x["email"] for x in to_delete])

    cnt = len(to_delete)
    await safe_edit_message(
        c.message,
        f"Будут удалены только неактивные аккаунты: {cnt} шт.\nПодтвердите удаление: ДА",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("emails:open"))
    )
    await state.set_state(EmailsClearFSM.confirm)
    await safe_cq_answer(c)



# ====== PRESETS (IMAP) ======
def presets_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Показать", callback_data="presets:show:1")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="presets:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="presets:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="presets:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="presets:clear")],
        # ВАЖНО: назад уводим в настройки
        *nav_row("settings:back")
    ])

def presets_pager_kb(page: int, total_pages: int) -> list[list[InlineKeyboardButton]]:
    return pager_row("presets:show:", page, total_pages)

def presets_manage_kb() -> list[list[InlineKeyboardButton]]:
    return [
        [InlineKeyboardButton(text="➕ Добавить", callback_data="presets:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="presets:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="presets:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="presets:clear")],
        *nav_row("presets:open")
    ]

# 4. Перевести build_imap_presets_text_and_kb на async
async def build_imap_presets_text_and_kb(user_id: int, page: int = 1, per_page: int = 10) -> Tuple[str, InlineKeyboardMarkup]:
    items = await list_presets_async(user_id)
    total = len(items)
    if total == 0:
        return "Пресетов пока нет.", presets_kb()

    def compose_page(pp: int) -> Tuple[str, int]:
        total_pages = max(1, math.ceil(total / pp))
        page_clamped = max(1, min(page, total_pages))
        start = (page_clamped - 1) * pp
        end = min(total, start + pp)
        slice_items = items[start:end]
        lines: list[str] = []
        for idx, p in enumerate(slice_items, start=start + 1):
            title = (p.title or "").strip()
            body = (p.body or "").strip()
            lines.append(f"Пресет №{idx}" + (f" — {title}" if title else ""))
            if body:
                lines.append(code(body))
            lines.append("")
        return "\n".join(lines).strip(), total_pages

    text, total_pages = compose_page(per_page)
    while len(text) > 3800 and per_page > 3:
        per_page -= 1
        text, total_pages = compose_page(per_page)

    ik = presets_pager_kb(page, total_pages)
    ik += presets_manage_kb()
    return text, InlineKeyboardMarkup(inline_keyboard=ik)

async def presets_inline_kb(user_id: int, back_cb: str) -> InlineKeyboardMarkup:
    items = await list_presets_async(user_id)
    rows = []
    for i, p in enumerate(items, start=1):
        title = (p.title or "").strip() or f"Пресет №{i}"
        if len(title) > 60:
            title = title[:57] + "..."
        rows.append([InlineKeyboardButton(text=f"📜 {title}", callback_data=f"presets:view:{p.id}:{back_cb}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "presets:open")
async def presets_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    # Используем safe_edit_message — она игнорирует "message is not modified"
    text = "Пресеты (IMAP):"
    kb = presets_kb()
    await safe_edit_message(c.message, text, reply_markup=kb)
    await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("presets:show"))
async def presets_show(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    parts = c.data.split(":")
    page = 1
    if len(parts) == 3 and parts[2].isdigit():
        page = int(parts[2])
    text, kb = await build_imap_presets_text_and_kb(await U(c), page=page, per_page=10)
    await safe_edit_message(c.message, text, reply_markup=kb); await safe_cq_answer(c)

@dp.callback_query(F.data == "presets:noop")
async def presets_noop(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_cq_answer(c)

@dp.callback_query(F.data == "presets:add")
async def presets_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state)
    await delete_message_safe(c.message)
    await ui_prompt(
        state,
        c.message.chat.id,
        "Введите заголовок пресета:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))
    )
    await state.set_state(PresetAddFSM.title)
    await safe_cq_answer(c)

@dp.message(PresetAddFSM.title)
async def presets_add_title(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    await state.update_data(title=(m.text or "").strip())
    await ui_clear_prompts(state)
    await ui_prompt(
        state,
        m.chat.id,
        "Введите текст пресета:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))
    )
    await state.set_state(PresetAddFSM.body)

@dp.message(PresetAddFSM.body)
async def presets_add_body(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    data = await state.get_data()
    await add_preset_async(await U(m), data["title"], m.text or "")
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Пресет добавлен.", reply_markup=presets_kb())
    await state.clear()

@dp.callback_query(F.data == "presets:edit")
async def presets_edit(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state)
    await delete_message_safe(c.message)
    await ui_prompt(
        state,
        c.message.chat.id,
        "Введите номер пресета по списку (например: 1):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))
    )
    await state.set_state(PresetEditFSM.preset_id)
    await safe_cq_answer(c)


@dp.message(PresetEditFSM.title)
async def presets_edit_title(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): 
        return
    await delete_message_safe(m)
    await state.update_data(title=(m.text or "").strip())
    await ui_clear_prompts(state)
    await ui_prompt(
        state,
        m.chat.id,
        "Новый текст:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))
    )
    await state.set_state(PresetEditFSM.body)

@dp.message(PresetEditFSM.body)
async def presets_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): 
        return
    await delete_message_safe(m)
    data = await state.get_data()
    await update_preset_async(await U(m), data["preset_id"], data.get("title", ""), m.text or "")
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Пресет обновлён.", reply_markup=presets_kb())
    await state.clear()

@dp.callback_query(F.data == "presets:delete")
async def presets_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state)
    await delete_message_safe(c.message)
    await ui_prompt(
        state, c.message.chat.id,
        "Введите номера пресетов для удаления (например: 1 3 4):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))
    )
    await state.set_state(PresetDeleteFSM.preset_id)
    await safe_cq_answer(c)

@dp.message(PresetDeleteFSM.preset_id)
async def presets_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    try:
        ordinals = [int(x) for x in (m.text or "").replace(",", " ").split()]
    except Exception:
        await ui_clear_prompts(state)
        await ui_prompt(
            state, m.chat.id,
            "Неверный ввод. Пример: 1 2 3",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))
        )
        return
    presets = await list_presets_async(await U(m))
    ids_to_delete = [presets[o-1].id for o in ordinals if 1 <= o <= len(presets)]
    if ids_to_delete:
        await delete_presets_by_ids_async(await U(m), ids_to_delete)
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Удалено.", reply_markup=presets_kb())
    await state.clear()

@dp.callback_query(F.data == "presets:clear")
async def presets_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state)
    await delete_message_safe(c.message)
    await ui_prompt(
        state,
        c.message.chat.id,
        "Подтвердите очистку пресетов: ДА",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open"))
    )
    await state.set_state(PresetClearFSM.confirm)
    await safe_cq_answer(c)

@dp.message(PresetClearFSM.confirm)
async def presets_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if (m.text or "").strip().upper() == "ДА":
        await clear_presets_async(await U(m))
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Все пресеты удалены.", reply_markup=presets_kb())
    else:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Отменено.", reply_markup=presets_kb())
    await state.clear()


# ====== SMART PRESETS ======
def smart_settings_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Показать пресеты", callback_data="smart:show:1")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="smart:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="smart:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="smart:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="smart:clear")],
        *nav_row("settings:back")
    ])

def smart_pager_kb(page: int, total_pages: int) -> list[list[InlineKeyboardButton]]:
    return pager_row("smart:show:", page, total_pages)

def smart_manage_kb() -> list[list[InlineKeyboardButton]]:
    return [
        [InlineKeyboardButton(text="➕ Добавить", callback_data="smart:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="smart:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="smart:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="smart:clear")],
        *nav_row("smart:open")
    ]

# 5. Перевести build_smart_text_and_kb на async
async def build_smart_text_and_kb(user_id: int, page: int = 1, per_page: int = 10) -> Tuple[str, InlineKeyboardMarkup]:
    items = await list_smart_presets_async(user_id)
    total = len(items)
    if total == 0:
        return "Пресетов пока нет.", smart_settings_kb()

    def compose_page(pp: int) -> Tuple[str, int]:
        total_pages = max(1, math.ceil(total / pp))
        page_clamped = max(1, min(page, total_pages))
        start = (page_clamped - 1) * pp
        end = min(total, start + pp)
        slice_items = items[start:end]
        lines: list[str] = []
        for i, p in enumerate(slice_items, start=start + 1):
            lines.append(f"Пресет №{i}")
            lines.append(code((p.body or "").strip()))
            lines.append("")
        return "\n".join(lines).strip(), total_pages

    text, total_pages = compose_page(per_page)
    while len(text) > 3800 and per_page > 3:
        per_page -= 1
        text, total_pages = compose_page(per_page)

    ik = smart_pager_kb(page, total_pages)
    ik += smart_manage_kb()
    return text, InlineKeyboardMarkup(inline_keyboard=ik)

@dp.callback_query(F.data == "smart:open")
async def smart_open(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_edit_message(c.message, "Настройки умных пресетов:", reply_markup=smart_settings_kb()); await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("smart:show"))
async def smart_show(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    parts = c.data.split(":")
    page = 1
    if len(parts) == 3 and parts[2].isdigit():
        page = int(parts[2])
    text, kb = await build_smart_text_and_kb(await U(c), page=page, per_page=10)
    await safe_edit_message(c.message, text, reply_markup=kb); await safe_cq_answer(c)

@dp.callback_query(F.data == "smart:noop")
async def smart_noop(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    await safe_cq_answer(c)

@dp.callback_query(F.data == "smart:add")
async def smart_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    example = (
        "OFFER\n"
        "Hallo SELLER! Noch verfügbar das Angebot? Alles okay damit?\n"
        "=\n"
        "OFFER\n"
        "Hi SELLER! Ist der Artikel noch aktuell? Zustand gut?"
    )
    await ui_prompt(
        state,
        c.message.chat.id,
        "Введите текст пресета.\n"
        "Можно сразу несколько — разделяйте блоки строкой, где только знак '='.\n\n"
        "Пример двух пресетов:\n"
        f"{code(example)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open"))
    )
    await state.set_state(SmartPresetAddFSM.body); await safe_cq_answer(c)

@dp.message(SmartPresetAddFSM.body)
async def smart_add_body(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)

    blocks = parse_smart_preset_blocks(m.text or "")
    if not blocks:
        await ui_clear_prompts(state)
        await bot.send_message(
            m.chat.id,
            "Не распознан ни один пресет. Разделяйте пресеты строкой, где только '='.",
            reply_markup=smart_settings_kb()
        )
        await state.clear()
        return

    uid = await U(m)
    added = 0
    # Внутривводные пустые/дубликаты уже отфильтрованы на уровне блоков
    for body in blocks:
        try:
            await add_smart_preset_async(uid, body)
            added += 1
        except Exception:
            pass

    try:
        invalidate_user_ctx(uid)
    except Exception:
        pass

    await ui_clear_prompts(state)
    msg = f"Пресетов добавлено: {added}" if added > 1 else "Пресет добавлен."
    await bot.send_message(m.chat.id, msg, reply_markup=smart_settings_kb())
    await state.clear()

@dp.callback_query(F.data == "smart:edit")
async def smart_edit(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номер умного пресета для изменения (например: 1):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open")))
    await state.set_state(SmartPresetEditFSM.preset_id); await safe_cq_answer(c)

@dp.message(SmartPresetEditFSM.preset_id)
async def smart_edit_pick(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if not (m.text or "").strip().isdigit():
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Нужен номер пресета (например: 1).", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open")))
        return
    ordinal = int((m.text or "").strip())
    presets = await list_smart_presets_async(await U(m))
    chosen = presets[ordinal-1] if 1 <= ordinal <= len(presets) else None
    if not chosen:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный номер.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open")))
        return
    await state.update_data(preset_id=int(chosen.id))
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Введите новый текст пресета:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open")))
    await state.set_state(SmartPresetEditFSM.body)

@dp.message(SmartPresetEditFSM.body)
async def smart_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    data = await state.get_data()
    await update_smart_preset_async(await U(m), int(data.get("preset_id", 0)), (m.text or "").strip())
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Пресет обновлён.", reply_markup=smart_settings_kb())
    await state.clear()


@dp.callback_query(F.data == "smart:delete")
async def smart_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номера умных пресетов для удаления (например: 1 3 4):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open")))
    await state.set_state(SmartPresetDeleteFSM.preset_id); await safe_cq_answer(c)

@dp.message(SmartPresetDeleteFSM.preset_id)
async def smart_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    try:
        ordinals = [int(x) for x in (m.text or "").replace(",", " ").split()]
    except Exception:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный ввод. Пример: 1 2 3", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open")))
        return
    presets = await list_smart_presets_async(await U(m))
    ids_to_delete = [presets[o-1].id for o in ordinals if 1 <= o <= len(presets)]
    if ids_to_delete:
        await delete_smart_presets_by_ids_async(await U(m), ids_to_delete)
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Удалено.", reply_markup=smart_settings_kb())
    await state.clear()


@dp.callback_query(F.data == "smart:clear")
async def smart_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Подтвердите очистку всех умных пресетов: ДА", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("smart:open")))
    await state.set_state(SmartPresetClearFSM.confirm); await safe_cq_answer(c)

@dp.message(SmartPresetClearFSM.confirm)
async def smart_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if (m.text or "").strip().upper() == "ДА":
        await clear_smart_presets_async(await U(m))
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Все умные пресеты удалены.", reply_markup=smart_settings_kb())
    else:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Отменено.", reply_markup=smart_settings_kb())
    await state.clear()

# ====== SUBJECTS ======
# 3. Перевести subjects_text_page на async
async def subjects_text_page(user_id: int, page: int = 1, per_page: int = 10) -> Tuple[str, InlineKeyboardMarkup]:
    items = await list_subjects_async(user_id)
    if not items:
        return "Тем пока нет.", subjects_kb()
    total = len(items)
    total_pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    end = min(total, start + per_page)
    slice_items = items[start:end]
    lines = ["Ваши темы:", ""]
    for i, x in enumerate(slice_items, start=start + 1):
        lines.append(f"Тема №{i} {code(x.title)}")
    rows = pager_row("subjects:show:", page, total_pages)
    rows += subjects_kb().inline_keyboard
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)

def subjects_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 Показать", callback_data="subjects:show:1")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="subjects:add"),
         InlineKeyboardButton(text="✏️ Изменить", callback_data="subjects:edit")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data="subjects:delete"),
         InlineKeyboardButton(text="🧹 Очистить", callback_data="subjects:clear")],
        *nav_row("settings:back")
    ])

@dp.callback_query(F.data == "subjects:open")
async def subjects_open(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    await safe_edit_message(c.message, "Темы:", reply_markup=subjects_kb())
    await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("subjects:show"))
async def subjects_list(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    parts = c.data.split(":")
    page = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
    text, kb = await subjects_text_page(await U(c), page=page, per_page=10)
    await safe_edit_message(c.message, text, reply_markup=kb); await safe_cq_answer(c)

@dp.callback_query(F.data == "subjects:add")
async def subjects_add(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(
        state,
        c.message.chat.id,
        "Введите название темы.\nМожно сразу несколько — по одной на строке ✍️",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open"))
    )
    await state.set_state(SubjectAddFSM.title); await safe_cq_answer(c)

@dp.message(SubjectAddFSM.title)
async def subjects_add_title(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    raw = (m.text or "")
    subjects = parse_subject_lines(raw)
    if not subjects:
        await ui_clear_prompts(state)
        await bot.send_message(
            m.chat.id,
            "Не распознано ни одной темы. Отправьте текст, где каждая строка — отдельная тема.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open"))
        )
        return

    uid = await U(m)
    added = 0
    # Добавляем все распознанные (внутривводные дубликаты уже убраны)
    for title in subjects:
        try:
            await add_subject_async(uid, title)
            added += 1
        except Exception:
            pass

    try:
        invalidate_user_ctx(uid)
    except Exception:
        pass

    await ui_clear_prompts(state)
    msg = f"Тем добавлено: {added}" if added > 1 else "Тема добавлена."
    await bot.send_message(m.chat.id, msg, reply_markup=subjects_kb())
    await state.clear()

@dp.callback_query(F.data == "subjects:edit")
async def subjects_edit(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номер темы (например: 1):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectEditFSM.subject_id); await safe_cq_answer(c)

@dp.message(SubjectEditFSM.subject_id)
async def subjects_edit_pick(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if not (m.text or "").strip().isdigit():
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Нужен номер темы.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
        return
    ordinal = int((m.text or "").strip())
    subjects = await list_subjects_async(await U(m))
    chosen = subjects[ordinal-1] if 1 <= ordinal <= len(subjects) else None
    if not chosen:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный номер.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
        return
    await state.update_data(subject_id=int(chosen.id))
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Новое название темы:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectEditFSM.title)

@dp.message(SubjectEditFSM.title)
async def subjects_edit_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    data = await state.get_data()
    await update_subject_async(await U(m), data["subject_id"], (m.text or "").strip())
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Тема обновлена.", reply_markup=subjects_kb()); await state.clear()

@dp.callback_query(F.data == "subjects:delete")
async def subjects_delete(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите номера тем для удаления (например: 2 4 5):", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectDeleteFSM.subject_id); await safe_cq_answer(c)

@dp.message(SubjectDeleteFSM.subject_id)
async def subjects_delete_do(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    try:
        ordinals = [int(x) for x in (m.text or "").replace(",", " ").split()]
    except Exception:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный ввод. Пример: 1 2 3", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
        return
    subjects = await list_subjects_async(await U(m))
    ids_to_delete = [subjects[o-1].id for o in ordinals if 1 <= o <= len(subjects)]
    if ids_to_delete:
        await delete_subjects_by_ids_async(await U(m), ids_to_delete)
    try:
        invalidate_user_ctx(await U(m))
    except Exception:
        pass
    await ui_clear_prompts(state)
    await bot.send_message(m.chat.id, "Удалено.", reply_markup=subjects_kb()); await state.clear()

@dp.callback_query(F.data == "subjects:clear")
async def subjects_clear(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Подтвердите очистку тем: ДА", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("subjects:open")))
    await state.set_state(SubjectClearFSM.confirm); await safe_cq_answer(c)

@dp.message(SubjectClearFSM.confirm)
async def subjects_clear_confirm(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    if (m.text or "").strip().upper() == "ДА":
        await clear_subjects_async(await U(m))
        try:
            invalidate_user_ctx(await U(m))
        except Exception:
            pass
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Все темы удалены.", reply_markup=subjects_kb())
    else:
        await ui_clear_prompts(state)
        await bot.send_message(m.chat.id, "Отменено.", reply_markup=subjects_kb())
    await state.clear()

# ====== CHECK NICKS (XLSX) ======
def after_xlsx_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📘 Выполнить проверку email", callback_data="check:verify_emails")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ])

def after_verify_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Запустить сендинг", callback_data="send:start")],
        [InlineKeyboardButton(text="📊 Статус", callback_data="send:status"),
         InlineKeyboardButton(text="🛑 Стоп", callback_data="send:stop")],
        [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
    ])

@dp.message(F.text.in_(["📖 Проверка ников", "Проверка ников"]))
async def btn_check(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)

    # Ставим состояние ожидания файла
    await state.set_state(CheckNicksFSM.file)
    # Чистим предыдущие промпты (если были)
    await ui_clear_prompts(state)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Отмена", callback_data="checknicks:cancel")],
            [InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")]
        ]
    )

    caption_text = "Пришлите .xlsx файл для проверки."

    # Пытаемся отправить фото с caption
    if LOGO_FILE_PATH.is_file():
        try:
            msg = await bot.send_photo(
                m.chat.id,
                photo=FSInputFile(str(LOGO_FILE_PATH)),
                caption=caption_text,
                reply_markup=kb
            )
            # Добавляем в трекинг, чтобы удалялось при отмене
            await _ui_msgs_add(state, m.chat.id, msg.message_id)
            return
        except Exception as e_logo:
            log_send_event(f"LOGO SEND ERROR: {e_logo}")

    # Fallback — просто текст, если картинка не найдена или ошибка отправки
    await ui_prompt(
        state,
        m.chat.id,
        caption_text,
        reply_markup=kb
    )
    
@dp.callback_query(F.data == "checknicks:cancel")
async def checknicks_cancel(c: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await delete_message_safe(c.message)
    await bot.send_message(
        c.message.chat.id,
        "Проверка отменена.",
        reply_markup=reply_main_kb(admin=is_admin(c.from_user.id))
    )
    await safe_cq_answer(c)

@dp.message(Command("check"))
async def cmd_check(m: types.Message, state: FSMContext):
    await btn_check(m, state)

@dp.message(F.text.regexp(r"(?i)проверка\s*ников"))
async def btn_check_regex(m: types.Message, state: FSMContext):
    await btn_check(m, state)

def pick_columns_via_smtp25(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str], Optional[str]]:
    seller_col: Optional[str] = None
    title_col: Optional[str] = None
    try:
        col_map = smtp25.detect_columns(df) or {}
        seller_col = col_map.get("seller_nick")
        title_col = col_map.get("title")
    except Exception:
        pass
    if not seller_col:
        for cand in ("seller_nick", "Имя продавца"):
            if cand in df.columns:
                seller_col = cand; break
    if not title_col:
        for cand in ("title", "Название", "Название товара"):
            if cand in df.columns:
                title_col = cand; break
    rename = {}
    if seller_col: rename[seller_col] = "seller_nick"
    if title_col: rename[title_col] = "title"
    return df.rename(columns=rename).copy(), seller_col, title_col
    
# добавить рядом с pick_columns_via_smtp25

def extract_bases_sync_from_df(df: pd.DataFrame) -> Tuple[Optional[str], List[str]]:
    """
    Синхронная (CPU) функция: определяет колонку seller_nick (через pick_columns_via_smtp25)
    и проходит по строкам, генерируя email base'ы (smtp25.generate_email).
    Запускать через asyncio.to_thread для избегания блокировки event loop.
    Возвращает (seller_col, bases_list).
    """
    seller_col = None
    try:
        df2, seller_col, _ = pick_columns_via_smtp25(df)
    except Exception:
        # В случае проблем с определением колонок — попробовать использовать оригинальный df без переименования
        df2 = df.copy()
    bases: List[str] = []
    seen: set[str] = set()
    import re as _re
    for row in df2.itertuples(index=False):
        nick = str(getattr(row, "seller_nick", "")).strip()
        if not nick:
            continue
        normalized = _normalize_nick_local(nick)
        tokens = set(_re.findall(r"[a-z]+", normalized))
        if any(w in tokens for w in STOPWORDS):
            continue
        parts = smtp25.extract_name_parts(nick)
        if not parts:
            continue
        first, last = parts
        # минимальные фильтры, как в старой логике
        if len(first) < 3 or (last and len(last) < 3):
            continue
        base = smtp25.generate_email(first, last)
        if base and base not in seen:
            seen.add(base)
            bases.append(base)
    return seller_col, bases
    
def _verify_emails_from_df_for_user_sync(ctx: smtp25.UserContext, df: pd.DataFrame, chat_id: int | str | None = None, username: str | None = None) -> List[Dict[str, Any]]:
    """Синхронная реализация верификации"""
    df2, _, _ = pick_columns_via_smtp25(df)
    keep = [c for c in ("seller_nick", "title") if c in df2.columns]
    if not keep:
        return []
    df2 = df2[keep].copy()

    rows: List[Dict[str, str]] = []
    for row in df2.itertuples(index=False):
        nick = str(getattr(row, "seller_nick", "")).strip()
        if not nick:
            continue
        parts = smtp25.extract_name_parts(nick)
        if not parts or len(parts) != 2:
            continue
        first, last = parts
        if not first or not last:
            continue
        email_base = smtp25.generate_email(first, last)
        rows.append({"base": email_base, "title": getattr(row, "title", "")})

    priority = getattr(ctx, "domains", []) or []
    bases = [r["base"] for r in rows]
    # Передаем chat_id и username в API для логирования
    emails_dict = smtp25.verify_emails_via_api(bases, priority, chat_id=chat_id, username=username)

    results: List[Dict[str, Any]] = []
    for row in rows:
        base = row["base"]
        email = emails_dict.get(base)
        if email:
            results.append({
                "email": email,
                "seller_name": base,
                "title": row["title"]
            })
    return results  

@dp.message(CheckNicksFSM.file, F.document)
@time_it
async def on_xlsx_received(m: types.Message, state: FSMContext, **kwargs: Any):
    """
    Парсинг XLSX (объявления):
      - Для каждой строки с (seller_nick + ссылка) генерируется стабильный ad_id = sha1(raw_nick||link)[:12]
      - Кэш:
          AD_ADS_BY_ID_PER_CHAT[chat_id][ad_id] = {...}
          AD_LOCAL2ID_PER_CHAT[chat_id][variant] = ad_id
        variant = нормализованный local part / варианты first.last / ник в нормализованном виде
      - BASES_PER_CHAT остаётся для возможной верификации email (старый функционал)
      - В конце выводится список email-баз (если есть) и сообщение о завершении.
      - Финальный текст изменён на: "Выполнено успешно✅"
      - ДОБАВЛЕНО: сохранение кэша на диск (save_ad_cache)
    """
    filename = (m.document.file_name or "").lower()
    if not filename.endswith(".xlsx"):
        await bot.send_message(m.chat.id, "Ожидается .xlsx файл.")
        return

    async with XLSX_SEMAPHORE:
        buf = None
        try:
            buf = BytesIO()
            f = await bot.get_file(m.document.file_id)
            await bot.download(f, destination=buf)
            file_data = buf.getvalue()
            # Сохраняем username вместе с файлом
            LAST_XLSX_PER_CHAT[m.chat.id] = {
                "data": file_data,
                "timestamp": time.time(),
                "username": (m.from_user.username or "")
            }

            loop = asyncio.get_running_loop()
            df = await loop.run_in_executor(SHARED_EXECUTOR, pd.read_excel, BytesIO(file_data))

            # Определяем колонку с ником и ссылкой
            seller_col = None
            link_col = None
            lowered = {str(c).strip().lower(): c for c in df.columns}

            # Ник
            for cand in ("seller_nick", "имя продавца"):
                if cand in lowered:
                    seller_col = lowered[cand]
                    break
            if not seller_col:
                try:
                    df2, sc, _ = pick_columns_via_smtp25(df)
                    if sc:
                        seller_col = sc
                        df = df2
                except Exception:
                    pass

            # Ссылка
            if "ссылка на объявление" in lowered:
                link_col = lowered["ссылка на объявление"]
            else:
                for c in df.columns:
                    nm = str(c).strip().lower()
                    if "ссылка" in nm and "объяв" in nm:
                        link_col = c
                        break

            if not (seller_col and link_col and seller_col in df.columns and link_col in df.columns):
                await bot.send_message(m.chat.id, "Не найдены необходимые столбцы (seller_nick / Ссылка на объявление).")
                return

            ads_map = AD_ADS_BY_ID_PER_CHAT.setdefault(m.chat.id, {})
            local_map = AD_LOCAL2ID_PER_CHAT.setdefault(m.chat.id, {})

            import re as _re, unicodedata, hashlib as _hashlib
            def _norm_local(s: str) -> str:
                s = (s or "").replace("\u00A0", " ")
                s = unicodedata.normalize("NFKC", s)
                s = s.replace(".", " ").replace("_", " ").replace("-", " ")
                s = _re.sub(r"\s+", " ", s.strip().lower())
                return s

            bases: list[str] = []
            seen_bases: set[str] = set()

            seller_series = df[seller_col]
            link_series = df[link_col]

            rows_added = 0
            for raw_nick, raw_link in zip(seller_series, link_series):
                raw_nick = str(raw_nick).strip()
                raw_link = str(raw_link).strip()
                if not raw_nick or not raw_link:
                    continue

                norm_nick = _norm_local(raw_nick)
                ad_id = _hashlib.sha1(f"{raw_nick}||{raw_link}".encode("utf-8")).hexdigest()[:12]

                ad_entry = ads_map.get(ad_id)
                if not ad_entry:
                    ad_entry = {
                        "ad_id": ad_id,
                        "raw_nick": raw_nick,
                        "norm_nick": norm_nick,
                        "link": raw_link,
                        "variants": set()
                    }
                    ads_map[ad_id] = ad_entry
                    rows_added += 1

                parts = smtp25.extract_name_parts(raw_nick) or []
                variants_local: set[str] = set()

                if len(parts) == 2:
                    first, last = parts
                    try:
                        base_email = smtp25.generate_email(first, last)
                    except Exception:
                        base_email = ""
                    if base_email:
                        base_norm = _norm_local(base_email)
                        variants_local.add(base_norm)
                        if base_email not in seen_bases:
                            seen_bases.add(base_email)
                            bases.append(base_email)

                    f = first.lower()
                    l = last.lower()
                    for v in _gen_base_variants(f, l):
                        variants_local.add(_norm_local(v))

                variants_local.add(norm_nick)

                for v in variants_local:
                    if v not in local_map:
                        local_map[v] = ad_id
                    ad_entry["variants"].add(v)

            BASES_PER_CHAT[m.chat.id] = bases
            AD_CHAT_TS[m.chat.id] = time.time()

            if bases:
                for chunk in join_batches([code(b) for b in bases], 50):
                    await bot.send_message(m.chat.id, chunk)

            # Изменённая финальная строка
            await bot.send_message(
                m.chat.id,
                "Выполнено успешно✅",
                reply_markup=after_xlsx_kb()
            )

            # сохранить кэш на диск
            await save_ad_cache_async(m.chat.id)

            # NEW: автозапуск проверки/сендинга в фоне (если включено)
            try:
                internal_uid = await U(m)
                await ai_xlsx_autoflow_maybe_start(internal_uid, m.chat.id)
            except Exception:
                pass

        except Exception as e:
            try:
                await bot.send_message(m.chat.id, f"Ошибка обработки XLSX: {type(e).__name__}: {e}")
            except Exception:
                pass
            log_send_event(f"XLSX PARSE ERROR chat={m.chat.id}: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        finally:
            try:
                await ui_clear_prompts(state)
            except Exception:
                pass
            try:
                if buf:
                    buf.close()
            except Exception:
                pass
            try:
                gc.collect()
            except Exception:
                pass
            try:
                await state.clear()
            except Exception:
                pass

@dp.message(CheckNicksFSM.file)
async def ignore_non_xlsx(m: types.Message):
    pass
    
def verify_emails_from_df_for_user_sync_with_ctx(ctx, df, chat_id: int | str | None, username: str | None):
    """Синхронная реализация верификации email с контекстом (ctx прокидывается в smtp25.verify_emails_via_api).
       Дополнительно: фильтруем валидные email с локальными частями вида
       privater.anbieter, private.person, private.anbieter, privater.nutzer.
       Плюс: прокидываем chat_id и username в тело запроса чекеру.
    """
    df2, _, _ = pick_columns_via_smtp25(df)
    keep = [c for c in ("seller_nick", "title") if c in df2.columns]
    if not keep:
        return []
    df2 = df2[keep].copy()

    rows = []
    for row in df2.itertuples(index=False):
        nick = str(getattr(row, "seller_nick", "")).strip()
        if not nick:
            continue
        parts = smtp25.extract_name_parts(nick)
        if not parts or len(parts) != 2:
            continue
        first, last = parts
        if not first or not last:
            continue
        email_base = smtp25.generate_email(first, last)
        rows.append({"base": email_base, "title": getattr(row, "title", "")})

    priority = getattr(ctx, "domains", []) or []
    bases = [r["base"] for r in rows]

    # Передаем chat_id и username в тело запроса к API
    emails_dict = smtp25.verify_emails_via_api(bases, priority, chat_id=chat_id, username=username)

    bad_locals = {"privater.anbieter", "private.person", "private.anbieter", "privater.nutzer"}

    def _is_bad_email(email: str) -> bool:
        try:
            local = email.split("@", 1)[0].lower()
        except Exception:
            return False
        return any(b in local for b in bad_locals)

    results = []
    for row in rows:
        base = row["base"]
        email = emails_dict.get(base)
        if email and not _is_bad_email(email):
            results.append({
                "email": email,
                "seller_name": base,
                "title": row["title"]
            })
    return results
    
async def _verify_emails_from_cache_once(uid: int, chat_id: int) -> list[dict]:
    """
    РОВНО как ручная кнопка, только без CallbackQuery.
    Всё берём из кэша LAST_XLSX_PER_CHAT и сохраняем в VERIFIED_ROWS_PER_CHAT.
    Печать писем и «Выполнено успешно ✅» — 1:1 как в ручном пути.
    """
    xls_entry = LAST_XLSX_PER_CHAT.get(chat_id)
    if not xls_entry:
        return []
    xls_bytes = xls_entry.get("data") if isinstance(xls_entry, dict) else xls_entry
    if not xls_bytes:
        return []
    username = (xls_entry.get("username") if isinstance(xls_entry, dict) else None) or ""

    status_msg = await bot.send_message(chat_id, "Проверка email выполняется…")
    try:
        # Контекст пользователя
        ctx = await get_user_ctx_async(uid)

        # Чтение Excel — в пуле, как в ручном хендлере
        loop = asyncio.get_running_loop()
        df = await loop.run_in_executor(SHARED_EXECUTOR, pd.read_excel, BytesIO(xls_bytes))

        # Передаем chat_id и username в тело запроса к API
        results = await loop.run_in_executor(
            SHARED_EXECUTOR,
            verify_emails_from_df_for_user_sync_with_ctx,
            ctx, df, chat_id, username
        )

        VERIFIED_ROWS_PER_CHAT[chat_id] = results or []

        # Ничего не нашли — тот же текст-подсказка
        if not results:
            await delete_message_safe(status_msg)
            hint = (
                "Не найдено ни одного валидного email.\n"
                "Проверьте:\n"
                "• колонку с никами (seller_nick/«Имя продавца»)\n"
                "• список доменов"
            )
            await bot.send_message(chat_id, hint)
            return []

        # Печатаем список, как кнопка
        emails = [r["email"] for r in results]
        for chunk in join_batches([f"№{i} {code(e)}" for i, e in enumerate(emails, start=1)], 50):
            await bot.send_message(chat_id, chunk)

        await delete_message_safe(status_msg)
        await bot.send_message(chat_id, "Выполнено успешно ✅", reply_markup=after_verify_kb())
        return results

    except Exception as e:
        await delete_message_safe(status_msg)
        try:
            await bot.send_message(chat_id, f"Ошибка проверки email: {escape_html(str(e))}")
        except Exception:
            pass
        return []
    

    
# === ИИ автопоток XLSX -> verify -> send ===






async def start_sending_programmatically(uid: int, chat_id: int) -> None:
    """
    Программный аналог send_start_cb: валидирует условия и запускает send_loop.
    Точно такие же проверки, как в кнопочном пути, чтобы поведение совпадало.
    """
    tg = chat_id  # чисто для логов, в send_start_cb использовался c.from_user.id
    log_send_event(f"SEND_START (auto) request uid={uid} tg={tg}")

    # Должны быть результаты верификации
    if chat_id not in VERIFIED_ROWS_PER_CHAT or not VERIFIED_ROWS_PER_CHAT[chat_id]:
        await bot.send_message(chat_id, "Автосендинг: нет результатов проверки email.")
        log_send_event(f"SEND_START (auto) BLOCKED uid={uid} tg={tg} reason=no_verified")
        return

    # Smart пресеты обязательны
    try:
        smart_items = await list_smart_presets_async(uid)
    except Exception as e:
        log_send_event(f"SEND_START (auto) ERROR uid={uid} tg={tg} failed to load smart_presets: {e}")
        await bot.send_message(chat_id, "Внутренняя ошибка при проверке пресетов.")
        return
    if not smart_items:
        await bot.send_message(chat_id, "Ошибка: добавьте умные пресеты ❗️")
        log_send_event(f"SEND_START (auto) BLOCKED uid={uid} tg={tg} reason=no_smart_presets")
        return

    # Контекст и активные аккаунты
    ctx = await get_user_ctx_async(uid)
    if not getattr(ctx, "accounts", None):
        await bot.send_message(chat_id, "Нет аккаунтов, включённых для рассылки. Используйте /sendacc.")
        log_send_event(f"SEND_START (auto) BLOCKED uid={uid} tg={tg} reason=no_active_accounts")
        return

    missing = []
    if not getattr(ctx, "templates", None):
        missing.append("шаблоны")
    if not getattr(ctx, "subjects", None):
        missing.append("темы")
    if missing:
        await bot.send_message(chat_id, f"Ошибка: добавьте {', '.join(missing)}!")
        log_send_event(f"SEND_START (auto) BLOCKED uid={uid} tg={tg} missing={missing}")
        return

    # Валидность send‑прокси (жёсткая проверка)
    proxies_rows = await list_proxies_async(uid, "send")
    if not proxies_rows:
        await bot.send_message(chat_id, "Ошибка: добавьте send‑прокси!")
        log_send_event(f"SEND_START (auto) BLOCKED uid={uid} tg={tg} reason=no_send_proxies")
        return

    target_host, target_port = _probe_target_for_kind("send")  # SMTP 587
    tests = [
        _test_proxy_async(p.host, p.port, p.user_login or "", p.password or "", target_host, target_port, timeout=5)
        for p in proxies_rows
    ]
    results = await asyncio.gather(*tests, return_exceptions=False)
    bad_ordinals = [i for i, (ok, _err) in enumerate(results, start=1) if not ok]
    if bad_ordinals:
        nums = _fmt_bad_ordinals(bad_ordinals)
        await bot.send_message(chat_id, f"Проверьте невалидные прокси {nums}")
        log_send_event(f"SEND_START (auto) BLOCKED uid={uid} tg={tg} invalid_proxy_ordinals={bad_ordinals}")
        return

    # Не запускать второй раз
    t = SEND_TASKS.get(uid)
    if t and not t.done():
        await bot.send_message(chat_id, "Сендинг уже запущен.")
        log_send_event(f"SEND_START (auto) BLOCKED uid={uid} tg={tg} reason=already_running")
        return

    total = len(VERIFIED_ROWS_PER_CHAT[chat_id])
    SEND_STATUS[uid] = {
        "running": True,
        "sent": 0,
        "failed": 0,
        "total": total,
        "cancel": False
    }
    SEND_TASKS[uid] = asyncio.create_task(send_loop(uid, chat_id))
    try:
        await bot.send_message(chat_id, "Сендинг запущен 🚀")
    except Exception:
        pass
    log_send_event(f"SEND_START (auto) STARTED uid={uid} tg={tg} total={total}")


@dp.callback_query(F.data == "check:verify_emails")
async def verify_emails_btn(c: types.CallbackQuery, state: FSMContext):
    """
    Асинхронный хендлер верификации: теперь получаем ctx заранее (в event loop),
    читаем Excel и вызываем синхронную верификацию в SHARED_EXECUTOR с передачей ctx + chat_id/username.
    """
    start = time.perf_counter()
    if not await ensure_approved(c):
        await safe_cq_answer(c)
        return
    chat_id = c.message.chat.id
    xls_entry = LAST_XLSX_PER_CHAT.get(chat_id)
    if not xls_entry:
        try:
            await c.answer("Сначала загрузите XLSX через «Проверка ников».", show_alert=True)
        except Exception:
            # Игнорируем ошибки устаревших callback queries
            pass
        return

    # xls_entry может быть либо raw bytes (старый код), либо dict {"data": bytes, "timestamp": ...}
    if isinstance(xls_entry, dict):
        xls_bytes = xls_entry.get("data")
    else:
        xls_bytes = xls_entry

    if not xls_bytes:
        try:
            await c.answer("Файл повреждён или отсутствует.", show_alert=True)
        except Exception:
            # Игнорируем ошибки устаревших callback queries
            pass
        return

    status_msg = await bot.send_message(chat_id, "Проверка email выполняется…")
    try:
        # Получаем ctx
        ctx = await get_user_ctx_async(await U(c))

        loop = asyncio.get_running_loop()
        # Читаем df
        df = await loop.run_in_executor(SHARED_EXECUTOR, pd.read_excel, BytesIO(xls_bytes))

        # Передаем chat_id и username в тело запроса к API
        results = await loop.run_in_executor(
            SHARED_EXECUTOR,
            verify_emails_from_df_for_user_sync_with_ctx,
            ctx, df, chat_id, c.from_user.username
        )
        VERIFIED_ROWS_PER_CHAT[chat_id] = results or []

        if not results:
            await delete_message_safe(status_msg)
            hint = (
                "Не найдено ни одного валидного email.\n"
                "Проверьте:\n"
                "• колонку с никами (seller_nick/«Имя продавца»)\n"
                "• список доменов"
            )
            await bot.send_message(chat_id, hint)
            return

        emails = [r["email"] for r in results]
        for chunk in join_batches([f"№{i} {code(e)}" for i, e in enumerate(emails, start=1)], 50):
            await bot.send_message(chat_id, chunk)

        await delete_message_safe(status_msg)
        await bot.send_message(chat_id, "Выполнено успешно ✅", reply_markup=after_verify_kb())
    except Exception as e:
        await delete_message_safe(status_msg)
        await bot.send_message(chat_id, f"Ошибка проверки email: {escape_html(str(e))}")
        elapsed = time.perf_counter() - start
        log_send_event(f"EMAIL VERIFY ERROR for chat={chat_id}, error={type(e).__name__}: {e}")
    finally:
        try:
            await ui_clear_prompts(state)
        except Exception:
            pass

# ====== SEND (batch) ======
async def _quick_check_send_proxies(uid: int) -> str:
    """
    Быстрая проверка SEND‑прокси.
    ВАЖНО: не блокируем event loop — каждая проверка выполняется в executor через _test_proxy_async.
    """
    ctx = await get_user_ctx_async(uid)
    if not ctx.send_proxies:
        return "Нет send‑прокси."

    target_host, target_port = _probe_target_for_kind("send")  # SMTP 587

    tasks = []
    for p in ctx.send_proxies:
        host = p.get("host") or ""
        port = int(p.get("port") or 0)
        user = p.get("user") or ""
        pwd = p.get("password") or ""
        if not host or not port:
            continue
        tasks.append(_test_proxy_async(host, port, user, pwd, target_host, target_port, timeout=5))

    if not tasks:
        return "Нет валидных записей прокси для проверки."

    results = await asyncio.gather(*tasks, return_exceptions=False)

    bad: list[str] = []
    idx = 0
    for p in ctx.send_proxies:
        host = p.get("host") or ""
        port = int(p.get("port") or 0)
        user = p.get("user") or ""
        pwd = p.get("password") or ""
        if not host or not port:
            continue
        ok, err = results[idx]
        idx += 1
        if not ok:
            bad.append(f"{host}:{port} (ID={p.get('id','?')}) - {err}")

    if bad:
        return "Неработающие прокси:\n" + "\n".join(bad)
    return "✅ Все прокси валидны"

def _render_message(ctx: smtp25.UserContext, subject: str, template: str, email: str, title: str) -> Tuple[str, str, str]:
    """
    Использует валидный email для подстановки SELLER.
    SELLER = слово до точки в local part email (до @), с большой буквы.
    Если точки нет — SELLER не подставляется.
    """
    subj_in = subject or smtp25.get_random_subject_ctx(ctx)
    tmpl_in = template or smtp25.get_random_template_ctx(ctx)

    seller_for_template = smtp25.extract_seller_name_from_email(email) or ""

    def repl(txt: str) -> str:
        if seller_for_template:
            txt = txt.replace("{SELLER}", seller_for_template).replace("SELLER", seller_for_template)
        else:
            txt = txt.replace("{SELLER}", "").replace("SELLER", "")
        return (txt
                .replace("{ITEM}", title or "")
                .replace("{OFFER}", title or "")
                .replace("OFFER", title or ""))

    subject_out = repl(subj_in).strip()
    body_out = repl(tmpl_in)

    import re as _re
    tmpl_lines = (tmpl_in or "").splitlines()
    offer_first = False
    for ln in tmpl_lines:
        s = (ln or "").strip()
        if not s:
            continue
        if _re.search(r'\{?OFFER\}?', s, flags=_re.I):
            offer_first = True
        break

    body_for_log = body_out
    if offer_first:
        body_lines = (body_out or "").splitlines()
        idx = next((i for i, l in enumerate(body_lines) if (l or "").strip()), None)
        if idx is not None:
            body_for_log = "\n".join(body_lines[:idx] + body_lines[idx + 1:]).lstrip("\n")

    return subject_out, body_out, body_for_log

# ====== SYNCHRONOUS EMAIL SENDING ======
async def _send_email_sync(uid: int, to_email: str, subject: str, body: str,
                          html: bool = False, photo_bytes: bytes | None = None, photo_name: str | None = None) -> bool:
    """
    Асинхронная функция для отправки email (запускает sync-отправку в executor).
    Возвращает True/False.
    ВАЖНО: Использует round-robin для выбора аккаунта (как и другие функции отправки).
    """
    try:
        # Получаем контекст асинхронно
        ctx = await get_user_ctx_async(uid)
        accs = list(getattr(ctx, "accounts", []) or [])
        if not accs or not ctx.send_proxies:
            return False

        # Пер‑пользовательский round‑robin без глобальных переменных
        if not hasattr(_send_email_sync, "_rr_idx"):
            _send_email_sync._rr_idx = {}  # type: ignore[attr-defined]
        rr = _send_email_sync._rr_idx  # type: ignore[attr-defined]
        last = int(rr.get(uid, -1))
        idx = (last + 1) % len(accs)
        rr[uid] = idx
        acc = accs[idx]

        # Используем синхронную версию отправки в отдельном потоке
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(
            SHARED_EXECUTOR,
            smtp25.send_email_with_proxy_fallback_sync,
            ctx, acc, to_email, subject, body, 2, html, photo_bytes, photo_name, None
        )
        return bool(res)
    except Exception as e:
        log_send_event(f"SEND SYNC ERROR for uid={uid}: {e}")
        return False

@time_it
async def _send_one(uid: int, to_email: str, subject: str, body: str, html: bool = False,
                    photo_bytes: bytes | None = None, photo_name: str | None = None) -> bool:
    """
    Асинхронная отправка email через thread pool.
    Аккаунт выбираем равномерно (round‑robin) по списку ctx.accounts.
    """
    async with SMTP_SEMAPHORE:
        try:
            ctx = await get_user_ctx_async(uid)
            accs = list(getattr(ctx, "accounts", []) or [])
            if not accs:
                log_send_event(f"SEND ERROR: no accounts for uid={uid}")
                return False

            # Пер‑пользовательский round‑robin без глобальных переменных
            if not hasattr(_send_one, "_rr_idx"):
                _send_one._rr_idx = {}  # type: ignore[attr-defined]
            rr = _send_one._rr_idx  # type: ignore[attr-defined]
            last = int(rr.get(uid, -1))
            idx = (last + 1) % len(accs)
            rr[uid] = idx
            acc = accs[idx]

            loop = asyncio.get_running_loop()
            ok = await loop.run_in_executor(
                SHARED_EXECUTOR,
                smtp25.send_email_with_proxy_fallback_sync,
                ctx, acc, to_email, subject, body, 2, html, photo_bytes, photo_name, None
            )
            return bool(ok)
        except Exception as e:
            log_send_event(f"SEND ERROR for uid={uid}, to={to_email}: {type(e).__name__}: {e}\n{traceback.format_exc()}")
            return False
            
async def _send_one_detailed(
    uid: int,
    to_email: str,
    subject: str,
    body: str,
    html: bool = False,
    photo_bytes: bytes | None = None,
    photo_name: str | None = None
) -> tuple[bool, int | None, str | None, int | None]:
    """
    Отправка одной записи с деталями:
      - ok: True/False
      - used_acc_id: id аккаунта, который выбрали по round-robin
      - used_acc_email: email выбранного аккаунта
      - proxy_no: № прокси (1-based) из round-robin по send‑прокси на момент попытки

    ВАЖНО: номер прокси здесь «плановый» (по нашему round-robin). Если smtp25 внутри
    выполнит собственный fallback, реальный прокси может отличаться. Для точного
    номера нужно возвращать метаданные из smtp25.
    """
    async with SMTP_SEMAPHORE:
        try:
            ctx = await get_user_ctx_async(uid)
            accs = list(getattr(ctx, "accounts", []) or [])
            if not accs:
                log_send_event(f"SEND ERROR: no accounts for uid={uid}")
                return False, None, None, None

            # Round-robin по аккаунтам (пер‑пользовательский)
            if not hasattr(_send_one_detailed, "_rr_idx"):
                _send_one_detailed._rr_idx = {}  # type: ignore[attr-defined]
            rr_acc = _send_one_detailed._rr_idx  # type: ignore[attr-defined]
            last_acc = int(rr_acc.get(uid, -1))
            acc_idx = (last_acc + 1) % len(accs)
            rr_acc[uid] = acc_idx

            acc = accs[acc_idx]
            used_acc_id = int(acc.get("id"))
            used_acc_email = str(acc.get("email") or "")

            # Round-robin по SEND‑прокси (только для лога номера)
            proxies = list(getattr(ctx, "send_proxies", []) or [])
            proxy_no: int | None = None
            if proxies:
                if not hasattr(_send_one_detailed, "_proxy_rr_idx"):
                    _send_one_detailed._proxy_rr_idx = {}  # type: ignore[attr-defined]
                rr_prx = _send_one_detailed._proxy_rr_idx  # type: ignore[attr-defined]
                last_px = int(rr_prx.get(uid, -1))
                px_idx = (last_px + 1) % len(proxies)
                rr_prx[uid] = px_idx
                proxy_no = px_idx + 1  # человекочитаемый №

            # Синхронная отправка в пуле (как в _send_one)
            loop = asyncio.get_running_loop()
            ok = await loop.run_in_executor(
                SHARED_EXECUTOR,
                smtp25.send_email_with_proxy_fallback_sync,
                ctx, acc, to_email, subject, body, 2, html, photo_bytes, photo_name, None
            )
            return bool(ok), used_acc_id, used_acc_email, proxy_no

        except Exception as e:
            log_send_event(f"SEND ERROR DETAILED uid={uid} to={to_email}: {type(e).__name__}: {e}\n{traceback.format_exc()}")
            return False, None, None, None

async def log_send_ok(chat_id: int, subject: str, body: str, to_email: str, reply_to_message_id: Optional[int] = None):
    import re as _re

    def safe_code(txt: str) -> str:
        txt = txt or ""
        return f"<code>{txt.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')}</code>"

    def _clean(s: str) -> str:
        return _re.sub(r'^(re|fw|fwd)\s*:\s*', '', (s or '').strip(), flags=_re.I)

    subj_clean = _clean(subject or "")
    body_lines = (body or "").splitlines()
    body_for_log = ""
    if body_lines:
        first_clean = _clean(body_lines[0])
        if first_clean.lower() == subj_clean.lower():
            body_for_log = "\n".join(body_lines[1:]).lstrip()
        else:
            body_for_log = body or ""

    text = f"Сообщение {safe_code(subject or '')}"
    if body_for_log:
        text += "\n" + safe_code(body_for_log)
    # ЗДЕСЬ ЗАМЕНА ИКОНКИ НА 🎉
    text += f"\nуспешно отправлено пользователю {safe_code(to_email)} 🎉"

    if reply_to_message_id:
        try:
            await bot.send_message(chat_id, text, reply_to_message_id=reply_to_message_id)
            return
        except Exception:
            pass
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        # Игнорируем сетевые ошибки при отправке логов - не критично
        pass

async def send_loop(uid: int, chat_id: int):
    start = time.perf_counter()
    SEND_STATUS[uid] = {"running": True, "sent": 0, "failed": 0, "total": 0, "cancel": False, "last_err": None}
    results = VERIFIED_ROWS_PER_CHAT.get(chat_id, [])
    SEND_STATUS[uid]["total"] = len(results)
    
    # Контекст заранее (для списка аккаунтов и прокси)
    ctx = await get_user_ctx_async(uid)
    
    # Быстрый отчёт по прокси (оставим как предварительный инфо-лог)
    proxy_report = await _quick_check_send_proxies(uid)
    try:
        await bot.send_message(chat_id, proxy_report)
    except Exception:
        # Игнорируем сетевые ошибки - не критично для работы сендинга
        pass
    
    vmin = int(await get_setting_async(uid, "send_delay_min", str(smtp25.MIN_SEND_DELAY)))
    vmax = int(await get_setting_async(uid, "send_delay_max", str(smtp25.MAX_SEND_DELAY)))

    # Кэшируем список аккаунтов из ctx (это список dict-ов)
    accs = list(getattr(ctx, "accounts", []) or [])

    for r in results:
        if SEND_STATUS[uid].get("cancel"):
            break

        to_email = r["email"]
        title = r.get("title", "")
        subject, body, body_for_log = _render_message(
            ctx,
            smtp25.get_random_subject_ctx(ctx),
            smtp25.get_random_template_ctx(ctx),
            to_email,
            title or ""
        )

        ok, used_acc_id, used_acc_email, proxy_no = await _send_one_detailed(
            uid, to_email, subject, body
        )

        if ok:
            SEND_STATUS[uid]["sent"] += 1
            try:
                await log_send_ok(chat_id, subject, body_for_log, to_email)
            except Exception:
                # Игнорируем ошибки при отправке логов - не критично для работы сендинга
                pass
        else:
            # 1) Логируем подробность: какой аккаунт и «плановый» № прокси
            try:
                prx_text = f"№ {proxy_no}" if proxy_no else "не определён"
                await bot.send_message(
                    chat_id,
                    f"Не удалось отправить пользователю {code(to_email)}.\n"
                    f"Проверьте аккаунт {code(used_acc_email or '—')} + прокси {code(prx_text)}"
                )
            except Exception:
                pass

            # 2) Повторная попытка через следующий аккаунт (один раз), не останавливая весь сендинг
            retried_ok = False
            try:
                if accs and used_acc_id is not None:
                    try:
                        cur_idx = next((i for i, a in enumerate(accs) if int(a.get("id")) == int(used_acc_id)), None)
                    except Exception:
                        cur_idx = None
                    if isinstance(cur_idx, int):
                        next_idx = (cur_idx + 1) % len(accs)
                        next_acc_id = int(accs[next_idx].get("id"))
                        next_acc_email = str(accs[next_idx].get("email") or "")
                        msgid = await send_email_via_account(uid, next_acc_id, to_email, subject, body, html=False, priority=True)
                        retried_ok = bool(msgid)
                        if retried_ok:
                            SEND_STATUS[uid]["sent"] += 1
                            try:
                                await log_send_ok(chat_id, subject, body_for_log, to_email)
                            except Exception:
                                # Игнорируем ошибки при отправке логов - не критично для работы сендинга
                                pass
                        else:
                            # Второй лог — на случай повторной неудачи
                            try:
                                # При желании можно вывести и «следующий» номер прокси
                                prx2 = None
                                prx_list = list(getattr(ctx, "send_proxies", []) or [])
                                if prx_list:
                                    # приблизительно следующий номер по кругу
                                    base = (proxy_no or 0)
                                    prx2 = (base % len(prx_list)) + 1
                                prx2_text = f"№ {prx2}" if prx2 else "не определён"
                                await bot.send_message(
                                    chat_id,
                                    f"Повтор не удался для {code(to_email)}.\n"
                                    f"Проверьте аккаунт {code(next_acc_email)} + прокси {code(prx2_text)}"
                                )
                            except Exception:
                                pass
            except Exception as e:
                log_send_event(f"SEND LOOP retry error uid={uid} tg={chat_id} to={to_email}: {e}")

            if not retried_ok:
                SEND_STATUS[uid]["failed"] += 1

        await asyncio.sleep(random.uniform(vmin, vmax))
    
    SEND_STATUS[uid]["running"] = False
    elapsed = time.perf_counter() - start
    log_send_event(
        f"SEND_LOOP done uid={uid} tg={chat_id}, total={SEND_STATUS[uid]['total']}, "
        f"sent={SEND_STATUS[uid]['sent']}, failed={SEND_STATUS[uid]['failed']}, time={elapsed:.3f}s"
    )
    await bot.send_message(chat_id, "Сендинг остановлен ⏹" if SEND_STATUS[uid].get("cancel") else "Сендинг завершён ✅")

    
async def log_html_reply_ok(chat_id: int, to_email: str, html_str: str, reply_to_message_id: int) -> Optional[int]:
    """
    Лог ответа с HTML‑вложением. Возвращает message_id созданного сообщения или None.
    """
    # ЗАМЕНА ИКОНКИ НА 🎉
    caption = f"Ответ с HTML‑вложением успешно отправлен пользователю {code(to_email)} 🎉"
    try:
        reply_params = types.ReplyParameters(message_id=reply_to_message_id) if reply_to_message_id else None
        msg = await bot.send_document(
            chat_id=chat_id,
            document=_make_html_file(html_str),
            caption=caption,
            reply_parameters=reply_params
        )
        return getattr(msg, "message_id", None)
    except Exception:
        try:
            msg = await bot.send_document(
                chat_id=chat_id,
                document=_make_html_file(html_str),
                caption=caption
            )
            return getattr(msg, "message_id", None)
        except Exception:
            try:
                msg2 = await bot.send_message(chat_id, caption, reply_to_message_id=reply_to_message_id or None)
                return getattr(msg2, "message_id", None)
            except Exception:
                return None

async def log_text_reply_ok(chat_id: int, body: str, to_email: str, reply_to_message_id: int) -> Optional[int]:
    """
    Лог текстового ответа. Возвращает message_id созданного сообщения или None.
    """
    # ЗАМЕНА ИКОНКИ НА 🎉
    text = code(body or "") + f"\nуспешно отправлено пользователю {code(to_email)} 🎉"
    try:
        msg = await bot.send_message(chat_id, text, reply_to_message_id=reply_to_message_id or None)
        return getattr(msg, "message_id", None)
    except Exception:
        try:
            msg2 = await bot.send_message(chat_id, text)
            return getattr(msg2, "message_id", None)
        except Exception:
            return None

@dp.callback_query(F.data == "send:start")
async def send_start_cb(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return

    uid = await U(c)
    tg = c.from_user.id
    chat_id = c.message.chat.id
    log_send_event(f"SEND_START request uid={uid} tg={tg}")

    # Нужно сначала пройти верификацию email (список VERIFIED_ROWS_PER_CHAT)
    if chat_id not in VERIFIED_ROWS_PER_CHAT or not VERIFIED_ROWS_PER_CHAT[chat_id]:
        await c.answer("Сначала выполните проверку email.", show_alert=True)
        return

    # Проверяем наличие smart пресетов (строго)
    try:
        smart_items = await list_smart_presets_async(uid)
    except Exception as e:
        log_send_event(f"SEND_START ERROR uid={uid} tg={tg} failed to load smart_presets: {e}")
        try:
            await c.answer("Внутренняя ошибка при проверке пресетов.", show_alert=True)
        except Exception:
            await bot.send_message(chat_id, "Внутренняя ошибка при проверке пресетов.")
        return

    if not smart_items:
        msg = "Ошибка: добавьте умные пресеты ❗️"
        try:
            await c.answer(msg, show_alert=True)
        except Exception:
            await bot.send_message(chat_id, msg)
        log_send_event(f"SEND_START BLOCKED uid={uid} tg={tg} reason=no_smart_presets")
        return

    # Контекст (фильтрует отключённые аккаунты)
    ctx = await get_user_ctx_async(uid)

    if not getattr(ctx, "accounts", None):
        await c.answer("Нет аккаунтов, включённых для рассылки. Используйте /sendacc.", show_alert=True)
        log_send_event(f"SEND_START BLOCKED uid={uid} tg={tg} reason=no_active_accounts")
        return

    missing = []
    if not getattr(ctx, "templates", None):
        missing.append("шаблоны")
    if not getattr(ctx, "subjects", None):
        missing.append("темы")
    if missing:
        msg = f"Ошибка: добавьте {', '.join(missing)}!"
        try:
            await c.answer(msg, show_alert=True)
        except Exception:
            await bot.send_message(chat_id, msg)
        log_send_event(f"SEND_START BLOCKED uid={uid} tg={tg} missing={missing}")
        return

    # ЖЁСТКАЯ проверка send‑прокси: все должны быть валидны
    proxies_rows = await list_proxies_async(uid, "send")
    if not proxies_rows:
        try:
            await c.answer("Ошибка: добавьте send‑прокси!", show_alert=True)
        except Exception:
            await bot.send_message(chat_id, "Ошибка: добавьте send‑прокси!")
        log_send_event(f"SEND_START BLOCKED uid={uid} tg={tg} reason=no_send_proxies")
        return

    target_host, target_port = _probe_target_for_kind("send")  # SMTP 587
    tests = [
        _test_proxy_async(p.host, p.port, p.user_login or "", p.password or "", target_host, target_port, timeout=5)
        for p in proxies_rows
    ]
    results = await asyncio.gather(*tests, return_exceptions=False)
    bad_ordinals = [i for i, (ok, _err) in enumerate(results, start=1) if not ok]

    if bad_ordinals:
        nums = _fmt_bad_ordinals(bad_ordinals)
        msg = f"Проверьте невалидные прокси {nums}"
        try:
            await c.answer(msg if len(msg) <= 180 else "Есть невалидные прокси — подробности в чате.", show_alert=True)
        except Exception:
            pass
        try:
            await bot.send_message(chat_id, msg)
        except Exception:
            pass
        log_send_event(f"SEND_START BLOCKED uid={uid} tg={tg} invalid_proxy_ordinals={bad_ordinals}")
        return

    # Не допускаем повторный старт
    if uid in SEND_TASKS and not SEND_TASKS[uid].done():
        await c.answer("Сендинг уже запущен.", show_alert=True)
        log_send_event(f"SEND_START BLOCKED uid={uid} tg={tg} reason=already_running")
        return

    total = len(VERIFIED_ROWS_PER_CHAT[chat_id])
    SEND_STATUS[uid] = {
        "running": True,
        "sent": 0,
        "failed": 0,
        "total": total,
        "cancel": False
    }
    SEND_TASKS[uid] = asyncio.create_task(send_loop(uid, chat_id))
    try:
        await c.message.answer("Сендинг запущен 🚀")
    except Exception:
        try:
            await bot.send_message(chat_id, "Сендинг запущен 🚀")
        except Exception:
            pass
    await safe_cq_answer(c)
    log_send_event(f"SEND_START STARTED uid={uid} tg={tg} total={total}")

@dp.callback_query(F.data == "send:status")
async def send_status_cb(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    uid = await U(c)
    st = SEND_STATUS.get(uid)
    if not st:
        await c.answer("Сендинг не запускался.", show_alert=True); return
    await c.message.answer(
        "Статус: " + ("идёт" if st.get("running") else "остановлен") + "\n"
        f"Отправлено: {st.get('sent',0)}\n"
        f"Не отправлено: {st.get('failed',0)}\n"
        f"Всего: {st.get('total',0)}"
    )
    await safe_cq_answer(c)

@dp.callback_query(F.data == "send:stop")
async def send_stop_cb(c: types.CallbackQuery):
    if not await ensure_approved(c): return
    uid = await U(c)
    t = SEND_TASKS.get(uid)
    if t and not t.done():
        SEND_STATUS[uid]["cancel"] = True
        await c.answer("Останавливаю…")
    else:
        await c.answer("Сендинг не запущен.", show_alert=True)

# ====== ONE‑OFF SEND ======
def onesend_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="onesend:cancel")]])

@dp.message(F.text.regexp(r"(?i)отправить\s*e-?mail"))
async def onesend_entry_btn(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    await state.set_state(SingleSendFSM.to)
    await bot.send_message(m.chat.id, "Введите email получателя✍️", reply_markup=onesend_kb())

@dp.message(F.text == "✉️ Отправить email")
async def onesend_entry_exact(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    await state.set_state(SingleSendFSM.to)
    await bot.send_message(m.chat.id, "Введите email получателя✍️", reply_markup=onesend_kb())

@dp.message(Command("send"))
async def cmd_send(m: types.Message, state: FSMContext):
    await onesend_entry_btn(m, state)

@dp.message(SingleSendFSM.to)
async def onesend_got_to(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    to = (m.text or "").strip()
    await delete_message_safe(m)
    if "@" not in to:
        await bot.send_message(m.chat.id, "Некорректный email.")
        await state.clear()
        return
    await state.update_data(to=to)
    await state.set_state(SingleSendFSM.body)
    await bot.send_message(m.chat.id, "Введите текст письма✍️", reply_markup=onesend_kb())

@dp.message(SingleSendFSM.body)
async def onesend_got_text(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    data = await state.get_data()
    await delete_message_safe(m)
    to = (data or {}).get("to")
    if not to:
        await bot.send_message(m.chat.id, "Адрес получателя потерян. Повторите ввод.", reply_markup=onesend_kb())
        await state.clear()
        return

    internal_uid = await U(m)

    # Subject
    try:
        ctx = await get_user_ctx_async(internal_uid)
        subject = smtp25.get_random_subject_ctx(ctx) if ctx else None
    except Exception:
        subject = None
    subject = subject or "Вопрос по товару"

    # Body
    user_text = (m.text or "").strip()
    try:
        template_choice = smtp25.get_random_template_ctx(ctx) if ctx else ""
    except Exception:
        template_choice = ""
    body = user_text or template_choice or (m.text or "")

    # Выбор аккаунта (per-user RR) и постановка в Outbox
    acc_id = await _pick_rr_account_id(internal_uid)
    if not acc_id:
        await bot.send_message(m.chat.id, "Нет email аккаунтов для отправки. Добавьте аккаунт в Настройках.")
        await state.clear()
        return

    try:
        await outbox_enqueue(
            internal_uid, m.chat.id, acc_id, to, subject, body,
            html=False,
            src_tg_mid=None  # нет reply-треда
        )
    except Exception as e:
        await bot.send_message(m.chat.id, f"Ошибка постановки отправки: {escape_html(str(e))}")
        await state.clear()
        return

    # Никаких «поставлено в очередь». Логи придут только после УСПЕШНОЙ отправки.
    await state.clear()

@dp.callback_query(F.data == "onesend:cancel")
async def onesend_cancel(c: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_message(c.message, "Отменено."); await safe_cq_answer(c)
    
@dp.callback_query(F.data == "quickadd:cancel")
async def quickadd_cancel_cb(c: types.CallbackQuery, state: FSMContext):
    try:
        await state.clear()
    except Exception:
        pass
    try:
        await ui_clear_prompts(state)
    except Exception:
        pass
    try:
        await delete_message_safe(c.message)
    except Exception:
        pass
    try:
        await c.answer("Отменено")
    except Exception:
        pass
    try:
        await bot.send_message(c.message.chat.id, "Отменено.", reply_markup=reply_main_kb(admin=is_admin(c.from_user.id)))
    except Exception:
        pass 
        
class _NoopAsyncCM:
    async def __aenter__(self):
        return None
    async def __aexit__(self, exc_type, exc, tb):
        return False

# ====== Reply на входящее ======
async def send_email_via_account(
    uid: int,
    acc_id: int,
    to_email: str,
    subject: str,
    body: str,
    html: bool = False,
    photo_bytes: bytes | None = None,
    photo_name: str | None = None,
    sender_name_override: Optional[str] = None,
    max_attempts: int = 3,
    priority: bool = False,
    tg_id: Optional[int] = None,  # для логов (Telegram chat/user id)
) -> Optional[str]:
    """
    Attempt to send via a specific account; returns Message-ID on success else None.

    Обновления:
      - Нормализует uid в internal id (во избежание int32 переполнения).
      - Логирует tg_id (если передан) в строках SEND_EMAIL_VIA_ACCOUNT.
      - priority=True: обходит SMTP_SEMAPHORE и делает одну попытку.
      - IPv4 fallback: синхронная отправка в пуле выполняется с форсированием A‑записей,
        чтобы обойти ошибку PySocks doesn't support IPv6.
      - TTL на обновление send‑прокси (>=60с) и PTR (>=10мин), чтобы не дёргать БД/сеть каждый раз.
    """
    # Вспомогательные парсеры результата
    def _normalize_msgid(maybe_id: Any) -> Optional[str]:
        try:
            if maybe_id is None:
                return None
            if isinstance(maybe_id, bytes):
                maybe_id = maybe_id.decode("utf-8", "ignore").strip()
            s = str(maybe_id).strip()
            if not s:
                return None
            if s.startswith("<") and s.endswith(">"):
                return s
            if "@" in s:
                return f"<{s.strip('<>')}>"
            return None
        except Exception:
            return None

    def _parse_send_result(res: Any) -> tuple[bool, Optional[str]]:
        """
        Поддерживаем разные варианты:
          - dict: {"msgid": "..."} или {"message_id": "..."} или {"success": bool}
          - (ok, msgid)
          - строка msgid
          - bool
          - None
        """
        if isinstance(res, dict):
            msgid = res.get("msgid") or res.get("message_id") or res.get("Message-Id") or res.get("Message-ID")
            if msgid:
                mid = _normalize_msgid(msgid)
                return (mid is not None), mid
            if "success" in res:
                return bool(res.get("success")), None
            return False, None
        if isinstance(res, (tuple, list)) and res:
            ok = bool(res[0])
            mid = _normalize_msgid(res[1]) if len(res) > 1 else None
            return ok or (mid is not None), mid
        if isinstance(res, (str, bytes)):
            mid = _normalize_msgid(res)
            return (mid is not None), mid
        if isinstance(res, bool):
            return res, None
        return False, None

    raw_uid = uid
    uid = await normalize_internal_user_id(uid)
    if uid != raw_uid:
        log_send_event(f"send_email_via_account: normalized tg_id={raw_uid} -> internal_id={uid} tg={tg_id if tg_id is not None else '-'}")

    last_err_msg: str = ""

    # Контекст пользователя
    try:
        ctx = await get_user_ctx_async(uid)
    except Exception as e:
        last_err_msg = f"Context error: {type(e).__name__}: {e}"
        log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} cannot load ctx: {type(e).__name__}: {e}")
        SEND_LAST_ERROR[uid] = last_err_msg
        return None

    # Аккаунт
    try:
        acc = await get_account_async(uid, acc_id)
    except Exception as e:
        last_err_msg = f"Account load error: {type(e).__name__}: {e}"
        log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} cannot load account id={acc_id}: {type(e).__name__}: {e}")
        SEND_LAST_ERROR[uid] = last_err_msg
        return None

    if not acc:
        last_err_msg = "Account not found"
        # ВАЖНО: Дополнительное логирование для диагностики
        try:
            from db_async import list_accounts_async
            all_accounts = await list_accounts_async(uid)
            account_ids = [int(getattr(a, "id")) for a in all_accounts]
            log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} account id={acc_id} not found. Available account IDs: {account_ids[:10]}")
        except Exception as e_list:
            log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} account id={acc_id} not found. Error listing accounts: {e_list}")
        SEND_LAST_ERROR[uid] = last_err_msg
        return None

    acc_dict = {
        "email": getattr(acc, "email", "") or "",
        "password": getattr(acc, "password", "") or "",
        "name": getattr(acc, "display_name", "") or getattr(acc, "name", "") or ""
    }

    attempts = 1 if priority else max_attempts

    # Совместимость с smtp25.initialize_smtp_sync (если используется где‑то глубже)
    try:
        if not hasattr(smtp25, "initialize_smtp_sync") and hasattr(smtp25, "initialize_smtp_ctx"):
            smtp25.initialize_smtp_sync = smtp25.initialize_smtp_ctx  # type: ignore[attr-defined]
    except Exception:
        pass

    overall_attempt = 1
    while overall_attempt <= attempts:
        log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} attempt={overall_attempt}/{attempts} from={acc_dict['email']} to={to_email}")
        try:
            sem_cm = (_NoopAsyncCM() if priority else SMTP_SEMAPHORE)

            async with sem_cm:  # type: ignore[misc]
                # ====== ОБНОВЛЕНИЕ ПРОКСИ С TTL (>=60s) ======
                try:
                    now = time.time()
                    last = float(getattr(ctx, "_proxies_refreshed_ts", 0.0) or 0.0)
                    if (now - last) >= 60.0 or not getattr(ctx, "send_proxies", None):
                        p_rows = await list_proxies_async(uid, "send")
                        ctx.send_proxies = [
                            {
                                "id": getattr(p, "id", None),
                                "host": getattr(p, "host", None),
                                "port": getattr(p, "port", None),
                                "user": getattr(p, "user_login", None),
                                "password": getattr(p, "password", None),
                            }
                            for p in (p_rows or [])
                        ]
                        setattr(ctx, "_proxies_refreshed_ts", now)
                except Exception as e_loadp:
                    last_err_msg = f"Load proxies error: {e_loadp}"
                    log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} failed to refresh proxies from DB: {e_loadp}")

                # ====== PTR С TTL (>=10 мин) ======
                try:
                    now = time.time()
                    last_ptr = float(getattr(ctx, "_ptrs_refreshed_ts", 0.0) or 0.0)
                    if hasattr(smtp25, "ensure_ptrs_in_ctx_async") and (now - last_ptr) >= 600.0:
                        await smtp25.ensure_ptrs_in_ctx_async(ctx)
                        setattr(ctx, "_ptrs_refreshed_ts", now)
                except Exception as e_ptr:
                    last_err_msg = f"PTR resolve error: {e_ptr}"
                    log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} ensure_ptrs_in_ctx_async error: {e_ptr}")

                # ====== ОТПРАВКА (синхронная реализация в пуле, IPv4‑обёртка) ======
                msgid: Optional[str] = None
                try:
                    loop = asyncio.get_running_loop()
                    send_sync = None
                    if hasattr(smtp25, "send_email_with_proxy_fallback_sync"):
                        send_sync = smtp25.send_email_with_proxy_fallback_sync
                    elif hasattr(smtp25, "send_email_with_proxy_fallback"):
                        send_sync = smtp25.send_email_with_proxy_fallback

                    if send_sync:
                        res = await asyncio.wait_for(
                            loop.run_in_executor(
                                SHARED_EXECUTOR,
                                _call_send_sync_ipv4,
                                send_sync,
                                ctx, acc_dict, to_email, subject, body,
                                3, html, photo_bytes, photo_name, sender_name_override
                            ),
                            timeout=180.0  # Увеличено до 180 секунд для медленных прокси
                        )
                        success, maybe_msgid = _parse_send_result(res)
                        if not success and isinstance(res, bool) and res is True:
                            success = True
                            maybe_msgid = None
                        if not success and last_err_msg == "" and res is None:
                            # Детализируем причину: проверяем наличие прокси и другие факторы
                            proxy_count = len(ctx.send_proxies) if ctx.send_proxies else 0
                            if proxy_count == 0:
                                last_err_msg = "smtp25 send returned None: no send proxies available"
                            else:
                                last_err_msg = f"smtp25 send returned None: all {proxy_count} proxy attempts failed (check proxy validity, SMTP server, account auth)"

                        msgid = maybe_msgid if success else None
                        if success and not msgid:
                            # На случай если smtp25 вернул только bool
                            msgid = _make_msgid()
                            if not last_err_msg:
                                last_err_msg = "smtp25 success without Message-ID, synthesized"
                    else:
                        last_err_msg = "No send implementation available"
                        log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} no send implementation found in smtp25")
                        msgid = None

                except asyncio.TimeoutError:
                    last_err_msg = "Timeout while sending (180s)"
                    log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} attempt={overall_attempt} send TIMEOUT")
                    msgid = None
                except Exception as e_send:
                    last_err_msg = f"{type(e_send).__name__}: {e_send}"
                    log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} attempt={overall_attempt} send error: {type(e_send).__name__}: {e_send}")
                    msgid = None

                if msgid:
                    try:
                        SEND_LAST_ERROR.pop(uid, None)
                    except Exception:
                        pass
                    log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} SENT acc={acc_dict['email']} to={to_email} msgid={msgid}")
                    return msgid
                else:
                    log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} attempt={overall_attempt} failed; retrying if left")
        except Exception as e_outer:
            last_err_msg = f"Outer error: {type(e_outer).__name__}: {e_outer}"
            log_send_event(f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} outer exception: {type(e_outer).__name__}: {e_outer}")

        overall_attempt += 1
        if not priority:
            await asyncio.sleep(0.5)

    if not last_err_msg:
        last_err_msg = "Unknown error"
    SEND_LAST_ERROR[uid] = last_err_msg
    log_send_event(
        f"SEND_EMAIL_VIA_ACCOUNT | uid={uid} tg={tg_id if tg_id is not None else '-'} FAILED after {attempts} attempts acc={acc_dict['email']} to={to_email}; reason={last_err_msg}"
    )
    return None
    
async def send_via_smtp25_bool(ctx: smtp25.UserContext, acc_dict: dict, to_email: str, subject: str, body: str, max_attempts: int = 3, html: bool = False, photo_bytes: bytes | None = None, photo_name: str | None = None) -> bool:
    """
    Wrapper that attempts to use smtp25.send_email_with_proxy_fallback_async if available,
    otherwise falls back to running the sync implementation in a thread and returns bool.
    Обновлено: sync-fallback выполняется через IPv4-обёртку (_call_send_sync_ipv4).
    """
    try:
        # Ensure compatibility alias for initialize if needed
        try:
            if not hasattr(smtp25, "initialize_smtp_sync") and hasattr(smtp25, "initialize_smtp_ctx"):
                smtp25.initialize_smtp_sync = smtp25.initialize_smtp_ctx  # type: ignore[attr-defined]
        except Exception:
            pass

        # Ensure proxies in ctx don't carry DB ptr_name (они резолвятся внутри smtp25)
        ctx.send_proxies = ctx.send_proxies or []
        for p in ctx.send_proxies:
            if "ptr_name" in p:
                p.pop("ptr_name", None)

        # Предпочитаем async-реализацию
        if hasattr(smtp25, "send_email_with_proxy_fallback_async"):
            try:
                res = await asyncio.wait_for(
                    smtp25.send_email_with_proxy_fallback_async(ctx, acc_dict, to_email, subject, body, max_attempts, html, photo_bytes, photo_name),
                    timeout=180.0  # Увеличено до 180 секунд для медленных прокси
                )
                return bool(res)
            except asyncio.TimeoutError:
                log_send_event("send_via_smtp25_bool: async send timeout (180s)")
                return False
            except Exception as e:
                log_send_event(f"send_via_smtp25_bool: async send error: {e}")

        # Fallback: sync в executor + IPv4‑обёртка
        if hasattr(smtp25, "send_email_with_proxy_fallback_sync"):
            loop = asyncio.get_running_loop()
            try:
                res = await asyncio.wait_for(
                    loop.run_in_executor(
                        SHARED_EXECUTOR,
                        _call_send_sync_ipv4,
                        smtp25.send_email_with_proxy_fallback_sync,
                        ctx, acc_dict, to_email, subject, body,
                        max_attempts, html, photo_bytes, photo_name, None
                    ),
                    timeout=180.0  # Увеличено до 180 секунд для медленных прокси
                )
                return bool(res)
            except asyncio.TimeoutError:
                log_send_event("send_via_smtp25_bool: sync send timeout (180s)")
                return False
            except Exception as e:
                log_send_event(f"send_via_smtp25_bool: sync send error: {e}")
                return False

        log_send_event("send_via_smtp25_bool: no smtp25 send function available")
        return False

    except Exception as e:
        log_send_event(f"send_via_smtp25_bool wrapper exception: {type(e).__name__}: {e}")
        return False
        
        
def _call_send_sync_ipv4(
    send_fn,
    ctx,
    acc_dict,
    to_email,
    subject,
    body,
    max_attempts,
    html,
    photo_bytes,
    photo_name,
    sender_name_override
):
    """
    Вызов синхронной send-функции с принудительным IPv4-резолвингом.
    Патчит socket.getaddrinfo только в текущем потоке (executor), чтобы исключить IPv6.
    """
    import socket
    orig_getaddrinfo = socket.getaddrinfo

    def getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
        res = orig_getaddrinfo(host, port, family, type, proto, flags)
        res4 = [r for r in res if r and r[0] == socket.AF_INET]
        return res4 or res  # если нет A-записей — возвращаем исходный список

    try:
        socket.getaddrinfo = getaddrinfo_ipv4
        try:
            log_send_event(f"SMTP_SEND IPv4 fallback enabled for {to_email} via {acc_dict.get('email', '')}")
        except Exception:
            pass
        return send_fn(ctx, acc_dict, to_email, subject, body, max_attempts, html, photo_bytes, photo_name, sender_name_override)
    finally:
        socket.getaddrinfo = orig_getaddrinfo
    
def reply_button_kb(caption: str = "✉️ Ответить") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=caption, callback_data="reply:msg")]])
    
def reply_html_auto_menu_kb() -> InlineKeyboardMarkup:
    """
    Меню авто‑HTML (как '🧾 HTML-шаблоны', теперь ДОБАВЛЕН CUSTOM).
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📄 GO",   callback_data="replyhtml:pick:GO"),
         InlineKeyboardButton(text="📲 PUSH", callback_data="replyhtml:pick:PUSH")],
        [InlineKeyboardButton(text="💬 SMS",  callback_data="replyhtml:pick:SMS"),
         InlineKeyboardButton(text="🔄 BACK", callback_data="replyhtml:pick:BACK")],
        [InlineKeyboardButton(text="💳 PAYPAL", callback_data="replyhtml:pick:PAYPAL")],
        [InlineKeyboardButton(text="📝 CUSTOM", callback_data="replyhtml:pick:CUSTOM")],
        [InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]
    ])

async def _mark_replied(chat_id: int, src_tg_mid: int):
    """
    Фиксирует факт ответа и обновляет клавиатуру (используя async build_incoming_reply_kb_async).
    """
    if not src_tg_mid:
        return
    try:
        REPLIED_MSGS.setdefault(chat_id, set()).add(src_tg_mid)
        kb = await build_incoming_reply_kb_async(chat_id, src_tg_mid)
        try:
            await safe_edit_reply_markup(chat_id, src_tg_mid, kb)
        except Exception as e:
            log_send_event(f"MARK_REPLIED fail chat={chat_id} mid={src_tg_mid}: {type(e).__name__}: {e}")
    except Exception as e:
        log_send_event(f"MARK_REPLIED outer exception chat={chat_id} mid={src_tg_mid}: {type(e).__name__}: {e}")


@dp.callback_query(F.data == "reply:msg")
async def reply_msg_cb(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        try:
            await c.answer()
        except Exception:
            pass
        return

    internal_uid = await U(c)          # internal user id
    tg_mid = c.message.message_id

    # 1. Пытаемся достать из БД (НОВАЯ семантика: функция принимает internal id)
    try:
        row = await get_incoming_message_by_tgmid_async(internal_uid, tg_mid)
    except Exception as e:
        log_send_event(f"reply:msg DB ERROR internal_uid={internal_uid} tg_mid={tg_mid}: {e}")
        row = None

    # 2. Если нет в БД — fallback к runtime
    if not row:
        rt = INCOMING_RT.get((internal_uid, tg_mid))
        if rt:
            acc_id = int(rt.get("acc_id"))
            to_email = rt.get("from_email") or ""
            subject = f"Re: {rt.get('subject','') or ''}"

            # ВАЖНО: Проверяем, существует ли аккаунт (возможно, был удален после получения письма)
            acc = None
            try:
                acc = await get_account_async(internal_uid, acc_id)
            except Exception as e_acc_check:
                log_send_event(f"reply_msg_cb (runtime): error checking account id={acc_id} for uid={internal_uid}: {e_acc_check}")
            
            # Если аккаунт не найден, пытаемся найти аккаунт по email отправителя или использовать первый активный
            if not acc:
                log_send_event(f"reply_msg_cb (runtime): account id={acc_id} not found for uid={internal_uid}, trying fallback")
                try:
                    # Пытаемся найти аккаунт по email отправителя (если есть)
                    all_accounts = await list_accounts_async(internal_uid)
                    if all_accounts:
                        # Ищем аккаунт, который мог получить это письмо (по email или просто первый активный)
                        fallback_acc = None
                        for a in all_accounts:
                            if getattr(a, "active", True):
                                fallback_acc = a
                                break
                        if fallback_acc:
                            acc_id = int(getattr(fallback_acc, "id"))
                            acc = fallback_acc
                            log_send_event(f"reply_msg_cb (runtime): using fallback account id={acc_id} for uid={internal_uid}")
                        else:
                            # Нет активных аккаунтов
                            try:
                                await c.answer("❌ Нет активных аккаунтов для ответа. Аккаунт, на который пришло письмо, был удален.", show_alert=True)
                            except Exception:
                                pass
                            return
                    else:
                        # Нет аккаунтов вообще
                        try:
                            await c.answer("❌ Нет аккаунтов для ответа. Аккаунт, на который пришло письмо, был удален.", show_alert=True)
                        except Exception:
                            pass
                        return
                except Exception as e_fallback:
                    log_send_event(f"reply_msg_cb (runtime): fallback account search failed for uid={internal_uid}: {e_fallback}")
                    try:
                        await c.answer("❌ Ошибка: аккаунт был удален и не удалось найти замену.", show_alert=True)
                    except Exception:
                        pass
                    return

            await state.set_state(ReplyFSM.compose)
            await state.update_data(
                acc_id=acc_id,
                to=to_email,
                subject=subject,
                src_tg_mid=int(tg_mid)
            )
            set_reply_context(internal_uid, acc_id, to_email, subject, int(tg_mid))

            try:
                await ui_prompt(
                    state,
                    c.message.chat.id,
                    "Введите сообщение или пришлите фото с подписью✍️",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [
                            InlineKeyboardButton(text="📬 Отправить пресет", callback_data="reply:use_preset"),
                            InlineKeyboardButton(text="5️⃣ Отправить HTML", callback_data="reply:use_html")
                        ],
                        [InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]
                    ])
                )
            except Exception as e_ui:
                log_send_event(f"reply:msg UI prompt (fallback) error internal_uid={internal_uid} tg_mid={tg_mid}: {e_ui}")
                try:
                    await c.answer("Не удалось открыть окно ответа.", show_alert=True)
                except Exception:
                    pass

            try:
                await safe_cq_answer(c)
            except Exception:
                try:
                    await c.answer()
                except Exception:
                    pass
            return

        try:
            await c.answer("Нет контекста письма", show_alert=True)
        except Exception:
            pass
        return

    # 3. Есть запись в БД
    acc_id = int(getattr(row, "account_id"))
    to_email = getattr(row, "from_email") or ""
    subject = f"Re: {getattr(row, 'subject', '') or ''}"

    # ВАЖНО: Проверяем, существует ли аккаунт (возможно, был удален после получения письма)
    acc = None
    try:
        acc = await get_account_async(internal_uid, acc_id)
    except Exception as e_acc_check:
        log_send_event(f"reply_msg_cb: error checking account id={acc_id} for uid={internal_uid}: {e_acc_check}")
    
    # Если аккаунт не найден, пытаемся найти аккаунт по email отправителя или использовать первый активный
    if not acc:
        log_send_event(f"reply_msg_cb: account id={acc_id} not found for uid={internal_uid}, trying fallback")
        try:
            # Пытаемся найти аккаунт по email отправителя (если есть)
            all_accounts = await list_accounts_async(internal_uid)
            if all_accounts:
                # Ищем аккаунт, который мог получить это письмо (по email или просто первый активный)
                fallback_acc = None
                for a in all_accounts:
                    if getattr(a, "active", True):
                        fallback_acc = a
                        break
                if fallback_acc:
                    acc_id = int(getattr(fallback_acc, "id"))
                    acc = fallback_acc
                    log_send_event(f"reply_msg_cb: using fallback account id={acc_id} for uid={internal_uid}")
                else:
                    # Нет активных аккаунтов
                    try:
                        await c.answer("❌ Нет активных аккаунтов для ответа. Аккаунт, на который пришло письмо, был удален.", show_alert=True)
                    except Exception:
                        pass
                    return
            else:
                # Нет аккаунтов вообще
                try:
                    await c.answer("❌ Нет аккаунтов для ответа. Аккаунт, на который пришло письмо, был удален.", show_alert=True)
                except Exception:
                    pass
                return
        except Exception as e_fallback:
            log_send_event(f"reply_msg_cb: fallback account search failed for uid={internal_uid}: {e_fallback}")
            try:
                await c.answer("❌ Ошибка: аккаунт был удален и не удалось найти замену.", show_alert=True)
            except Exception:
                pass
            return

    # 4. Ре-гидратация runtime (uniform key = (internal_uid, tg_mid))
    try:
        # Получаем created_at из БД для правильной очистки
        created_ts = time.time()
        if hasattr(row, "created_at") and row.created_at:
            try:
                from datetime import datetime, timezone
                created_at = row.created_at
                if isinstance(created_at, datetime):
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                    created_ts = created_at.timestamp()
            except Exception:
                pass
        
        INCOMING_RT[(internal_uid, tg_mid)] = {
            "acc_id": acc_id,
            "from_email": to_email,
            "from_name": getattr(row, "from_name", "") or "",
            "subject": getattr(row, "subject", "") or "",
            "created_ts": created_ts,  # ВАЖНО: сохраняем timestamp создания для правильной очистки
        }
    except Exception as e_rehyd:
        log_send_event(f"REHYDRATE INCOMING_RT fail internal_uid={internal_uid} mid={tg_mid}: {e_rehyd}")

    # 5. FSM
    await state.set_state(ReplyFSM.compose)
    await state.update_data(
        acc_id=acc_id,
        to=to_email,
        subject=subject,
        src_tg_mid=int(tg_mid)
    )
    set_reply_context(internal_uid, acc_id, to_email, subject, int(tg_mid))

    # 6. UI
    try:
        await ui_prompt(
            state,
            c.message.chat.id,
            "Введите сообщение или пришлите фото с подписью✍️",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="📬 Отправить пресет", callback_data="reply:use_preset"),
                    InlineKeyboardButton(text="5️⃣ Отправить HTML", callback_data="reply:use_html")
                ],
                [InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]
            ])
        )
    except Exception as e_ui2:
        log_send_event(f"reply:msg UI prompt error internal_uid={internal_uid} tg_mid={tg_mid}: {e_ui2}")
        try:
            await c.answer("Не удалось открыть окно ответа.", show_alert=True)
        except Exception:
            pass
        return

    # 7. Ответ на callback
    try:
        await safe_cq_answer(c)
    except Exception:
        try:
            await c.answer()
        except Exception:
            pass

@dp.callback_query(F.data.startswith("presets:view:"))
async def presets_view_cb(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return

    try:
        _, _, pid, back_cb = c.data.split(":", 3)
    except ValueError:
        await c.answer("Некорректные данные", show_alert=True)
        return

    preset = await get_preset_async(await U(c), int(pid))
    if not preset:
        await c.answer("Не найдено", show_alert=True)
        return

    internal_uid = await U(c)
    tg_chat = c.message.chat.id
    state_name = await state.get_state()
    data = await state.get_data()

    # Попытка реконструкции контекста, если FSM пустой
    if (not data or "acc_id" not in data) and state_name in {ReplyFSM.compose, ReplyFSM.html}:
        rt_ctx = get_reply_context(internal_uid)
        if rt_ctx:
            try:
                await state.update_data(
                    acc_id=rt_ctx.get("acc_id"),
                    to=rt_ctx.get("to"),
                    subject=rt_ctx.get("subject"),
                    src_tg_mid=rt_ctx.get("src_tg_mid"),
                )
                data = await state.get_data()
            except Exception:
                pass

    reply_mode = (
        state_name in {ReplyFSM.compose, ReplyFSM.html}
        and all(k in data for k in ("acc_id", "to", "subject"))
    )

    if reply_mode:
        try:
            acc_id = int(data["acc_id"])
            to_email = data["to"]
            subj = data.get("subject") or "Re:"
            src_mid = int(data.get("src_tg_mid", 0) or 0)
            body_template = (getattr(preset, "body", "") or "").strip()
            is_html = (state_name == ReplyFSM.html)

            # Подмена темы (HTML)
            try:
                override_flag = (await get_setting_async(internal_uid, "subject_override_html", "1")).strip().lower() in ("1", "true", "yes", "on")
            except Exception:
                override_flag = True
            try:
                subj_conf = (await get_setting_async(internal_uid, "subject_html_text", "")).strip()
            except Exception:
                subj_conf = ""
            if is_html and override_flag and subj_conf:
                subj = subj_conf

            # Если требуется LINK — ждём ссылку текстом
            import re as _re
            if _re.search(r"\{?LINK\}?", body_template, flags=_re.I):
                await state.update_data(
                    mode="await_link",
                    body_template=body_template,
                    is_html=is_html,
                    acc_id=acc_id,
                    to=to_email,
                    subject=subj,
                    src_tg_mid=src_mid
                )
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]]
                )
                await safe_edit_message(c.message, "Введите ссылку для подстановки в пресет (вместо LINK):", reply_markup=kb)
                await safe_cq_answer(c)
                return

            # Кладём в Outbox и сразу выходим (логи — только после успеха)
            await outbox_enqueue(
                internal_uid, tg_chat, acc_id, to_email, subj, body_template,
                html=is_html, src_tg_mid=src_mid
            )
            try:
                await delete_message_safe(c.message)
            except Exception:
                pass
            await state.clear()
            await safe_cq_answer(c, "OK")
            return
        except KeyError as e:
            log_send_event(f"presets_view_cb MISSING KEY {e} uid={internal_uid} tg={tg_chat} state={state_name} data_keys={list(data.keys())}")
            # падаем в режим просмотра

    # Просмотр пресета (настройки)
    body_view = (getattr(preset, "body", "") or "").strip() or "(пусто)"
    await safe_edit_message(
        c.message,
        code(body_view),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)]])
    )
    await safe_cq_answer(c)

@dp.callback_query(F.data == "reply:use_preset")
async def reply_use_preset(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    kb = await presets_inline_kb(await U(c), back_cb="reply:back")
    await safe_edit_message(c.message, "Выберите пресет:", reply_markup=kb)
    await safe_cq_answer(c)


@dp.callback_query(F.data == "reply:use_html")
async def reply_use_html(c: types.CallbackQuery, state: FSMContext):
    """
    Если ссылка СГЕНЕРИРОВАНА -> авто‑меню HTML. Если НЕТ -> legacy HTML.
    Для HTML-отправок теперь используем Outbox (быстрое завершение хендлера),
    лог в чат — только после успешной отправки.
    """
    if not await ensure_approved(c):
        return

    data = await state.get_data() or {}
    if not data:
        ctx_rt = get_reply_context(await U(c))
        if ctx_rt:
            data = ctx_rt.copy()
            try:
                await state.set_state(ReplyFSM.compose)
                await state.update_data(**{k: data.get(k) for k in ("acc_id", "to", "subject", "src_tg_mid")})
            except Exception:
                pass
        else:
            await c.answer("Нет контекста ответа. Нажмите «✉️ Ответить».", show_alert=True)
            return

    chat_id = c.message.chat.id
    uid = await U(c)
    acc_id = int(data["acc_id"])
    to_email = data["to"]
    subj = (data.get("subject") or "").strip()
    src_mid = int(data.get("src_tg_mid", 0) or 0)

    has_link, _entry = _has_generated_link(chat_id, to_email)

    if has_link:
        try:
            await safe_edit_message(
                c.message,
                "Выберите HTML шаблон:",
                reply_markup=reply_html_auto_menu_kb()
            )
        except Exception:
            try:
                await c.message.answer("Выберите HTML шаблон:", reply_markup=reply_html_auto_menu_kb())
            except Exception:
                pass
        await safe_cq_answer(c)
        return

    # ===== Legacy HTML (без ссылки) — ТЕПЕРЬ через Outbox =====
    # Имя отправителя (спуф) — оставляем как было (для консистентности From)
    try:
        acc_obj = await get_account_async(uid, acc_id)
        acc_display = (getattr(acc_obj, "display_name", "") or getattr(acc_obj, "name", "") or "").strip()
        if not acc_display and getattr(acc_obj, "email", ""):
            acc_display = acc_obj.email.split("@", 1)[0]
    except Exception:
        acc_display = ""
    sender_name_override = await get_spoof_sender_name(uid, acc_display_name=acc_display, tpl="HTML", chat_id=chat_id)

    # Подмена темы (если включена)
    try:
        override_flag = (await get_setting_async(uid, "subject_override_html", "1")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        override_flag = True
    try:
        subj_conf = (await get_setting_async(uid, "subject_html_text", "")).strip()
    except Exception:
        subj_conf = ""
    if override_flag and subj_conf:
        subj = subj_conf

    # Берём последний HTML из кеша (legacy путь)
    try:
        html_code = get_last_html(chat_id)
    except Exception:
        html_code = None

    if not html_code:
        try:
            await state.set_state(ReplyFSM.html)
            await state.update_data(**{k: data.get(k) for k in ("acc_id", "to", "subject", "src_tg_mid")})
        except Exception:
            pass
        rt = get_reply_context(uid) or {}
        rt.update({k: data.get(k) for k in ("acc_id", "to", "subject", "src_tg_mid")})
        rt["await_html_file"] = True
        REPLY_RUNTIME[uid] = rt
        await safe_edit_message(
            c.message,
            f"Нет готового HTML. Пришлите .txt или .html.\nFrom: {sender_name_override}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]])
        )
        await safe_cq_answer(c)
        return

    # Постановка в Outbox (лог придёт после УСПЕШНОЙ отправки)
    try:
        await outbox_enqueue(
            uid, chat_id, acc_id, to_email, subj,
            html_code,
            html=True,
            src_tg_mid=src_mid
        )
    except Exception as e:
        try:
            await safe_edit_message(c.message, f"Ошибка отправки ❌ {escape_html(str(e))}")
        except Exception:
            pass
        await safe_cq_answer(c)
        return

    # Быстрый выход: чистим UI/состояние
    try:
        await delete_message_safe(c.message)
    except Exception:
        pass
    clear_reply_context(uid)
    await state.clear()
    await safe_cq_answer(c, "OK")
    
@dp.callback_query(F.data.startswith("replyhtml:pick:"))
async def reply_html_auto_pick(c: types.CallbackQuery, state: FSMContext):
    """
    Авто‑HTML (GO/PUSH/SMS/BACK/PAYPAL/CUSTOM).
    Отправка переносится в Outbox — хендлер отвечает мгновенно.
    """
    if not await ensure_approved(c):
        return

    tpl = c.data.split(":")[2] if ":" in c.data else ""
    allowed = {"GO", "PUSH", "SMS", "BACK", "PAYPAL", "CUSTOM"}
    if tpl not in allowed:
        await c.answer("Недоступно", show_alert=True)
        return

    data = await state.get_data() or {}
    if not data:
        ctx_rt = get_reply_context(await U(c))
        if ctx_rt:
            data = ctx_rt.copy()
        else:
            await c.answer("Нет контекста", show_alert=True)
            return

    chat_id = c.message.chat.id
    internal_uid = await U(c)
    acc_id = int(data["acc_id"])
    to_email = data["to"]
    subj = (data.get("subject") or "").strip()
    src_mid = int(data.get("src_tg_mid", 0) or 0)

    has_link, entry = _has_generated_link(chat_id, to_email)
    if not has_link:
        await c.answer("Ссылка не создана", show_alert=True)
        return
    link_val = entry.get("short") or entry.get("original") or ""

    # Подмена темы (HTML)
    try:
        override_flag = (await get_setting_async(internal_uid, "subject_override_html", "1")).strip().lower() in ("1","true","yes","on")
    except Exception:
        override_flag = True
    try:
        subj_conf = (await get_setting_async(internal_uid, "subject_html_text", "")).strip()
    except Exception:
        subj_conf = ""
    if override_flag and subj_conf:
        subj = subj_conf

    # Имя отправителя (спуф)
    try:
        acc_obj = await get_account_async(internal_uid, acc_id)
        acc_display = (getattr(acc_obj, "display_name", "") or getattr(acc_obj, "name", "") or "").strip()
        if not acc_display and getattr(acc_obj, "email", ""):
            acc_display = acc_obj.email.split("@", 1)[0]
    except Exception:
        acc_display = ""
    sender_name_override = await get_spoof_sender_name(internal_uid, acc_display_name=acc_display, tpl=tpl, chat_id=chat_id)

    if tpl == "CUSTOM":
        # Переключаемся на ожидание текста CUSTOM
        try:
            await state.set_state(ReplyFSM.html)
            await state.update_data(
                acc_id=acc_id,
                to=to_email,
                subject=subj,
                src_tg_mid=src_mid,
                custom_tpl=True,
                custom_link=link_val
            )
        except Exception:
            pass
        try:
            await safe_edit_message(
                c.message,
                "Введите текст для CUSTOM‑шаблона (он будет вставлен в HTML):",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]]
                )
            )
        except Exception:
            try:
                await c.message.answer(
                    "Введите текст для CUSTOM‑шаблона (он будет вставлен в HTML):",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]]
                    )
                )
            except Exception:
                pass
        await safe_cq_answer(c)
        return

    # Обычные шаблоны (GO/PUSH/SMS/BACK/PAYPAL)
    offer = (entry.get("title") or "").strip()
    amount = (entry.get("price") or "").strip()

    txt_html, final_html, style_id = await _build_html(
        internal_uid,
        tpl,
        link_val,
        offer=offer,
        price=amount
    )

    try:
        set_last_html(chat_id, final_html)
        set_last_html_meta(chat_id, {"style": style_id, "tpl": tpl})
    except Exception:
        pass

    # В Outbox
    await outbox_enqueue(
        internal_uid, chat_id, acc_id, to_email, subj,
        final_html,
        html=True,
        src_tg_mid=src_mid
    )
    try:
        await state.clear()
    except Exception:
        pass
    try:
        await delete_message_safe(c.message)
    except Exception:
        pass
    await safe_cq_answer(c, "OK")
    
@dp.message(ReplyFSM.html)
async def reply_html_wait_file(m: types.Message, state: FSMContext):
    """
    Получение .html/.txt или текста для HTML‑ответа.
    Отправка переносится в Outbox — хендлер отвечает мгновенно.
    """
    if not await ensure_approved(m):
        return

    data = await state.get_data() or {}
    if not data:
        data = (get_reply_context(await U(m)) or {})
        if not data:
            await bot.send_message(m.chat.id, "Контекст ответа потерян. Нажмите «✉️ Ответить» снова.")
            await state.clear()
            return
        try:
            await state.set_state(ReplyFSM.html)
            await state.update_data(**{k: data.get(k) for k in ("acc_id", "to", "subject", "src_tg_mid")})
        except Exception:
            pass

    chat_id = m.chat.id
    acc_id = int(data["acc_id"])
    to_email = data["to"]
    subj = (data.get("subject") or "").strip()
    src_mid = int(data.get("src_tg_mid", 0) or 0)

    # Subject override (HTML)
    try:
        override_flag = (await get_setting_async(await U(m), "subject_override_html", "1")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        override_flag = True
    try:
        subj_conf = (await get_setting_async(await U(m), "subject_html_text", "")).strip()
    except Exception:
        subj_conf = ""
    if override_flag and subj_conf:
        subj = subj_conf

    # Имя отправителя (спуф)
    try:
        acc_obj = await get_account_async(await U(m), acc_id)
        acc_display = (getattr(acc_obj, "display_name", "") or getattr(acc_obj, "name", "") or "").strip()
        if not acc_display and getattr(acc_obj, "email", ""):
            acc_display = acc_obj.email.split("@", 1)[0]
    except Exception:
        acc_display = ""
    internal_uid = await U(m)
    sender_name_override = await get_spoof_sender_name(
        internal_uid,
        acc_display_name=acc_display,
        tpl="HTML",
        chat_id=chat_id
    )

    # РЕЖИМ CUSTOM: ожидаем текст, собираем HTML через _build_html(..., "CUSTOM")
    if data.get("custom_tpl") is True and not m.document:
        custom_text = (m.text or "").strip()
        if not custom_text:
            await bot.send_message(
                m.chat.id,
                "Текст пустой. Введите текст для CUSTOM‑шаблона:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]])
            )
            return

        # ссылка из состояния (если есть)
        link_val = data.get("custom_link") or ""
        if not link_val and "@" in to_email:
            import re, unicodedata
            def _norm(s: str) -> str:
                s = (s or "").replace("\u00A0", " ")
                s = unicodedata.normalize("NFKC", s)
                s = s.replace(".", " ").replace("_", " ").replace("-", " ")
                s = re.sub(r"\s+", " ", s.strip().lower())
                return s
            k_local = _norm(to_email.split("@", 1)[0])
            entry = AD_GENERATED_LINKS_PER_CHAT.get(chat_id, {}).get(k_local) or {}
            link_val = entry.get("short") or entry.get("original") or ""

        _, final_html, style_id = await _build_html(
            internal_uid,
            "CUSTOM",
            link_val,
            custom_text=custom_text
        )

        try:
            set_last_html(chat_id, final_html)
            set_last_html_meta(chat_id, {"style": style_id, "tpl": "CUSTOM"})
        except Exception:
            pass

        # В Outbox
        await outbox_enqueue(
            internal_uid, m.chat.id, acc_id, to_email, subj,
            final_html,
            html=True,
            src_tg_mid=src_mid
        )
        try:
            await state.clear()
        except Exception:
            pass
        clear_reply_context(internal_uid)
        return

    # Обычный путь (файл .html/.txt или текст = готовый HTML)
    html_str: str | None = None
    if m.document:
        try:
            fname = (m.document.file_name or "").lower()
            mime = (m.document.mime_type or "").lower()
            if (not fname.endswith((".txt", ".html", ".htm"))) and ("text" not in mime and "html" not in mime):
                await bot.send_message(
                    m.chat.id,
                    "Нужен файл .txt или .html.",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]])
                )
                return
            file_obj = await bot.get_file(m.document.file_id)
            bio = await bot.download(file_obj)
            raw = bio.read()
            try:
                html_str = raw.decode("utf-8", "ignore")
            except Exception:
                html_str = raw.decode("latin-1", "ignore")
        except Exception as e:
            log_send_event(f"reply_html_wait_file download/decode error uid={m.from_user.id}: {e}")
            html_str = None
    elif m.text:
        html_str = (m.text or "").strip()

    if not html_str:
        await bot.send_message(
            m.chat.id,
            "Не удалось прочитать HTML. Пришлите .txt/.html.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]])
        )
        return

    # Подстановка LINK, если есть сгенерированная ссылка
    import re, unicodedata
    def norm_key(s: str) -> str:
        s = (s or "").replace("\u00A0", " ")
        s = unicodedata.normalize("NFKC", s)
        s = s.replace(".", " ").replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s.strip().lower())
        return s
    local_part = to_email.split("@", 1)[0] if "@" in to_email else ""
    k_local = norm_key(local_part)
    gen_entry = AD_GENERATED_LINKS_PER_CHAT.get(chat_id, {}).get(k_local)
    if gen_entry:
        link_val = gen_entry.get("short") or gen_entry.get("original") or ""
        if link_val:
            html_str = re.sub(r"\{?LINK\}?", link_val, html_str, flags=re.I)

    # Обогащение (GO) — оставлено как было
    should_enrich = False
    try:
        meta = get_last_html_meta(chat_id) or {}
        should_enrich = (meta.get("style") == "klein") and (meta.get("tpl") == "GO")
    except Exception:
        pass
    if should_enrich:
        product, buyer, acc_display2 = await _collect_reply_context_for_html(internal_uid, acc_id, src_mid)
        from datetime import datetime
        meta2 = get_last_html_meta(chat_id) or {}
        order_no_meta = meta2.get("order_no") or ""
        date_str = datetime.now().strftime("%d-%m-%Y")
        html_str = _inject_klein_go_blocks(
            html_str,
            product,
            acc_display2 or buyer,
            order_no_meta or "",
            date_str
        )

    # В Outbox
    await outbox_enqueue(
        internal_uid, m.chat.id, acc_id, to_email, subj,
        html_str,
        html=True,
        src_tg_mid=src_mid
    )
    try:
        await state.clear()
    except Exception:
        pass
    clear_reply_context(internal_uid)
            
@dp.message(F.document)
async def reply_html_fallback_any_state(m: types.Message, state: FSMContext):
    # Сработает только если мы действительно ждём файл для HTML-ответа
    rt = get_reply_context(await U(m))
    if not rt or not rt.get("await_html_file"):
        return  # не наш кейс — отдаём другим хендлерам

    # Проксируем в хендлер ReplyFSM.html, чтобы не дублировать логику
    try:
        await state.set_state(ReplyFSM.html)
        await state.update_data(**{k: rt.get(k) for k in ("acc_id", "to", "subject", "src_tg_mid")})
    except Exception:
        pass
    await reply_html_wait_file(m, state)

@dp.callback_query(F.data == "reply:back")
async def reply_back(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await state.set_state(ReplyFSM.compose)
    await safe_edit_message(c.message, "Введите сообщение✍️", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📬 Отправить пресет", callback_data="reply:use_preset"),
         InlineKeyboardButton(text="5️⃣ Отправить HTML", callback_data="reply:use_html")],
        [InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]
    ])); await safe_cq_answer(c)

@dp.callback_query(F.data == "reply:cancel")
async def reply_cancel(c: types.CallbackQuery, state: FSMContext):
    try:
        clear_reply_context(await U(c))
    except Exception:
        pass
    await state.clear()
    await safe_edit_message(c.message, "Отменено.", reply_markup=None); await safe_cq_answer(c)

@dp.message(ReplyFSM.compose)
async def reply_compose_text_or_photo(m: types.Message, state: FSMContext):
    """
    Обработка текста/фото в режиме ответа.
    Отправка переносится в Outbox‑воркер — хендлер отвечает мгновенно.
    """
    if not await ensure_approved(m):
        return

    incoming_text = (m.text or "").strip()

    if incoming_text == "🧾 HTML-шаблоны":
        try:
            await open_html_templates_menu(m)
        except Exception:
            pass
        return

    await delete_message_safe(m)
    await ui_clear_prompts(state)

    data = await state.get_data()
    if not data:
        await bot.send_message(m.chat.id, "Контекст ответа потерян.", reply_markup=reply_button_kb())
        await state.clear()
        return

    acc_id = int(data.get("acc_id"))
    to_email = data.get("to")
    subj = (data.get("subject") or "Re:").strip()
    src_mid = int(data.get("src_tg_mid", 0) or 0)
    internal_uid = await U(m)
    
    # ВАЖНО: Проверяем существование аккаунта перед отправкой
    try:
        from db_async import get_account_async
        acc = await get_account_async(internal_uid, acc_id)
        if not acc:
            log_send_event(f"reply_compose: account id={acc_id} not found for uid={internal_uid} tg={m.from_user.id}")
            await bot.send_message(
                m.chat.id,
                f"❌ Ошибка: аккаунт с ID {acc_id} не найден. Возможно, он был удален.\n"
                f"Попробуйте ответить на другое входящее письмо.",
                reply_markup=reply_button_kb()
            )
            await state.clear()
            return
    except Exception as e_acc_check:
        log_send_event(f"reply_compose: error checking account id={acc_id} for uid={internal_uid}: {e_acc_check}")
        await bot.send_message(
            m.chat.id,
            f"❌ Ошибка при проверке аккаунта: {type(e_acc_check).__name__}",
            reply_markup=reply_button_kb()
        )
        await state.clear()
        return

    BLOCKED_MENU_TEXTS = {
        "📖 Проверка ников",
        "Настройки⚙️",
        "✉️ Отправить email",
        "➕ Быстрое добавление",
        "👑 Админка",
    }

    # LINK MODE (вставка ссылки в шаблон и постановка в очередь)
    if data.get("mode") == "await_link":
        if incoming_text in BLOCKED_MENU_TEXTS or incoming_text.startswith("/"):
            return
        if incoming_text == "":
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="reply:cancel")]])
            await bot.send_message(m.chat.id, "Ссылка пустая. Введите ссылку:", reply_markup=kb)
            return

        link = incoming_text
        body_template = data.get("body_template") or ""
        is_html = bool(data.get("is_html"))

        import re as _re
        body_filled = _re.sub(r"\{?LINK\}?", link, body_template)

        # Subject override (HTML)
        try:
            override_flag = (await get_setting_async(internal_uid, "subject_override_html", "1")).strip().lower() in ("1", "true", "yes", "on")
        except Exception:
            override_flag = True
        try:
            subj_conf = (await get_setting_async(internal_uid, "subject_html_text", "")).strip()
        except Exception:
            subj_conf = ""
        if is_html and override_flag and subj_conf:
            subj = subj_conf

        # В Outbox
        await outbox_enqueue(
            internal_uid, m.chat.id, acc_id, to_email, subj,
            body_filled,
            html=is_html,
            src_tg_mid=src_mid
        )
        await state.clear()
        return

    # NORMAL MODE (не‑HTML): текст или фото (caption)
    if incoming_text in BLOCKED_MENU_TEXTS or incoming_text.startswith("/"):
        return

    photo_bytes = None
    photo_name = None
    body = None

    try:
        if m.photo:
            ph = m.photo[-1]
            f: File = await bot.get_file(ph.file_id)
            buf = BytesIO()
            await bot.download(f, destination=buf)
            photo_bytes = buf.getvalue()
            photo_name = "image.jpg"
            body = (m.caption or "").strip()
        elif m.document and (getattr(m.document, "mime_type", "") or "").startswith("image/"):
            f: File = await bot.get_file(m.document.file_id)
            buf = BytesIO()
            await bot.download(f, destination=buf)
            photo_bytes = buf.getvalue()
            photo_name = (m.document.file_name or "image")
            if "." not in photo_name:
                photo_name += ".jpg"
            body = (m.caption or "").strip()
        else:
            body = incoming_text

        if (not body) and (photo_bytes is None):
            return

        # В Outbox
        await outbox_enqueue(
            internal_uid, m.chat.id, acc_id, to_email, subj,
            body or "",
            html=False,
            photo_bytes=photo_bytes,
            photo_name=photo_name,
            src_tg_mid=src_mid
        )
    except Exception as e:
        log_send_event(f"reply_compose enqueue exception uid={internal_uid} acc_id={acc_id} err={e}")

    await state.clear()


# ====== QUICK ADD ======
def quickadd_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1️⃣ Одно имя", callback_data="quickadd:one"),
         InlineKeyboardButton(text="1️⃣2️⃣3️⃣4️⃣ Разные имена", callback_data="quickadd:many")],
        *nav_row("ui:hide")
    ])

def quickadd_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Отмена", callback_data="quickadd:cancel")]])

@dp.message(F.text == "➕ Быстрое добавление")
async def quickadd_start(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    await state.set_state(QuickAddFSM.mode)
    await bot.send_message(m.chat.id, "Выберите опцию:", reply_markup=quickadd_menu_kb())

@dp.message(Command("quickadd"))
async def cmd_quickadd(m: types.Message, state: FSMContext):
    await quickadd_start(m, state)

@dp.callback_query(F.data == "quickadd:one")
async def quickadd_one(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await state.update_data(mode="one")
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Введите отображаемое имя и фамилию. Например: Jessy Jackson ✍️", reply_markup=quickadd_cancel_kb()); await state.set_state(QuickAddFSM.name); await safe_cq_answer(c)

@dp.callback_query(F.data == "quickadd:many")
async def quickadd_many(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c): return
    await state.update_data(mode="many")
    await ui_clear_prompts(state); await delete_message_safe(c.message)
    await ui_prompt(state, c.message.chat.id, "Отправьте данные текстом:\n\nemail1:password1:Имя Фамилия\nemail2:password2:Имя Фамилия", reply_markup=quickadd_cancel_kb()); await state.set_state(QuickAddFSM.lines); await safe_cq_answer(c)

@dp.message(QuickAddFSM.name)
async def quickadd_got_name(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    await state.update_data(name=(m.text or "").strip())
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Теперь отправьте строки вида:\nemail:password", reply_markup=quickadd_cancel_kb())
    await state.set_state(QuickAddFSM.lines)
    
def parse_lines_one(text: str) -> List[Tuple[str, str]]:
    """
    Формат:
      email:password
    По одной паре на строку.
    """
    rows: List[Tuple[str, str]] = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        if ":" not in ln:
            continue
        email, password = ln.split(":", 1)
        email = email.strip()
        password = password.strip()
        if email and password:
            rows.append((email, password))
    return rows

def parse_lines_many(text: str) -> List[Tuple[str, str, str]]:
    """
    Формат:
      email:password:Имя Фамилия
    По одной запись на строку.
    """
    rows: List[Tuple[str, str, str]] = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        parts = ln.split(":", 2)
        if len(parts) != 3:
            continue
        email, password, name = (p.strip() for p in parts)
        if email and password:
            rows.append((email, password, name))
    return rows

def parse_proxy_lines(text: str) -> List[Tuple[str, int, str, str]]:
    """
    Формат:
      host:port:login:password
    Пароль может содержать двоеточия (склеиваем хвост).
    Возвращает список (host, port, login, password).
    Невалидные строки пропускаются.
    """
    out: List[Tuple[str, int, str, str]] = []
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        parts = ln.split(":")
        if len(parts) < 4:
            continue
        host = parts[0].strip()
        port_str = parts[1].strip()
        user = parts[2].strip()
        pwd = ":".join(parts[3:]).strip()
        if not host or not port_str.isdigit() or not user or not pwd:
            continue
        out.append((host, int(port_str), user, pwd))
    return out
    
def parse_subject_lines(text: str) -> List[str]:
    """
    Каждая непустая строка = отдельная тема.
    Дубликаты внутри одного ввода отфильтровываем, сохраняя порядок.
    """
    seen: set[str] = set()
    out: List[str] = []
    for ln in (text or "").splitlines():
        s = (ln or "").strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def parse_smart_preset_blocks(text: str) -> List[str]:
    """
    Делит ввод на блоки по строке-разделителю '=' (строка, содержащая только '=').
    Блоки сохраняются с переводами строк, пустые блоки игнорируются.
    """
    blocks: List[List[str]] = []
    cur: List[str] = []
    for ln in (text or "").splitlines():
        if (ln or "").strip() == "=":
            blk = "\n".join(cur).strip()
            if blk:
                blocks.append([blk])
            cur = []
        else:
            cur.append(ln)
    # хвост
    tail = "\n".join(cur).strip()
    if tail:
        blocks.append([tail])

    # распакуем
    return [b[0] for b in blocks]

# 7. Перевести quickadd_lines_text на async
@dp.message(QuickAddFSM.lines)
async def quickadd_lines_text(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): 
        return
    await delete_message_safe(m)
    uid = await U(m)
    tg = m.from_user.id

    data = await state.get_data()
    mode = data.get("mode")
    added = 0
    total = 0
    invalid_entries: list[str] = []
    skipped_existing = 0
    skipped_due_to_limit = 0

    default_proxy_id = await pick_proxy_for_account(uid)

    # Соберём кандидатов и проверим формат email
    candidates: list[tuple[str, str]] = []  # (email, password)
    display_for_email: dict[str, str] = {}  # email -> display_name

    if mode == "one":
        name = (data.get("name", "") or "").strip()
        pairs = parse_lines_one(m.text or "")
        total = len(pairs)
        for email_addr, password in pairs:
            e = (email_addr or "").strip()
            if not is_valid_email(e):
                invalid_entries.append(e)
                continue
            candidates.append((e, password))
            display_for_email[e] = name or e.split("@")[0]
    else:
        triples = parse_lines_many(m.text or "")
        total = len(triples)
        for email_addr, password, name in triples:
            e = (email_addr or "").strip()
            if not is_valid_email(e):
                invalid_entries.append(e)
                continue
            candidates.append((e, password))
            display_for_email[e] = (name or "").strip() or e.split("@")[0]

    if not candidates and not invalid_entries:
        await ui_clear_prompts(state)
        await bot.send_message(
            m.chat.id,
            "Не распознано ни одной строки.",
            reply_markup=emails_menu_kb()
        )
        await state.clear()
        return

    # Применим лимит MAX_EMAILS_PER_USER
    left = await limit_remaining_slots(uid)
    allowed = candidates[:left] if left > 0 else []
    skipped_due_to_limit = max(0, len(candidates) - len(allowed))

    # ЭТАП 1: Добавляем все аккаунты в БД сразу
    added_accounts = []  # Список добавленных аккаунтов для последующего запуска: [(acc, email, password, display_name), ...]
    
    for email_addr, password in allowed:
        try:
            disp = display_for_email.get(email_addr, email_addr.split("@")[0])
            res = await add_account_async(
                uid, disp, email_addr, password,
                auto_bind_proxy=True, proxy_id=default_proxy_id
            )
            if not res:
                skipped_existing += 1
                continue

            # Найдём только что добавленный аккаунт
            accs = await list_accounts_async(uid)
            acc = next((a for a in accs if a.email == email_addr), None)
            if acc:
                # ВАЖНО: Сначала деактивируем аккаунт, чтобы воркеры не схватили старые письма
                try:
                    await set_account_active_async(uid, acc.id, False)
                except Exception:
                    pass

                # Помечаем все UNSEEN письма как прочитанные в момент добавления
                try:
                    await mark_all_unseen_as_read_async(uid, acc.id)
                    # ВАЖНО: Даем время серверу применить флаги перед запуском IMAP процесса
                    await asyncio.sleep(2)
                except Exception:
                    pass

                # Сохраняем информацию об аккаунте для последующего запуска
                added_accounts.append((acc, email_addr, password, disp))
                added += 1
        except Exception:
            continue

    # ЭТАП 2: Постепенно запускаем IMAP процессы для каждого аккаунта
    # ВАЖНО: Даем дополнительное время серверу применить все флаги перед запуском IMAP процессов
    # Это гарантирует, что все старые письма будут помечены как прочитанные до начала чтения
    try:
        await asyncio.sleep(3)
    except Exception:
        pass
    
    # Получаем контекст для прокси один раз (для всех аккаунтов)
    ctx = await get_user_ctx_async(uid)
    
    for idx, (acc, email_addr, password, disp) in enumerate(added_accounts):
        try:
            # Активируем аккаунт уже после пометки всех писем
            try:
                await set_account_active_async(uid, acc.id, True)
            except Exception:
                pass

            # ВАЖНО: Устанавливаем "карантин" публикации ДО запуска IMAP процесса
            # Это гарантирует, что старые письма не будут опубликованы
            try:
                QUICK_ADD_ACTIVATED_AT[(uid, acc.id)] = time.time()
                log_send_event(
                    f"QUICK_ADD: Аккаунт активирован, установлен период карантина "
                    f"uid={uid} acc_id={acc.id} email={email_addr}"
                )
            except Exception:
                pass

            # ВАЖНО: Запускаем IMAP процесс для аккаунта постепенно
            try:
                # Получаем прокси для аккаунта (контекст уже получен выше)
                proxy = smtp25.get_next_proxy_ctx(ctx, "send")
                if proxy:
                    success = await start_imap_process(
                        user_id=uid,
                        acc_id=int(acc.id),
                        email=email_addr,
                        password=password,
                        display_name=disp,
                        chat_id=m.chat.id,
                        proxy=proxy
                    )
                    if success:
                        # ВАЖНО: Обновляем статус аккаунта в IMAP_STATUS для отображения в статусе
                        st_imap = ensure_user_imap_status(uid)
                        async with st_imap.lock:
                            st_imap.running = True
                            st_imap.account_status.setdefault(email_addr, {})
                            st_imap.account_status[email_addr]["active"] = True
                        
                        log_send_event(
                            f"QUICK_ADD: IMAP процесс запущен для аккаунта "
                            f"uid={uid} acc_id={acc.id} email={email_addr}"
                        )
                    else:
                        log_send_event(
                            f"QUICK_ADD: Не удалось запустить IMAP процесс для аккаунта "
                            f"uid={uid} acc_id={acc.id} email={email_addr} (проверьте логи выше)"
                        )
                else:
                    log_send_event(
                        f"QUICK_ADD: Пропуск запуска IMAP процесса для аккаунта "
                        f"uid={uid} acc_id={acc.id} email={email_addr}: нет прокси"
                    )
            except Exception as e:
                log_send_event(
                    f"QUICK_ADD: Ошибка при запуске IMAP процесса для аккаунта "
                    f"uid={uid} acc_id={acc.id} email={email_addr}: {e}"
                )

            # Сброс одноразовых лог-флагов старта/ошибок
            try:
                key = (uid, email_addr)
                START_LOG_SENT.pop(key, None)
                ERROR_LOG_SENT.pop(key, None)
            except Exception:
                pass

            # Небольшая задержка между запусками IMAP процессов (чтобы не перегружать систему)
            try:
                await asyncio.sleep(0.5)
            except Exception:
                pass
        except Exception:
            continue

    if mode != "one":
        try:
            invalidate_user_cache(uid)
        except Exception:
            pass

    # Примечание: IMAP процессы уже запущены постепенно в ЭТАПЕ 2,
    # поэтому дополнительный вызов _ensure_imap_started_for_user не требуется

    await ui_clear_prompts(state)
    summary_lines = [f"Добавлено аккаунтов: {added} из {total}"]
    if skipped_existing:
        summary_lines.append(f"Пропущено (уже существует): {skipped_existing}")
    if invalid_entries:
        examples = ", ".join(invalid_entries[:10])
        more = f", ...(+{len(invalid_entries)-10})" if len(invalid_entries) > 10 else ""
        summary_lines.append(f"Пропущено (неверный формат email): {examples}{more} ❗")
    if skipped_due_to_limit > 0:
        summary_lines.append("Максимальное допустимое количество Email: 97 ❗️")

    await bot.send_message(m.chat.id, "\n".join(summary_lines), reply_markup=emails_menu_kb())
    await state.clear()

    log_send_event(
        f"QUICKADD summary uid={uid} tg={tg} mode={mode} total={total} added={added} "
        f"skipped_existing={skipped_existing} invalid={len(invalid_entries)} skipped_limit={skipped_due_to_limit}"
    )

# ====== FALLBACK кнопки (текст) ======
@dp.message(F.text.regexp(r"(?i)\bпроверка\s+ников\b"))
async def fallback_btn_check(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await btn_check(m, state)

@dp.message(F.text == "🧾 HTML-шаблоны")
async def open_html_templates_menu(m: types.Message):
    if not await ensure_approved(m): 
        return
    await delete_message_safe(m)
    await bot.send_message(m.chat.id, "Выберите шаблон:", reply_markup=html_menu_kb())

# ====== IMAP PROCESS POOL ARCHITECTURE ======
# Новая архитектура: process pool с постоянными соединениями
# Каждый процесс держит постоянное соединение для аккаунта и проверяет раз в 5-7 секунд

import asyncio, time, random, itertools
from dataclasses import dataclass
from multiprocessing import Manager
from typing import Optional, Dict, Any, Tuple
import pickle



# ===== NEW IMAP RUNTIME WRAPPERS =====
# These functions now wrap the new imap_runtime module

def init_imap_worker_pool() -> bool:
    """
    Initialize IMAP runtime - now wraps imap_runtime module.
    This is a synchronous wrapper for compatibility with existing code.
    """
    try:
        # Run the async init in a new event loop (sync context)
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If already in async context, schedule it
                asyncio.create_task(imap_runtime.init_imap_runtime())
                return True
        except RuntimeError:
            pass
        
        # Create new loop for sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(imap_runtime.init_imap_runtime())
            return result
        finally:
            loop.close()
    except Exception as e:
        log_send_event(f"IMAP: init_imap_worker_pool error: {e}")
        return False


def shutdown_imap_worker_pool():
    """
    Shutdown IMAP runtime - now wraps imap_runtime module.
    This is a synchronous wrapper for compatibility with existing code.
    """
    try:
        # Run the async shutdown in a new event loop (sync context)
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If already in async context, schedule it
                asyncio.create_task(imap_runtime.shutdown_imap_runtime())
                return
        except RuntimeError:
            pass
        
        # Create new loop for sync context
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(imap_runtime.shutdown_imap_runtime())
        finally:
            loop.close()
    except Exception as e:
        log_send_event(f"IMAP: shutdown_imap_worker_pool error: {e}")


# ===== REMOVED OLD WORKER CODE - NOW USING imap_runtime MODULE =====

async def start_imap_process(user_id: int, acc_id: int, email: str, password: str, display_name: str, chat_id: int, proxy: Optional[Dict[str, Any]] = None) -> bool:
    """
    Start IMAP worker for account - now wraps imap_runtime module.
    Proxy is required (IMAP connections use SOCKS proxy).
    """
    # Validate proxy
    if not proxy:
        log_send_event(f"IMAP: Cannot start account uid={user_id} acc_id={acc_id} email={email}: proxy is required")
        return False
    
    if not isinstance(proxy, dict) or "host" not in proxy or "port" not in proxy:
        log_send_event(f"IMAP: Cannot start account uid={user_id} acc_id={acc_id} email={email}: invalid proxy configuration")
        return False
    
    # Start worker using new runtime
    try:
        success = await imap_runtime.start_imap_for_account(
            user_id=user_id,
            acc_id=acc_id,
            email=email,
            password=password,
            display_name=display_name,
            chat_id=chat_id,
            proxy=proxy
        )
        
        if success:
            # Update compatibility status structures
            key = (user_id, acc_id)
            IMAP_ACCOUNT_STATUS[key] = {"active": True, "added_at": time.time()}
            
            # Update UserImapStatus for compatibility with UI
            try:
                st = ensure_user_imap_status(user_id)
                async with st.lock:
                    st.running = True
                    accounts = await list_accounts_async(user_id)
                    acc = next((a for a in accounts if int(getattr(a, "id")) == acc_id), None)
                    if acc:
                        if not hasattr(st, "accounts"):
                            st.accounts = {}
                        st.accounts[email] = acc
                        st.account_status.setdefault(email, {})
                        st.account_status[email]["active"] = True
            except Exception as e:
                log_send_event(f"IMAP: Failed to update status for uid={user_id} acc_id={acc_id}: {e}")
            
            log_send_event(f"IMAP: Account started uid={user_id} acc_id={acc_id} email={email}")
            
            # Schedule start log notification
            try:
                schedule_start_log(user_id, chat_id, email)
            except Exception as e:
                log_send_event(f"IMAP: Failed to schedule start log: {e}")
        
        return success
        
    except Exception as e:
        log_send_event(f"IMAP: Failed to start account uid={user_id} acc_id={acc_id} email={email}: {e}")
        return False


async def stop_imap_process(user_id: int, acc_id: int) -> bool:
    """
    Stop IMAP worker for account - now wraps imap_runtime module.
    """
    try:
        success = await imap_runtime.stop_imap_for_account(user_id, acc_id)
        
        # Update compatibility status structures
        key = (user_id, acc_id)
        if key in IMAP_ACCOUNT_STATUS:
            IMAP_ACCOUNT_STATUS[key] = {"active": False}
        
        # Update UserImapStatus for compatibility with UI
        try:
            st = ensure_user_imap_status(user_id)
            async with st.lock:
                accounts = await list_accounts_async(user_id)
                acc = next((a for a in accounts if int(getattr(a, "id")) == acc_id), None)
                if acc:
                    email = getattr(acc, "email", "")
                    if email:
                        st.account_status.setdefault(email, {})
                        st.account_status[email]["active"] = False
        except Exception as e:
            log_send_event(f"IMAP: Failed to update status on stop: {e}")
        
        log_send_event(f"IMAP: Account stopped uid={user_id} acc_id={acc_id}")
        return success
        
    except Exception as e:
        log_send_event(f"IMAP: Failed to stop account uid={user_id} acc_id={acc_id}: {e}")
        return False


async def _process_imap_results_global():
    """
    Global IMAP result processor - now wraps imap_runtime module.
    This function processes results from the new imap_runtime result queue.
    """
    async def process_result_callback(result: dict):
        """Callback to process a single IMAP result"""
        try:
            # Extract data from result
            user_id = result.get("user_id")
            acc_id = result.get("acc_id")
            email = result.get("email")
            chat_id = result.get("chat_id")
            
            if not user_id or not acc_id or not chat_id:
                return
            
            # Publish to Telegram chat using existing function
            await publish_incoming_to_chat_async(
                user_id=user_id,
                chat_id=chat_id,
                from_email=result.get("from", ""),
                subject=result.get("subject", ""),
                body=result.get("body", ""),
                email_account=email,
                display_name=result.get("display_name", "")
            )
            
        except Exception as e:
            log_send_event(f"IMAP: Error processing result: {e}")
            traceback.print_exc()
    
    # Run the result processing loop from imap_runtime
    try:
        await imap_runtime.process_imap_results_loop(process_result_callback)
    except Exception as e:
        log_send_event(f"IMAP: Result processor error: {e}")


async def _imap_watchdog():
    """
    IMAP watchdog - now a stub (imap_runtime manages its own processes).
    Kept for compatibility but does nothing.
    """
    while True:
        try:
            await asyncio.sleep(30.0)
            # The new imap_runtime module manages its own processes
            # No watchdog needed
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_send_event(f"IMAP watchdog error: {e}")
            await asyncio.sleep(10.0)

# ===== END NEW IMAP RUNTIME WRAPPERS =====

def get_account_proxy(user_id: int, email: str) -> Optional[Dict[str, Any]]:
    """
    Получает закрепленный прокси для аккаунта (для использования в SMTP отправках).
    Возвращает None, если прокси не закреплен.
    """
    try:
        st = ensure_user_imap_status(user_id)
        # Используем синхронный доступ к данным (без async with, т.к. это может быть вызвано из синхронного кода)
        # Но нужно быть осторожным - лучше использовать async версию
        proxy_map = st.account_status.get("_proxy_map", {})
        return proxy_map.get(email)
    except Exception:
        return None

async def get_account_proxy_async(user_id: int, email: str) -> Optional[Dict[str, Any]]:
    """
    Асинхронная версия получения закрепленного прокси для аккаунта.
    """
    try:
        st = ensure_user_imap_status(user_id)
        async with st.lock:
            proxy_map = st.account_status.get("_proxy_map", {})
            return proxy_map.get(email)
    except Exception:
        return None

async def _schedule_all_active_accounts(uid: int, chat_id: int):
    """
    Сканирует активные аккаунты пользователя и запускает процессы IMAP для каждого.
    """
    st = ensure_user_imap_status(uid)
    async with st.lock:
        st.running = True
        st.account_status.setdefault("_meta", {})["chat_id"] = chat_id

    accounts = await list_accounts_async(uid)
    active = [a for a in accounts if getattr(a, "active", False) and getattr(a, "email", "")]
    async with st.lock:
        st.accounts = {a.email: a for a in active}
        st.last_accounts_check = time.time()

    # Получаем контекст для прокси
    ctx = await get_user_ctx_async(uid)
    
    # Запускаем процесс для каждого активного аккаунта
    for a in active:
        try:
            # Получаем прокси для аккаунта (ОБЯЗАТЕЛЬНО)
            proxy = None
            try:
                proxy = smtp25.get_next_proxy_ctx(ctx, "send")
            except Exception as e:
                log_send_event(f"IMAP: Failed to get proxy for uid={uid} acc_id={getattr(a, 'id')} email={getattr(a, 'email', '')}: {e}")
            
            # Проверка наличия прокси перед запуском
            if not proxy:
                log_send_event(f"IMAP: Skipping account uid={uid} acc_id={getattr(a, 'id')} email={getattr(a, 'email', '')}: no proxy available (proxy is required)")
                # Обновляем статус аккаунта - отключаем чтение из-за отсутствия прокси
                st = ensure_user_imap_status(uid)
                async with st.lock:
                    st.account_status.setdefault(getattr(a, "email", ""), {})
                    st.account_status[getattr(a, "email", "")].update({
                        "active": False,
                        "last_err": "No proxy available (proxy is required for IMAP)",
                    })
                continue
            
            # Запускаем процесс (прокси уже проверен в start_imap_process, но проверяем здесь для логирования)
            # ВАЖНО: start_imap_process сам обновляет st.accounts и st.account_status, поэтому здесь не нужно дублировать
            success = await start_imap_process(
                user_id=uid,
                acc_id=int(getattr(a, "id")),
                email=getattr(a, "email", ""),
                password=getattr(a, "password", ""),
                display_name=getattr(a, "display_name", "") or getattr(a, "name", "") or "",
                chat_id=chat_id,
                proxy=proxy
            )
            if not success:
                log_send_event(f"IMAP: Failed to start process uid={uid} acc_id={getattr(a, 'id')} email={getattr(a, 'email', '')} (check logs above for reason)")
        except Exception as e:
            log_send_event(f"IMAP: Exception starting process uid={uid} acc_id={getattr(a, 'id')}: {e}")

def resolve_imap_host(email_addr: str) -> str:
    domain = (email_addr.split("@", 1)[1] if "@" in email_addr else "").lower()
    if domain in IMAP_HOST_MAP:
        return IMAP_HOST_MAP[domain]
    return f"imap.{domain}" if domain else "imap.gmail.com"

def _decode_header(s: Optional[str]) -> str:
    if not s:
        return ""
    try:
        decoded = str(make_header(decode_header(s)))
    except Exception:
        decoded = s
    # Разрешаем HTML‑сущности вида &#x2F; -> /
    try:
        from html import unescape as _unescape
        return _unescape(decoded)
    except Exception:
        return decoded

def _extract_text_from_html(html_text: str) -> str:
    """
    Извлекает чистый текст из HTML, обрабатывает HTML entities и удаляет подписи.
    """
    import html as html_module
    from html.parser import HTMLParser
    
    class TextExtractor(HTMLParser):
        def __init__(self):
            super().__init__()
            self.text_parts = []
            self.in_script = False
            self.in_style = False
            
        def handle_starttag(self, tag, attrs):
            tag_lower = tag.lower()
            if tag_lower in ('script', 'style'):
                if tag_lower == 'script':
                    self.in_script = True
                else:
                    self.in_style = True
            # Обрабатываем br как самозакрывающийся тег
            elif tag_lower == 'br':
                self.text_parts.append('\n')
                
        def handle_endtag(self, tag):
            tag_lower = tag.lower()
            if tag_lower in ('script', 'style'):
                if tag_lower == 'script':
                    self.in_script = False
                else:
                    self.in_style = False
            # Добавляем перенос строки для блочных элементов
            elif tag_lower in ('div', 'p', 'li', 'ul', 'ol', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'tr', 'blockquote', 'pre'):
                self.text_parts.append('\n')
            elif tag_lower in ('td', 'th'):
                # Для ячеек таблицы добавляем табуляцию вместо переноса
                self.text_parts.append('\t')
            elif tag_lower == 'br':
                # br может быть и закрывающим тегом
                self.text_parts.append('\n')
                
        def handle_data(self, data):
            # Сохраняем все данные, кроме скриптов и стилей
            # Подписи будем фильтровать позже по содержимому, а не по тегам
            if not self.in_script and not self.in_style:
                self.text_parts.append(data)
    
    try:
        # Декодируем HTML entities
        html_text = html_module.unescape(html_text)
        
        # Извлекаем текст
        parser = TextExtractor()
        parser.feed(html_text)
        text = ''.join(parser.text_parts)
        
        # Удаляем HTML теги простым regex (на случай если парсер что-то пропустил)
        text = re.sub(r'<[^>]+>', ' ', text)
        
        # Обрабатываем HTML entities и пробелы (сохраняем переносы строк)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        # Заменяем множественные пробелы на один (но не трогаем переносы строк)
        text = re.sub(r'[ \t]+', ' ', text)  # Только пробелы и табы, не переносы
        # Нормализуем переносы строк: удаляем пробелы вокруг переносов, но сохраняем сами переносы
        text = re.sub(r'[ \t]*\n[ \t]*', '\n', text)
        # Удаляем только избыточные пустые строки (более 2 подряд)
        text = re.sub(r'\n{3,}', '\n\n', text)
        # Убираем пробелы в начале и конце строк, но сохраняем структуру
        lines = text.split('\n')
        text = '\n'.join(line.rstrip() for line in lines)
        
        # Удаляем подписи GMX (разные варианты)
        # Ищем подписи только в конце текста (последние 5 строк)
        lines = text.split('\n')
        if len(lines) > 5:
            # Разделяем на основной текст и возможную подпись (последние 5 строк)
            main_lines = lines[:-5]
            signature_candidate = lines[-5:]
            
            # Проверяем, есть ли маркеры подписи в последних строках
            signature_text = '\n'.join(signature_candidate).lower()
            has_signature = any(marker in signature_text for marker in [
                'gesendet mit der gmx',
                'sent with gmx',
                'gmx mail app',
            ])
            
            # Если нашли подпись, удаляем последние строки
            if has_signature:
                # Находим где начинается подпись (обычно после "--" или пустой строки)
                sig_start = len(main_lines)
                for i in range(len(signature_candidate) - 1, -1, -1):
                    line_lower = signature_candidate[i].lower().strip()
                    if any(marker in line_lower for marker in [
                        'gesendet mit der gmx',
                        'sent with gmx',
                        'gmx mail app',
                    ]):
                        # Ищем начало подписи (обычно "--" на отдельной строке перед этим)
                        for j in range(i - 1, -1, -1):
                            if signature_candidate[j].strip() == '--':
                                sig_start = len(main_lines) + j
                                break
                        break
                
                filtered_lines = lines[:sig_start] if sig_start < len(lines) else main_lines
            else:
                filtered_lines = lines
        else:
            # Если текста мало (5 строк или меньше), более осторожно обрабатываем подписи
            # Пропускаем удаление подписи, если текст очень короткий (меньше 3 строк)
            # чтобы не удалить весь контент
            if len(lines) < 3:
                # Очень короткий текст - не удаляем подписи, чтобы не потерять контент
                filtered_lines = lines
            else:
                # Проверяем наличие подписи в конце
                text_lower = text.lower()
                has_signature_marker = any(marker in text_lower for marker in [
                    'gesendet mit der gmx',
                    'sent with gmx',
                    'gmx mail app',
                ])
                
                if has_signature_marker:
                    # Ищем подпись с конца текста
                    filtered_lines = []
                    sig_start_idx = len(lines)
                    
                    # Проверяем последние 3 строки на наличие подписи
                    for i in range(len(lines) - 1, max(-1, len(lines) - 4), -1):
                        line_lower = lines[i].lower().strip()
                        if any(marker in line_lower for marker in [
                            'gesendet mit der gmx',
                            'sent with gmx',
                            'gmx mail app',
                        ]):
                            # Нашли подпись, ищем ее начало
                            sig_start_idx = i
                            # Проверяем, есть ли "--" перед подписью
                            if i > 0 and lines[i-1].strip() == '--':
                                sig_start_idx = i - 1
                            break
                    
                    # Если подпись найдена в конце, удаляем только её
                    if sig_start_idx < len(lines):
                        filtered_lines = lines[:sig_start_idx]
                    else:
                        filtered_lines = lines
                else:
                    filtered_lines = lines
        
        text = '\n'.join(filtered_lines).strip()
        return text
    except Exception:
        # Fallback: простое удаление тегов и декодирование entities
        try:
            text = html_module.unescape(html_text)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'&nbsp;', ' ', text)
            text = re.sub(r'\s+', ' ', text)
            return text.strip()
        except Exception:
            return html_text

def _extract_body(msg) -> str:
    text_parts = []
    html_parts = []
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disp = str(part.get("Content-Disposition") or "")
            if "attachment" in disp.lower():
                continue
            try:
                payload = part.get_payload(decode=True) or b""
                text = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
            except Exception:
                continue
            if ctype == "text/plain":
                text_parts.append(text)
            elif ctype == "text/html":
                html_parts.append(re.sub(r"<[^>]+>", " ", text))
    else:
        try:
            payload = msg.get_payload(decode=True) or b""
            text = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
            if msg.get_content_type() == "text/plain":
                text_parts.append(text)
            else:
                html_parts.append(re.sub(r"<[^>]+>", " ", text))
        except Exception:
            pass
    body = "\n".join(text_parts) if text_parts else "\n".join(html_parts)
    body = re.sub(r"\s+\n", "\n", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    return body[:3500]

def _get_or_connect_imap(
    ctx: "smtp25.UserContext",
    acc: Any,
    timeout: int,
    prev_imap: imaplib.IMAP4 | None,
    sticky_proxy: dict | None,
) -> tuple[imaplib.IMAP4, str, dict]:
    """
    Возвращает рабочее IMAP‑соединение:
      1) если prev_imap жив — используем его (проверка через _imap_alive_and_ready);
      2) иначе пробуем переподключиться через sticky_proxy;
      3) иначе берём новый SEND‑прокси из ctx.
    Возвращает (imap, via_descr, used_proxy).
    """
    host = resolve_imap_host(getattr(acc, "email", "") or "")

    # 1) reuse (проверка «живости» — теперь лёгкая: NOOP, SELECT только как fallback)
    if prev_imap and _imap_alive_and_ready(prev_imap):
        return prev_imap, "reuse", (sticky_proxy or {})

    # 2) reconnect via sticky proxy
    if sticky_proxy:
        try:
            imap, via_descr = _connect_imap_via_proxy(host, acc.email, acc.password, sticky_proxy, timeout)
            return imap, via_descr, sticky_proxy
        except Exception:
            pass  # попробуем свежий прокси

    # 3) pick new proxy from ctx
    last_err: Exception | None = None
    for _ in range(3):
        try:
            proxy = smtp25.get_next_proxy_ctx(ctx, "send")
            if not proxy:
                raise RuntimeError("No SEND proxy available")
            imap, via_descr = _connect_imap_via_proxy(host, acc.email, acc.password, proxy, timeout)
            return imap, via_descr, proxy
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    raise RuntimeError(f"IMAP connect failed: {type(last_err).__name__}: {last_err}")
    
def _incoming_is_dsn_failure(subj: str) -> bool:
    return (subj or "").strip().lower() == "delivery status notification (failure)"






def is_automated_sender_email(email: str) -> bool:
    """
    Проверка, является ли отправитель автоматическим (AI/робот).
    Возвращает True для адресов типа noreply@google.com, no-reply@..., automated@... и т.д.
    """
    if not email or "@" not in email:
        return False
    
    email_lower = email.lower().strip()
    
    # Список паттернов для автоматических отправителей (проверяется в локальной части email до @)
    automated_patterns = [
        "noreply",
        "no-reply",
        "no_reply",
        "donotreply",
        "do-not-reply",
        "do_not_reply",
        "automated",
        "auto-reply",
        "auto_reply",
        "autoreply",
        "mailer-daemon",
        "mailerdaemon",
        "mailer_daemon",
        "postmaster",
        "daemon",
        "bounce",
        "bounced",
        "undeliverable",
        "undelivered",
        "delivery-failure",
        "delivery_failure",
        "deliveryfailure",
        "mail-delivery",
        "mail_delivery",
        "system",
        "support",
        "notifications",
        "notification",
        "alert",
        "alerts",
        "service",
        "services",
    ]
    
    # Паттерны, которые должны быть точным совпадением локальной части (не подстрока)
    exact_match_patterns = [
        "noreply",
        "no-reply",
        "postmaster",
        "mailer-daemon",
        "mailerdaemon",
    ]
    
    # Проверка паттернов в локальной части email (до @)
    local_part = email_lower.split("@", 1)[0]
    
    # Сначала проверяем точные совпадения
    for pattern in exact_match_patterns:
        if local_part == pattern:
            return True
    
    # Затем проверяем паттерны как подстроки
    for pattern in automated_patterns:
        if pattern in local_part:
            return True
    
    # Список конкретных доменов для автоматических писем
    automated_domains = [
        "mailer-daemon",
        "mailerdaemon",
    ]
    
    # Проверка домена (после @)
    domain_part = email_lower.split("@", 1)[1] if "@" in email_lower else ""
    for domain in automated_domains:
        if domain in domain_part:
            return True
    
    # Конкретные адреса (точное совпадение)
    specific_automated_emails = [
        "noreply@google.com",
        "no-reply@google.com",
        "noreply@accounts.google.com",
        "no-reply@accounts.google.com",  # Добавлено для фильтрации
        "noreply@mail.google.com",
        "noreply@gmail.com",
        "mailer-daemon@googlemail.com",
        "mailer-daemon@gmail.com",
    ]
    
    if email_lower in specific_automated_emails:
        return True
    
    return False


async def publish_incoming_to_chat_async(
    user_id: int,
    acc,
    chat_id: int,
    mdat: dict
) -> None:
    """
    Публикация одного входящего письма в чат + сохранение в БД + клавиатуры.
    ВНИМАНИЕ: вложение HTML отключено по требованию (ускорение публикации).
    Пропускает автоматические письма от AI/роботов (noreply@google.com и т.д.).
    """
    def format_body_with_quote(body: str) -> str:
        import re
        if not body:
            return ""
        b = re.sub(r"\r\n", "\n", body)
        b = re.sub(r"\u00A0", " ", b)
        b = re.sub(r"\n{3,}", "\n\n", b)
        patterns = [
            r"\n(On .+?wrote:)",
            r"\n([^\n]+wrote on .+?:)",
            r"\n(Am .+?schrieb .+?:)",
            r"\n([^\n]+<[^>]+>@[^>]+> schrieb am .+?:)",
            r"\n([^\n]*\b(?:пн|вт|ср|чт|пт|сб|вс)\b[^\n]*<[^>]+>:\s*)",
            r"\n([^\n]*<[^>\n]+>@?[^>\n]*>:\s*)",
        ]
        for pat in patterns:
            m = re.search(pat, b, flags=re.IGNORECASE)
            if m:
                split_idx = m.start(1)
                main_text = b[:split_idx].rstrip()
                quote_text = b[split_idx:].lstrip()
                if main_text.endswith("\n\n"):
                    return f"{main_text}{quote_text}"
                if main_text.endswith("\n"):
                    return f"{main_text}\n{quote_text}"
                return f"{main_text}\n\n{quote_text}"
        return b.strip()

    def extract_offer_subject(subj: str) -> str:
        import re, html as _html
        s = _html.unescape(subj or "").strip()
        while True:
            s2 = re.sub(r'^(?:(?:re|fw|fwd)\s*:)\s*', '', s, flags=re.I)
            if s2 == s:
                break
            s = s2.strip()
        if "?" in s:
            s = s.split("?")[-1]
        elif ":" in s:
            s = s.split(":")[-1]
        s = re.sub(r'^[\-\—\:\.\s]+', '', s).strip()
        s = re.sub(r'\s{2,}', ' ', s)
        return s or (subj or "")

    from_email = mdat.get("from_email") or ""
    from_name  = mdat.get("from_name") or ""
    subject    = mdat.get("subject")    or ""
    body       = mdat.get("body")       or ""
    uid_str    = mdat.get("uid")        or ""
    
    # Пропускаем автоматические письма от AI/роботов (noreply@google.com и т.д.)
    if is_automated_sender_email(from_email):
        log_send_event(
            f"IMAP: Пропуск автоматического письма от {from_email} "
            f"uid={user_id} acc_id={getattr(acc, 'id', '?')} subject={subject[:50]}"
        )
        return
    
    # Проверка на блокировку: Delivery Status Notification (Failure) или ** Message blocked **
    subject_lower = subject.lower()
    body_lower = body.lower()
    is_blocked = False
    block_reason = ""
    
    if "delivery status notification (failure)" in subject_lower:
        is_blocked = True
        block_reason = "Delivery Status Notification (Failure)"
    elif "** message blocked **" in body_lower:
        is_blocked = True
        block_reason = "** Message blocked **"
    
    if is_blocked:
        # ВАЖНО: Отключаем аккаунт для массового сендинга ПРИНУДИТЕЛЬНО
        # Даже если аккаунт был вручную включен через /sendacc, при получении нового письма с такой темой
        # он должен быть отключен снова (принудительно, независимо от текущего состояния)
        try:
            acc_id = int(getattr(acc, "id"))
            acc_email = getattr(acc, 'email', '')
            
            # Загружаем текущее состояние (на случай если аккаунт был включен вручную)
            await ensure_send_disabled_loaded(user_id)
            disabled = SEND_DISABLED_ACCOUNTS.setdefault(user_id, set())
            
            # Проверяем, был ли аккаунт включен до этого
            was_enabled = acc_id not in disabled
            
            # ПРИНУДИТЕЛЬНО отключаем аккаунт (независимо от текущего состояния)
            disabled.add(acc_id)  # Добавляем в кэш (если уже был там - останется, если нет - добавится)
            await set_setting_async(user_id, f"send_disabled_{acc_id}", "1")  # Сохраняем в БД
            
            # ВАЖНО: Инвалидируем контекст пользователя, чтобы при следующем запуске сендинга
            # использовался обновленный список аккаунтов без отключенного аккаунта
            try:
                invalidate_user_ctx(user_id)
            except Exception as ctx_err:
                log_send_event(f"IMAP: ошибка при инвалидации контекста uid={user_id}: {ctx_err}")
            
            if was_enabled:
                log_send_event(f"IMAP: обнаружено письмо {block_reason}, аккаунт {acc_email} (acc_id={acc_id}) ПРИНУДИТЕЛЬНО отключен для массового сендинга (был включен вручную)")
            else:
                log_send_event(f"IMAP: обнаружено письмо {block_reason}, аккаунт {acc_email} (acc_id={acc_id}) ПРИНУДИТЕЛЬНО отключен для массового сендинга")
        except Exception as e:
            log_send_event(f"IMAP: ошибка при отключении аккаунта для массового сендинга uid={user_id} acc_id={getattr(acc, 'id', 'unknown')}: {e}")
        
        # Не публикуем сообщение, только логируем
        return
    
    # Subject для показа
    subject_display = extract_offer_subject(subject)

    # reply_to: тред на последнее исходящее
    reply_to_mid = None
    try:
        reply_to_mid = int(
            THREAD_LAST_OUT.get((user_id, int(getattr(acc, "id")), from_email)) or 0
        )
        if reply_to_mid <= 0:
            reply_to_mid = None
    except Exception:
        reply_to_mid = None

    fio_display = ((getattr(acc, "display_name", "") or getattr(acc, "name", "") or "").strip())
    if not fio_display:
        try:
            fio_display = (getattr(acc, "email", "") or "").split("@", 1)[0]
        except Exception:
            fio_display = ""

    text = (
        f"💸 Получено сообщение на {code(getattr(acc, 'email', ''))} от {code(from_email)}\n"
        f"({code(from_name)} &lt;{code(from_email)}&gt;)\n"
        f"ФИО: {code(fio_display)}\n\n"
        f"Тема:\n{code(subject_display)}\n\n"
        f"Текст:\n{code(format_body_with_quote(body))}"
    )

    kb_initial = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✉️ Ответить", callback_data="reply:msg"),
        InlineKeyboardButton(text="Создать ссылку", callback_data=f"adlink:create:0")
    ]])

    # Сообщение в чат
    tg_msg = await bot.send_message(
        chat_id,
        text,
        reply_to_message_id=reply_to_mid,
        reply_markup=kb_initial
    )

    # runtime-трек
    try:
        INCOMING_RT[(user_id, int(tg_msg.message_id))] = {
            "acc_id": int(getattr(acc, "id")),
            "from_email": from_email,
            "from_name": from_name,
            "subject": subject,
            "created_ts": time.time(),  # ВАЖНО: сохраняем timestamp создания для правильной очистки
        }
    except Exception:
        pass

    # Второй проход клавиатуры (оставляем)
    try:
        kb2 = await build_incoming_reply_kb_async(chat_id, tg_msg.message_id)
        await safe_edit_reply_markup(chat_id, tg_msg.message_id, kb2)
    except Exception as e_kb:
        log_send_event(f"KB second edit failed chat={chat_id} mid={tg_msg.message_id}: {e_kb}")

    # Вложение HTML — УДАЛЕНО по требованию (раньше здесь отправлялся документ)

    # Сохранение в БД (если новый UID)
    try:
        exists = await incoming_message_exists_async(acc.id, uid_str)
        if not exists:
            await add_incoming_message_async(
                user_id=user_id,
                account_id=acc.id,
                uid=uid_str,
                from_name=from_name,
                from_email=from_email,
                subject=subject,
                body=body,
                tg_message_id=tg_msg.message_id
            )
    except Exception as e_db:
        log_send_event(f"DB SAVE ERROR incoming uid={user_id} acc={acc.id} tg_mid={tg_msg.message_id}: {e_db}")

    # Пин (оставляем)
    try:
        await bot.pin_chat_message(chat_id, tg_msg.message_id, disable_notification=True)
    except Exception:
        pass
    
    # Запуск ИИ-сценария с проверкой кэша (только для новых отправителей)
    try:
        await ai_autostart_if_allowed(
            user_id,
            from_email,
            maybe_schedule_ai_assistant,
            user_id,
            chat_id,
            acc,
            tg_msg.message_id,
            from_email,
            subject
        )
    except Exception as e:
        log_send_event(f"AI autostart error in publish_incoming uid={user_id} from={from_email}: {e}")
        
# ====== УДАЛЕНО: Старая логика IMAP (async воркеры) ======
# Используется только новая архитектура с process pool
# Все старые функции удалены:
# - async def _refresh_active_accounts_for_user - УДАЛЕНО
# - async def _pick_next_email - УДАЛЕНО
# - async def _user_imap_worker - УДАЛЕНО


# Старая функция _sync_imap_fetch удалена - используется process pool архитектура

# Старая функция fetch_and_post_new_mails удалена - используется process pool архитектура
# Логика публикации сообщений перенесена в _process_imap_results
        
@dp.callback_query(F.data.startswith("adlink:create:"))
async def adlink_create_cb(c: types.CallbackQuery):
    """
    Генерация ссылки с тремя логиками:
      - NurPaypal (nur) — использует goo_worker_key, goo_team_key, goo_profile_id
      - Dolce — новая логика по документу /custom-api/create-link-url (только для админа)
        Использует dolce_team_base, dolce_worker_token (не использует profileID)
      - Aqua team — логика как у Goo (для всех)
        Использует aqua_worker_key, aqua_team_key, aqua_profile_id

    Правила:
      - Только админ может выбрать Dolce (через «🛠 Команда: …»).
      - Для не-админов доступны nur и aqua_team (dolce недоступна).
      - Храним и показываем токены и профили раздельно для каждой команды.
      - Переключение команды не стирает значения токенов и профилей.
      - При генерации ссылки используются токены и профиль выбранной команды.
    """
    if not await ensure_approved(c):
        return

    await safe_cq_answer(c)  # ранний ACK

    import json, re, unicodedata, asyncio, urllib.parse

    chat_id = c.message.chat.id
    uid = await U(c)
    admin = is_admin(c.from_user.id)
    team_mode_raw = await get_setting_async(uid, "team_mode", "nur")
    team_mode = (team_mode_raw or "nur").strip().lower()
    # Нормализуем значение team_mode (заменяем пробелы на подчеркивания, приводим к нижнему регистру)
    team_mode = team_mode.replace(" ", "_").replace("-", "_")
    # Проверяем и нормализуем значение
    if "aqua" in team_mode and "team" in team_mode:
        team_mode = "aqua_team"
    elif team_mode == "aquateam" or team_mode == "aqua_team":
        team_mode = "aqua_team"
    elif "nur" in team_mode or team_mode == "nurpaypal":
        team_mode = "nur"
    elif team_mode == "dolce":
        team_mode = "dolce"
    else:
        # Если значение некорректное, сбрасываем на nur
        team_mode = "nur"
        await set_setting_async(uid, "team_mode", "nur")
    # Для не-админов: если установлен dolce, сбрасываем на nur (dolce только для админа)
    if not admin and team_mode == "dolce":
        team_mode = "nur"
        await set_setting_async(uid, "team_mode", "nur")
    
    # Отладочное логирование (можно убрать после проверки)
    import sys
    try:
        print(f"[DEBUG adlink_create] uid={uid}, team_mode_raw='{team_mode_raw}', team_mode='{team_mode}', admin={admin}", file=sys.stderr, flush=True)
    except:
        pass

    # Вытаскиваем исходный mid
    try:
        origin_mid = int(c.data.split(":")[2])
    except Exception:
        origin_mid = c.message.message_id

    # Контекст входящего
    rt = INCOMING_RT.get((uid, origin_mid)) or INCOMING_RT.get((uid, c.message.message_id))
    if not rt:
        await safe_cq_answer(c, "Нет данных письма", show_alert=True)
        return

    from_email = (rt.get("from_email") or "").strip()
    if "@" not in from_email:
        await safe_cq_answer(c, "Email некорректен", show_alert=True)
        return
    local_part = from_email.split("@", 1)[0]

    def _n(s: str) -> str:
        s = (s or "").replace("\u00A0", " ")
        s = unicodedata.normalize("NFKC", s)
        s = s.replace(".", " ").replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s.strip().lower())
        return s

    k_local = _n(local_part)
    ad_id = AD_LOCAL2ID_PER_CHAT.get(chat_id, {}).get(k_local)
    if not ad_id:
        await safe_cq_answer(c, "ID не найден", show_alert=True)
        return

    ad_entry = AD_ADS_BY_ID_PER_CHAT.get(chat_id, {}).get(ad_id)
    if not ad_entry:
        await safe_cq_answer(c, "Объявление не найдено", show_alert=True)
        return

    original = ad_entry["link"]

    gen_for_chat = AD_GENERATED_LINKS_PER_CHAT.setdefault(chat_id, {})
    if k_local in gen_for_chat:
        entry = gen_for_chat[k_local]
        mid = int(entry.get("result_msg_id") or 0)
        if mid:
            try:
                await bot.send_message(chat_id, "⬆️ Уже есть", reply_to_message_id=mid)
            except Exception:
                pass
        await safe_cq_answer(c, "Уже создано")
        return

    # Функция метаданных (название/цена/фото)
    title, price, photo_url = await fetch_ad_metadata(original)

    # ==== ВЕТКА DOLCE (доступна только админу при выбранной команде 'dolce') ====
    # Использует свои токены (dolce_team_base, dolce_worker_token)
    # Dolce не использует profileID (использует другую логику API)
    if admin and team_mode == "dolce":
        team_base = (await get_setting_async(uid, "dolce_team_base", "")).strip()
        worker_token = (await get_setting_async(uid, "dolce_worker_token", "")).strip()

        if not team_base or not worker_token:
            await safe_cq_answer(c, "Dolce: не задан base URL или token", show_alert=True)
            return

        # Сборка endpoint
        base = team_base.rstrip("/")
        endpoint = f"{base}/custom-api/create-link-url"

        # Формируем payload согласно описанию:
        # - token (из «воркера»)
        # - url (оригинал из кэша)
        # - link_data.settings.subtype = "2.0" (всегда)
        # - service — всегда "kleinanzeigen.de"
        payload = {
            "token": worker_token,
            "url": original,
            "service": "kleinanzeigen.de",  # всегда
            "link_data": {
                "id": ad_id,  # не обязательно
                "title": title or None,
                # price по доке — число, уберём символы и попытаемся сконвертировать
                "price": (lambda p: (float(re.sub(r"[^\d.,]", "", p).replace(",", "."))
                                     if p else None))(price),
                "image_url": photo_url or None,
                "settings": {
                    "subtype": "2.0"  # всегда
                }
            }
        }

        # Чистим None, чтобы не слать пустые поля
        def _strip_none(obj):
            if isinstance(obj, dict):
                return {k: _strip_none(v) for k, v in obj.items() if v is not None}
            if isinstance(obj, list):
                return [_strip_none(v) for v in obj if v is not None]
            return obj

        payload = _strip_none(payload)

        session = await get_http_session()
        short_link = ""
        last_raw = ""
        try:
            async with session.post(endpoint, json=payload, timeout=30) as resp:
                last_raw = await resp.text()
                if 200 <= resp.status < 300:
                    try:
                        data = json.loads(last_raw)
                    except Exception:
                        data = {}

                    # Пытаемся извлечь короткую ссылку
                    short_link = (
                        str(data.get("short") or
                            data.get("short_url") or
                            data.get("url") or
                            data.get("link") or "").strip()
                    )
                    if not short_link:
                        inner = data.get("data") if isinstance(data, dict) else None
                        if isinstance(inner, dict):
                            short_link = str(
                                inner.get("short") or inner.get("short_url") or inner.get("url") or inner.get("link") or ""
                            ).strip()

                if not short_link:
                    await safe_cq_answer(c, "Dolce: ошибка API", show_alert=True)
                    diag = (f"Dolce не вернул ссылку.\nstatus={resp.status}\nresp={last_raw[:1500]}")
                    try:
                        await bot.send_message(chat_id, diag[:3800], reply_to_message_id=origin_mid)
                    except Exception:
                        pass
                    return
        except asyncio.TimeoutError:
            await safe_cq_answer(c, "Dolce: timeout", show_alert=True)
            return
        except Exception as e:
            await safe_cq_answer(c, f"Dolce: {type(e).__name__}", show_alert=True)
            return

        # Сохраняем как в Goo
        gen_for_chat[k_local] = {
            "ad_id": ad_id,
            "short": short_link,
            "original": original,
            "title": title,
            "price": price,
            "photo_url": photo_url,
            "result_msg_id": 0,
            "service": "dolce",
            "profile_id": "",
            "ts": time.time(),
        }
        AD_CHAT_TS[chat_id] = time.time()

        # Оформление лога — как в Goo
        def _cv(v: str) -> str:
            return f"<code>{tg(v)}</code>" if v else ""

        caption_parts = []
        if title:
            caption_parts.append(f"Название: {_cv(title)}")
        if price:
            caption_parts.append(f"Цена: {_cv(price)}")
        caption_parts.append(f"Ссылка: {_cv(short_link)}")
        caption = "\n".join(caption_parts)

        polya_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Polya (5%)", callback_data=f"polya:ask:{ad_id}:{k_local}")]
            ]
        )

        sent_photo = False
        if photo_url:
            try:
                session = await get_http_session()
                async with session.get(photo_url, timeout=15) as r:
                    if r.status == 200:
                        img = await r.read()
                        pfile = types.BufferedInputFile(img, filename="ad.jpg")
                        pmsg = await bot.send_photo(
                            chat_id,
                            photo=pfile,
                            caption=caption,
                            reply_to_message_id=origin_mid,
                            reply_markup=polya_kb
                        )
                        gen_for_chat[k_local]["result_msg_id"] = pmsg.message_id
                        sent_photo = True
            except Exception:
                pass

        if not sent_photo:
            tmsg = await bot.send_message(
                chat_id,
                caption,
                reply_to_message_id=origin_mid,
                reply_markup=polya_kb
            )
            gen_for_chat[k_local]["result_msg_id"] = tmsg.message_id

        try:
            base_mid = origin_mid or c.message.message_id
            kb_new = await build_incoming_reply_kb_async(chat_id, base_mid)
            await safe_edit_reply_markup(chat_id, base_mid, kb_new)
        except Exception:
            pass

        await save_ad_cache_async(chat_id)
        await safe_cq_answer(c, "Создано")
        return

    # ==== ВЕТКА AQUA TEAM (логика как у Goo; для всех, если выбран aqua_team) ====
    # Использует свои токены (aqua_worker_key, aqua_team_key) и свой профиль (aqua_profile_id)
    # ВАЖНО: Проверка должна быть ПЕРЕД веткой GOO/NUR, чтобы не попасть в ветку GOO по умолчанию
    # Использует свой эндпоинт: https://api-aq.goo.network/api/generate/single/parse
    # Проверяем явно на aqua_team (после нормализации должно быть именно это значение)
    # Используем более гибкую проверку на случай разных вариантов написания
    is_aqua_team = (team_mode == "aqua_team" or 
                    team_mode == "aquateam" or 
                    (team_mode and "aqua" in team_mode.lower() and "team" in team_mode.lower()))
    
    if is_aqua_team:
        # Нормализуем на случай, если значение было в другом формате
        team_mode = "aqua_team"
        try:
            print(f"[DEBUG adlink_create] ВЕТКА AQUA TEAM выбрана для uid={uid}, team_mode='{team_mode}'", file=sys.stderr, flush=True)
        except:
            pass
        user_key = (await get_setting_async(uid, "aqua_worker_key", "")).strip()
        team_key = (await get_setting_async(uid, "aqua_team_key", "")).strip()
        profile_id = (await get_setting_async(uid, "aqua_profile_id", "")).strip()
        if not user_key or not team_key or not profile_id:
            await safe_cq_answer(c, "Токены или profileID Aqua team не заданы", show_alert=True)
            return

        def _extract_domain(u: str) -> str:
            try:
                return urllib.parse.urlparse(u).netloc.lower()
            except Exception:
                return ""

        domain = _extract_domain(original)
        if "kleinanzeigen" in domain:
            services = ["kleinanzeigen_de", "ebay_kleinanzeigen_de", "ebay_de"]
        elif "ebay." in domain:
            services = ["ebay_de"]
        else:
            services = [GOO_DEFAULT_SERVICE]
        if GOO_DEFAULT_SERVICE not in services:
            services.append(GOO_DEFAULT_SERVICE)

        # Кэш Aqua team (используем тот же кэш, но с префиксом)
        cache_key = (original, profile_id, tuple(services), "aqua_team")
        try:
            cached = GOO_LINK_CACHE.get(cache_key)  # type: ignore[name-defined]
        except Exception:
            cached = None
        if cached:
            short_link, last_service = cached
        else:
            endpoint = "https://api-aq.goo.network/api/generate/single/parse"
            headers = {
                "Authorization": f"Apikey {user_key}",
                "X-Team-Key": team_key,
                "Host": "api-aq.goo.network",
                "Content-Type": "application/json",
            }

            session = await get_http_session()

            async def one_service(sv: str):
                payload = {
                    "service": sv,
                    "url": original,
                    "isNeedBalanceChecker": False,
                    "profileID": profile_id
                }
                try:
                    async with session.post(endpoint, headers=headers, json=payload, timeout=20) as resp:
                        raw = await resp.text()
                        if 200 <= resp.status < 300:
                            try:
                                data = json.loads(raw)
                            except Exception:
                                data = {}
                            if data.get("status") is True and data.get("message"):
                                return sv, str(data["message"]).strip(), resp.status, raw[:1500]
                        return sv, "", resp.status, raw[:1500]
                except asyncio.TimeoutError:
                    return sv, "", "timeout", ""
                except Exception as e_req:
                    return sv, "", f"exc:{type(e_req).__name__}", str(e_req)[:1500]

            tasks = [asyncio.create_task(one_service(sv)) for sv in services]
            short_link = ""
            last_status = None
            last_raw = ""
            last_service = ""
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=25)
                for d in done:
                    sv, link, st, raw = await d
                    last_status = st; last_raw = raw; last_service = sv
                    if link:
                        short_link = link
                        break
                if not short_link:
                    for d in pending:
                        try:
                            sv, link, st, raw = await asyncio.wait_for(d, timeout=10)
                            last_status = st; last_raw = raw; last_service = sv
                            if link:
                                short_link = link
                                break
                        except Exception:
                            pass
            finally:
                for p in tasks:
                    if not p.done():
                        p.cancel()

            if not short_link:
                await safe_cq_answer(c, "Aqua team: ошибка API", show_alert=True)
                diag = (f"Aqua team не вернул ссылку.\nservice={last_service}\nstatus={last_status}\nresp={last_raw}")
                try:
                    await bot.send_message(chat_id, diag[:3800], reply_to_message_id=origin_mid)
                except Exception:
                    pass
                return

            try:
                GOO_LINK_CACHE[cache_key] = (short_link, last_service)  # type: ignore[name-defined]
            except Exception:
                pass

        gen_for_chat[k_local] = {
            "ad_id": ad_id,
            "short": short_link,
            "original": original,
            "title": title,
            "price": price,
            "photo_url": photo_url,
            "result_msg_id": 0,
            "service": last_service,
            "profile_id": profile_id,
            "ts": time.time(),
        }
        AD_CHAT_TS[chat_id] = time.time()

        def _cv(v: str) -> str:
            return f"<code>{tg(v)}</code>" if v else ""

        caption_parts = []
        if title:
            caption_parts.append(f"Название: {_cv(title)}")
        if price:
            caption_parts.append(f"Цена: {_cv(price)}")
        caption_parts.append(f"Ссылка: {_cv(short_link)}")
        caption = "\n".join(caption_parts)

        polya_kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Polya (5%)", callback_data=f"polya:ask:{ad_id}:{k_local}")]
            ]
        )

        sent_photo = False
        if photo_url:
            try:
                session = await get_http_session()
                async with session.get(photo_url, timeout=15) as r:
                    if r.status == 200:
                        img = await r.read()
                        pfile = types.BufferedInputFile(img, filename="ad.jpg")
                        pmsg = await bot.send_photo(
                            chat_id,
                            photo=pfile,
                            caption=caption,
                            reply_to_message_id=origin_mid,
                            reply_markup=polya_kb
                        )
                        gen_for_chat[k_local]["result_msg_id"] = pmsg.message_id
                        sent_photo = True
            except Exception:
                pass

        if not sent_photo:
            tmsg = await bot.send_message(
                chat_id,
                caption,
                reply_to_message_id=origin_mid,
                reply_markup=polya_kb
            )
            gen_for_chat[k_local]["result_msg_id"] = tmsg.message_id

        try:
            base_mid = origin_mid or c.message.message_id
            kb_new = await build_incoming_reply_kb_async(chat_id, base_mid)
            await safe_edit_reply_markup(chat_id, base_mid, kb_new)
        except Exception:
            pass

        await save_ad_cache_async(chat_id)
        await safe_cq_answer(c, "Создано")
        return

    # ==== ВЕТКА GOO/NUR (логика для NurPaypal; используется goo_worker_key, goo_team_key, goo_profile_id) ====
    # Эта ветка выполняется когда team_mode == "nur" или по умолчанию
    # Использует свои токены (goo_worker_key, goo_team_key) и свой профиль (goo_profile_id)
    # Использует свой эндпоинт: https://api.goo.network/api/generate/single/parse
    try:
        print(f"[DEBUG adlink_create] ВЕТКА GOO/NUR выбрана для uid={uid}, team_mode='{team_mode}'", file=sys.stderr, flush=True)
    except:
        pass
    user_key = (await get_setting_async(uid, "goo_worker_key", "")).strip()
    team_key = (await get_setting_async(uid, "goo_team_key", "")).strip()
    profile_id = (await get_setting_async(uid, "goo_profile_id", "")).strip()
    if not user_key or not team_key or not profile_id:
        await safe_cq_answer(c, "Токены или profileID NurPaypal не заданы", show_alert=True)
        return

    def _extract_domain(u: str) -> str:
        try:
            return urllib.parse.urlparse(u).netloc.lower()
        except Exception:
            return ""

    domain = _extract_domain(original)
    if "kleinanzeigen" in domain:
        services = ["kleinanzeigen_de", "ebay_kleinanzeigen_de", "ebay_de"]
    elif "ebay." in domain:
        services = ["ebay_de"]
    else:
        services = [GOO_DEFAULT_SERVICE]
    if GOO_DEFAULT_SERVICE not in services:
        services.append(GOO_DEFAULT_SERVICE)

    # Кэш Goo
    cache_key = (original, profile_id, tuple(services))
    try:
        cached = GOO_LINK_CACHE.get(cache_key)  # type: ignore[name-defined]
    except Exception:
        cached = None
    if cached:
        short_link, last_service = cached
    else:
        endpoint = "https://api.goo.network/api/generate/single/parse"
        headers = {
            "Authorization": f"Apikey {user_key}",
            "X-Team-Key": team_key,
            "Host": "api.goo.network",
            "Content-Type": "application/json",
        }

        session = await get_http_session()

        async def one_service(sv: str):
            payload = {
                "service": sv,
                "url": original,
                "isNeedBalanceChecker": False,
                "profileID": profile_id
            }
            try:
                async with session.post(endpoint, headers=headers, json=payload, timeout=20) as resp:
                    raw = await resp.text()
                    if 200 <= resp.status < 300:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            data = {}
                        if data.get("status") is True and data.get("message"):
                            return sv, str(data["message"]).strip(), resp.status, raw[:1500]
                    return sv, "", resp.status, raw[:1500]
            except asyncio.TimeoutError:
                return sv, "", "timeout", ""
            except Exception as e_req:
                return sv, "", f"exc:{type(e_req).__name__}", str(e_req)[:1500]

        tasks = [asyncio.create_task(one_service(sv)) for sv in services]
        short_link = ""
        last_status = None
        last_raw = ""
        last_service = ""
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=25)
            for d in done:
                sv, link, st, raw = await d
                last_status = st; last_raw = raw; last_service = sv
                if link:
                    short_link = link
                    break
            if not short_link:
                for d in pending:
                    try:
                        sv, link, st, raw = await asyncio.wait_for(d, timeout=10)
                        last_status = st; last_raw = raw; last_service = sv
                        if link:
                            short_link = link
                            break
                    except Exception:
                        pass
        finally:
            for p in tasks:
                if not p.done():
                    p.cancel()

        if not short_link:
            await safe_cq_answer(c, "Ошибка API", show_alert=True)
            diag = (f"Goo не вернул ссылку.\nservice={last_service}\nstatus={last_status}\nresp={last_raw}")
            try:
                await bot.send_message(chat_id, diag[:3800], reply_to_message_id=origin_mid)
            except Exception:
                pass
            return

        try:
            GOO_LINK_CACHE[cache_key] = (short_link, last_service)  # type: ignore[name-defined]
        except Exception:
            pass

    gen_for_chat[k_local] = {
        "ad_id": ad_id,
        "short": short_link,
        "original": original,
        "title": title,
        "price": price,
        "photo_url": photo_url,
        "result_msg_id": 0,
        "service": last_service,
        "profile_id": profile_id,
        "ts": time.time(),
    }
    AD_CHAT_TS[chat_id] = time.time()

    def _cv(v: str) -> str:
        return f"<code>{tg(v)}</code>" if v else ""

    caption_parts = []
    if title:
        caption_parts.append(f"Название: {_cv(title)}")
    if price:
        caption_parts.append(f"Цена: {_cv(price)}")
    caption_parts.append(f"Ссылка: {_cv(short_link)}")
    caption = "\n".join(caption_parts)

    polya_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Polya (5%)", callback_data=f"polya:ask:{ad_id}:{k_local}")]
        ]
    )

    sent_photo = False
    if photo_url:
        try:
            session = await get_http_session()
            async with session.get(photo_url, timeout=15) as r:
                if r.status == 200:
                    img = await r.read()
                    pfile = types.BufferedInputFile(img, filename="ad.jpg")
                    pmsg = await bot.send_photo(
                        chat_id,
                        photo=pfile,
                        caption=caption,
                        reply_to_message_id=origin_mid,
                        reply_markup=polya_kb
                    )
                    gen_for_chat[k_local]["result_msg_id"] = pmsg.message_id
                    sent_photo = True
        except Exception:
            pass

    if not sent_photo:
        tmsg = await bot.send_message(
            chat_id,
            caption,
            reply_to_message_id=origin_mid,
            reply_markup=polya_kb
        )
        gen_for_chat[k_local]["result_msg_id"] = tmsg.message_id

    try:
        base_mid = origin_mid or c.message.message_id
        kb_new = await build_incoming_reply_kb_async(chat_id, base_mid)
        await safe_edit_reply_markup(chat_id, base_mid, kb_new)
    except Exception:
        pass

    await save_ad_cache_async(chat_id)
    await safe_cq_answer(c, "Создано")
    
POLYA_API_KEY = "03ae8669-1c91-49b9-b395-92007f22043c"

@dp.callback_query(F.data.startswith("polya:ask:"))
async def polya_ask_email_cb(c: types.CallbackQuery, state: FSMContext):
    """
    Нажата кнопка Polya (5%) — просим у пользователя email для отправки.
    callback_data формат: polya:ask:{ad_id}:{k_local}
    """
    if not await ensure_approved(c):
        return
    parts = c.data.split(":")
    if len(parts) < 4:
        await c.answer("Некорректные данные", show_alert=True)
        return
    ad_id = parts[2]
    k_local = parts[3]

    gen_entry = AD_GENERATED_LINKS_PER_CHAT.get(c.message.chat.id, {}).get(k_local)
    if not gen_entry or gen_entry.get("ad_id") != ad_id:
        await c.answer("Нет данных объявления", show_alert=True)
        return

    await state.set_state(PolyaFSM.email)
    await state.update_data(polya_ad_id=ad_id, polya_k_local=k_local)
    try:
        await c.message.answer("Введите email для отправки (Polya 5%):")
    except Exception:
        pass
    await safe_cq_answer(c)

@dp.message(PolyaFSM.email)
async def polya_send_email(m: types.Message, state: FSMContext):
    """
    Получаем email пользователя и отправляем запрос на Polya.
    К ссылке в теле запроса добавляем параметр r=0x15:
      - https://example.com -> https://example.com?r=0x15
      - https://example.com?a=1 -> https://example.com?a=1&r=0x15
      - Сохраняем #fragment и не дублируем существующий r.
    """
    if not await ensure_approved(m):
        return
    email_to = (m.text or "").strip()
    await delete_message_safe(m)

    if not is_valid_email(email_to):
        await bot.send_message(m.chat.id, "Некорректный email. Введите снова или /cancel.")
        return

    data = await state.get_data()
    ad_id = data.get("polya_ad_id")
    k_local = data.get("polya_k_local")

    gen_entry = AD_GENERATED_LINKS_PER_CHAT.get(m.chat.id, {}).get(k_local)
    if not gen_entry or gen_entry.get("ad_id") != ad_id:
        await bot.send_message(m.chat.id, "Контекст объявления потерян.")
        await state.clear()
        return

    article_name = gen_entry.get("title") or ""
    amount = gen_entry.get("price") or ""
    link_val = gen_entry.get("short") or gen_entry.get("original") or ""

    # Добавляем r=0x15 к ссылке
    def _append_r_param(u: str) -> str:
        try:
            from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
            sp = urlsplit(u)
            q = parse_qsl(sp.query, keep_blank_values=True)
            # Удаляем существующий r и добавляем нужный
            q = [(k, v) for (k, v) in q if k.lower() != "r"]
            q.append(("r", "0x15"))
            new_query = urlencode(q)
            return urlunsplit((sp.scheme, sp.netloc, sp.path, new_query, sp.fragment))
        except Exception:
            # Фолбэк: простая конкатенация, учитывая фрагмент
            if "#" in u:
                before_frag, frag = u.split("#", 1)
                sep = "&" if "?" in before_frag else "?"
                return f"{before_frag}{sep}r=0x15#{frag}"
            sep = "&" if "?" in u else "?"
            return f"{u}{sep}r=0x15"

    link_for_polya = _append_r_param(link_val)

    # Отправка
    import aiohttp
    payload = {
        "api_key": POLYA_API_KEY,
        "templateName": "kleinanzeigen",
        "recipientEmail": email_to,
        "articleName": article_name,
        "amount": amount,
        "link": link_for_polya,  # ссылка с ?r=0x15
    }

    ok = False
    err_txt = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.zd2.net/api/send-email",
                                    json=payload,
                                    timeout=30) as resp:
                if resp.status == 200:
                    ok = True
                else:
                    err_txt = f"HTTP {resp.status}"
    except Exception as e:
        err_txt = f"{type(e).__name__}: {e}"

    if ok:
        try:
            await bot.send_message(
                m.chat.id,
                f"Письмо успешно отправлено на <code>{tg(email_to)}</code> ✔️"
            )
        except Exception:
            pass
    else:
        try:
            await bot.send_message(
                m.chat.id,
                f"Ошибка отправки Polya ❌ {tg(err_txt)}"
            )
        except Exception:
            pass

    await state.clear()


@dp.callback_query(F.data.startswith("adlink:open:"))
async def adlink_open_cb(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    chat_id = c.message.chat.id
    internal_uid = await U(c)
    try:
        origin_mid = int(c.data.split(":")[2])
    except Exception:
        origin_mid = c.message.message_id

    rt = INCOMING_RT.get((internal_uid, origin_mid))
    if not rt:
        rt = INCOMING_RT.get((internal_uid, c.message.message_id))
    if not rt:
        await c.answer("Нет контекста", show_alert=True)
        return

    from_email = rt.get("from_email") or ""
    if "@" not in from_email:
        await c.answer("Email?", show_alert=True)
        return
    local_part = from_email.split("@", 1)[0]

    import unicodedata, re
    def _norm(s: str) -> str:
        s = (s or "").replace("\u00A0", " ")
        s = unicodedata.normalize("NFKC", s)
        s = s.replace(".", " ").replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s.strip().lower())
        return s
    k_local = _norm(local_part)

    entry = AD_GENERATED_LINKS_PER_CHAT.get(chat_id, {}).get(k_local)
    if not entry:
        await c.answer("Ссылка не найдена", show_alert=True)
        return

    # Обновляем клавиатуру (async версия)
    try:
        kb = await build_incoming_reply_kb_async(chat_id, origin_mid)
        await safe_edit_reply_markup(chat_id, origin_mid, kb)
    except Exception:
        pass

    res_mid = int(entry.get("result_msg_id") or 0)
    if res_mid > 0:
        try:
            await bot.send_message(chat_id, "⬆️ Лог ссылки выше", reply_to_message_id=res_mid)
        except Exception:
            pass
        await safe_cq_answer(c)
        return

    def _cv(v: str) -> str:
        return f"<code>{tg(v)}</code>" if v else ""
    lines = []
    if entry.get("title"):
        lines.append(f"Название: {_cv(entry.get('title',''))}")
    if entry.get("price"):
        lines.append(f"Цена: {_cv(entry.get('price',''))}")
    lines.append(f"Ссылка: {_cv(entry.get('short') or entry.get('original') or '')}")
    try:
        msg = await bot.send_message(chat_id, "\n".join(lines), reply_to_message_id=origin_mid)
        entry["result_msg_id"] = msg.message_id
    except Exception:
        pass
    await safe_cq_answer(c)
    
# === AI Assistant (auto-replies and timed actions) ===
class AiFSM(StatesGroup):
    wait_interval = State()
    wait_order = State()
    pick_slot = State()     # 'reply1' or 'reply2'
    pick_preset = State()

# Пер-юнит трекинг отложенных задач ИИ: (uid, base_tg_mid) -> asyncio.Task
AI_ASSISTANT_TASKS: dict[tuple[int, int], asyncio.Task] = {}

_AI_ALLOWED_STEPS = ("reply1", "link", "html", "polya", "reply2")

async def _ai_get_cfg(uid: int) -> dict:
    def _to_int(s: str, default: int) -> int:
        try:
            return max(0, int(str(s).strip()))
        except Exception:
            return default
    enabled = (await get_setting_async(uid, "ai_enabled", "0")).strip().lower() in ("1", "true", "yes", "on")
    steps_raw = (await get_setting_async(uid, "ai_steps", "reply1,link,html,polya")).strip()
    steps = _ai_normalize_steps(steps_raw)
    preset1_id = _to_int(await get_setting_async(uid, "ai_reply1_preset_id", "0"), 0)
    preset2_id = _to_int(await get_setting_async(uid, "ai_reply2_preset_id", "0"), 0)
    cfg = {
        "enabled": enabled,
        "steps": steps,
        "preset1_id": preset1_id,
        "preset2_id": preset2_id,
        "intervals": {
            "reply1": _to_int(await get_setting_async(uid, "ai_interval_reply1", "0"), 0),
            "link":   _to_int(await get_setting_async(uid, "ai_interval_link", "0"), 0),
            "html":   _to_int(await get_setting_async(uid, "ai_interval_html", "0"), 0),  # NEW
            "polya":  _to_int(await get_setting_async(uid, "ai_interval_polya", "0"), 0),
            "reply2": _to_int(await get_setting_async(uid, "ai_interval_reply2", "0"), 0),
        }
    }
    return cfg

def _ai_normalize_steps(steps_raw: str) -> list[str]:
    # Приводит строку порядка к списку допустимых шагов + соблюдает правила:
    # - link должен быть до html/polya
    toks = [t.strip().lower() for t in (steps_raw or "").replace(">", ",").split(",")]
    allowed = set(_AI_ALLOWED_STEPS)
    seen: list[str] = []
    for t in toks:
        if t in allowed and t not in seen:
            seen.append(t)

    # Если нет link — удаляем html и polya (они завязаны на ссылку)
    if "link" not in seen:
        seen = [t for t in seen if t not in ("html", "polya")]

    # Если html раньше link — переносим html сразу после link
    if "link" in seen and "html" in seen and seen.index("html") < seen.index("link"):
        seen.remove("html")
        seen.insert(seen.index("link") + 1, "html")

    # Если polya раньше link — переносим polya сразу после link
    if "link" in seen and "polya" in seen and seen.index("polya") < seen.index("link"):
        seen.remove("polya")
        seen.insert(seen.index("link") + 1, "polya")

    return seen

async def _ai_cfg_text(uid: int) -> str:
    cfg = await _ai_get_cfg(uid)
    def onoff(b: bool) -> str:
        return "🟢 Включен" if b else "⚪ Выключен"
    async def ptitle(pid: int) -> str:
        if not pid:
            return "— не выбран"
        try:
            p = await get_preset_async(uid, int(pid))
        except Exception:
            p = None
        if p and getattr(p, "title", ""):
            return f"#{pid}: {getattr(p, 'title')}"
        return f"#{pid}"

    try:
        af_raw = (await get_setting_async(uid, "ai_xlsx_autoflow", "1")).strip().lower()
        af_enabled = af_raw in ("1", "true", "yes", "on")
    except Exception:
        af_enabled = True

    p1 = await ptitle(int(cfg.get("preset1_id") or 0))
    p2 = await ptitle(int(cfg.get("preset2_id") or 0))

    ivals = cfg.get("intervals") or {}
    i_reply1 = int(ivals.get("reply1", 0) or 0)
    i_link   = int(ivals.get("link", 0) or 0)
    i_html   = int(ivals.get("html", 0) or 0)   # NEW
    i_polya  = int(ivals.get("polya", 0) or 0)
    i_reply2 = int(ivals.get("reply2", 0) or 0)

    steps = cfg.get("steps") or []

    lines = [
        f"ИИ Помощник: {onoff(bool(cfg.get('enabled')))}",
        f"Автопоток XLSX (Чекер/Сендинг): {onoff(af_enabled)}",
        "",
        f"Порядок шагов: {', '.join(steps) if steps else '—'}",
        "",
        f"Ответ №1: {p1}",
        f"Интервал №1: {i_reply1} сек",
        "",
        f"Ссылка: интервал {i_link} сек",
        f"HTML (GO): интервал {i_html} сек",     # NEW
        f"Polya: интервал {i_polya} сек",
        "",
        f"Ответ №2: {p2}",
        f"Интервал №2: {i_reply2} сек",
    ]
    return "\n".join(lines)

def _ai_settings_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📜 Ответ №1 (пресет)", callback_data="ai:preset:open:reply1"),
         InlineKeyboardButton(text="⏱ Интервал №1", callback_data="ai:interval:set:reply1")],
        [InlineKeyboardButton(text="🔗 Ссылка (Интервал)", callback_data="ai:interval:set:link"),
         InlineKeyboardButton(text="🧾 HTML (Интервал)", callback_data="ai:interval:set:html")],  # NEW
        [InlineKeyboardButton(text="📨 Polya (Интервал)", callback_data="ai:interval:set:polya")],
        [InlineKeyboardButton(text="📜 Ответ №2 (пресет)", callback_data="ai:preset:open:reply2"),
         InlineKeyboardButton(text="⏱ Интервал №2", callback_data="ai:interval:set:reply2")],
        [InlineKeyboardButton(text="🔁 Чекер/Сендинг (вкл/выкл)", callback_data="ai:xlsxautoflow:toggle")],
        [InlineKeyboardButton(text="🧩 Порядок шагов", callback_data="ai:order:set")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:back")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data == "settings:ai:toggle")
async def ai_toggle_cb(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    uid = await U(c)
    cur = (await get_setting_async(uid, "ai_enabled", "0")).strip().lower() in ("1","true","yes","on")
    await set_setting_async(uid, "ai_enabled", "0" if cur else "1")
    kb = await dynamic_settings_kb(uid)
    try:
        await c.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        try:
            await c.message.edit_text("Настройки:", reply_markup=kb)
        except Exception:
            pass
    await safe_cq_answer(c, "Выключено" if cur else "Включено")

@dp.callback_query(F.data == "settings:ai:open")
async def ai_open_cb(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    uid = await U(c)
    text = await _ai_cfg_text(uid)
    kb = _ai_settings_kb()
    await safe_edit_message(c.message, text, reply_markup=kb)
    await safe_cq_answer(c)
    
@dp.callback_query(F.data == "ai:xlsxautoflow:toggle")
async def ai_xlsx_autoflow_toggle_cb(c: types.CallbackQuery):
    """
    Тумблер автопотока после XLSX (Чекер/Сендинг).
    Клавиша в ИИ-настройках. Меняет настройку ai_xlsx_autoflow (по умолчанию 1).
    """
    if not await ensure_approved(c):
        return

    uid = await U(c)
    try:
        cur = (await get_setting_async(uid, "ai_xlsx_autoflow", "1")).strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        cur = True
    try:
        await set_setting_async(uid, "ai_xlsx_autoflow", "0" if cur else "1")
    except Exception:
        pass

    # Перерисовываем экран ИИ-настроек (покажет актуальный статус в тексте)
    await ai_open_cb(c)
    await safe_cq_answer(c, "Выключено" if cur else "Включено")

# ==== Выбор пресета для слота reply1/reply2 ====
async def _ai_presets_kb(uid: int, slot: str) -> InlineKeyboardMarkup:
    items = await list_presets_async(uid)
    rows: list[list[InlineKeyboardButton]] = []
    for p in items:
        title = (getattr(p, "title", "") or "").strip() or f"Пресет #{p.id}"
        if len(title) > 60:
            title = title[:57] + "..."
        rows.append([InlineKeyboardButton(text=f"📜 {title}", callback_data=f"ai:preset:set:{slot}:{p.id}")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:ai:open")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.callback_query(F.data.startswith("ai:preset:open:"))
async def ai_preset_open_cb(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    slot = c.data.split(":")[-1]  # reply1 / reply2
    if slot not in ("reply1","reply2"):
        await safe_cq_answer(c, "Слот неизвестен")
        return
    kb = await _ai_presets_kb(await U(c), slot)
    await safe_edit_message(c.message, f"Выберите пресет для {slot.upper()}:", reply_markup=kb)
    await safe_cq_answer(c)

@dp.callback_query(F.data.startswith("ai:preset:set:"))
async def ai_preset_set_cb(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    _, _, _, slot, pid = c.data.split(":")
    uid = await U(c)
    if slot not in ("reply1","reply2"):
        await c.answer("Слот?", show_alert=True); return
    key = "ai_reply1_preset_id" if slot == "reply1" else "ai_reply2_preset_id"
    await set_setting_async(uid, key, str(int(pid)))
    await ai_open_cb(c)

# ==== Ввод интервала (сек) ====
@dp.callback_query(F.data.startswith("ai:interval:set:"))
async def ai_interval_set_cb(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    kind = c.data.split(":")[-1]  # reply1/link/html/polya/reply2
    if kind not in ("reply1","link","html","polya","reply2"):
        await c.answer("Неизвестный тип", show_alert=True); return
    await state.set_state(AiFSM.wait_interval)
    await state.update_data(ai_interval_kind=kind, back_mid=c.message.message_id, chat_id=c.message.chat.id)
    await c.message.answer(f"Введите интервал для {kind.upper()} в секундах (целое число).", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:ai:open")]]))
    await safe_cq_answer(c)

@dp.message(AiFSM.wait_interval)
async def ai_interval_set_value(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    val = (m.text or "").strip()
    await delete_message_safe(m)
    data = await state.get_data()
    kind = data.get("ai_interval_kind")
    uid = await U(m)
    try:
        sec = max(0, int(val))
    except Exception:
        await bot.send_message(m.chat.id, "Нужно целое число (секунды).", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:ai:open")]]))
        return
    key_map = {
        "reply1": "ai_interval_reply1",
        "link":   "ai_interval_link",
        "html":   "ai_interval_html",   # NEW
        "polya":  "ai_interval_polya",
        "reply2": "ai_interval_reply2",
    }
    await set_setting_async(uid, key_map[kind], str(sec))
    await state.clear()
    await bot.send_message(m.chat.id, "Сохранено.", reply_markup=_ai_settings_kb())

# ==== Порядок шагов ====
@dp.callback_query(F.data == "ai:order:set")
async def ai_order_prompt(c: types.CallbackQuery, state: FSMContext):
    if not await ensure_approved(c):
        return
    await state.set_state(AiFSM.wait_order)
    txt = "Отправьте порядок шагов через запятую из набора: reply1, link, html, polya, reply2.\nПример: reply1, link, html, polya, reply2\nПримечание: HTML/Polya всегда будут после Link."
    await c.message.answer(txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="settings:ai:open")]]))
    await safe_cq_answer(c)

@dp.message(AiFSM.wait_order)
async def ai_order_save(m: types.Message, state: FSMContext):
    if not await ensure_approved(m):
        return
    raw = (m.text or "")
    await delete_message_safe(m)
    uid = await U(m)
    steps = _ai_normalize_steps(raw)
    if not steps:
        await bot.send_message(m.chat.id, "Не распознан порядок. Допустимые токены: reply1, link, polya, reply2.")
        return
    await set_setting_async(uid, "ai_steps", ",".join(steps))
    await state.clear()
    await bot.send_message(m.chat.id, "Порядок сохранён.", reply_markup=_ai_settings_kb())

# ==== Автосценарий: планирование ====
async def maybe_schedule_ai_assistant(
    uid: int,
    chat_id: int,
    acc_obj,
    base_tg_message_id: int,
    from_email: str,
    subject: str
):
    # Исключение отправителей со словом "google" в локальной части email
    try:
        if from_email and "@" in from_email:
            local_part = from_email.split("@", 1)[0].lower()
            if "google" in local_part:
                # Не обрабатываем ИИ для таких отправителей
                return
    except Exception:
        pass
    
    cfg = await _ai_get_cfg(uid)
    if not cfg.get("enabled"):
        return
    steps: list[str] = cfg.get("steps") or []
    if not steps:
        return

    key = (uid, int(base_tg_message_id))

    async def _run_flow():
        try:
            try:
                await bot.send_message(chat_id, "🤖 ИИ: сценарий запущен", reply_to_message_id=base_tg_message_id)
            except Exception:
                pass

            link_ok = False
            for step in steps:
                delay = int(cfg["intervals"].get(step, 0) or 0)
                if delay > 0:
                    await asyncio.sleep(delay)

                if step == "reply1":
                    pid = int(cfg.get("preset1_id") or 0)
                    if pid:
                        await _ai_send_preset_reply(uid, chat_id, acc_obj, base_tg_message_id, from_email, subject, pid, slot_tag="1")
                elif step == "link":
                    link_ok = await _ai_generate_link(uid, chat_id, base_tg_message_id)
                elif step == "html":
                    if link_ok:
                        await _ai_send_html_go(uid, chat_id, acc_obj, base_tg_message_id, to_email=from_email, subj_orig=subject)  # NEW
                elif step == "polya":
                    if link_ok:
                        await _ai_send_polya(uid, chat_id, base_tg_message_id, to_email=from_email)
                elif step == "reply2":
                    pid = int(cfg.get("preset2_id") or 0)
                    if pid:
                        await _ai_send_preset_reply(uid, chat_id, acc_obj, base_tg_message_id, from_email, subject, pid, slot_tag="2")
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log_send_event(f"AI_FLOW error uid={uid} mid={base_tg_message_id}: {e}")
        finally:
            try:
                AI_ASSISTANT_TASKS.pop(key, None)
            except Exception:
                pass

    t = asyncio.create_task(_run_flow())
    AI_ASSISTANT_TASKS[key] = t

async def _ai_send_preset_reply(
    uid: int,
    chat_id: int,
    acc_obj,
    base_tg_message_id: int,
    to_email: str,
    subj_orig: str,
    preset_id: int,
    slot_tag: str = ""
):
    try:
        p = await get_preset_async(uid, int(preset_id))
        body = (getattr(p, "body", "") or "").strip()
        if not body:
            return
        subj = f"Re: {subj_orig or ''}".strip()
        await outbox_enqueue(
            uid, chat_id, int(getattr(acc_obj, "id")), to_email,
            subj, body, html=False, src_tg_mid=base_tg_message_id
        )
        # тихо — логи придут по факту успеха Outbox
    except Exception as e:
        log_send_event(f"AI reply{slot_tag} enqueue error uid={uid}: {e}")

async def _ai_generate_link(uid: int, chat_id: int, origin_mid: int) -> bool:
    """
    Генерация ссылки в автошаге ИИ с полным паритетом ручной кнопки:
      - выбор Goo (Nur) как вручную
      - лог-пост (фото/текст) с кнопкой Polya
      - обновление inline-клавиатуры у базового входящего
      - кэширование и сохранение на диск
      - если ссылка уже есть — шлёт «⬆️ Уже есть» на прежний лог
    """
    # Контекст входящего
    rt = INCOMING_RT.get((uid, origin_mid))
    if not rt:
        return False
    from_email = (rt.get("from_email") or "").strip()
    if "@" not in from_email:
        return False
    local_part = from_email.split("@", 1)[0]

    import unicodedata, re, asyncio as _aio, urllib.parse, json
    def _norm(s: str) -> str:
        s = (s or "").replace("\u00A0", " ")
        s = unicodedata.normalize("NFKC", s)
        s = s.replace(".", " ").replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s.strip().lower())
        return s
    k_local = _norm(local_part)

    # ad_id и объявление
    ad_id = AD_LOCAL2ID_PER_CHAT.get(chat_id, {}).get(k_local)
    if not ad_id:
        return False
    ad_entry = AD_ADS_BY_ID_PER_CHAT.get(chat_id, {}).get(ad_id)
    if not ad_entry:
        return False

    # Если уже есть — ведём себя как ручная кнопка: «⬆️ Уже есть»
    gen_for_chat = AD_GENERATED_LINKS_PER_CHAT.setdefault(chat_id, {})
    if k_local in gen_for_chat and (gen_for_chat[k_local].get("short") or gen_for_chat[k_local].get("original")):
        prev_mid = int(gen_for_chat[k_local].get("result_msg_id") or 0)
        if prev_mid:
            try:
                await bot.send_message(chat_id, "⬆️ Уже есть", reply_to_message_id=prev_mid)
            except Exception:
                pass
        # Обновим клавиатуру у базового входящего
        try:
            kb_new = await build_incoming_reply_kb_async(chat_id, origin_mid)
            await safe_edit_reply_markup(chat_id, origin_mid, kb_new)
        except Exception:
            pass
        return True

    # Метаданные объявления
    original = ad_entry["link"]
    title, price, photo_url = await fetch_ad_metadata(original)

    # Определяем team_mode (как в ручной генерации)
    admin = is_admin(uid)
    team_mode_raw = await get_setting_async(uid, "team_mode", "nur")
    team_mode = (team_mode_raw or "nur").strip().lower()
    team_mode = team_mode.replace(" ", "_").replace("-", "_")
    if "aqua" in team_mode and "team" in team_mode:
        team_mode = "aqua_team"
    elif team_mode == "aquateam" or team_mode == "aqua_team":
        team_mode = "aqua_team"
    elif team_mode == "dolce":
        team_mode = "dolce"
    elif "nur" in team_mode or team_mode == "nurpaypal":
        team_mode = "nur"
    else:
        team_mode = "nur"
    if not admin and team_mode == "dolce":
        team_mode = "nur"

    def _extract_domain(u: str) -> str:
        try:
            return urllib.parse.urlparse(u).netloc.lower()
        except Exception:
            return ""
    domain = _extract_domain(original)
    if "kleinanzeigen" in domain:
        services = ["kleinanzeigen_de", "ebay_kleinanzeigen_de", "ebay_de"]
    elif "ebay." in domain:
        services = ["ebay_de"]
    else:
        services = [GOO_DEFAULT_SERVICE]
    if GOO_DEFAULT_SERVICE not in services:
        services.append(GOO_DEFAULT_SERVICE)

    # ВЕТКА DOLCE (только для админов)
    if team_mode == "dolce" and admin:
        team_base = (await get_setting_async(uid, "dolce_team_base", "")).strip()
        worker_token = (await get_setting_async(uid, "dolce_worker_token", "")).strip()
        if not team_base or not worker_token:
            return False
        
        cache_key = (original, team_base, worker_token, tuple(services), "dolce")
        try:
            cached = GOO_LINK_CACHE.get(cache_key)  # type: ignore[name-defined]
        except Exception:
            cached = None
        
        if cached:
            short_link, last_service = cached
        else:
            endpoint = f"{team_base}/api/generate/single/parse"
            headers = {
                "Authorization": f"Bearer {worker_token}",
                "Content-Type": "application/json",
            }
            session = await get_http_session()
            async def one_service(sv: str):
                payload = {"service": sv, "url": original, "isNeedBalanceChecker": False}
                try:
                    async with session.post(endpoint, headers=headers, json=payload, timeout=20) as resp:
                        raw = await resp.text()
                        if 200 <= resp.status < 300:
                            try:
                                data = json.loads(raw)
                            except Exception:
                                data = {}
                            if data.get("status") is True and data.get("message"):
                                return sv, str(data["message"]).strip(), resp.status, raw[:1500]
                        return sv, "", resp.status, raw[:1500]
                except _aio.TimeoutError:
                    return sv, "", "timeout", ""
                except Exception as e_req:
                    return sv, "", f"exc:{type(e_req).__name__}", str(e_req)[:1500]
            
            tasks = [asyncio.create_task(one_service(sv)) for sv in services]
            short_link = ""
            last_status = None
            last_raw = ""
            last_service = ""
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=25)
                for d in done:
                    sv, link, st, raw = await d
                    last_status = st; last_raw = raw; last_service = sv
                    if link:
                        short_link = link
                        break
                if not short_link:
                    for d in pending:
                        try:
                            sv, link, st, raw = await asyncio.wait_for(d, timeout=10)
                            last_status = st; last_raw = raw; last_service = sv
                            if link:
                                short_link = link
                                break
                        except Exception:
                            pass
            finally:
                for p in tasks:
                    if not p.done():
                        p.cancel()
            
            if not short_link:
                return False
            
            try:
                GOO_LINK_CACHE[cache_key] = (short_link, last_service)  # type: ignore[name-defined]
            except Exception:
                pass
            profile_id = ""
    
    # ВЕТКА AQUA TEAM
    elif team_mode == "aqua_team":
        user_key = (await get_setting_async(uid, "aqua_worker_key", "")).strip()
        team_key = (await get_setting_async(uid, "aqua_team_key", "")).strip()
        profile_id = (await get_setting_async(uid, "aqua_profile_id", "")).strip()
        if not user_key or not team_key or not profile_id:
            return False
        
        cache_key = (original, profile_id, tuple(services), "aqua_team")
        try:
            cached = GOO_LINK_CACHE.get(cache_key)  # type: ignore[name-defined]
        except Exception:
            cached = None
        
        if cached:
            short_link, last_service = cached
        else:
            endpoint = "https://api-aq.goo.network/api/generate/single/parse"
            headers = {
                "Authorization": f"Apikey {user_key}",
                "X-Team-Key": team_key,
                "Host": "api-aq.goo.network",
                "Content-Type": "application/json",
            }
            session = await get_http_session()
            async def one_service(sv: str):
                payload = {"service": sv, "url": original, "isNeedBalanceChecker": False, "profileID": profile_id}
                try:
                    async with session.post(endpoint, headers=headers, json=payload, timeout=20) as resp:
                        raw = await resp.text()
                        if 200 <= resp.status < 300:
                            try:
                                data = json.loads(raw)
                            except Exception:
                                data = {}
                            if data.get("status") is True and data.get("message"):
                                return sv, str(data["message"]).strip(), resp.status, raw[:1500]
                        return sv, "", resp.status, raw[:1500]
                except _aio.TimeoutError:
                    return sv, "", "timeout", ""
                except Exception as e_req:
                    return sv, "", f"exc:{type(e_req).__name__}", str(e_req)[:1500]
            
            tasks = [asyncio.create_task(one_service(sv)) for sv in services]
            short_link = ""
            last_status = None
            last_raw = ""
            last_service = ""
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=25)
                for d in done:
                    sv, link, st, raw = await d
                    last_status = st; last_raw = raw; last_service = sv
                    if link:
                        short_link = link
                        break
                if not short_link:
                    for d in pending:
                        try:
                            sv, link, st, raw = await asyncio.wait_for(d, timeout=10)
                            last_status = st; last_raw = raw; last_service = sv
                            if link:
                                short_link = link
                                break
                        except Exception:
                            pass
            finally:
                for p in tasks:
                    if not p.done():
                        p.cancel()
            
            if not short_link:
                return False
            
            try:
                GOO_LINK_CACHE[cache_key] = (short_link, last_service)  # type: ignore[name-defined]
            except Exception:
                pass
    
    # ВЕТКА GOO/NUR (по умолчанию)
    else:
        user_key = (await get_setting_async(uid, "goo_worker_key", "")).strip()
        team_key = (await get_setting_async(uid, "goo_team_key", "")).strip()
        profile_id = (await get_setting_async(uid, "goo_profile_id", "")).strip()
        if not (user_key and team_key and profile_id):
            return False
        
        cache_key = (original, profile_id, tuple(services))
        try:
            cached = GOO_LINK_CACHE.get(cache_key)  # type: ignore[name-defined]
        except Exception:
            cached = None
        
        if cached:
            short_link, last_service = cached
        else:
            endpoint = "https://api.goo.network/api/generate/single/parse"
            headers = {
                "Authorization": f"Apikey {user_key}",
                "X-Team-Key": team_key,
                "Host": "api.goo.network",
                "Content-Type": "application/json",
            }
            session = await get_http_session()
            async def one_service(sv: str):
                payload = {"service": sv, "url": original, "isNeedBalanceChecker": False, "profileID": profile_id}
                try:
                    async with session.post(endpoint, headers=headers, json=payload, timeout=20) as resp:
                        raw = await resp.text()
                        if 200 <= resp.status < 300:
                            try:
                                data = json.loads(raw)
                            except Exception:
                                data = {}
                            if data.get("status") is True and data.get("message"):
                                return sv, str(data["message"]).strip(), resp.status, raw[:1500]
                        return sv, "", resp.status, raw[:1500]
                except _aio.TimeoutError:
                    return sv, "", "timeout", ""
                except Exception as e_req:
                    return sv, "", f"exc:{type(e_req).__name__}", str(e_req)[:1500]
            
            tasks = [asyncio.create_task(one_service(sv)) for sv in services]
            short_link = ""
            last_status = None
            last_raw = ""
            last_service = ""
            try:
                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=25)
                for d in done:
                    sv, link, st, raw = await d
                    last_status = st; last_raw = raw; last_service = sv
                    if link:
                        short_link = link
                        break
                if not short_link:
                    for d in pending:
                        try:
                            sv, link, st, raw = await asyncio.wait_for(d, timeout=10)
                            last_status = st; last_raw = raw; last_service = sv
                            if link:
                                short_link = link
                                break
                        except Exception:
                            pass
            finally:
                for p in tasks:
                    if not p.done():
                        p.cancel()
            
            if not short_link:
                return False
            
            try:
                GOO_LINK_CACHE[cache_key] = (short_link, last_service)  # type: ignore[name-defined]
            except Exception:
                pass

    # Сохраняем и логируем — как в ручном обработчике
    gen_for_chat[k_local] = {
        "ad_id": ad_id,
        "short": short_link,
        "original": original,
        "title": title,
        "price": price,
        "photo_url": photo_url,
        "result_msg_id": 0,
        "service": last_service,
        "profile_id": profile_id,
        "ts": time.time(),
    }
    AD_CHAT_TS[chat_id] = time.time()

    def _cv(v: str) -> str:
        return f"<code>{tg(v)}</code>" if v else ""

    caption_parts = []
    if title:
        caption_parts.append(f"Название: {_cv(title)}")
    if price:
        caption_parts.append(f"Цена: {_cv(price)}")
    caption_parts.append(f"Ссылка: {_cv(short_link)}")
    caption = "\n".join(caption_parts)

    polya_kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Polya (5%)", callback_data=f"polya:ask:{ad_id}:{k_local}")]
        ]
    )

    sent_mid = 0
    if photo_url:
        try:
            session = await get_http_session()
            async with session.get(photo_url, timeout=15) as r:
                if r.status == 200:
                    img = await r.read()
                    pfile = types.BufferedInputFile(img, filename="ad.jpg")
                    pmsg = await bot.send_photo(
                        chat_id,
                        photo=pfile,
                        caption=caption,
                        reply_to_message_id=origin_mid,
                        reply_markup=polya_kb
                    )
                    sent_mid = getattr(pmsg, "message_id", 0) or 0
        except Exception:
            sent_mid = 0

    if not sent_mid:
        try:
            tmsg = await bot.send_message(
                chat_id,
                caption,
                reply_to_message_id=origin_mid,
                reply_markup=polya_kb
            )
            sent_mid = getattr(tmsg, "message_id", 0) or 0
        except Exception:
            sent_mid = 0

    if sent_mid:
        gen_for_chat[k_local]["result_msg_id"] = sent_mid

    try:
        kb_new = await build_incoming_reply_kb_async(chat_id, origin_mid)
        await safe_edit_reply_markup(chat_id, origin_mid, kb_new)
    except Exception:
        pass
    try:
        await save_ad_cache_async(chat_id)
    except Exception:
        pass

    return True

async def _ai_send_polya(uid: int, chat_id: int, origin_mid: int, to_email: str):
    """
    Авто-Polya: логирование 1:1 с ручным polya_send_email:
      - успех: 'Письмо успешно отправлено на <email> ✔️'
      - ошибка: 'Ошибка отправки Polya ❌ <текст>'
    """
    # Получаем ссылку из кэша по локалу отправителя
    rt = INCOMING_RT.get((uid, origin_mid)) or {}
    from_email = (rt.get("from_email") or "").strip()
    if "@" not in from_email:
        return
    local_part = from_email.split("@", 1)[0]

    import unicodedata, re
    def _norm(s: str) -> str:
        s = (s or "").replace("\u00A0", " ")
        s = unicodedata.normalize("NFKC", s)
        s = s.replace(".", " ").replace("_", " ").replace("-", " ")
        s = re.sub(r"\s+", " ", s.strip().lower())
        return s
    k_local = _norm(local_part)
    gen_entry = AD_GENERATED_LINKS_PER_CHAT.get(chat_id, {}).get(k_local)
    if not gen_entry:
        return
    link_val = gen_entry.get("short") or gen_entry.get("original") or ""
    if not link_val:
        return

    # r=0x15
    def _append_r_param(u: str) -> str:
        try:
            from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
            sp = urlsplit(u)
            q = parse_qsl(sp.query, keep_blank_values=True)
            q = [(k, v) for (k, v) in q if k.lower() != "r"]
            q.append(("r", "0x15"))
            new_query = urlencode(q)
            return urlunsplit((sp.scheme, sp.netloc, sp.path, new_query, sp.fragment))
        except Exception:
            if "#" in u:
                before_frag, frag = u.split("#", 1)
                sep = "&" if "?" in before_frag else "?"
                return f"{before_frag}{sep}r=0x15#{frag}"
            sep = "&" if "?" in u else "?"
            return f"{u}{sep}r=0x15"

    link_for_polya = _append_r_param(link_val)
    article_name = gen_entry.get("title") or ""
    amount = gen_entry.get("price") or ""

    import aiohttp
    payload = {
        "api_key": POLYA_API_KEY,
        "templateName": "kleinanzeigen",
        "recipientEmail": to_email,
        "articleName": article_name,
        "amount": amount,
        "link": link_for_polya,
    }

    ok = False
    err_txt = ""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://api.zd2.net/api/send-email",
                                    json=payload,
                                    timeout=30) as resp:
                if resp.status == 200:
                    ok = True
                else:
                    err_txt = f"HTTP {resp.status}"
    except Exception as e:
        err_txt = f"{type(e).__name__}: {e}"

    if ok:
        try:
            await bot.send_message(
                chat_id,
                f"Письмо успешно отправлено на <code>{tg(to_email)}</code> ✔️",
                reply_to_message_id=origin_mid
            )
        except Exception:
            pass
    else:
        try:
            await bot.send_message(
                chat_id,
                f"Ошибка отправки Polya ❌ {tg(err_txt)}",
                reply_to_message_id=origin_mid
            )
        except Exception:
            pass
            
async def _ai_send_html_go(
    uid: int,
    chat_id: int,
    acc_obj,
    base_tg_message_id: int,
    to_email: str,
    subj_orig: str
) -> bool:
    """
    Авто-отправка HTML шаблона GO:
      - Требует, чтобы ссылка уже была создана (как в шаге 'link')
      - Подставляет ссылку/название/цену из AD_GENERATED_LINKS_PER_CHAT
      - Применяет subject_override_html и spoof-имя
      - Отправляет через Outbox (лог придёт после успеха)
    """
    # Проверяем, что ссылка есть
    has_link, entry = _has_generated_link(chat_id, to_email)
    if not has_link:
        return False
    link_val = entry.get("short") or entry.get("original") or ""
    offer = (entry.get("title") or "").strip()
    amount = (entry.get("price") or "").strip()

    internal_uid = uid
    acc_id = int(getattr(acc_obj, "id"))
    subj = (subj_orig or "").strip()

    # Подмена темы (HTML)
    try:
        override_flag = (await get_setting_async(internal_uid, "subject_override_html", "1")).strip().lower() in ("1","true","yes","on")
    except Exception:
        override_flag = True
    try:
        subj_conf = (await get_setting_async(internal_uid, "subject_html_text", "")).strip()
    except Exception:
        subj_conf = ""
    if override_flag and subj_conf:
        subj = subj_conf or subj

    # Имя отправителя (спуф) — для корректного From в html‑сценарии
    try:
        acc_display = (getattr(acc_obj, "display_name", "") or getattr(acc_obj, "name", "") or "").strip()
        if not acc_display and getattr(acc_obj, "email", ""):
            acc_display = acc_obj.email.split("@", 1)[0]
    except Exception:
        acc_display = ""
    try:
        sender_name_override = await get_spoof_sender_name(internal_uid, acc_display_name=acc_display, tpl="GO", chat_id=chat_id)
    except Exception:
        sender_name_override = None  # не критично

    # Сборка HTML (как в reply_html_auto_pick для GO)
    txt_html, final_html, style_id = await _build_html(
        internal_uid,
        "GO",
        link_val,
        offer=offer,
        price=amount
    )

    # Сохраним «последний HTML» в кеш (для консистентности)
    try:
        set_last_html(chat_id, final_html)
        set_last_html_meta(chat_id, {"style": style_id, "tpl": "GO"})
    except Exception:
        pass

    # В Outbox — быстрый возврат, логи придут по факту успеха
    try:
        await outbox_enqueue(
            internal_uid, chat_id, acc_id, to_email, subj,
            final_html,
            html=True,
            src_tg_mid=base_tg_message_id
        )
        return True
    except Exception:
        return False 

async def ai_assistant_cancel_all_for_user(uid: int):
    """Отменяет все отложенные сценарии ИИ для пользователя и полностью очищает трекинг."""
    try:
        keys = [k for k in list(AI_ASSISTANT_TASKS.keys()) if k[0] == uid]
        for k in keys:
            t = AI_ASSISTANT_TASKS.pop(k, None)
            if t and not t.done():
                try:
                    t.cancel()
                    await t
                except Exception:
                    pass
    except Exception:
        pass
        
async def _ai_is_xlsx_autoflow_enabled(uid: int) -> bool:
    """
    Включает автопоток после XLSX:
      - ai_enabled == true
      - и (ai_xlsx_autoflow == '1'|'true'|'on'), по умолчанию '1'
    """
    try:
        ai_enabled = (await get_setting_async(uid, "ai_enabled", "0")).strip().lower() in ("1","true","yes","on")
        if not ai_enabled:
            return False
        flag = (await get_setting_async(uid, "ai_xlsx_autoflow", "1")).strip().lower() in ("1","true","yes","on")
        return flag
    except Exception:
        return False

async def ai_auto_verify_and_send(uid: int, chat_id: int, max_retries: int = 1) -> None:
    """
    Полностью синхронизированная с ручной проверкой версия.
    Делает тот же самый вызов verify_emails_from_df_for_user_sync_with_ctx.
    """
    import time, traceback
    from io import BytesIO
    import pandas as pd

    try:
        ctx = await get_user_ctx_async(uid)
    except Exception as e:
        log_send_event(f"[AI AUTO] get_user_ctx_async failed: {e}")
        return

    # Берём последний загруженный Excel
    entry = LAST_XLSX_PER_CHAT.get(chat_id)
    if not entry:
        log_send_event(f"[AI AUTO] no file in LAST_XLSX_PER_CHAT for chat {chat_id}")
        return

    file_data = entry.get("data")
    username = entry.get("username") or f"tg_{chat_id}"
    if not file_data:
        log_send_event(f"[AI AUTO] empty file_data for chat {chat_id}")
        return

    try:
        df = pd.read_excel(BytesIO(file_data))
    except Exception as e:
        log_send_event(f"[AI AUTO] failed to read Excel: {e}")
        return

    # Передаем chat_id и username в тело запроса к API
    try:
        verified = verify_emails_from_df_for_user_sync_with_ctx(ctx, df, chat_id, username)
    except Exception as e:
        log_send_event(f"[AI AUTO] verify_emails_from_df_for_user_sync_with_ctx failed: {type(e).__name__} {e}")
        log_send_event(traceback.format_exc())
        return

    if not verified:
        log_send_event(f"[AI AUTO] no verified emails for chat {chat_id}, user {username}")
        return

    # Отправляем результат пользователю
    text = "\n".join(f"{r['email']} — {r['title']}" for r in verified)
    try:
        await bot.send_message(chat_id, f"✅ Найдено {len(verified)} адресов:\n\n{text}")
    except Exception as e:
        log_send_event(f"[AI AUTO] send_message failed: {e}")


async def ai_start_send_after_verify(uid: int, chat_id: int) -> None:
    """
    Программный запуск сендинга (эквивалент send:start):
      - все те же проверки, те же логи
      - планирует send_loop в SEND_TASKS[uid]
    """
    # Нужно наличие результатов
    if chat_id not in VERIFIED_ROWS_PER_CHAT or not VERIFIED_ROWS_PER_CHAT[chat_id]:
        try:
            await bot.send_message(chat_id, "Сначала выполните проверку email.")
        except Exception:
            pass
        return

    # Проверяем наличие smart пресетов
    try:
        smart_items = await list_smart_presets_async(uid)
    except Exception:
        try:
            await bot.send_message(chat_id, "Внутренняя ошибка при проверке пресетов.")
        except Exception:
            pass
        return

    if not smart_items:
        try:
            await bot.send_message(chat_id, "Ошибка: добавьте умные пресеты ❗️")
        except Exception:
            pass
        return

    # Контекст (фильтрует отключённые аккаунты)
    ctx = await get_user_ctx_async(uid)
    if not getattr(ctx, "accounts", None):
        try:
            await bot.send_message(chat_id, "Нет аккаунтов, включённых для рассылки. Используйте /sendacc.")
        except Exception:
            pass
        return

    missing = []
    if not getattr(ctx, "templates", None):
        missing.append("шаблоны")
    if not getattr(ctx, "subjects", None):
        missing.append("темы")
    if missing:
        try:
            await bot.send_message(chat_id, f"Ошибка: добавьте {', '.join(missing)}!")
        except Exception:
            pass
        return

    # Жёсткая проверка send‑прокси — как в ручном
    proxies_rows = await list_proxies_async(uid, "send")
    if not proxies_rows:
        try:
            await bot.send_message(chat_id, "Ошибка: добавьте send‑прокси!")
        except Exception:
            pass
        return

    target_host, target_port = _probe_target_for_kind("send")
    tests = [
        _test_proxy_async(p.host, p.port, p.user_login or "", p.password or "", target_host, target_port, timeout=5)
        for p in proxies_rows
    ]
    results = await asyncio.gather(*tests, return_exceptions=False)
    bad_ordinals = [i for i, (ok, _err) in enumerate(results, start=1) if not ok]
    if bad_ordinals:
        nums = _fmt_bad_ordinals(bad_ordinals)
        msg = f"Проверьте невалидные прокси {nums}"
        try:
            await bot.send_message(chat_id, msg)
        except Exception:
            pass
        return

    # Не допускаем повторный старт
    if uid in SEND_TASKS and not SEND_TASKS[uid].done():
        try:
            await bot.send_message(chat_id, "Сендинг уже запущен.")
        except Exception:
            pass
        return

    total = len(VERIFIED_ROWS_PER_CHAT[chat_id])
    SEND_STATUS[uid] = {
        "running": True,
        "sent": 0,
        "failed": 0,
        "total": total,
        "cancel": False
    }
    SEND_TASKS[uid] = asyncio.create_task(send_loop(uid, chat_id))
    try:
        await bot.send_message(chat_id, "Сендинг запущен 🚀")
    except Exception:
        pass 
        
async def ai_xlsx_verify_and_send_once(uid: int, chat_id: int) -> None:
    """
    Однократный автозапуск ИИ после загрузки XLSX:
      - проверка (строго как вручную) из кэша LAST_XLSX_PER_CHAT
      - если есть результаты — запуск сендинга теми же проверками
    Никаких циклов, никаких «умностей».
    """
    # Автопоток включен?
    if not await _ai_is_xlsx_autoflow_enabled(uid):
        return

    results = await _verify_emails_from_cache_once(uid, chat_id)
    if not results:
        return

    # Запуск сендинга с теми же проверками, что и кнопка
    await ai_start_send_after_verify(uid, chat_id)

def schedule_ai_xlsx_autoverify(uid: int, chat_id: int) -> None:
    """
    Планирует фоновую проверку РОВНО как вручную и потом старт сендинга.
    Одна задача на (uid, chat_id): новая замещает старую.
    """
    if not hasattr(schedule_ai_xlsx_autoverify, "_tasks"):
        schedule_ai_xlsx_autoverify._tasks = {}  # type: ignore[attr-defined]
    tasks: dict[tuple[int, int], asyncio.Task] = schedule_ai_xlsx_autoverify._tasks  # type: ignore[attr-defined]

    key = (uid, chat_id)
    old = tasks.pop(key, None)
    if old and not old.done():
        try: old.cancel()
        except Exception: pass

    async def _runner():
        try:
            await ai_xlsx_verify_and_send_once(uid, chat_id)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            try:
                log_send_event(f"AI_XLSX_VERIFY runner error uid={uid} chat={chat_id}: {e}")
            except Exception:
                pass
        finally:
            try:
                cur = schedule_ai_xlsx_autoverify._tasks.get(key)  # type: ignore[attr-defined]
                if cur is asyncio.current_task():
                    schedule_ai_xlsx_autoverify._tasks.pop(key, None)  # type: ignore[attr-defined]
            except Exception:
                pass

    tasks[key] = asyncio.create_task(_runner())


async def ai_xlsx_autoflow_maybe_start(uid: int, chat_id: int) -> None:
    """
    Обёртка для вызова из on_xlsx_received:
      - проверяет, включён ли ИИ и автопоток XLSX
      - планирует задачу автопроверки с защитой от дублей
    """
    try:
        if await _ai_is_xlsx_autoflow_enabled(uid):
            schedule_ai_xlsx_autoverify(uid, chat_id)
    except Exception:
        pass


async def ai_xlsx_autoverify_cancel_all_for_user(uid: int) -> int:
    """
    Отменяет все фоновые задачи автопроверки XLSX для пользователя.
    Возвращает количество отменённых задач.
    """
    try:
        if not hasattr(schedule_ai_xlsx_autoverify, "_tasks"):
            return 0
        tasks: dict[tuple[int, int], asyncio.Task] = schedule_ai_xlsx_autoverify._tasks  # type: ignore[attr-defined]
        keys = [k for k in list(tasks.keys()) if k[0] == uid]
        cancelled = 0
        for k in keys:
            t = tasks.pop(k, None)
            if t and not t.done():
                try:
                    t.cancel()
                    try:
                        await t
                    except Exception:
                        pass
                except Exception:
                    pass
                cancelled += 1
        return cancelled
    except Exception:
        return 0   
# === /AI Assistant ===





# ====== IMAP loop + UI (/read, /stop, /status) ======
# Старая функция imap_loop_optimized удалена - используется process pool архитектура
        
async def _ensure_imap_stopped_for_user(uid: int):
    """
    Останавливает все процессы IMAP для пользователя.
    ВАЖНО: Принудительно останавливаем все аккаунты и очищаем статусы,
    чтобы гарантировать освобождение памяти и предотвратить накопление процессов.
    """
    try:
        st = ensure_user_imap_status(uid)
        async with st.lock:
            st.running = False
        
        # Останавливаем все процессы пользователя
        # Останавливаем все аккаунты пользователя (новая архитектура)
        keys_to_stop = [key for key in list(IMAP_ACCOUNT_STATUS.keys()) if key[0] == uid]
        if keys_to_stop:
            log_send_event(f"IMAP: Stopping {len(keys_to_stop)} accounts for uid={uid}")
            for key in keys_to_stop:
                try:
                    await stop_imap_process(key[0], key[1])
                    # Принудительно помечаем как неактивный
                    IMAP_ACCOUNT_STATUS[key] = {"active": False}
                except Exception as e:
                    log_send_event(f"IMAP: Error stopping account uid={uid} acc_id={key[1]}: {e}")
                    # Даже при ошибке помечаем как неактивный
                    try:
                        IMAP_ACCOUNT_STATUS[key] = {"active": False}
                    except Exception:
                        pass
            
            # Дополнительная очистка: удаляем все записи статусов для пользователя
            # Это гарантирует, что воркеры не будут обрабатывать эти аккаунты
            for key in keys_to_stop:
                try:
                    IMAP_ACCOUNT_STATUS.pop(key, None)
                except Exception:
                    pass
            
            log_send_event(f"IMAP: All accounts stopped and cleaned up for uid={uid}")
        else:
            log_send_event(f"IMAP: No accounts to stop for uid={uid}")
        
        # Дополнительная очистка: очищаем статусы аккаунтов в runtime статусе
        try:
            async with st.lock:
                for email in list(st.account_status.keys()):
                    if email != "_meta":
                        acc_status = st.account_status.get(email, {})
                        if isinstance(acc_status, dict):
                            acc_status["active"] = False
        except Exception as e:
            log_send_event(f"IMAP: Error cleaning up account_status for uid={uid}: {e}")
    except Exception as e:
        log_send_event(f"IMAP: Error in _ensure_imap_stopped_for_user uid={uid}: {e}")


async def _get_user_accounts(uid: int) -> List[Any]:
    return await list_accounts_async(uid)

def _split_active_inactive(accounts: List[Account]) -> Tuple[List[Account], List[Account]]:
    act = [a for a in accounts if a.active]
    ina = [a for a in accounts if not a.active]
    return act, ina
    
def _runtime_is_active(uid: int, email: str) -> bool:
    """
    Проверяет runtime-статус аккаунта: проверяет наличие активного аккаунта в обработке (новая архитектура).
    """
    try:
        st = IMAP_STATUS.get(uid)
        if isinstance(st, dict):
            st = ensure_user_imap_status(uid)
        
        if not st or not getattr(st, "running", False):
            return False
        
        # Проверяем наличие активного аккаунта в обработке
        # Ищем в accounts по email
        acc = st.accounts.get(email) if hasattr(st, "accounts") else None
        if not acc:
            return False
        
        acc_id = int(getattr(acc, "id", 0))
        if not acc_id:
            return False
        
        # Проверяем, есть ли активный аккаунт в статусе (новая архитектура)
        key = (uid, acc_id)
        account_status = IMAP_ACCOUNT_STATUS.get(key)
        if account_status and account_status.get("active", False):
            return True
        
        return False
    except Exception:
        return False

async def _kb_read_menu(uid: int) -> InlineKeyboardMarkup:
    accounts = await _get_user_accounts(uid)
    need_start = [a for a in accounts if not _runtime_is_active(uid, a.email)]
    rows: list[list[InlineKeyboardButton]] = []
    for i, a in enumerate(need_start, start=1):
        rows.append([InlineKeyboardButton(text=f"E‑mail №{i}: {a.email}", callback_data=f"imap:start:{a.id}")])
    rows.append([InlineKeyboardButton(text="Запустить все потоки", callback_data="imap:start_all")])
    rows.append([InlineKeyboardButton(text="Скрыть", callback_data="ui:hide")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

async def _kb_stop_menu(uid: int) -> InlineKeyboardMarkup:
    accounts = await _get_user_accounts(uid)
    act_runtime = [a for a in accounts if _runtime_is_active(uid, a.email)]
    rows: list[list[InlineKeyboardButton]] = []
    for i, a in enumerate(act_runtime, start=1):
        rows.append([InlineKeyboardButton(text=f"E‑mail №{i}: {a.email}", callback_data=f"imap:stop:{a.id}")])
    rows.append([InlineKeyboardButton(text="Остановить все потоки", callback_data="imap:stop_all")])
    rows.append([InlineKeyboardButton(text="Скрыть", callback_data="ui:hide")])
    return InlineKeyboardMarkup(inline_keyboard=rows)



@dp.message(Command("read"))
async def cmd_read(m: types.Message):
    if not await ensure_approved(m): 
        return
    await delete_message_safe(m)
    uid = await U(m)
    accounts = await _get_user_accounts(uid)
    if not accounts:
        await bot.send_message(m.chat.id, "Аккаунтов не найдено.")
        return
    need_start_exists = any(not _runtime_is_active(uid, a.email) for a in accounts)
    text = "Нажмите на E‑mail для запуска потока чтения:" if need_start_exists else "Все потоки уже запущены."
    kb = await _kb_read_menu(uid)
    await bot.send_message(m.chat.id, text, reply_markup=kb)

@dp.message(Command("stop"))
async def cmd_stop(m: types.Message):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    uid = await U(m)
    accounts = await _get_user_accounts(uid)
    act, _ = _split_active_inactive(accounts)
    if not accounts:
        await bot.send_message(m.chat.id, "Аккаунтов не найдено."); return
    text = "Нажмите на E‑mail для остановки потока чтения:" if act else "Нет активных потоков."
    kb = await _kb_stop_menu(uid)
    await bot.send_message(m.chat.id, text, reply_markup=kb)
    
@dp.message(Command("ai_stop"))
async def ai_stop_cmd(m: types.Message):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)
    uid = await U(m)

    # Отменяем сценарии авто‑reply/линк/поля (AI_ASSISTANT_TASKS)
    total_for_user = sum(1 for (u, _k) in AI_ASSISTANT_TASKS.keys() if u == uid)

    stopped = 0
    try:
        keys = [k for k in list(AI_ASSISTANT_TASKS.keys()) if k[0] == uid]
        for k in keys:
            t = AI_ASSISTANT_TASKS.pop(k, None)
            if t and not t.done():
                try:
                    t.cancel()
                    await t
                except Exception:
                    pass
            stopped += 1
    except Exception:
        pass

    # Дополнительно: останавливаем автопроверки XLSX (ИИ‑автопоток)
    try:
        stopped_xlsx = await ai_xlsx_autoverify_cancel_all_for_user(uid)
    except Exception:
        stopped_xlsx = 0

    # NEW: очищаем «дедуп» отправителей — даём шанс автозапуску снова
    try:
        ai_sender_dedup_reset(uid)
    except Exception:
        pass

    if total_for_user > 0 or stopped_xlsx > 0:
        msg = f"ИИ: остановлено сценариев — {stopped}. Автопроверок XLSX — {stopped_xlsx}. Дедуп очищен."
    else:
        msg = "ИИ: активных сценариев не найдено. Дедуп очищен."

    try:
        await bot.send_message(m.chat.id, msg)
    except Exception:
        pass
    
def send_accounts_menu_kb(items: list, disabled: set[int]) -> InlineKeyboardMarkup:
    """
    Клавиатура для переключения аккаунтов массовой рассылки.
    🟢 email  — аккаунт участвует в массовой рассылке
    🔴 email  — отключён (но читает входящие и доступен для reply)
    """
    rows = []
    for a in items:
        acc_id = getattr(a, "id", None)
        if acc_id is None:
            continue
        email = getattr(a, "email", "") or ""
        enabled = acc_id not in disabled
        mark = "🟢" if enabled else "🔴"
        rows.append([InlineKeyboardButton(
            text=f"{mark} {email}",
            callback_data=f"sendacc:toggle:{acc_id}"
        )])
    rows.append([InlineKeyboardButton(text="♻️ Скрыть", callback_data="ui:hide")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("sendacc"))
async def sendacc_cmd(m: types.Message):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)

    internal_id = await U(m)          # внутренний ID (для БД)
    chat_id = m.chat.id               # реальный Telegram chat_id (куда шлём ответы)

    accounts = await list_accounts_async(internal_id)
    if not accounts:
        await bot.send_message(chat_id, "Аккаунтов нет.")
        return

    await ensure_send_disabled_loaded(internal_id)
    kb = send_accounts_menu_kb(
        accounts,
        SEND_DISABLED_ACCOUNTS.get(internal_id, set())
    )

    await bot.send_message(
        chat_id,
        "Переключите аккаунты (массовая рассылка ON/OFF):",
        reply_markup=kb
    )


@dp.callback_query(F.data.startswith("sendacc:toggle:"))
async def sendacc_toggle_cb(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    try:
        acc_id = int(c.data.split(":")[2])
    except Exception:
        await c.answer("Ошибка ID", show_alert=True)
        return
    uid = await U(c)
    acc = await get_account_async(uid, acc_id)
    if not acc:
        await c.answer("Аккаунт не найден", show_alert=True)
        return
    new_enabled = await toggle_account_send_enabled(uid, acc_id)
    # Инвалидируем ctx, чтобы массовый сендинг не использовал отключённый аккаунт
    try:
        invalidate_user_ctx(uid)
    except Exception:
        pass
    # Перестройка клавиатуры
    accounts = await list_accounts_async(uid)
    await ensure_send_disabled_loaded(uid)
    kb = send_accounts_menu_kb(accounts, SEND_DISABLED_ACCOUNTS.get(uid, set()))
    try:
        await c.message.edit_reply_markup(reply_markup=kb)
    except Exception:
        try:
            await safe_edit_message(c.message, "Переключите аккаунты (массовая рассылка ON/OFF):", reply_markup=kb)
        except Exception:
            pass
    await safe_cq_answer(c, "ON" if new_enabled else "OFF")

async def _status_text(uid: int) -> str:
    """
    Возвращает текст статуса IMAP потоков и статуса участия аккаунтов в массовой рассылке.
    Убрана метка [AUTH_FAIL] — аккаунты с permanent auth error теперь показываются как обычные
    'неактивен ❌'. Логику perm_auth_error сохраняем (можно использовать в будущем).
    
    ВАЖНО: Использует ту же логику проверки активности, что и /read команда (_runtime_is_active),
    чтобы статус совпадал с реальным состоянием процессов.
    """
    accounts = await _get_user_accounts(uid)
    if not accounts:
        return "Аккаунтов не найдено."
    
    await ensure_send_disabled_loaded(uid)
    disabled = SEND_DISABLED_ACCOUNTS.get(uid, set())

    lines: list[str] = []
    for a in accounts:
        # ВАЖНО: Используем ту же логику, что и в /read команде
        is_active = _runtime_is_active(uid, a.email)
        acc_id = getattr(a, "id", 0)
        send_enabled = acc_id not in disabled
        send_mark = "🟢send" if send_enabled else "🔴send"

        # Раньше тут была отдельная ветка с [AUTH_FAIL].
        # Теперь просто показываем как 'неактивен ❌'.
        lines.append(
            f"{a.email} {'активен ✅' if is_active else 'неактивен ❌'} [{send_mark}]"
        )

    return "\n".join(lines)


@dp.message(Command("status"))
async def cmd_status(m: types.Message):
    if not await ensure_approved(m):
        return
    await delete_message_safe(m)

    # Базовый текст статуса
    text = await _status_text(await U(m))

    # Компактируем вывод:
    # - убираем слово "неактивен"
    # - убираем слово "активен" (оставляем только иконки)
    # - сжимаем метку send: "[🟢send]" -> "[🟢]", "[🔴send]" -> "[🔴]"
    import re
    compact = re.sub(r"\bнеактивен\b", "", text, flags=re.IGNORECASE)
    compact = re.sub(r"\bактивен\s*", "", compact, flags=re.IGNORECASE)
    compact = (compact
               .replace("[🟢send]", "[🟢]")
               .replace("[🔴send]", "[🔴]")
               .replace("[ 🟢 send ]", "[🟢]")
               .replace("[ 🔴 send ]", "[🔴]")
               .replace("[🟢 send]", "[🟢]")
               .replace("[🔴 send]", "[🔴]"))
    # Убираем лишние пробелы перед переводами строк и подряд идущие пробелы
    compact = re.sub(r"[ \t]+", " ", compact)
    compact = re.sub(r"\s+\n", "\n", compact).strip()

    # Дополнительное ужатие, если всё ещё близко к лимиту:
    # удаляем лишние пробелы вокруг скобок
    compact = compact.replace(" [", " [").replace("] ", "] ")
    compact = re.sub(r"\s{2,}", " ", compact)

    # Кнопка «Скрыть» — как и раньше
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Скрыть", callback_data="ui:hide")]
        ]
    )

    # Пытаемся отправить одним сообщением; если внезапно всё ещё длиннее лимита — делаем
    # сверхкомпактизацию (убираем иконки активности), и снова пробуем одним сообщением.
    try:
        await bot.send_message(m.chat.id, compact, reply_markup=kb)
    except TelegramBadRequest as e:
        if "message is too long" in str(e):
            ultra = compact.replace("✅", "").replace("❌", "")
            ultra = re.sub(r"\s+\n", "\n", ultra).strip()
            await bot.send_message(m.chat.id, ultra, reply_markup=kb)
        else:
            raise

@dp.callback_query(F.data.startswith("imap:start:"))
async def imap_start_one(c: types.CallbackQuery):
    """Запуск процесса IMAP для одного аккаунта"""
    if not await ensure_approved(c): return
    uid = await U(c)
    acc_id = int(c.data.split(":")[2])

    acc = await get_account_async(uid, acc_id)
    if not acc:
        await c.answer("Аккаунт не найден", show_alert=True); return

    await set_account_active_async(uid, acc_id, True)
    email = getattr(acc, "email", "")

    key = (uid, email)
    START_LOG_SENT.pop(key, None)
    ERROR_LOG_SENT.pop(key, None)

    st = ensure_user_imap_status(uid)
    async with st.lock:
        st.running = True
        st.account_status.setdefault("_meta", {})["chat_id"] = c.message.chat.id
        st.account_status.setdefault(email, {})
        st.account_status[email].update({
            "retry_at": 0,
            "retries": 0,
            "last_err": None,
            "active": False,
        })

    # Получаем контекст для прокси
    ctx = await get_user_ctx_async(uid)
    # Получаем прокси для аккаунта (ОБЯЗАТЕЛЬНО)
    proxy = None
    try:
        proxy = smtp25.get_next_proxy_ctx(ctx, "send")
    except Exception as e:
        log_send_event(f"IMAP: Failed to get proxy for uid={uid} acc_id={acc_id} email={email}: {e}")
    
    # Проверка наличия прокси перед запуском
    if not proxy:
        await safe_cq_answer(c, f"❌ Не удалось запустить чтение: нет доступного прокси (прокси обязателен для IMAP)")
        log_send_event(f"IMAP: Cannot start process for uid={uid} acc_id={acc_id} email={email}: no proxy available (proxy is required)")
        return
    
    # Запускаем процесс (прокси уже проверен в start_imap_process)
    success = await start_imap_process(
        user_id=uid,
        acc_id=acc_id,
        email=email,
        password=getattr(acc, "password", ""),
        display_name=getattr(acc, "display_name", "") or getattr(acc, "name", "") or "",
        chat_id=c.message.chat.id,
        proxy=proxy
    )
    
    if not success:
        await safe_cq_answer(c, f"❌ Не удалось запустить чтение (проверьте логи)")
        return
    
    kb = await _kb_read_menu(uid)
    await safe_edit_message(c.message, "Нажмите на E‑mail для запуска потока чтения:", reply_markup=kb)
    await safe_cq_answer(c, "Запущено")

@dp.callback_query(F.data == "imap:start_all")
async def imap_start_all(c: types.CallbackQuery):
    """
    Запустить все потоки: активируем все аккаунты и инициируем "startup burst".
    Добавлен случайный джиттер для account_backoff, чтобы избежать лавины соединений.
    """
    if not await ensure_approved(c): 
        return
    uid = await U(c)

    accounts = await list_accounts_async(uid)
    # Активируем все неактивные
    to_activate_emails = [getattr(row, "email") for row in accounts if not getattr(row, "active", False)]
    if to_activate_emails:
        await activate_all_accounts_async(uid)

    # Собираем актуальный список активных аккаунтов
    accounts = await list_accounts_async(uid)
    active_accounts = [a for a in accounts if getattr(a, "active", False) and getattr(a, "email", "")]

    st = ensure_user_imap_status(uid)
    async with st.lock:
        now = time.time()
        # Сброс лог‑флагов и установка «почти немедленного» due с джиттером
        for a in active_accounts:
            email = getattr(a, "email", "")
            key = (uid, email)
            START_LOG_SENT.pop(key, None)
            ERROR_LOG_SENT.pop(key, None)
            st.account_status.setdefault(email, {})
            st.account_status[email].update({
                "retry_at": 0,
                "retries": 0,
                "last_err": None,
                # НЕ устанавливаем active=False здесь - это будет установлено в start_imap_process
            })
            # небольшой джиттер до 2 секунд, чтобы не стартовали все одновременно
            st.account_backoff[email] = now + random.uniform(0.0, 2.0)

        # Кэш активных аккаунтов и подготовка очереди обхода
        st.accounts = {getattr(a, "email"): a for a in active_accounts}
        st.last_accounts_check = time.time()

        meta = st.account_status.setdefault("_meta", {})
        meta["poll_list"] = list(st.accounts.keys())
        meta["poll_idx"] = 0
        meta["startup_burst"] = True  # включаем залповый первый проход

    # Стартуем фоновую задачу (если ещё не запущена)
    await _ensure_imap_started_for_user(uid, c.message.chat.id)

    kb = await _kb_stop_menu(uid)
    await safe_edit_message(c.message, "Все потоки запущены.", reply_markup=kb)
    await safe_cq_answer(c, "OK")

@dp.callback_query(F.data.startswith("imap:stop:"))
async def imap_stop_one(c: types.CallbackQuery):
    """Остановка процесса IMAP для одного аккаунта"""
    if not await ensure_approved(c):
        return
    uid = await U(c)
    acc_id = int(c.data.split(":")[2])

    acc = await get_account_async(uid, acc_id)
    if not acc:
        await c.answer("Аккаунт не найден", show_alert=True)
        return

    await set_account_active_async(uid, acc_id, False)
    email = getattr(acc, "email", "")

    # Останавливаем процесс
    success = await stop_imap_process(uid, acc_id)
    if success:
        log_send_event(f"IMAP: Account stopped successfully uid={uid} acc_id={acc_id} email={email}")
    else:
        log_send_event(f"IMAP: Account stop completed (was not in queue) uid={uid} acc_id={acc_id} email={email}")

    st = ensure_user_imap_status(uid)
    async with st.lock:
        st.account_status.setdefault(email, {})
        st.account_status[email].update({"active": False})

    # Ограничение скорости: не более 1 сообщения в 2 секунды
    chat_id = c.message.chat.id
    now = time.time()
    last_ts = _LAST_STOP_MESSAGE_TS.get(chat_id, 0)
    elapsed = now - last_ts
    
    if elapsed < STOP_MESSAGE_MIN_INTERVAL:
        # Ждем, пока пройдет минимальный интервал
        wait_time = STOP_MESSAGE_MIN_INTERVAL - elapsed
        await asyncio.sleep(wait_time)
        now = time.time()
    
    await c.message.answer(f"Поток для {code(email)} остановлен⚡")
    _LAST_STOP_MESSAGE_TS[chat_id] = now

    accounts = await list_accounts_async(uid)
    has_active = any(getattr(a, "active", False) for a in accounts)
    if not has_active:
        await _ensure_imap_stopped_for_user(uid)

    text = "Нажмите на E‑mail для остановки потока чтения:" if has_active else "Нет активных потоков."
    kb = await _kb_stop_menu(uid)
    await safe_edit_message(c.message, text, reply_markup=kb)
    await safe_cq_answer(c, "Остановлено")
    
@dp.callback_query(F.data == "imap:stop_all")
async def imap_stop_all(c: types.CallbackQuery):
    if not await ensure_approved(c):
        return
    uid = await U(c)

    accounts = await list_accounts_async(uid)
    emails = [getattr(a, "email") for a in accounts if getattr(a, "active", False)]
    if emails:
        await deactivate_all_accounts_async(uid)

    st = ensure_user_imap_status(uid)
    chat_id = c.message.chat.id
    
    # Обновляем статусы аккаунтов (быстро, внутри lock)
    async with st.lock:
        for email in emails:
            st.account_status.setdefault(email, {})
            st.account_status[email].update({"active": False})
    
    # Отправляем сообщения об остановке с ограничением скорости (вне lock)
    for email in emails:
        try:
            # Ограничение скорости: не более 1 сообщения в 2 секунды
            now = time.time()
            last_ts = _LAST_STOP_MESSAGE_TS.get(chat_id, 0)
            elapsed = now - last_ts
            
            if elapsed < STOP_MESSAGE_MIN_INTERVAL:
                # Ждем, пока пройдет минимальный интервал
                wait_time = STOP_MESSAGE_MIN_INTERVAL - elapsed
                await asyncio.sleep(wait_time)
                now = time.time()
            
            await c.message.answer(f"Поток для {code(email)} остановлен⚡")
            _LAST_STOP_MESSAGE_TS[chat_id] = now
        except Exception:
            pass

    # Останавливаем все процессы IMAP для пользователя
    log_send_event(f"IMAP: Stopping all accounts for uid={uid} (count={len(emails)})")
    await _ensure_imap_stopped_for_user(uid)
    log_send_event(f"IMAP: All accounts stopped for uid={uid}")
    kb = await _kb_stop_menu(uid)
    await safe_edit_message(c.message, "Нет активных потоков.", reply_markup=kb)
    await safe_cq_answer(c, "Остановлено")
    
@dp.message(PresetEditFSM.preset_id)
async def presets_edit_pick(m: types.Message, state: FSMContext):
    if not await ensure_approved(m): return
    await delete_message_safe(m)
    text = (m.text or "").strip()
    if not text.isdigit():
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Нужен номер пресета (например: 1).", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
        return
    ordinal = int(text)
    presets = await list_presets_async(await U(m))
    chosen = presets[ordinal-1] if 1 <= ordinal <= len(presets) else None
    if not chosen:
        await ui_clear_prompts(state)
        await ui_prompt(state, m.chat.id, "Неверный номер.", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
        return
    await state.update_data(preset_id=int(chosen.id))
    await ui_clear_prompts(state)
    await ui_prompt(state, m.chat.id, "Введите заголовок пресета:", reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_row("presets:open")))
    await state.set_state(PresetEditFSM.title)
    
# ====== MAIN ======
async def set_bot_commands(bot: Bot):
    commands = [
        BotCommand(command="start", description="Начать работу"),
        BotCommand(command="settings", description="Настройки"),
        BotCommand(command="check", description="Проверка ников (XLSX)"),
        BotCommand(command="send", description="Отправить email"),
        BotCommand(command="quickadd", description="Быстрое добавление"),
        BotCommand(command="read", description="IMAP: запуск потоков"),
        BotCommand(command="status", description="IMAP: статус"),
        BotCommand(command="stop", description="IMAP: остановка потоков"),
        BotCommand(command="sendacc", description="ON/OFF аккаунтов для рассылки"),
        BotCommand(command="ai_stop", description="Остановить ИИ-сценарии"),  # ← добавить
        BotCommand(command="admin", description="Админка"),
    ]
    await bot.set_my_commands(commands)
    
def invalidate_user_cache(user_id: int):
    """Эффективная инвалидация"""
    # Для ACCOUNTS_CACHE
    keys_to_remove = []
    for key in list(ACCOUNTS_CACHE.keys()):
        if isinstance(key, str) and key.startswith(f"accounts_{user_id}"):
            keys_to_remove.append(key)
        elif isinstance(key, tuple) and len(key) > 0 and key[0] == user_id:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        ACCOUNTS_CACHE.pop(key, None)
    
    # Для других кэшей
    USER_CTX_CACHE.pop(user_id, None)
    DOMAINS_CACHE.pop(f"domains_{user_id}", None)
    
async def cleanup_user_runtime(user_id: int):
    """
    Полная очистка рантайма пользователя: остановка IMAP/SEND задач,
    Outbox, кэши и временные структуры.
    ДОБАВЛЕНО: отмена всех отложенных ИИ‑сценариев пользователя.
    ДОБАВЛЕНО: отмена фоновых задач автопроверки XLSX (ИИ).
    """
    # 1) Остановить SEND-таск, если идёт
    try:
        t = SEND_TASKS.pop(user_id, None)
        if t and not t.done():
            try:
                SEND_STATUS.setdefault(user_id, {})["cancel"] = True
            except Exception:
                pass
            try:
                t.cancel()
            except Exception:
                pass
            try:
                await t
            except Exception:
                pass
    except Exception:
        pass
    try:
        SEND_STATUS.pop(user_id, None)
    except Exception:
        pass

    # 2) Остановить IMAP луп и почистить статусы
    try:
        await _ensure_imap_stopped_for_user(user_id)
    except Exception:
        pass
    try:
        IMAP_TASKS.pop(user_id, None)
    except Exception:
        pass
    try:
        IMAP_STATUS.pop(user_id, None)
    except Exception:
        pass

    # 3) Почистить лог‑флаги по аккаунтам пользователя
    try:
        for k in list(START_LOG_SENT.keys()):
            if isinstance(k, tuple) and len(k) >= 1 and k[0] == user_id:
                START_LOG_SENT.pop(k, None)
    except Exception:
        pass
    try:
        for k in list(ERROR_LOG_SENT.keys()):
            if isinstance(k, tuple) and len(k) >= 1 and k[0] == user_id:
                ERROR_LOG_SENT.pop(k, None)
    except Exception:
        pass

    # 4) Очистить runtime‑контексты и кэши
    try:
        USER_CTX.pop(user_id, None)
    except Exception:
        pass
    try:
        REPLY_RUNTIME.pop(user_id, None)
    except Exception:
        pass
    try:
        for k in list(INCOMING_RT.keys()):
            if isinstance(k, tuple) and len(k) >= 1 and k[0] == user_id:
                INCOMING_RT.pop(k, None)
    except Exception:
        pass

    # 5.5) Остановить все процессы IMAP для пользователя
    try:
        # Останавливаем все аккаунты пользователя (новая архитектура)
        keys_to_stop = [key for key in IMAP_ACCOUNT_STATUS.keys() if key[0] == user_id]
        for key in keys_to_stop:
            try:
                await stop_imap_process(key[0], key[1])
            except Exception:
                pass
    except Exception:
        pass

    # 5.6) Остановить Outbox‑воркер и очистить очередь
    try:
        t = OUTBOX_TASKS.pop(user_id, None)
        if t and not t.done():
            t.cancel()
            try:
                await t
            except Exception:
                pass
    except Exception:
        pass
    try:
        q = OUTBOX_QUEUES.pop(user_id, None)
        if q:
            while not q.empty():
                try:
                    q.get_nowait()
                    q.task_done()
                except Exception:
                    break
    except Exception:
        pass
    try:
        for k in list(_LAST_OUTBOX_TS.keys()):
            if isinstance(k, tuple) and k and k[0] == user_id:
                _LAST_OUTBOX_TS.pop(k, None)
    except Exception:
        pass

    # 6) Инвалидация кэшей (аккаунты/домены/ctx)
    try:
        invalidate_user_cache(user_id)
    except Exception:
        pass

    # 7) Побочно: возможные кеши по chat_id (обычно равен tg_id пользователя)
    try:
        chat_id = user_id
        LAST_XLSX_PER_CHAT.pop(chat_id, None)
        VERIFIED_ROWS_PER_CHAT.pop(chat_id, None)
        BASES_PER_CHAT.pop(chat_id, None)
    except Exception:
        pass

    # 8) ДОБАВЛЕНО: снять все отложенные сценарии ИИ (без потери состояния шагов)
    try:
        await ai_assistant_cancel_all_for_user(user_id)
    except Exception:
        pass

    # 9) ДОБАВЛЕНО: остановить фоновые задачи автопроверки XLSX (ИИ)
    try:
        await ai_xlsx_autoverify_cancel_all_for_user(user_id)
    except Exception:
        pass

    # 10) ДОБАВЛЕНО: сброс анти‑дубликата автозапуска ИИ по отправителям
    try:
        AI_SENDER_DEDUP.pop(user_id, None)
    except Exception:
        pass
    
    # 11) ДОБАВЛЕНО: очистка записей периода карантина для быстро добавленных аккаунтов
    try:
        keys_to_remove = [key for key in QUICK_ADD_ACTIVATED_AT.keys() if key[0] == user_id]
        for key in keys_to_remove:
            QUICK_ADD_ACTIVATED_AT.pop(key, None)
    except Exception:
        pass





    




async def main():
    """
    Полный main с инициализацией бота, неблокирующим логгером, прогревом кэшей,
    запуском планировщиков (cleanup + глобальные IMAP‑воркеры), установкой /команд
    и корректным завершением.
    """
    global bot

    # 1) Инициализация бота (aiogram v3)
    # Настраиваем TCPConnector для использования только IPv4 (избегаем проблем с PySocks и IPv6)
    # ВАЖНО: PySocks не поддерживает IPv6, поэтому принудительно используем только IPv4
    # Проблема: aiohttp использует aiohappyeyeballs, который пытается использовать IPv6,
    # даже если мы указали family=socket.AF_INET. Решение: модифицируем socket.getaddrinfo
    # на глобальном уровне для фильтрации IPv6 адресов.
    import socket
    
    # Сохраняем оригинальные функции для возможного восстановления
    _original_socket_getaddrinfo = socket.getaddrinfo
    
    def ipv4_only_socket_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        """
        Обертка над socket.getaddrinfo, которая фильтрует IPv6 адреса.
        Возвращает только IPv4 адреса (AF_INET).
        Это предотвращает использование IPv6 через aiohappyeyeballs в aiohttp,
        что вызывает ошибки при работе с PySocks (который не поддерживает IPv6).
        """
        # Принудительно используем только IPv4
        if family == 0 or family == socket.AF_UNSPEC:
            family = socket.AF_INET
        elif family != socket.AF_INET:
            # Если явно запрашивается IPv6, возвращаем пустой список
            return []
        
        # Вызываем оригинальный getaddrinfo с принудительным IPv4
        try:
            results = _original_socket_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
            # Фильтруем результаты - оставляем только IPv4 (на всякий случай)
            ipv4_results = [r for r in results if r and len(r) > 0 and r[0] == socket.AF_INET]
            return ipv4_results if ipv4_results else results
        except Exception:
            # В случае ошибки возвращаем пустой список
            return []
    
    # Заменяем socket.getaddrinfo на глобальном уровне
    # ВАЖНО: Это влияет на все HTTP-запросы в основном потоке (aiogram, aiohttp),
    # но не влияет на executor-потоки (где используется локальная модификация для SMTP)
    socket.getaddrinfo = ipv4_only_socket_getaddrinfo
    
    # Также модифицируем asyncio.getaddrinfo, если он используется (aiohappyeyeballs может использовать его)
    try:
        import asyncio
        _original_asyncio_getaddrinfo = asyncio.getaddrinfo
        
        async def ipv4_only_asyncio_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            """
            Обертка над asyncio.getaddrinfo, которая фильтрует IPv6 адреса.
            Возвращает только IPv4 адреса (AF_INET).
            """
            # Принудительно используем только IPv4
            if family == 0 or family == socket.AF_UNSPEC:
                family = socket.AF_INET
            elif family != socket.AF_INET:
                # Если явно запрашивается IPv6, возвращаем пустой список
                return []
            
            # Вызываем оригинальный getaddrinfo с принудительным IPv4
            try:
                results = await _original_asyncio_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
                # Фильтруем результаты - оставляем только IPv4
                ipv4_results = [r for r in results if r and len(r) > 0 and r[0] == socket.AF_INET]
                return ipv4_results if ipv4_results else results
            except Exception:
                # В случае ошибки возвращаем пустой список
                return []
        
        asyncio.getaddrinfo = ipv4_only_asyncio_getaddrinfo
        log_send_event("STARTUP: IPv4-only mode enabled for HTTP requests (socket.getaddrinfo and asyncio.getaddrinfo patched)")
    except (ImportError, AttributeError):
        # Если asyncio.getaddrinfo недоступен, используем только socket.getaddrinfo
        log_send_event("STARTUP: IPv4-only mode enabled for HTTP requests (socket.getaddrinfo patched)")
    
    # Создаем AiohttpSession (патч socket.getaddrinfo уже обеспечивает использование только IPv4)
    # AiohttpSession создает коннектор внутри себя, но патч getaddrinfo гарантирует IPv4
    bot_session = AiohttpSession()
    bot = Bot(
        token=_get_bot_token(),
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=bot_session
    )
    log_send_event("STARTUP: Bot initialized with IPv4-only mode (via socket.getaddrinfo patch)")

    # 2) Логгер отправки — неблокирующий (фоновая нитка пишет в файл)
    setup_nonblocking_send_logger()

    # 3) Прогрев персистентных кэшей (необязательно, но ускоряет старт)
    try:
        load_user_ctx_caches_on_start(ttl_seconds=172800)  # 48ч
    except Exception as e:
        log_send_event(f"STARTUP: load_user_ctx_caches_on_start error: {e}")
    try:
        load_ad_caches_on_start()
    except Exception as e:
        log_send_event(f"STARTUP: load_ad_caches_on_start error: {e}")
    try:
        load_ai_sender_dedup_caches_on_start(ttl_seconds=604800)  # 7 дней
    except Exception as e:
        log_send_event(f"STARTUP: load_ai_sender_dedup_caches_on_start error: {e}")

    # 4) Фоновые задачи: планировщик одноразовой очистки каждые 48ч
    cleanup_task: asyncio.Task | None = None
    try:
        cleanup_task = asyncio.create_task(cleanup_scheduler())
    except Exception as e:
        log_send_event(f"STARTUP: cannot start cleanup_scheduler: {e}")

    # 5) IMAP пул воркеров и обработка результатов
    # Инициализируем пул воркеров при старте приложения
    try:
        if init_imap_worker_pool():
            log_send_event("STARTUP: IMAP worker pool initialized")
        else:
            log_send_event("STARTUP: IMAP worker pool initialization failed")
    except Exception as e:
        log_send_event(f"STARTUP: IMAP worker pool initialization error: {e}")
    
    # Запускаем глобальный обработчик результатов из очереди воркеров
    imap_result_processor_task: asyncio.Task | None = None
    try:
        imap_result_processor_task = asyncio.create_task(_process_imap_results_global())
        log_send_event("STARTUP: IMAP result processor started")
    except Exception as e:
        log_send_event(f"STARTUP: cannot start IMAP result processor: {e}")
    
    # Запускаем watchdog для перезапуска упавших воркеров
    watchdog_task: asyncio.Task | None = None
    try:
        watchdog_task = asyncio.create_task(_imap_watchdog())
        log_send_event("STARTUP: IMAP watchdog started")
    except Exception as e:
        log_send_event(f"STARTUP: cannot start IMAP watchdog: {e}")

    # 6) /команды бота
    try:
        await set_bot_commands(bot)
    except Exception as e:
        log_send_event(f"STARTUP: set_bot_commands failed: {e}")

    # 7) Запуск polling
    try:
        log_send_event("STARTUP: Starting bot polling...")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except KeyboardInterrupt:
        log_send_event("SHUTDOWN: Bot stopped by user (KeyboardInterrupt)")
        raise
    except SystemExit:
        log_send_event("SHUTDOWN: Bot stopped by system (SystemExit)")
        raise
    except Exception as e:
        # Критическая ошибка в polling - логируем и пробрасываем
        log_send_event(f"CRITICAL: Bot polling failed: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        raise
    finally:
        # 1) Cancel & await все корутины, которые могут обращаться к bot или к сетевым сессиям
        # ВАЖНО: отменяем ДО закрытия bot.session и сетевых сессий
        
        # 1.1) Остановить планировщик очистки
        try:
            if cleanup_task:
                cleanup_task.cancel()
                try:
                    await cleanup_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        except Exception:
            pass

        # 1.2) Остановить IMAP result processor
        # ВАЖНО: отменяем ДО shutdown_imap_worker_pool(), чтобы не читать из удалённой очереди
        try:
            if imap_result_processor_task:
                imap_result_processor_task.cancel()
                try:
                    await imap_result_processor_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        except Exception:
            pass
        
        # 1.3) Остановить IMAP watchdog
        try:
            if watchdog_task:
                watchdog_task.cancel()
                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
        except Exception:
            pass

        # 1.4) Отменить и await все OUTBOX_TASKS (per-user фоновые задачи)
        # ВАЖНО: эти задачи могут блокироваться на q.get() или bot.send_message()
        try:
            outbox_tasks_to_cancel = list(OUTBOX_TASKS.values())
            for task in outbox_tasks_to_cancel:
                try:
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                except Exception:
                    pass
            OUTBOX_TASKS.clear()
            OUTBOX_QUEUES.clear()
        except Exception:
            pass

        # 1.5) Отменить и await все START_LOG_DRAINERS (per-user дренеры логов)
        # ВАЖНО: эти дренеры могут обращаться к bot.send_message()
        try:
            drainer_tasks_to_cancel = list(START_LOG_DRAINERS.values())
            for task in drainer_tasks_to_cancel:
                try:
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                except Exception:
                    pass
            START_LOG_DRAINERS.clear()
        except Exception:
            pass

        # 1.6) Отменить и await все SEND_TASKS (per-user задачи отправки)
        # ВАЖНО: эти задачи могут обращаться к bot.send_message()
        try:
            send_tasks_to_cancel = list(SEND_TASKS.values())
            for task in send_tasks_to_cancel:
                try:
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                except Exception:
                    pass
            SEND_TASKS.clear()
        except Exception:
            pass

        # 1.7) Отменить и await все AI_ASSISTANT_TASKS (per-user AI сценарии)
        # ВАЖНО: эти задачи могут обращаться к bot.send_message()
        try:
            ai_tasks_to_cancel = list(AI_ASSISTANT_TASKS.values())
            for task in ai_tasks_to_cancel:
                try:
                    if task and not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass
                except Exception:
                    pass
            AI_ASSISTANT_TASKS.clear()
        except Exception:
            pass

        # 1.8) Отменить и await все schedule_ai_xlsx_autoverify._tasks (per-user задачи автопроверки XLSX)
        # ВАЖНО: эти задачи могут обращаться к bot.send_message()
        try:
            if hasattr(schedule_ai_xlsx_autoverify, "_tasks"):
                xlsx_tasks_to_cancel = list(schedule_ai_xlsx_autoverify._tasks.values())  # type: ignore[attr-defined]
                for task in xlsx_tasks_to_cancel:
                    try:
                        if task and not task.done():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
                            except Exception:
                                pass
                    except Exception:
                        pass
                schedule_ai_xlsx_autoverify._tasks.clear()  # type: ignore[attr-defined]
        except Exception:
            pass

        # 2) Закрыть FSM‑хранилище
        try:
            await dp.storage.close()
            await dp.storage.wait_closed()
        except Exception:
            pass

        # 3) Закрыть HTTP‑сессию бота
        # ВАЖНО: закрываем ПОСЛЕ отмены всех задач, которые могут использовать bot
        try:
            await bot.session.close()
        except Exception:
            pass

        # 4) Остановить неблокирующий логгер
        try:
            stop_nonblocking_send_logger()
        except Exception:
            pass

        # 5) Закрыть глобальную HTTP сессию
        # ВАЖНО: _HTTP_SESSION может остаться открытой, что приведет к ResourceWarning
        try:
            global _HTTP_SESSION
            if _HTTP_SESSION and not _HTTP_SESSION.closed:
                await _HTTP_SESSION.close()
                _HTTP_SESSION = None
        except Exception:
            pass

        # 6) Остановить IMAP пул воркеров
        # ВАЖНО: вызываем ПОСЛЕ отмены imap_result_processor_task, чтобы он не пытался
        # читать из уже удалённой очереди. shutdown_imap_worker_pool() ставит stop-event
        # и безопасно terminate()/kill() зависшие процессы
        try:
            shutdown_imap_worker_pool()
        except Exception:
            pass

        # 7) Завершить executors
        try:
            IMAP_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        try:
            SHARED_EXECUTOR.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass


if __name__ == "__main__":
    import asyncio
    import sys
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Корректное завершение по Ctrl+C
        print("Bot stopped by user (KeyboardInterrupt)")
        sys.exit(0)
    except SystemExit:
        # Системный выход (может быть из systemd или другого сервиса)
        raise
    except Exception as e:
        # Критическая ошибка - логируем и завершаем с кодом ошибки
        try:
            # Пытаемся записать в лог, если возможно
            error_msg = f"CRITICAL ERROR: Bot crashed with exception: {type(e).__name__}: {e}\n{traceback.format_exc()}"
            print(error_msg, file=sys.stderr)
            # Также пытаемся записать в файл лога, если есть доступ
            try:
                with open("bot_crash.log", "a", encoding="utf-8") as f:
                    f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {error_msg}\n")
            except Exception:
                pass
        except Exception:
            # Если даже логирование не удалось, просто выводим в stderr
            print(f"CRITICAL ERROR: Bot crashed: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
# === Internal user id resolver with cache (U) ===

