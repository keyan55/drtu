# -*- coding: utf-8 -*-
"""
IMAP Runtime - Persistent connection IMAP architecture
======================================================

This module implements a persistent IMAP connection architecture where:
- Each account has its own process with a persistent connection
- Connections stay open and use NOOP/SEARCH every ~5 seconds
- Global connection limits prevent system overload
- Backoff mechanism handles problematic accounts
- Results are sent to main process via queue for Telegram publishing
"""

import asyncio
import time
import imaplib
import ssl
import socket
import socks
import re
import traceback
from typing import Optional, Dict, Any, Callable, Tuple, List
from dataclasses import dataclass, field
from multiprocessing import Queue, Process, Event
from html.parser import HTMLParser
from html import unescape as html_unescape
import html as html_module

# ===== CONSTANTS =====
# Global connection limits
MAX_IMAP_CONNECTIONS = 300  # Total concurrent IMAP connections across all users
MAX_IMAP_CONNECTIONS_PER_USER = 100  # Per-user limit (can be high or disabled)

# Poll intervals
IMAP_POLL_INTERVAL_MIN = 5.0  # Minimum seconds between polls
IMAP_POLL_INTERVAL_MAX = 7.0  # Maximum seconds between polls

# Timeouts
IMAP_CONNECTION_TIMEOUT = 10
IMAP_READ_TIMEOUT = 8
IMAP_WRITE_TIMEOUT = 6
IMAP_NOOP_TIMEOUT = 3

# Reconnect and backoff
IMAP_RECONNECT_DELAY = 2.0
IMAP_MAX_RECONNECT_ATTEMPTS = 3
IMAP_BACKOFF_DURATION = 1800  # 30 minutes in seconds

# Error thresholds for backoff
IMAP_ERROR_THRESHOLD = 5  # Number of errors before backoff
IMAP_ERROR_WINDOW = 300  # Time window in seconds

# Queue sizes
IMAP_RESULT_QUEUE_MAXSIZE = 2048

# IMAP port
IMAP_PORT_SSL = 993

# ===== DATACLASSES =====

@dataclass
class ImapAccountConfig:
    """Configuration for a single IMAP account"""
    user_id: int
    acc_id: int
    email: str
    password: str
    display_name: str
    chat_id: int
    host: str
    proxy: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> dict:
        """Serialize for queue transmission"""
        return {
            "user_id": self.user_id,
            "acc_id": self.acc_id,
            "email": self.email,
            "password": self.password,
            "display_name": self.display_name,
            "chat_id": self.chat_id,
            "host": self.host,
            "proxy": self.proxy
        }
    
    @classmethod
    def from_dict(cls, d: dict) -> "ImapAccountConfig":
        """Deserialize from queue"""
        return cls(
            user_id=d["user_id"],
            acc_id=d["acc_id"],
            email=d["email"],
            password=d["password"],
            display_name=d["display_name"],
            chat_id=d["chat_id"],
            host=d["host"],
            proxy=d.get("proxy")
        )


@dataclass
class ImapAccountState:
    """Runtime state for a single IMAP account"""
    config: ImapAccountConfig
    state: str = "idle"  # idle, connecting, connected, backoff, error
    last_error: Optional[str] = None
    error_count: int = 0
    error_timestamps: List[float] = field(default_factory=list)
    backoff_until: float = 0.0
    last_uid: int = 0
    reconnect_attempts: int = 0
    last_check: float = 0.0


# ===== GLOBAL RUNTIME STATE =====

class ImapRuntimeManager:
    """Global manager for IMAP runtime state"""
    
    def __init__(self):
        # Account states: (user_id, acc_id) -> ImapAccountState
        self.account_states: Dict[Tuple[int, int], ImapAccountState] = {}
        
        # Active processes: (user_id, acc_id) -> Process
        self.active_processes: Dict[Tuple[int, int], Process] = {}
        
        # Connection counts
        self.global_connection_count = 0
        self.per_user_connection_count: Dict[int, int] = {}
        
        # Control events
        self.shutdown_event: Optional[Event] = None
        
        # Result queue
        self.result_queue: Optional[Queue] = None
        
        # Multiprocessing context
        self.mp_context = None
        
        # Background tasks
        self.result_processor_task: Optional[asyncio.Task] = None
    
    def can_start_connection(self, user_id: int) -> bool:
        """Check if we can start a new connection given global limits"""
        if self.global_connection_count >= MAX_IMAP_CONNECTIONS:
            return False
        
        user_count = self.per_user_connection_count.get(user_id, 0)
        if user_count >= MAX_IMAP_CONNECTIONS_PER_USER:
            return False
        
        return True
    
    def register_connection(self, user_id: int):
        """Register a new connection"""
        self.global_connection_count += 1
        self.per_user_connection_count[user_id] = self.per_user_connection_count.get(user_id, 0) + 1
    
    def unregister_connection(self, user_id: int):
        """Unregister a connection"""
        self.global_connection_count = max(0, self.global_connection_count - 1)
        if user_id in self.per_user_connection_count:
            self.per_user_connection_count[user_id] = max(0, self.per_user_connection_count[user_id] - 1)
            if self.per_user_connection_count[user_id] == 0:
                del self.per_user_connection_count[user_id]


# Global manager instance
_runtime_manager: Optional[ImapRuntimeManager] = None


def get_runtime_manager() -> ImapRuntimeManager:
    """Get or create the global runtime manager"""
    global _runtime_manager
    if _runtime_manager is None:
        _runtime_manager = ImapRuntimeManager()
    return _runtime_manager


# ===== SOCKS IMAP SSL CLASS =====

class SocksIMAP4SSL(imaplib.IMAP4_SSL):
    """IMAP4_SSL через SOCKS5-прокси"""
    
    def __init__(self, host: str, port: int, proxy: Optional[Dict[str, Any]] = None, timeout: float = 10.0):
        self.proxy_config = proxy
        self.connection_timeout = timeout
        self._sock = None
        # НЕ вызываем parent.__init__ напрямую — это откроет соединение без прокси
        # Вместо этого инициализируем атрибуты и вызываем open вручную
        self.host = host
        self.port = port
        imaplib.IMAP4.__init__(self, host, port)
        
    def open(self, host: str, port: int):
        """Переопределение open() для работы через SOCKS5 прокси"""
        if not self.proxy_config or not self.proxy_config.get('host') or not self.proxy_config.get('port'):
            raise ValueError("SocksIMAP4SSL requires proxy configuration with host and port")
        
        proxy_host = self.proxy_config['host']
        proxy_port = int(self.proxy_config['port'])
        proxy_user = self.proxy_config.get('user')
        proxy_pass = self.proxy_config.get('password')
        
        # Создаём SOCKS5 сокет
        raw_sock = socks.socksocket(socket.AF_INET, socket.SOCK_STREAM)
        raw_sock.set_proxy(
            proxy_type=socks.SOCKS5,
            addr=proxy_host,
            port=proxy_port,
            username=proxy_user,
            password=proxy_pass
        )
        raw_sock.settimeout(self.connection_timeout)
        
        # Подключаемся к IMAP серверу через прокси
        raw_sock.connect((host, port))
        
        # Оборачиваем в SSL
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        self.sock = ssl_context.wrap_socket(raw_sock, server_hostname=host)
        self.file = self.sock.makefile('rb')
        self._sock = raw_sock


# ===== UTILITY FUNCTIONS =====

def extract_text_from_html(html_text: str) -> str:
    """Extract plain text from HTML, handling HTML entities and removing signatures"""
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
            elif tag_lower == 'br':
                self.text_parts.append('\n')
                
        def handle_endtag(self, tag):
            tag_lower = tag.lower()
            if tag_lower in ('script', 'style'):
                if tag_lower == 'script':
                    self.in_script = False
                else:
                    self.in_style = False
            elif tag_lower in ('div', 'p', 'li', 'ul', 'ol', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'tr', 'blockquote', 'pre'):
                self.text_parts.append('\n')
            elif tag_lower in ('td', 'th'):
                self.text_parts.append('\t')
            elif tag_lower == 'br':
                self.text_parts.append('\n')
                
        def handle_data(self, data):
            if not self.in_script and not self.in_style:
                self.text_parts.append(data)
    
    try:
        html_text = html_module.unescape(html_text)
        parser = TextExtractor()
        parser.feed(html_text)
        text = ''.join(parser.text_parts)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'[ \t]*\n[ \t]*', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        lines = text.split('\n')
        text = '\n'.join(line.rstrip() for line in lines)
        
        # Remove GMX signatures
        lines = text.split('\n')
        if len(lines) > 5:
            main_lines = lines[:-5]
            signature_candidate = lines[-5:]
            signature_text = '\n'.join(signature_candidate).lower()
            has_signature = any(marker in signature_text for marker in [
                'gesendet mit der gmx', 'sent with gmx', 'gmx mail app',
            ])
            if has_signature:
                sig_start = len(main_lines)
                for i in range(len(signature_candidate) - 1, -1, -1):
                    line_lower = signature_candidate[i].lower().strip()
                    if any(marker in line_lower for marker in [
                        'gesendet mit der gmx', 'sent with gmx', 'gmx mail app',
                    ]):
                        for j in range(i - 1, -1, -1):
                            if signature_candidate[j].strip() == '--':
                                sig_start = len(main_lines) + j
                                break
                        break
                filtered_lines = lines[:sig_start] if sig_start < len(lines) else main_lines
            else:
                filtered_lines = lines
        else:
            filtered_lines = lines
        
        return '\n'.join(filtered_lines).strip()
    except Exception:
        # Fallback: simple tag removal
        text = html_unescape(html_text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        return text.strip()


def with_socket_timeout(imap_obj, timeout_val, fn, *args, **kwargs):
    """Execute IMAP operation with socket-level timeout"""
    if not imap_obj:
        raise ValueError("imap_obj is None")
    
    if hasattr(imap_obj, 'sock') and imap_obj.sock:
        old_timeout = imap_obj.sock.gettimeout()
        try:
            imap_obj.sock.settimeout(timeout_val)
            result = fn(*args, **kwargs)
            return result
        finally:
            try:
                if old_timeout is not None:
                    imap_obj.sock.settimeout(old_timeout)
                else:
                    imap_obj.sock.settimeout(IMAP_READ_TIMEOUT)
            except Exception:
                pass
    else:
        return fn(*args, **kwargs)


# ===== WORKER PROCESS =====

def imap_account_worker(
    config_dict: dict,
    result_queue: Queue,
    stop_event: Event,
    log_prefix: str = "IMAP_WORKER"
):
    """
    Persistent IMAP worker for a single account.
    
    This worker:
    1. Connects via proxy and logs in
    2. Selects INBOX
    3. Runs a persistent loop:
       - Every 5-7 seconds, performs SEARCH for new messages
       - FETCHes new messages and sends to result queue
       - Uses NOOP to keep connection alive
    4. Handles errors with backoff mechanism
    """
    import random
    import email
    from email.header import decode_header
    
    config = ImapAccountConfig.from_dict(config_dict)
    imap_obj = None
    last_uid = 0
    error_count = 0
    error_timestamps = []
    
    def log(msg: str):
        """Simple logging function"""
        print(f"[{log_prefix}] uid={config.user_id} acc_id={config.acc_id} email={config.email}: {msg}")
    
    def should_backoff() -> bool:
        """Check if we should enter backoff state"""
        now = time.time()
        # Clean old timestamps
        error_timestamps[:] = [ts for ts in error_timestamps if now - ts < IMAP_ERROR_WINDOW]
        return len(error_timestamps) >= IMAP_ERROR_THRESHOLD
    
    def record_error():
        """Record an error occurrence"""
        nonlocal error_count
        error_count += 1
        error_timestamps.append(time.time())
    
    def connect() -> Tuple[bool, Optional[imaplib.IMAP4_SSL], Optional[str]]:
        """Connect to IMAP server"""
        try:
            log("Connecting...")
            
            # Create IMAP connection
            imap = SocksIMAP4SSL(
                config.host,
                IMAP_PORT_SSL,
                proxy=config.proxy,
                timeout=IMAP_CONNECTION_TIMEOUT
            )
            
            # Login
            log("Logging in...")
            typ, data = with_socket_timeout(
                imap,
                IMAP_CONNECTION_TIMEOUT,
                imap.login,
                config.email,
                config.password
            )
            
            if typ.upper() != 'OK':
                error_msg = f"Login failed: {typ}"
                log(error_msg)
                return False, None, "auth_error"
            
            # Select INBOX
            log("Selecting INBOX...")
            typ, data = with_socket_timeout(
                imap,
                IMAP_READ_TIMEOUT,
                imap.select,
                'INBOX'
            )
            
            if typ.upper() != 'OK':
                error_msg = f"INBOX select failed: {typ}"
                log(error_msg)
                return False, None, "select_error"
            
            log("Connected successfully")
            return True, imap, None
            
        except Exception as e:
            error_msg = f"Connection error: {type(e).__name__}: {e}"
            log(error_msg)
            return False, None, "connection_error"
    
    def check_connection(imap: imaplib.IMAP4_SSL) -> bool:
        """Check if connection is still alive using NOOP"""
        try:
            typ, data = with_socket_timeout(
                imap,
                IMAP_NOOP_TIMEOUT,
                imap.noop
            )
            return typ.upper() == 'OK'
        except Exception as e:
            log(f"NOOP failed: {e}")
            return False
    
    def search_new_messages(imap: imaplib.IMAP4_SSL, last_uid: int) -> Tuple[bool, List[int]]:
        """Search for new messages with UID > last_uid"""
        try:
            # Search for all messages
            typ, data = with_socket_timeout(
                imap,
                IMAP_READ_TIMEOUT,
                imap.uid,
                'SEARCH',
                None,
                'ALL'
            )
            
            if typ.upper() != 'OK':
                log(f"SEARCH failed: {typ}")
                return False, []
            
            # Parse UIDs
            uid_list = []
            if data and data[0]:
                uid_bytes = data[0]
                if isinstance(uid_bytes, bytes):
                    uid_str = uid_bytes.decode('ascii', errors='ignore')
                    uid_list = [int(x) for x in uid_str.split() if x.isdigit()]
            
            # Filter for new UIDs
            new_uids = [uid for uid in uid_list if uid > last_uid]
            
            if new_uids:
                log(f"Found {len(new_uids)} new message(s)")
            
            return True, new_uids
            
        except Exception as e:
            log(f"SEARCH error: {type(e).__name__}: {e}")
            return False, []
    
    def fetch_message(imap: imaplib.IMAP4_SSL, uid: int) -> Optional[dict]:
        """Fetch a single message by UID"""
        try:
            typ, data = with_socket_timeout(
                imap,
                IMAP_READ_TIMEOUT,
                imap.uid,
                'FETCH',
                str(uid),
                '(RFC822)'
            )
            
            if typ.upper() != 'OK' or not data or not data[0]:
                log(f"FETCH failed for UID {uid}: {typ}")
                return None
            
            # Parse message
            raw_email = data[0][1]
            if isinstance(raw_email, bytes):
                msg = email.message_from_bytes(raw_email)
            else:
                msg = email.message_from_string(raw_email)
            
            # Extract fields
            from_header = msg.get('From', '')
            subject_header = msg.get('Subject', '')
            
            # Decode subject
            subject = ''
            try:
                decoded_parts = decode_header(subject_header)
                subject_parts = []
                for part, encoding in decoded_parts:
                    if isinstance(part, bytes):
                        subject_parts.append(part.decode(encoding or 'utf-8', errors='replace'))
                    else:
                        subject_parts.append(str(part))
                subject = ''.join(subject_parts)
            except Exception:
                subject = subject_header
            
            # Extract body
            body = ''
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    if content_type == 'text/plain':
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or 'utf-8'
                            body = payload.decode(charset, errors='replace')
                            break
                    elif content_type == 'text/html' and not body:
                        payload = part.get_payload(decode=True)
                        if payload:
                            charset = part.get_content_charset() or 'utf-8'
                            html_body = payload.decode(charset, errors='replace')
                            body = extract_text_from_html(html_body)
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    content_type = msg.get_content_type()
                    charset = msg.get_content_charset() or 'utf-8'
                    if content_type == 'text/html':
                        html_body = payload.decode(charset, errors='replace')
                        body = extract_text_from_html(html_body)
                    else:
                        body = payload.decode(charset, errors='replace')
            
            return {
                'user_id': config.user_id,
                'acc_id': config.acc_id,
                'email': config.email,
                'display_name': config.display_name,
                'chat_id': config.chat_id,
                'uid': uid,
                'from': from_header,
                'subject': subject,
                'body': body,
                'timestamp': time.time()
            }
            
        except Exception as e:
            log(f"FETCH error for UID {uid}: {type(e).__name__}: {e}")
            return None
    
    # Main worker loop
    log("Worker started")
    
    try:
        # Initial connection
        success, imap_obj, error_type = connect()
        
        if not success:
            record_error()
            if should_backoff():
                log("Too many errors, entering backoff")
                time.sleep(IMAP_BACKOFF_DURATION)
            return
        
        # Main loop
        while not stop_event.is_set():
            try:
                # Check connection health
                if not check_connection(imap_obj):
                    log("Connection lost, reconnecting...")
                    try:
                        imap_obj.logout()
                    except Exception:
                        pass
                    
                    success, imap_obj, error_type = connect()
                    if not success:
                        record_error()
                        if should_backoff():
                            log("Too many reconnect errors, entering backoff")
                            time.sleep(IMAP_BACKOFF_DURATION)
                            return
                        time.sleep(IMAP_RECONNECT_DELAY)
                        continue
                
                # Search for new messages
                success, new_uids = search_new_messages(imap_obj, last_uid)
                
                if not success:
                    record_error()
                    if should_backoff():
                        log("Too many search errors, entering backoff")
                        time.sleep(IMAP_BACKOFF_DURATION)
                        return
                    time.sleep(IMAP_RECONNECT_DELAY)
                    continue
                
                # Fetch new messages
                for uid in new_uids:
                    if stop_event.is_set():
                        break
                    
                    msg_data = fetch_message(imap_obj, uid)
                    if msg_data:
                        try:
                            result_queue.put(msg_data, timeout=5.0)
                            last_uid = max(last_uid, uid)
                        except Exception as e:
                            log(f"Failed to enqueue message: {e}")
                
                # Sleep before next poll
                poll_interval = random.uniform(IMAP_POLL_INTERVAL_MIN, IMAP_POLL_INTERVAL_MAX)
                time.sleep(poll_interval)
                
            except Exception as e:
                log(f"Loop error: {type(e).__name__}: {e}")
                record_error()
                if should_backoff():
                    log("Too many loop errors, entering backoff")
                    time.sleep(IMAP_BACKOFF_DURATION)
                    return
                time.sleep(IMAP_RECONNECT_DELAY)
    
    finally:
        # Cleanup
        if imap_obj:
            try:
                imap_obj.logout()
            except Exception:
                pass
        log("Worker stopped")


# ===== PUBLIC API =====

async def init_imap_runtime() -> bool:
    """Initialize the IMAP runtime system"""
    import multiprocessing as mp
    
    manager = get_runtime_manager()
    
    # Create multiprocessing context
    manager.mp_context = mp.get_context("spawn")
    
    # Create result queue
    manager.result_queue = manager.mp_context.Queue(maxsize=IMAP_RESULT_QUEUE_MAXSIZE)
    
    # Create shutdown event
    manager.shutdown_event = manager.mp_context.Event()
    
    return True


async def shutdown_imap_runtime():
    """Shutdown the IMAP runtime system"""
    manager = get_runtime_manager()
    
    # Signal shutdown
    if manager.shutdown_event:
        manager.shutdown_event.set()
    
    # Stop all processes
    for (user_id, acc_id), process in list(manager.active_processes.items()):
        try:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2.0)
                if process.is_alive():
                    process.kill()
        except Exception:
            pass
    
    manager.active_processes.clear()
    
    # Stop result processor task
    if manager.result_processor_task:
        manager.result_processor_task.cancel()
        try:
            await manager.result_processor_task
        except asyncio.CancelledError:
            pass
    
    # Close queue
    if manager.result_queue:
        try:
            manager.result_queue.close()
        except Exception:
            pass


async def start_imap_for_account(
    user_id: int,
    acc_id: int,
    email: str,
    password: str,
    display_name: str,
    chat_id: int,
    proxy: Optional[Dict[str, Any]]
) -> bool:
    """Start IMAP worker for a single account"""
    manager = get_runtime_manager()
    
    # Check if already running
    key = (user_id, acc_id)
    if key in manager.active_processes:
        process = manager.active_processes[key]
        if process.is_alive():
            return True  # Already running
    
    # Check connection limits
    if not manager.can_start_connection(user_id):
        return False
    
    # Create config
    # Determine IMAP host from email domain
    domain = email.split('@')[1] if '@' in email else ''
    host_map = {
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
    host = host_map.get(domain, f"imap.{domain}")
    
    config = ImapAccountConfig(
        user_id=user_id,
        acc_id=acc_id,
        email=email,
        password=password,
        display_name=display_name,
        chat_id=chat_id,
        host=host,
        proxy=proxy
    )
    
    # Start worker process
    log_prefix = f"IMAP[{user_id}/{acc_id}]"
    process = manager.mp_context.Process(
        target=imap_account_worker,
        args=(
            config.to_dict(),
            manager.result_queue,
            manager.shutdown_event,
            log_prefix
        ),
        name=f"imap-{user_id}-{acc_id}",
        daemon=True
    )
    
    process.start()
    manager.active_processes[key] = process
    manager.register_connection(user_id)
    
    return True


async def stop_imap_for_account(user_id: int, acc_id: int) -> bool:
    """Stop IMAP worker for a single account"""
    manager = get_runtime_manager()
    
    key = (user_id, acc_id)
    if key not in manager.active_processes:
        return False
    
    process = manager.active_processes.pop(key)
    
    try:
        if process.is_alive():
            process.terminate()
            process.join(timeout=2.0)
            if process.is_alive():
                process.kill()
    except Exception:
        pass
    
    manager.unregister_connection(user_id)
    return True


async def start_imap_for_user(user_id: int, chat_id: int) -> int:
    """
    Start IMAP workers for all active accounts of a user.
    Returns the number of accounts started.
    """
    # This would need to query the database for active accounts
    # For now, return 0 as placeholder
    # The actual implementation will be done when integrating with bot.py
    return 0


async def stop_imap_for_user(user_id: int) -> int:
    """
    Stop all IMAP workers for a user.
    Returns the number of accounts stopped.
    """
    manager = get_runtime_manager()
    
    count = 0
    keys_to_stop = [k for k in manager.active_processes.keys() if k[0] == user_id]
    
    for key in keys_to_stop:
        success = await stop_imap_for_account(key[0], key[1])
        if success:
            count += 1
    
    return count


async def process_imap_results_loop(callback: Callable):
    """
    Background task that processes IMAP results from the queue.
    
    Args:
        callback: Async function to call with each result
    """
    manager = get_runtime_manager()
    
    if not manager.result_queue:
        return
    
    while True:
        try:
            # Get result with timeout
            try:
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    manager.result_queue.get,
                    True,  # block
                    1.0   # timeout
                )
            except Exception:
                # Timeout or queue closed
                if manager.shutdown_event and manager.shutdown_event.is_set():
                    break
                continue
            
            # Process result
            if result:
                try:
                    await callback(result)
                except Exception as e:
                    print(f"Error processing IMAP result: {e}")
                    traceback.print_exc()
        
        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Error in IMAP result loop: {e}")
            traceback.print_exc()
            await asyncio.sleep(1.0)


def get_imap_status_for_user(user_id: int) -> dict:
    """Get IMAP status for a user"""
    manager = get_runtime_manager()
    
    active_accounts = []
    for (uid, acc_id), process in manager.active_processes.items():
        if uid == user_id:
            active_accounts.append({
                'acc_id': acc_id,
                'is_alive': process.is_alive(),
                'pid': process.pid if process.is_alive() else None
            })
    
    return {
        'user_id': user_id,
        'active_account_count': len(active_accounts),
        'active_accounts': active_accounts,
        'connection_count': manager.per_user_connection_count.get(user_id, 0)
    }
