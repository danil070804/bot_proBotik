from glob import glob
from config import *
import os
import json
import sqlite3
import python_socks
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.crypto import AuthKey


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_JSON_KEYS = (
    'session', 'string_session', 'session_string', 'string',
    'telethon_session', 'telethonStringSession', 'stringSession',
)
SESSION_JSON_NESTED_KEYS = ('data', 'account', 'telegram', 'client', 'session_data')


def get_chats():
    chats = []
    if not os.path.exists(file1):
        return chats
    with open(file1, 'r', encoding='utf-8') as file:
        for chat in file.readlines():
            chat = chat.replace('\n', '')
            chats.append(chat)
    return chats


def generate_chats_list(sessions=None):
    chats = get_chats()
    sessions_count = len(sessions or get_sessions())
    if sessions_count == 0 or len(chats) == 0:
        return []
    my_len = len(chats) // sessions_count
    if my_len <= 0:
        my_len = 1
    if my_len > full_chats:
        chats_gen = chats_for_acc(chats, full_chats)
    else:
        chats_gen = chats_for_acc(chats, my_len)
    chats_list = []
    for c in chats_gen:
        chats_list.append(c)
    return chats_list


def get_proxy():
    if ip != '0':
        if proxy_type == 'HTTP':
            prox_type = python_socks.ProxyType.HTTP
        elif proxy_type == 'SOCKS5':
            prox_type = python_socks.ProxyType.SOCKS5
        elif proxy_type == 'SOCKS4':
            prox_type = python_socks.ProxyType.SOCKS4
        prox = (prox_type, ip, port, True, login, password)
        return prox
    else:
        return None


def link_convert(chat):
    if chat[:22] == 'https://t.me/joinchat/':
        chat = chat.replace('https://t.me/joinchat/', '')
        t = 'close'
    elif chat[:13] == 'https://t.me/':
        t = 'open'
        chat = chat.replace('https://t.me/', '')
    else:
        t = 'open'
        chat = chat.replace('@', '')
    return t, chat


def _resolve_local_path(path):
    ref = str(path or '').strip()
    if not ref:
        return ''
    if os.path.isabs(ref):
        return ref
    return os.path.join(BASE_DIR, ref)


def _looks_like_string_session(value):
    value = str(value or '').strip()
    if len(value) < 32:
        return False
    return not any(ch.isspace() for ch in value)


def _extract_string_session_value(data):
    if isinstance(data, dict):
        for container in [data] + [data.get(key) for key in SESSION_JSON_NESTED_KEYS if isinstance(data.get(key), dict)]:
            for key in SESSION_JSON_KEYS:
                value = container.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return ''
    if isinstance(data, str):
        return data.strip()
    return ''


def _read_json_session_value(path):
    with open(_resolve_local_path(path), 'r', encoding='utf-8') as f:
        raw = f.read().strip()
    data = None
    try:
        data = json.loads(raw)
    except Exception:
        data = None
    value = _extract_string_session_value(data)
    if not value and data is None and _looks_like_string_session(raw):
        value = raw
    return value


def _is_session_json_path(path):
    filename = os.path.basename(str(path or '')).lower()
    if filename.startswith('progress_'):
        return False
    try:
        return bool(_read_json_session_value(path))
    except Exception:
        return False


def get_sessions():
    sessions = []
    for path in sorted(glob(os.path.join(BASE_DIR, '*.session'))):
        sessions.append(os.path.basename(path))
    for path in sorted(glob(os.path.join(BASE_DIR, '*.json'))):
        if _is_session_json_path(path):
            sessions.append(os.path.basename(path))
    return list(dict.fromkeys(sessions))


def get_usable_sessions(allow_limited=None):
    from db import get_account_health_record, get_app_setting

    if allow_limited is None:
        allow_limited = str(get_app_setting('accounts_allow_limited', '0')).strip().lower() in ['1', 'true', 'yes', 'on']

    allowed_statuses = {'working'}
    if allow_limited:
        allowed_statuses.add('limited')

    result = []
    for session in get_sessions():
        status = str((get_account_health_record(session) or {}).get('status') or '').strip().lower()
        if status in allowed_statuses:
            result.append(session)
    return result


def _dc_endpoint(dc_id):
    mapping = {
        1: ('149.154.175.53', 443),
        2: ('149.154.167.51', 443),
        3: ('149.154.175.100', 443),
        4: ('149.154.167.91', 443),
        5: ('149.154.171.5', 443),
    }
    return mapping.get(int(dc_id or 2), ('149.154.167.51', 443))


def _read_pyrogram_sqlite_session(path):
    try:
        conn = sqlite3.connect(_resolve_local_path(path))
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {r[0] for r in cur.fetchall()}
        if 'sessions' not in tables or 'entities' in tables:
            conn.close()
            return None
        cur.execute('PRAGMA table_info(sessions)')
        cols = [r[1] for r in cur.fetchall()]
        if 'dc_id' not in cols or 'auth_key' not in cols:
            conn.close()
            return None
        cur.execute('SELECT dc_id, auth_key FROM sessions LIMIT 1')
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        dc_id, auth_key = row[0], row[1]
        if auth_key is None:
            return None
        if not isinstance(auth_key, (bytes, bytearray)):
            try:
                auth_key = bytes(auth_key)
            except Exception:
                return None
        if len(auth_key) < 32:
            return None
        return int(dc_id), bytes(auth_key)
    except Exception:
        return None


def _telethon_string_from_pyrogram(path):
    data = _read_pyrogram_sqlite_session(path)
    if not data:
        return ''
    dc_id, auth_key = data
    host, port = _dc_endpoint(dc_id)
    sess = StringSession()
    sess.set_dc(dc_id, host, port)
    sess.auth_key = AuthKey(data=auth_key)
    return sess.save()


def build_telegram_client(session_ref, api_id, api_hash, proxy=None):
    ref = str(session_ref or '').strip()
    resolved_ref = _resolve_local_path(ref)
    if ref.lower().endswith('.session'):
        converted = _telethon_string_from_pyrogram(resolved_ref)
        if converted:
            return TelegramClient(StringSession(converted), api_id, api_hash, proxy=proxy)
        return TelegramClient(resolved_ref, api_id, api_hash, proxy=proxy)
    if ref.lower().endswith('.json'):
        value = _read_json_session_value(resolved_ref)
        if not value:
            raise RuntimeError(
                f'JSON не содержит StringSession: {ref}. '
                f'Ожидаются поля session/string_session/session_string или строка StringSession целиком.'
            )
        return TelegramClient(StringSession(value), api_id, api_hash, proxy=proxy)
    return TelegramClient(ref, api_id, api_hash, proxy=proxy)


def chats_for_acc(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
