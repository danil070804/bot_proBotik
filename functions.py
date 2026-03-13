from glob import glob
from config import *
import os
import json
import sqlite3
import python_socks
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.crypto import AuthKey


def get_chats():
    chats = []
    if not os.path.exists(file1):
        return chats
    with open(file1, 'r', encoding='utf-8') as file:
        for chat in file.readlines():
            chat = chat.replace('\n', '')
            chats.append(chat)
    return chats


def generate_chats_list():
    chats = get_chats()
    sessions_count = len(get_sessions())
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


def get_sessions():
    sessions = []
    sessions.extend(glob('*.session'))
    sessions.extend(glob('*.json'))
    return sessions


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
        conn = sqlite3.connect(path)
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
    if ref.lower().endswith('.session'):
        converted = _telethon_string_from_pyrogram(ref)
        if converted:
            return TelegramClient(StringSession(converted), api_id, api_hash, proxy=proxy)
    if ref.lower().endswith('.json'):
        with open(ref, 'r', encoding='utf-8') as f:
            raw = f.read().strip()
        data = None
        try:
            data = json.loads(raw)
        except Exception:
            data = None
        if isinstance(data, dict):
            keys = [
                'session', 'string_session', 'session_string', 'string',
                'telethon_session', 'telethonStringSession', 'stringSession'
            ]
            value = ''
            for k in keys:
                v = data.get(k)
                if isinstance(v, str) and v.strip():
                    value = v.strip()
                    break
            if not value:
                for nk in ['data', 'account', 'telegram', 'client', 'session_data']:
                    nested = data.get(nk)
                    if isinstance(nested, dict):
                        for k in keys:
                            v = nested.get(k)
                            if isinstance(v, str) and v.strip():
                                value = v.strip()
                                break
                    if value:
                        break
        elif isinstance(data, str):
            value = data.strip()
        else:
            value = raw
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
