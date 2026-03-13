from glob import glob
from config import *
import os
import json
import python_socks
from telethon import TelegramClient
from telethon.sessions import StringSession


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


def build_telegram_client(session_ref, api_id, api_hash, proxy=None):
    ref = str(session_ref or '').strip()
    if ref.lower().endswith('.json'):
        with open(ref, 'r', encoding='utf-8') as f:
            raw = f.read().strip()
        data = None
        try:
            data = json.loads(raw)
        except Exception:
            data = None
        if isinstance(data, dict):
            value = str(data.get('session') or data.get('string_session') or data.get('string') or '').strip()
        elif isinstance(data, str):
            value = data.strip()
        else:
            value = raw
        if not value:
            raise RuntimeError(f'Пустой StringSession в файле {ref}')
        return TelegramClient(StringSession(value), api_id, api_hash, proxy=proxy)
    return TelegramClient(ref, api_id, api_hash, proxy=proxy)


def chats_for_acc(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
