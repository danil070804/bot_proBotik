from glob import glob
from config import *
import os
import python_socks


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
    return glob('*.session')


def chats_for_acc(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
