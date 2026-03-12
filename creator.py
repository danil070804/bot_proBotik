from telethon import TelegramClient, sync, connection
import os
import python_socks

print('''Для выхода из программы используйте Ctrl+C''')
ip = ''  # ip прокси (0, если без прокси)
port = 0  # порт прокси
login = ''  # логин прокси
password = ''  # пароль прокси


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


proxy = get_proxy()

while True:
    try:
        client = TelegramClient(
            'unknown', 1887863, 'b5d95495ae82dd61e13dee4a9fc95a01', proxy=proxy)
        client.start()
        info = client.get_me()
        client.disconnect()
        os.rename('unknown.session', f'{info.phone}.session')
    except KeyboardInterrupt:
        client.disconnect()
        print('Выход...')
        os.remove('unknown.session')
        break
