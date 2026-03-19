import os
from dotenv import load_dotenv

load_dotenv()


def _int_env(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


def _str_env(name, default=''):
    value = os.getenv(name, default)
    if value is None:
        return ''
    return str(value).strip()


def _bool_env(name, default=False):
    value = str(os.getenv(name, '') or '').strip().lower()
    if value in {'1', 'true', 'yes', 'on'}:
        return True
    if value in {'0', 'false', 'no', 'off'}:
        return False
    return bool(default)


max_flood = _int_env('MAX_FLOOD', 999)
file1 = os.getenv('CHATS_FILE', 'uslug.txt')
full_chats = _int_env('FULL_CHATS', 30)
slp = _int_env('SLEEP_BETWEEN_CHECKS', 5)
join_sleep = _int_env('JOIN_SLEEP', 0)
from_join_sleep = _int_env('FROM_JOIN_SLEEP', 10)
to_join_sleep = _int_env('TO_JOIN_SLEEP', 30)

proxy_type = os.getenv('PROXY_TYPE', 'SOCKS5')
ip = os.getenv('PROXY_IP', '0')
port = _int_env('PROXY_PORT', 0)
login = os.getenv('PROXY_LOGIN', '')
password = os.getenv('PROXY_PASSWORD', '')

admin = _int_env('ADMIN_ID', 0)
_admins_raw = os.getenv('ADMIN_IDS', '').strip()
if _admins_raw:
    admins = []
    for item in _admins_raw.split(','):
        item = item.strip()
        if item.lstrip('-').isdigit():
            admins.append(int(item))
    if admin and admin not in admins:
        admins.append(int(admin))
else:
    admins = [int(admin)] if admin else []
bot_invite_token = os.getenv('BOT_INVITE_TOKEN', os.getenv('BOT_TOKEN', ''))
bot_invite_name = os.getenv('BOT_INVITE_NAME', '')
bot_api_token = os.getenv('BOT_API_TOKEN', bot_invite_token)
bot_api_base_url = os.getenv('BOT_API_BASE_URL', 'https://api.telegram.org')
bot_api_timeout = _int_env('BOT_API_TIMEOUT', 30)
_railway_public_domain = _str_env('RAILWAY_PUBLIC_DOMAIN')
is_railway = any(str(key).startswith('RAILWAY_') for key in os.environ.keys()) or _bool_env('RAILWAY', False)
webhook_base_url = _str_env('WEBHOOK_BASE_URL').rstrip('/')
if not webhook_base_url and _railway_public_domain:
    webhook_base_url = f'https://{_railway_public_domain}'.rstrip('/')
webhook_path = os.getenv('WEBHOOK_PATH', '/webhook')
webhook_secret_token = _str_env('WEBHOOK_SECRET_TOKEN')
_bot_transport_raw = _str_env('BOT_TRANSPORT').lower()
if _bot_transport_raw in {'polling', 'webhook'}:
    bot_transport = _bot_transport_raw
elif webhook_base_url:
    bot_transport = 'webhook'
elif is_railway:
    bot_transport = 'webhook'
else:
    bot_transport = 'polling'
port = _int_env('PORT', 8000)
worker_poll_interval = _int_env('WORKER_POLL_INTERVAL', 15)
join_request_batch_size = _int_env('JOIN_REQUEST_BATCH_SIZE', 50)

# Telethon credentials
API_ID = _int_env('TG_API_ID', 0)
API_HASH = os.getenv('TG_API_HASH', '')

# Database connection for Railway Postgres
DATABASE_URL = os.getenv('DATABASE_URL', '')

# Inviter safety controls
invite_per_account_limit = _int_env('INVITE_PER_ACCOUNT_LIMIT', 25)
invite_max_flood_wait = _int_env('INVITE_MAX_FLOOD_WAIT', 600)

# Account health checks
account_check_chat = os.getenv('ACCOUNT_CHECK_CHAT', '').strip()
account_monitor_interval = _int_env('ACCOUNT_MONITOR_INTERVAL', 300)
