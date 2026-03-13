import os
from dotenv import load_dotenv

load_dotenv()


def _int_env(name, default):
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return int(default)


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
bot_invite_token = os.getenv('BOT_INVITE_TOKEN', os.getenv('BOT_TOKEN', ''))
bot_invite_name = os.getenv('BOT_INVITE_NAME', '')

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
