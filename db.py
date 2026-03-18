import json
import sqlite3 as sql
from contextlib import contextmanager

from config import DATABASE_URL, full_chats

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None

IS_POSTGRES = bool(DATABASE_URL and psycopg2)


@contextmanager
def get_connection():
    conn = None
    try:
        if IS_POSTGRES:
            conn = psycopg2.connect(DATABASE_URL)
        else:
            conn = sql.connect('base.db')
        yield conn
    finally:
        if conn is not None:
            conn.close()


def init_db():
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('CREATE TABLE IF NOT EXISTS chats(acc TEXT NOT NULL, chat TEXT UNIQUE NOT NULL)')
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS ugc_users("
                "id BIGINT PRIMARY KEY, data TEXT DEFAULT 'Нет', ref BIGINT DEFAULT 0, "
                "ref_colvo INTEGER DEFAULT 0, akk TEXT DEFAULT '0')"
            )
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS invite("
                "id BIGINT PRIMARY KEY, son_akk TEXT DEFAULT 'Нет', time_vst TEXT DEFAULT 'Нет', "
                "random_son TEXT DEFAULT 'Нет', akk TEXT DEFAULT 'Нет', chat_akk TEXT DEFAULT 'Нет')"
            )
            cursor.execute('CREATE TABLE IF NOT EXISTS akk(id BIGSERIAL PRIMARY KEY, user_id BIGINT, name_akk TEXT, proxi TEXT)')
            cursor.execute('CREATE TABLE IF NOT EXISTS list_chat(id BIGSERIAL PRIMARY KEY, status TEXT, colvo_send INTEGER DEFAULT 0)')
            cursor.execute('CREATE TABLE IF NOT EXISTS logi(id BIGSERIAL PRIMARY KEY)')
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS parsed_usernames('
                'target TEXT NOT NULL, username TEXT NOT NULL, user_id BIGINT, '
                'created_at TIMESTAMP DEFAULT NOW(), UNIQUE(target, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS parsed_comments('
                'target TEXT NOT NULL, message_id BIGINT NOT NULL, username TEXT NOT NULL, '
                'user_id BIGINT, text TEXT, created_at TIMESTAMP DEFAULT NOW(), '
                'UNIQUE(target, message_id, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS invited_users('
                'source_target TEXT NOT NULL, invite_target TEXT NOT NULL, username TEXT NOT NULL, '
                'user_id BIGINT, status TEXT NOT NULL, error TEXT, created_at TIMESTAMP DEFAULT NOW(), '
                'UNIQUE(invite_target, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS app_settings('
                'key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS source_filters('
                'mode TEXT NOT NULL, source TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW(), '
                'UNIQUE(mode, source))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS user_filters('
                'mode TEXT NOT NULL, username TEXT NOT NULL, created_at TIMESTAMP DEFAULT NOW(), '
                'UNIQUE(mode, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS account_cooldowns('
                'session TEXT PRIMARY KEY, cooldown_until TIMESTAMP NOT NULL, reason TEXT, updated_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS account_health('
                'session TEXT PRIMARY KEY, status TEXT NOT NULL, details TEXT, '
                'last_check TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS session_files('
                'name TEXT PRIMARY KEY, content BYTEA NOT NULL, created_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS users('
                'id BIGSERIAL PRIMARY KEY, telegram_user_id BIGINT UNIQUE, username TEXT, first_name TEXT, '
                'source TEXT, consent_status TEXT DEFAULT \'unknown\', consent_datetime TIMESTAMP NULL, '
                'tags TEXT DEFAULT \'[]\', is_blacklisted BOOLEAN DEFAULT FALSE, unsubscribed_at TIMESTAMP NULL, '
                'created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS communities('
                'id BIGSERIAL PRIMARY KEY, chat_id BIGINT UNIQUE NOT NULL, title TEXT NOT NULL, type TEXT DEFAULT \'group\', '
                'default_invite_mode TEXT DEFAULT \'invite_link\', auto_approve_join_requests BOOLEAN DEFAULT FALSE, '
                'strict_moderation BOOLEAN DEFAULT FALSE, is_active BOOLEAN DEFAULT TRUE, '
                'created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS campaigns('
                'id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, status TEXT DEFAULT \'draft\', segment_id BIGINT NULL, '
                'community_id BIGINT NOT NULL, invite_mode TEXT DEFAULT \'auto\', resolved_invite_mode TEXT NULL, '
                'message_template TEXT NOT NULL, rate_limit_per_minute INTEGER DEFAULT 20, '
                'rate_limit_per_hour INTEGER DEFAULT 300, max_attempts INTEGER DEFAULT 1, '
                'stop_on_error_rate DOUBLE PRECISION DEFAULT 0.30, allow_auto_approve BOOLEAN DEFAULT TRUE, '
                'start_at TIMESTAMP NULL, end_at TIMESTAMP NULL, created_by BIGINT NULL, '
                'created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS invite_links('
                'id BIGSERIAL PRIMARY KEY, community_id BIGINT NOT NULL, campaign_id BIGINT NULL, '
                'invite_mode TEXT NOT NULL, telegram_invite_link TEXT UNIQUE NOT NULL, invite_link_hash TEXT, '
                'creates_join_request BOOLEAN DEFAULT FALSE, expire_at TIMESTAMP NULL, member_limit INTEGER NULL, '
                'is_revoked BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS campaign_recipients('
                'id BIGSERIAL PRIMARY KEY, campaign_id BIGINT NOT NULL, user_id BIGINT NOT NULL, '
                'community_id BIGINT NOT NULL, invite_link_id BIGINT NULL, delivery_status TEXT DEFAULT \'pending\', '
                'join_status TEXT DEFAULT \'none\', attempts INTEGER DEFAULT 0, last_error TEXT, '
                'last_sent_at TIMESTAMP NULL, created_at TIMESTAMP DEFAULT NOW(), updated_at TIMESTAMP DEFAULT NOW(), '
                'UNIQUE(campaign_id, user_id, community_id))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS join_requests('
                'id BIGSERIAL PRIMARY KEY, community_id BIGINT NOT NULL, campaign_id BIGINT NULL, user_id BIGINT NULL, '
                'telegram_user_id BIGINT NOT NULL, invite_link_id BIGINT NULL, request_key TEXT UNIQUE, '
                'status TEXT DEFAULT \'pending\', decision_type TEXT, decision_reason TEXT, moderator_id BIGINT NULL, '
                'requested_at TIMESTAMP DEFAULT NOW(), decided_at TIMESTAMP NULL, created_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS audit_log('
                'id BIGSERIAL PRIMARY KEY, actor_id BIGINT NULL, action TEXT NOT NULL, entity_type TEXT NOT NULL, '
                'entity_id BIGINT NULL, payload_json TEXT, created_at TIMESTAMP DEFAULT NOW())'
            )
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(telegram_user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_communities_chat_id ON communities(chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_invite_links_lookup ON invite_links(community_id, campaign_id, is_revoked)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaign_recipients_delivery ON campaign_recipients(campaign_id, delivery_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaign_recipients_join ON campaign_recipients(campaign_id, join_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_join_requests_status ON join_requests(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_join_requests_lookup ON join_requests(community_id, telegram_user_id)')
        else:
            cursor.execute('CREATE TABLE IF NOT EXISTS chats(acc TEXT, chat TEXT UNIQUE)')
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS ugc_users('
                'id INTEGER PRIMARY KEY, data TEXT DEFAULT "Нет", ref INTEGER DEFAULT 0, '
                'ref_colvo INTEGER DEFAULT 0, akk TEXT DEFAULT "0")'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS invite('
                'id INTEGER PRIMARY KEY, son_akk TEXT DEFAULT "Нет", time_vst TEXT DEFAULT "Нет", '
                'random_son TEXT DEFAULT "Нет", akk TEXT DEFAULT "Нет", chat_akk TEXT DEFAULT "Нет")'
            )
            cursor.execute('CREATE TABLE IF NOT EXISTS akk(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, name_akk TEXT, proxi TEXT)')
            cursor.execute('CREATE TABLE IF NOT EXISTS list_chat(id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT, colvo_send INTEGER DEFAULT 0)')
            cursor.execute('CREATE TABLE IF NOT EXISTS logi(id INTEGER PRIMARY KEY AUTOINCREMENT)')
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS parsed_usernames('
                'target TEXT, username TEXT, user_id INTEGER, created_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                'UNIQUE(target, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS parsed_comments('
                'target TEXT, message_id INTEGER, username TEXT, user_id INTEGER, text TEXT, '
                'created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(target, message_id, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS invited_users('
                'source_target TEXT, invite_target TEXT, username TEXT, user_id INTEGER, '
                'status TEXT, error TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                'UNIQUE(invite_target, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS app_settings('
                'key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS source_filters('
                'mode TEXT, source TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(mode, source))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS user_filters('
                'mode TEXT, username TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(mode, username))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS account_cooldowns('
                'session TEXT PRIMARY KEY, cooldown_until INTEGER NOT NULL, reason TEXT, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS account_health('
                'session TEXT PRIMARY KEY, status TEXT NOT NULL, details TEXT, '
                'last_check TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS session_files('
                'name TEXT PRIMARY KEY, content BLOB NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS users('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, telegram_user_id INTEGER UNIQUE, username TEXT, '
                'first_name TEXT, source TEXT, consent_status TEXT DEFAULT "unknown", consent_datetime TEXT NULL, '
                'tags TEXT DEFAULT "[]", is_blacklisted INTEGER DEFAULT 0, unsubscribed_at TEXT NULL, '
                'created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS communities('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER UNIQUE NOT NULL, title TEXT NOT NULL, '
                'type TEXT DEFAULT "group", default_invite_mode TEXT DEFAULT "invite_link", '
                'auto_approve_join_requests INTEGER DEFAULT 0, strict_moderation INTEGER DEFAULT 0, '
                'is_active INTEGER DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                'updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS campaigns('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, status TEXT DEFAULT "draft", '
                'segment_id INTEGER NULL, community_id INTEGER NOT NULL, invite_mode TEXT DEFAULT "auto", '
                'resolved_invite_mode TEXT NULL, message_template TEXT NOT NULL, '
                'rate_limit_per_minute INTEGER DEFAULT 20, rate_limit_per_hour INTEGER DEFAULT 300, '
                'max_attempts INTEGER DEFAULT 1, stop_on_error_rate REAL DEFAULT 0.30, '
                'allow_auto_approve INTEGER DEFAULT 1, start_at TEXT NULL, end_at TEXT NULL, '
                'created_by INTEGER NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                'updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS invite_links('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, community_id INTEGER NOT NULL, campaign_id INTEGER NULL, '
                'invite_mode TEXT NOT NULL, telegram_invite_link TEXT UNIQUE NOT NULL, invite_link_hash TEXT, '
                'creates_join_request INTEGER DEFAULT 0, expire_at TEXT NULL, member_limit INTEGER NULL, '
                'is_revoked INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                'updated_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS campaign_recipients('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, campaign_id INTEGER NOT NULL, user_id INTEGER NOT NULL, '
                'community_id INTEGER NOT NULL, invite_link_id INTEGER NULL, delivery_status TEXT DEFAULT "pending", '
                'join_status TEXT DEFAULT "none", attempts INTEGER DEFAULT 0, last_error TEXT, '
                'last_sent_at TEXT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                'updated_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE(campaign_id, user_id, community_id))'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS join_requests('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, community_id INTEGER NOT NULL, campaign_id INTEGER NULL, '
                'user_id INTEGER NULL, telegram_user_id INTEGER NOT NULL, invite_link_id INTEGER NULL, '
                'request_key TEXT UNIQUE, status TEXT DEFAULT "pending", decision_type TEXT, '
                'decision_reason TEXT, moderator_id INTEGER NULL, requested_at TEXT DEFAULT CURRENT_TIMESTAMP, '
                'decided_at TEXT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute(
                'CREATE TABLE IF NOT EXISTS audit_log('
                'id INTEGER PRIMARY KEY AUTOINCREMENT, actor_id INTEGER NULL, action TEXT NOT NULL, '
                'entity_type TEXT NOT NULL, entity_id INTEGER NULL, payload_json TEXT, '
                'created_at TEXT DEFAULT CURRENT_TIMESTAMP)'
            )
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_tg_id ON users(telegram_user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_communities_chat_id ON communities(chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_invite_links_lookup ON invite_links(community_id, campaign_id, is_revoked)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaign_recipients_delivery ON campaign_recipients(campaign_id, delivery_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_campaign_recipients_join ON campaign_recipients(campaign_id, join_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_join_requests_status ON join_requests(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_join_requests_lookup ON join_requests(community_id, telegram_user_id)')
        conn.commit()
        cursor.close()


def insert_chat_db(acc, chat):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO chats(acc, chat) VALUES (%s, %s) ON CONFLICT (chat) DO NOTHING',
                (acc, chat),
            )
        else:
            cursor.execute('INSERT OR IGNORE INTO chats VALUES(?, ?)', (acc, chat))
        conn.commit()
        cursor.close()


def get_all_chats():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT chat FROM chats')
        rows = cursor.fetchall()
        cursor.close()
        return [i[0] for i in rows]


def is_full(acc):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT COUNT(*) FROM chats WHERE acc = %s', (acc,))
        else:
            cursor.execute('SELECT COUNT(*) FROM chats WHERE acc = ?', (acc,))
        val = cursor.fetchone()[0]
        cursor.close()
        return val >= full_chats


def save_parsed_user(target, username, user_id):
    username = (username or '').lstrip('@').lower()
    if not username:
        return
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO parsed_usernames(target, username, user_id) VALUES (%s, %s, %s) '
                'ON CONFLICT (target, username) DO NOTHING',
                (target, username, user_id),
            )
        else:
            cursor.execute(
                'INSERT OR IGNORE INTO parsed_usernames(target, username, user_id) VALUES (?, ?, ?)',
                (target, username, user_id),
            )
        conn.commit()
        cursor.close()


def save_parsed_comment(target, message_id, username, user_id, text):
    username = (username or '').lstrip('@').lower()
    if not username:
        return
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO parsed_comments(target, message_id, username, user_id, text) '
                'VALUES (%s, %s, %s, %s, %s) '
                'ON CONFLICT (target, message_id, username) DO NOTHING',
                (target, message_id, username, user_id, text),
            )
        else:
            cursor.execute(
                'INSERT OR IGNORE INTO parsed_comments(target, message_id, username, user_id, text) '
                'VALUES (?, ?, ?, ?, ?)',
                (target, message_id, username, user_id, text),
            )
        conn.commit()
        cursor.close()


def get_usernames_for_invite(source_target, invite_target, limit):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'WITH src AS ('
                '  SELECT p.username, p.user_id, p.created_at '
                '  FROM parsed_usernames p WHERE p.target = %s '
                '  UNION '
                '  SELECT c.username, c.user_id, c.created_at '
                '  FROM parsed_comments c WHERE c.target = %s'
                ') '
                'SELECT s.username, MAX(s.user_id) '
                'FROM src s '
                'WHERE NOT EXISTS ('
                '  SELECT 1 FROM invited_users i '
                '  WHERE i.invite_target = %s AND i.username = s.username'
                ') '
                'GROUP BY s.username '
                'ORDER BY MIN(s.created_at) ASC '
                'LIMIT %s',
                (source_target, source_target, invite_target, limit),
            )
        else:
            cursor.execute(
                'WITH src AS ('
                '  SELECT p.username AS username, p.user_id AS user_id, p.created_at AS created_at '
                '  FROM parsed_usernames p WHERE p.target = ? '
                '  UNION '
                '  SELECT c.username AS username, c.user_id AS user_id, c.created_at AS created_at '
                '  FROM parsed_comments c WHERE c.target = ?'
                ') '
                'SELECT s.username, MAX(s.user_id) '
                'FROM src s '
                'WHERE NOT EXISTS ('
                '  SELECT 1 FROM invited_users i '
                '  WHERE i.invite_target = ? AND i.username = s.username'
                ') '
                'GROUP BY s.username '
                'ORDER BY MIN(s.created_at) ASC '
                'LIMIT ?',
                (source_target, source_target, invite_target, limit),
            )
        rows = cursor.fetchall()
        cursor.close()
        return rows


def get_usernames_for_invite_all(invite_target, limit):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'WITH src AS ('
                '  SELECT p.target AS source, p.username, p.user_id, p.created_at FROM parsed_usernames p '
                '  UNION '
                '  SELECT c.target AS source, c.username, c.user_id, c.created_at FROM parsed_comments c'
                ') '
                'SELECT s.source, s.username, MAX(s.user_id) '
                'FROM src s '
                'WHERE NOT EXISTS ('
                '  SELECT 1 FROM invited_users i WHERE i.invite_target = %s AND i.username = s.username'
                ') '
                'GROUP BY s.source, s.username '
                'ORDER BY MIN(s.created_at) ASC '
                'LIMIT %s',
                (invite_target, limit),
            )
        else:
            cursor.execute(
                'WITH src AS ('
                '  SELECT p.target AS source, p.username AS username, p.user_id AS user_id, p.created_at AS created_at FROM parsed_usernames p '
                '  UNION '
                '  SELECT c.target AS source, c.username AS username, c.user_id AS user_id, c.created_at AS created_at FROM parsed_comments c'
                ') '
                'SELECT s.source, s.username, MAX(s.user_id) '
                'FROM src s '
                'WHERE NOT EXISTS ('
                '  SELECT 1 FROM invited_users i WHERE i.invite_target = ? AND i.username = s.username'
                ') '
                'GROUP BY s.source, s.username '
                'ORDER BY MIN(s.created_at) ASC '
                'LIMIT ?',
                (invite_target, limit),
            )
        rows = cursor.fetchall()
        cursor.close()
        return rows


def mark_invite_result(source_target, invite_target, username, user_id, status, error=''):
    username = (username or '').lstrip('@').lower()
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO invited_users(source_target, invite_target, username, user_id, status, error) '
                'VALUES (%s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (invite_target, username) DO UPDATE SET '
                'status = EXCLUDED.status, error = EXCLUDED.error, source_target = EXCLUDED.source_target',
                (source_target, invite_target, username, user_id, status, error),
            )
        else:
            cursor.execute(
                'INSERT OR REPLACE INTO invited_users(source_target, invite_target, username, user_id, status, error) '
                'VALUES (?, ?, ?, ?, ?, ?)',
                (source_target, invite_target, username, user_id, status, error),
            )
        conn.commit()
        cursor.close()


def get_app_setting(key, default_value=''):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT value FROM app_settings WHERE key = %s', (key,))
        else:
            cursor.execute('SELECT value FROM app_settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else default_value


def set_app_setting(key, value):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO app_settings(key, value) VALUES (%s, %s) '
                'ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()',
                (key, str(value)),
            )
        else:
            cursor.execute('INSERT OR REPLACE INTO app_settings(key, value) VALUES (?, ?)', (key, str(value)))
        conn.commit()
        cursor.close()


def _normalize_source(source):
    return (source or '').strip().lower()


def _normalize_username(username):
    return (username or '').strip().lstrip('@').lower()


def add_source_filter(mode, source):
    mode = (mode or '').strip().lower()
    source = _normalize_source(source)
    if mode not in ('whitelist', 'blacklist') or not source:
        return
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO source_filters(mode, source) VALUES (%s, %s) ON CONFLICT (mode, source) DO NOTHING',
                (mode, source),
            )
        else:
            cursor.execute('INSERT OR IGNORE INTO source_filters(mode, source) VALUES (?, ?)', (mode, source))
        conn.commit()
        cursor.close()


def remove_source_filter(mode, source):
    mode = (mode or '').strip().lower()
    source = _normalize_source(source)
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('DELETE FROM source_filters WHERE mode = %s AND source = %s', (mode, source))
        else:
            cursor.execute('DELETE FROM source_filters WHERE mode = ? AND source = ?', (mode, source))
        conn.commit()
        cursor.close()


def get_source_filters():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT mode, source FROM source_filters')
        rows = cursor.fetchall()
        cursor.close()
    whitelist = set()
    blacklist = set()
    for mode, source in rows:
        if mode == 'whitelist':
            whitelist.add(source)
        elif mode == 'blacklist':
            blacklist.add(source)
    return whitelist, blacklist


def is_source_allowed(source):
    source = _normalize_source(source)
    wl, bl = get_source_filters()
    if source in bl:
        return False
    if wl and source not in wl:
        return False
    return True


def add_user_filter(mode, username):
    mode = (mode or '').strip().lower()
    username = _normalize_username(username)
    if mode not in ('whitelist', 'blacklist') or not username:
        return
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO user_filters(mode, username) VALUES (%s, %s) ON CONFLICT (mode, username) DO NOTHING',
                (mode, username),
            )
        else:
            cursor.execute('INSERT OR IGNORE INTO user_filters(mode, username) VALUES (?, ?)', (mode, username))
        conn.commit()
        cursor.close()


def remove_user_filter(mode, username):
    mode = (mode or '').strip().lower()
    username = _normalize_username(username)
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('DELETE FROM user_filters WHERE mode = %s AND username = %s', (mode, username))
        else:
            cursor.execute('DELETE FROM user_filters WHERE mode = ? AND username = ?', (mode, username))
        conn.commit()
        cursor.close()


def get_user_filters():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT mode, username FROM user_filters')
        rows = cursor.fetchall()
        cursor.close()
    whitelist = set()
    blacklist = set()
    for mode, username in rows:
        if mode == 'whitelist':
            whitelist.add(username)
        elif mode == 'blacklist':
            blacklist.add(username)
    return whitelist, blacklist


def is_username_allowed(username):
    username = _normalize_username(username)
    if not username:
        return False
    wl, bl = get_user_filters()
    if username in bl:
        return False
    if wl and username not in wl:
        return False
    return True


def set_account_cooldown(session, seconds, reason=''):
    try:
        seconds = int(seconds)
    except Exception:
        seconds = 0
    if seconds <= 0:
        return
    session = str(session)
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO account_cooldowns(session, cooldown_until, reason) '
                'VALUES (%s, NOW() + (%s * INTERVAL \'1 second\'), %s) '
                'ON CONFLICT (session) DO UPDATE SET '
                'cooldown_until = EXCLUDED.cooldown_until, reason = EXCLUDED.reason, updated_at = NOW()',
                (session, seconds, reason),
            )
        else:
            import time as _time
            until_ts = int(_time.time()) + seconds
            cursor.execute(
                'INSERT OR REPLACE INTO account_cooldowns(session, cooldown_until, reason) VALUES (?, ?, ?)',
                (session, until_ts, reason),
            )
        conn.commit()
        cursor.close()


def get_account_cooldown_remaining(session):
    session = str(session)
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                "SELECT GREATEST(0, EXTRACT(EPOCH FROM (cooldown_until - NOW())))::INT FROM account_cooldowns WHERE session = %s",
                (session,),
            )
            row = cursor.fetchone()
            remaining = int(row[0]) if row and row[0] is not None else 0
        else:
            import time as _time
            cursor.execute('SELECT cooldown_until FROM account_cooldowns WHERE session = ?', (session,))
            row = cursor.fetchone()
            until_ts = int(row[0]) if row else 0
            remaining = max(0, until_ts - int(_time.time()))
        cursor.close()
        return remaining


def set_account_warmup(session, seconds):
    session = str(session or '').strip()
    if not session:
        return
    try:
        seconds = int(seconds)
    except Exception:
        seconds = 0
    if seconds <= 0:
        set_app_setting(f'warmup_until:{session}', '0')
        return
    import time as _time
    set_app_setting(f'warmup_until:{session}', str(int(_time.time()) + seconds))


def get_account_warmup_remaining(session):
    session = str(session or '').strip()
    if not session:
        return 0
    import time as _time
    raw = get_app_setting(f'warmup_until:{session}', '0')
    try:
        until_ts = int(raw)
    except Exception:
        until_ts = 0
    return max(0, until_ts - int(_time.time()))


def save_session_file(name, content):
    name = str(name or '').strip()
    if not name:
        return
    if not isinstance(content, (bytes, bytearray)):
        content = bytes(content or b'')
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO session_files(name, content) VALUES (%s, %s) '
                'ON CONFLICT (name) DO UPDATE SET content = EXCLUDED.content',
                (name, psycopg2.Binary(bytes(content))),
            )
        else:
            cursor.execute(
                'INSERT OR REPLACE INTO session_files(name, content) VALUES (?, ?)',
                (name, bytes(content)),
            )
        conn.commit()
        cursor.close()


def delete_session_file(name):
    name = str(name or '').strip()
    if not name:
        return
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('DELETE FROM session_files WHERE name = %s', (name,))
        else:
            cursor.execute('DELETE FROM session_files WHERE name = ?', (name,))
        conn.commit()
        cursor.close()


def get_session_files():
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT name, content FROM session_files')
        rows = cursor.fetchall()
        cursor.close()
        result = []
        for name, content in rows:
            if content is None:
                continue
            result.append((str(name), bytes(content)))
        return result


def set_account_health(session, status, details=''):
    session = str(session or '').strip()
    status = str(status or 'unknown').strip().lower()
    details = str(details or '').strip()
    if not session:
        return ''
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT status FROM account_health WHERE session = %s', (session,))
        else:
            cursor.execute('SELECT status FROM account_health WHERE session = ?', (session,))
        row = cursor.fetchone()
        prev_status = row[0] if row else ''
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO account_health(session, status, details) VALUES (%s, %s, %s) '
                'ON CONFLICT (session) DO UPDATE SET '
                'status = EXCLUDED.status, details = EXCLUDED.details, last_check = NOW(), '
                'updated_at = CASE WHEN account_health.status <> EXCLUDED.status THEN NOW() ELSE account_health.updated_at END',
                (session, status, details),
            )
        else:
            cursor.execute(
                'INSERT INTO account_health(session, status, details, last_check, updated_at) '
                'VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) '
                'ON CONFLICT(session) DO UPDATE SET '
                'status = excluded.status, details = excluded.details, last_check = CURRENT_TIMESTAMP, '
                'updated_at = CASE WHEN account_health.status <> excluded.status THEN CURRENT_TIMESTAMP ELSE account_health.updated_at END',
                (session, status, details),
            )
        conn.commit()
        cursor.close()
        return prev_status


def get_account_health(session):
    session = str(session or '').strip()
    if not session:
        return ('unknown', '', '')
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT status, details, last_check FROM account_health WHERE session = %s', (session,))
        else:
            cursor.execute('SELECT status, details, last_check FROM account_health WHERE session = ?', (session,))
        row = cursor.fetchone()
        cursor.close()
        if not row:
            return ('unknown', '', '')
        return (row[0] or 'unknown', row[1] or '', str(row[2] or ''))

init_db()


def get_main_connection():
    if not IS_POSTGRES:
        raise RuntimeError('Set DATABASE_URL for main bot database')
    return psycopg2.connect(DATABASE_URL)


def _json_text(value, default=''):
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _row_to_dict(cursor, row):
    if row is None or cursor.description is None:
        return None
    columns = [col[0] for col in cursor.description]
    return {columns[idx]: row[idx] for idx in range(len(columns))}


def _rows_to_dicts(cursor, rows):
    if cursor.description is None:
        return []
    columns = [col[0] for col in cursor.description]
    return [{columns[idx]: row[idx] for idx in range(len(columns))} for row in rows]


def _normalize_bool(value):
    return 1 if bool(value) else 0


def _normalize_link_hash(link):
    value = str(link or '').strip()
    if 'joinchat/' in value:
        return value.split('joinchat/', 1)[1].strip().strip('/')
    if 't.me/+' in value:
        return value.split('t.me/+', 1)[1].strip().strip('/')
    if value.startswith('+'):
        return value[1:].strip()
    return ''


def get_user(user_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM users WHERE id = %s', (user_id,))
        else:
            cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def get_user_by_telegram_user_id(telegram_user_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM users WHERE telegram_user_id = %s', (telegram_user_id,))
        else:
            cursor.execute('SELECT * FROM users WHERE telegram_user_id = ?', (telegram_user_id,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def upsert_user(
    telegram_user_id,
    username='',
    first_name='',
    source='',
    consent_status='unknown',
    consent_datetime=None,
    tags=None,
    is_blacklisted=False,
    unsubscribed_at=None,
):
    if telegram_user_id in (None, ''):
        raise ValueError('telegram_user_id is required')
    tags_text = _json_text(tags, default='[]')
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO users('
                'telegram_user_id, username, first_name, source, consent_status, consent_datetime, '
                'tags, is_blacklisted, unsubscribed_at'
                ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (telegram_user_id) DO UPDATE SET '
                'username = COALESCE(NULLIF(EXCLUDED.username, \'\'), users.username), '
                'first_name = COALESCE(NULLIF(EXCLUDED.first_name, \'\'), users.first_name), '
                'source = COALESCE(NULLIF(EXCLUDED.source, \'\'), users.source), '
                'consent_status = COALESCE(NULLIF(EXCLUDED.consent_status, \'\'), users.consent_status), '
                'consent_datetime = COALESCE(EXCLUDED.consent_datetime, users.consent_datetime), '
                'tags = COALESCE(NULLIF(EXCLUDED.tags, \'\'), users.tags), '
                'is_blacklisted = EXCLUDED.is_blacklisted, '
                'unsubscribed_at = COALESCE(EXCLUDED.unsubscribed_at, users.unsubscribed_at), '
                'updated_at = NOW()',
                (
                    telegram_user_id,
                    str(username or '').strip(),
                    str(first_name or '').strip(),
                    str(source or '').strip(),
                    str(consent_status or 'unknown').strip(),
                    consent_datetime,
                    tags_text,
                    bool(is_blacklisted),
                    unsubscribed_at,
                ),
            )
        else:
            cursor.execute(
                'INSERT INTO users('
                'telegram_user_id, username, first_name, source, consent_status, consent_datetime, '
                'tags, is_blacklisted, unsubscribed_at'
                ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(telegram_user_id) DO UPDATE SET '
                'username = COALESCE(NULLIF(excluded.username, \'\'), users.username), '
                'first_name = COALESCE(NULLIF(excluded.first_name, \'\'), users.first_name), '
                'source = COALESCE(NULLIF(excluded.source, \'\'), users.source), '
                'consent_status = COALESCE(NULLIF(excluded.consent_status, \'\'), users.consent_status), '
                'consent_datetime = COALESCE(excluded.consent_datetime, users.consent_datetime), '
                'tags = COALESCE(NULLIF(excluded.tags, \'\'), users.tags), '
                'is_blacklisted = excluded.is_blacklisted, '
                'unsubscribed_at = COALESCE(excluded.unsubscribed_at, users.unsubscribed_at), '
                'updated_at = CURRENT_TIMESTAMP',
                (
                    telegram_user_id,
                    str(username or '').strip(),
                    str(first_name or '').strip(),
                    str(source or '').strip(),
                    str(consent_status or 'unknown').strip(),
                    consent_datetime,
                    tags_text,
                    _normalize_bool(is_blacklisted),
                    unsubscribed_at,
                ),
            )
        conn.commit()
        cursor.close()
    return get_user_by_telegram_user_id(telegram_user_id)


create_user = upsert_user


def blacklist_user(user_id, is_blacklisted=True):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'UPDATE users SET is_blacklisted = %s, updated_at = NOW() WHERE id = %s',
                (bool(is_blacklisted), user_id),
            )
        else:
            cursor.execute(
                'UPDATE users SET is_blacklisted = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                (_normalize_bool(is_blacklisted), user_id),
            )
        conn.commit()
        cursor.close()
    return get_user(user_id)


def unsubscribe_user(user_id, unsubscribed_at=None):
    with get_connection() as conn:
        cursor = conn.cursor()
        if unsubscribed_at is None:
            if IS_POSTGRES:
                cursor.execute(
                    'UPDATE users SET consent_status = %s, unsubscribed_at = NOW(), updated_at = NOW() WHERE id = %s',
                    ('unsubscribed', user_id),
                )
            else:
                cursor.execute(
                    'UPDATE users SET consent_status = ?, unsubscribed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                    ('unsubscribed', user_id),
                )
        else:
            if IS_POSTGRES:
                cursor.execute(
                    'UPDATE users SET consent_status = %s, unsubscribed_at = %s, updated_at = NOW() WHERE id = %s',
                    ('unsubscribed', unsubscribed_at, user_id),
                )
            else:
                cursor.execute(
                    'UPDATE users SET consent_status = ?, unsubscribed_at = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                    ('unsubscribed', unsubscribed_at, user_id),
                )
        conn.commit()
        cursor.close()
    return get_user(user_id)


def create_community(
    chat_id,
    title,
    community_type='group',
    default_invite_mode='invite_link',
    auto_approve_join_requests=False,
    strict_moderation=False,
    is_active=True,
):
    with get_connection() as conn:
        cursor = conn.cursor()
        params = (
            chat_id,
            str(title or '').strip(),
            str(community_type or 'group').strip(),
            str(default_invite_mode or 'invite_link').strip(),
            auto_approve_join_requests,
            strict_moderation,
            is_active,
        )
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO communities('
                'chat_id, title, type, default_invite_mode, auto_approve_join_requests, strict_moderation, is_active'
                ') VALUES (%s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (chat_id) DO UPDATE SET '
                'title = EXCLUDED.title, type = EXCLUDED.type, default_invite_mode = EXCLUDED.default_invite_mode, '
                'auto_approve_join_requests = EXCLUDED.auto_approve_join_requests, '
                'strict_moderation = EXCLUDED.strict_moderation, is_active = EXCLUDED.is_active, updated_at = NOW()',
                params,
            )
        else:
            cursor.execute(
                'INSERT INTO communities('
                'chat_id, title, type, default_invite_mode, auto_approve_join_requests, strict_moderation, is_active'
                ') VALUES (?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(chat_id) DO UPDATE SET '
                'title = excluded.title, type = excluded.type, default_invite_mode = excluded.default_invite_mode, '
                'auto_approve_join_requests = excluded.auto_approve_join_requests, '
                'strict_moderation = excluded.strict_moderation, is_active = excluded.is_active, '
                'updated_at = CURRENT_TIMESTAMP',
                (
                    chat_id,
                    str(title or '').strip(),
                    str(community_type or 'group').strip(),
                    str(default_invite_mode or 'invite_link').strip(),
                    _normalize_bool(auto_approve_join_requests),
                    _normalize_bool(strict_moderation),
                    _normalize_bool(is_active),
                ),
            )
        conn.commit()
        cursor.close()
    return get_community_by_chat_id(chat_id)


def get_community(community_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM communities WHERE id = %s', (community_id,))
        else:
            cursor.execute('SELECT * FROM communities WHERE id = ?', (community_id,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def get_community_by_chat_id(chat_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM communities WHERE chat_id = %s', (chat_id,))
        else:
            cursor.execute('SELECT * FROM communities WHERE chat_id = ?', (chat_id,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def list_communities(is_active=None):
    with get_connection() as conn:
        cursor = conn.cursor()
        if is_active is None:
            cursor.execute('SELECT * FROM communities ORDER BY id DESC')
        elif IS_POSTGRES:
            cursor.execute('SELECT * FROM communities WHERE is_active = %s ORDER BY id DESC', (bool(is_active),))
        else:
            cursor.execute('SELECT * FROM communities WHERE is_active = ? ORDER BY id DESC', (_normalize_bool(is_active),))
        rows = cursor.fetchall()
        result = _rows_to_dicts(cursor, rows)
        cursor.close()
        return result


def update_community(community_id, **fields):
    allowed = {
        'title',
        'type',
        'default_invite_mode',
        'auto_approve_join_requests',
        'strict_moderation',
        'is_active',
    }
    updates = {key: fields[key] for key in fields if key in allowed}
    if not updates:
        return get_community(community_id)
    with get_connection() as conn:
        cursor = conn.cursor()
        values = []
        assignments = []
        for key, value in updates.items():
            assignments.append(f'{key} = {"%s" if IS_POSTGRES else "?"}')
            if key in {'auto_approve_join_requests', 'strict_moderation', 'is_active'} and not IS_POSTGRES:
                value = _normalize_bool(value)
            values.append(value)
        assignments.append('updated_at = NOW()' if IS_POSTGRES else 'updated_at = CURRENT_TIMESTAMP')
        values.append(community_id)
        sql_text = f'UPDATE communities SET {", ".join(assignments)} WHERE id = {"%s" if IS_POSTGRES else "?"}'
        cursor.execute(sql_text, tuple(values))
        conn.commit()
        cursor.close()
    return get_community(community_id)


def get_campaign(campaign_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM campaigns WHERE id = %s', (campaign_id,))
        else:
            cursor.execute('SELECT * FROM campaigns WHERE id = ?', (campaign_id,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def create_campaign(
    name,
    community_id,
    segment_id=None,
    invite_mode='auto',
    message_template='',
    rate_limit_per_minute=20,
    rate_limit_per_hour=300,
    max_attempts=1,
    start_at=None,
    end_at=None,
    stop_on_error_rate=0.30,
    created_by=None,
    allow_auto_approve=True,
    recipients=None,
):
    campaign_id = None
    with get_connection() as conn:
        cursor = conn.cursor()
        params = (
            str(name or '').strip(),
            'draft',
            segment_id,
            community_id,
            str(invite_mode or 'auto').strip(),
            str(message_template or '').strip(),
            int(rate_limit_per_minute or 20),
            int(rate_limit_per_hour or 300),
            int(max_attempts or 1),
            float(stop_on_error_rate or 0.0),
            allow_auto_approve,
            start_at,
            end_at,
            created_by,
        )
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO campaigns('
                'name, status, segment_id, community_id, invite_mode, message_template, '
                'rate_limit_per_minute, rate_limit_per_hour, max_attempts, stop_on_error_rate, '
                'allow_auto_approve, start_at, end_at, created_by'
                ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id',
                params,
            )
            campaign_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                'INSERT INTO campaigns('
                'name, status, segment_id, community_id, invite_mode, message_template, '
                'rate_limit_per_minute, rate_limit_per_hour, max_attempts, stop_on_error_rate, '
                'allow_auto_approve, start_at, end_at, created_by'
                ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (
                    str(name or '').strip(),
                    'draft',
                    segment_id,
                    community_id,
                    str(invite_mode or 'auto').strip(),
                    str(message_template or '').strip(),
                    int(rate_limit_per_minute or 20),
                    int(rate_limit_per_hour or 300),
                    int(max_attempts or 1),
                    float(stop_on_error_rate or 0.0),
                    _normalize_bool(allow_auto_approve),
                    start_at,
                    end_at,
                    created_by,
                ),
            )
            campaign_id = cursor.lastrowid
        conn.commit()
        cursor.close()
    if recipients:
        bulk_upsert_campaign_recipients(campaign_id, community_id, recipients)
    return get_campaign(campaign_id)


def update_campaign(campaign_id, **fields):
    allowed = {
        'name',
        'status',
        'segment_id',
        'community_id',
        'invite_mode',
        'resolved_invite_mode',
        'message_template',
        'rate_limit_per_minute',
        'rate_limit_per_hour',
        'max_attempts',
        'stop_on_error_rate',
        'allow_auto_approve',
        'start_at',
        'end_at',
        'created_by',
    }
    updates = {key: fields[key] for key in fields if key in allowed}
    if not updates:
        return get_campaign(campaign_id)
    with get_connection() as conn:
        cursor = conn.cursor()
        values = []
        assignments = []
        for key, value in updates.items():
            assignments.append(f'{key} = {"%s" if IS_POSTGRES else "?"}')
            if key == 'allow_auto_approve' and not IS_POSTGRES:
                value = _normalize_bool(value)
            values.append(value)
        assignments.append('updated_at = NOW()' if IS_POSTGRES else 'updated_at = CURRENT_TIMESTAMP')
        values.append(campaign_id)
        sql_text = f'UPDATE campaigns SET {", ".join(assignments)} WHERE id = {"%s" if IS_POSTGRES else "?"}'
        cursor.execute(sql_text, tuple(values))
        conn.commit()
        cursor.close()
    return get_campaign(campaign_id)


def update_campaign_status(campaign_id, status):
    return update_campaign(campaign_id, status=status)


def list_campaigns(status=None):
    with get_connection() as conn:
        cursor = conn.cursor()
        if status is None:
            cursor.execute('SELECT * FROM campaigns ORDER BY id DESC')
        elif IS_POSTGRES:
            cursor.execute('SELECT * FROM campaigns WHERE status = %s ORDER BY id DESC', (status,))
        else:
            cursor.execute('SELECT * FROM campaigns WHERE status = ? ORDER BY id DESC', (status,))
        rows = cursor.fetchall()
        result = _rows_to_dicts(cursor, rows)
        cursor.close()
        return result


def get_campaign_recipient(recipient_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'SELECT r.*, u.telegram_user_id, u.username, u.first_name, u.source, u.is_blacklisted, u.unsubscribed_at '
                'FROM campaign_recipients r '
                'JOIN users u ON u.id = r.user_id '
                'WHERE r.id = %s',
                (recipient_id,),
            )
        else:
            cursor.execute(
                'SELECT r.*, u.telegram_user_id, u.username, u.first_name, u.source, u.is_blacklisted, u.unsubscribed_at '
                'FROM campaign_recipients r '
                'JOIN users u ON u.id = r.user_id '
                'WHERE r.id = ?',
                (recipient_id,),
            )
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def get_campaign_recipient_by_key(campaign_id, user_id, community_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'SELECT r.*, u.telegram_user_id, u.username, u.first_name, u.source, u.is_blacklisted, u.unsubscribed_at '
                'FROM campaign_recipients r '
                'JOIN users u ON u.id = r.user_id '
                'WHERE r.campaign_id = %s AND r.user_id = %s AND r.community_id = %s',
                (campaign_id, user_id, community_id),
            )
        else:
            cursor.execute(
                'SELECT r.*, u.telegram_user_id, u.username, u.first_name, u.source, u.is_blacklisted, u.unsubscribed_at '
                'FROM campaign_recipients r '
                'JOIN users u ON u.id = r.user_id '
                'WHERE r.campaign_id = ? AND r.user_id = ? AND r.community_id = ?',
                (campaign_id, user_id, community_id),
            )
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def upsert_campaign_recipient(
    campaign_id,
    user_id,
    community_id,
    invite_link_id=None,
    delivery_status='pending',
    join_status='none',
    attempts=0,
    last_error='',
    last_sent_at=None,
):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO campaign_recipients('
                'campaign_id, user_id, community_id, invite_link_id, delivery_status, join_status, attempts, last_error, last_sent_at'
                ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (campaign_id, user_id, community_id) DO UPDATE SET '
                'invite_link_id = COALESCE(EXCLUDED.invite_link_id, campaign_recipients.invite_link_id), '
                'delivery_status = EXCLUDED.delivery_status, join_status = EXCLUDED.join_status, '
                'attempts = EXCLUDED.attempts, last_error = EXCLUDED.last_error, '
                'last_sent_at = COALESCE(EXCLUDED.last_sent_at, campaign_recipients.last_sent_at), '
                'updated_at = NOW()',
                (
                    campaign_id,
                    user_id,
                    community_id,
                    invite_link_id,
                    str(delivery_status or 'pending').strip(),
                    str(join_status or 'none').strip(),
                    int(attempts or 0),
                    str(last_error or '').strip(),
                    last_sent_at,
                ),
            )
        else:
            cursor.execute(
                'INSERT INTO campaign_recipients('
                'campaign_id, user_id, community_id, invite_link_id, delivery_status, join_status, attempts, last_error, last_sent_at'
                ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(campaign_id, user_id, community_id) DO UPDATE SET '
                'invite_link_id = COALESCE(excluded.invite_link_id, campaign_recipients.invite_link_id), '
                'delivery_status = excluded.delivery_status, join_status = excluded.join_status, '
                'attempts = excluded.attempts, last_error = excluded.last_error, '
                'last_sent_at = COALESCE(excluded.last_sent_at, campaign_recipients.last_sent_at), '
                'updated_at = CURRENT_TIMESTAMP',
                (
                    campaign_id,
                    user_id,
                    community_id,
                    invite_link_id,
                    str(delivery_status or 'pending').strip(),
                    str(join_status or 'none').strip(),
                    int(attempts or 0),
                    str(last_error or '').strip(),
                    last_sent_at,
                ),
            )
        conn.commit()
        cursor.close()
    return get_campaign_recipient_by_key(campaign_id, user_id, community_id)


def bulk_upsert_campaign_recipients(campaign_id, community_id, user_ids):
    unique_user_ids = []
    seen = set()
    for user_id in user_ids or []:
        if user_id in seen:
            continue
        seen.add(user_id)
        unique_user_ids.append(user_id)
    items = []
    for user_id in unique_user_ids:
        items.append(upsert_campaign_recipient(campaign_id, user_id, community_id))
    return items


def update_campaign_recipient_status(
    recipient_id,
    delivery_status=None,
    join_status=None,
    attempts_increment=0,
    last_error=None,
    last_sent_at=None,
    invite_link_id=None,
):
    assignments = []
    values = []
    if delivery_status is not None:
        assignments.append(f'delivery_status = {"%s" if IS_POSTGRES else "?"}')
        values.append(str(delivery_status or '').strip())
    if join_status is not None:
        assignments.append(f'join_status = {"%s" if IS_POSTGRES else "?"}')
        values.append(str(join_status or '').strip())
    if last_error is not None:
        assignments.append(f'last_error = {"%s" if IS_POSTGRES else "?"}')
        values.append(str(last_error or '').strip())
    if invite_link_id is not None:
        assignments.append(f'invite_link_id = {"%s" if IS_POSTGRES else "?"}')
        values.append(invite_link_id)
    if attempts_increment:
        assignments.append(f'attempts = attempts + {"%s" if IS_POSTGRES else "?"}')
        values.append(int(attempts_increment))
    if last_sent_at is not None:
        assignments.append(f'last_sent_at = {"%s" if IS_POSTGRES else "?"}')
        values.append(last_sent_at)
    if not assignments:
        return get_campaign_recipient(recipient_id)
    assignments.append('updated_at = NOW()' if IS_POSTGRES else 'updated_at = CURRENT_TIMESTAMP')
    values.append(recipient_id)
    with get_connection() as conn:
        cursor = conn.cursor()
        sql_text = f'UPDATE campaign_recipients SET {", ".join(assignments)} WHERE id = {"%s" if IS_POSTGRES else "?"}'
        cursor.execute(sql_text, tuple(values))
        conn.commit()
        cursor.close()
    return get_campaign_recipient(recipient_id)


def list_campaign_recipients(campaign_id, delivery_statuses=None, join_statuses=None, limit=None):
    clauses = ['r.campaign_id = %s' if IS_POSTGRES else 'r.campaign_id = ?']
    params = [campaign_id]
    if delivery_statuses:
        placeholders = ', '.join(['%s'] * len(delivery_statuses) if IS_POSTGRES else ['?'] * len(delivery_statuses))
        clauses.append(f'r.delivery_status IN ({placeholders})')
        params.extend(list(delivery_statuses))
    if join_statuses:
        placeholders = ', '.join(['%s'] * len(join_statuses) if IS_POSTGRES else ['?'] * len(join_statuses))
        clauses.append(f'r.join_status IN ({placeholders})')
        params.extend(list(join_statuses))
    sql_text = (
        'SELECT r.*, u.telegram_user_id, u.username, u.first_name, u.source, u.is_blacklisted, u.unsubscribed_at '
        'FROM campaign_recipients r '
        'JOIN users u ON u.id = r.user_id '
        f'WHERE {" AND ".join(clauses)} '
        'ORDER BY r.id ASC'
    )
    if limit:
        sql_text += f' LIMIT {"%s" if IS_POSTGRES else "?"}'
        params.append(int(limit))
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_text, tuple(params))
        rows = cursor.fetchall()
        result = _rows_to_dicts(cursor, rows)
        cursor.close()
        return result


def find_latest_campaign_recipient(community_id, telegram_user_id, campaign_id=None):
    clauses = ['r.community_id = %s' if IS_POSTGRES else 'r.community_id = ?', 'u.telegram_user_id = %s' if IS_POSTGRES else 'u.telegram_user_id = ?']
    params = [community_id, telegram_user_id]
    if campaign_id is not None:
        clauses.append('r.campaign_id = %s' if IS_POSTGRES else 'r.campaign_id = ?')
        params.append(campaign_id)
    sql_text = (
        'SELECT r.*, u.telegram_user_id, u.username, u.first_name, u.source, u.is_blacklisted, u.unsubscribed_at '
        'FROM campaign_recipients r '
        'JOIN users u ON u.id = r.user_id '
        f'WHERE {" AND ".join(clauses)} '
        'ORDER BY r.id DESC '
        'LIMIT 1'
    )
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_text, tuple(params))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def create_invite_link(
    community_id,
    campaign_id,
    invite_mode,
    telegram_invite_link,
    creates_join_request=False,
    expire_at=None,
    member_limit=None,
    is_revoked=False,
):
    link_hash = _normalize_link_hash(telegram_invite_link)
    invite_link_id = None
    with get_connection() as conn:
        cursor = conn.cursor()
        params = (
            community_id,
            campaign_id,
            str(invite_mode or 'invite_link').strip(),
            str(telegram_invite_link or '').strip(),
            link_hash,
            creates_join_request,
            expire_at,
            member_limit,
            is_revoked,
        )
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO invite_links('
                'community_id, campaign_id, invite_mode, telegram_invite_link, invite_link_hash, '
                'creates_join_request, expire_at, member_limit, is_revoked'
                ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) '
                'ON CONFLICT (telegram_invite_link) DO UPDATE SET '
                'community_id = EXCLUDED.community_id, campaign_id = EXCLUDED.campaign_id, '
                'invite_mode = EXCLUDED.invite_mode, invite_link_hash = EXCLUDED.invite_link_hash, '
                'creates_join_request = EXCLUDED.creates_join_request, expire_at = EXCLUDED.expire_at, '
                'member_limit = EXCLUDED.member_limit, is_revoked = EXCLUDED.is_revoked, updated_at = NOW() '
                'RETURNING id',
                (
                    community_id,
                    campaign_id,
                    str(invite_mode or 'invite_link').strip(),
                    str(telegram_invite_link or '').strip(),
                    link_hash,
                    bool(creates_join_request),
                    expire_at,
                    member_limit,
                    bool(is_revoked),
                ),
            )
            invite_link_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                'INSERT INTO invite_links('
                'community_id, campaign_id, invite_mode, telegram_invite_link, invite_link_hash, '
                'creates_join_request, expire_at, member_limit, is_revoked'
                ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(telegram_invite_link) DO UPDATE SET '
                'community_id = excluded.community_id, campaign_id = excluded.campaign_id, '
                'invite_mode = excluded.invite_mode, invite_link_hash = excluded.invite_link_hash, '
                'creates_join_request = excluded.creates_join_request, expire_at = excluded.expire_at, '
                'member_limit = excluded.member_limit, is_revoked = excluded.is_revoked, '
                'updated_at = CURRENT_TIMESTAMP',
                (
                    community_id,
                    campaign_id,
                    str(invite_mode or 'invite_link').strip(),
                    str(telegram_invite_link or '').strip(),
                    link_hash,
                    _normalize_bool(creates_join_request),
                    expire_at,
                    member_limit,
                    _normalize_bool(is_revoked),
                ),
            )
            invite_link_id = cursor.lastrowid
            if not invite_link_id:
                cursor.execute('SELECT id FROM invite_links WHERE telegram_invite_link = ?', (telegram_invite_link,))
                row = cursor.fetchone()
                invite_link_id = row[0] if row else None
        conn.commit()
        cursor.close()
    return get_invite_link(invite_link_id)


def get_invite_link(invite_link_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM invite_links WHERE id = %s', (invite_link_id,))
        else:
            cursor.execute('SELECT * FROM invite_links WHERE id = ?', (invite_link_id,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def get_invite_link_by_url(telegram_invite_link):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM invite_links WHERE telegram_invite_link = %s', (telegram_invite_link,))
        else:
            cursor.execute('SELECT * FROM invite_links WHERE telegram_invite_link = ?', (telegram_invite_link,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def get_active_invite_link(community_id, campaign_id=None, invite_mode=None):
    clauses = ['community_id = %s' if IS_POSTGRES else 'community_id = ?', 'is_revoked = %s' if IS_POSTGRES else 'is_revoked = ?']
    params = [community_id, False if IS_POSTGRES else 0]
    if campaign_id is not None:
        clauses.append('campaign_id = %s' if IS_POSTGRES else 'campaign_id = ?')
        params.append(campaign_id)
    if invite_mode:
        clauses.append('invite_mode = %s' if IS_POSTGRES else 'invite_mode = ?')
        params.append(invite_mode)
    sql_text = f'SELECT * FROM invite_links WHERE {" AND ".join(clauses)} ORDER BY id DESC LIMIT 1'
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_text, tuple(params))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def revoke_invite_link(invite_link_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('UPDATE invite_links SET is_revoked = TRUE, updated_at = NOW() WHERE id = %s', (invite_link_id,))
        else:
            cursor.execute('UPDATE invite_links SET is_revoked = 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?', (invite_link_id,))
        conn.commit()
        cursor.close()
    return get_invite_link(invite_link_id)


def get_join_request(join_request_id):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM join_requests WHERE id = %s', (join_request_id,))
        else:
            cursor.execute('SELECT * FROM join_requests WHERE id = ?', (join_request_id,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def get_join_request_by_request_key(request_key):
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT * FROM join_requests WHERE request_key = %s', (request_key,))
        else:
            cursor.execute('SELECT * FROM join_requests WHERE request_key = ?', (request_key,))
        row = cursor.fetchone()
        result = _row_to_dict(cursor, row)
        cursor.close()
        return result


def create_join_request(
    community_id,
    campaign_id,
    user_id,
    telegram_user_id,
    status='pending',
    decision_type=None,
    decision_reason='',
    moderator_id=None,
    requested_at=None,
    decided_at=None,
    invite_link_id=None,
    request_key=None,
):
    request_key = str(request_key or f'{community_id}:{telegram_user_id}:{requested_at or ""}').strip()
    join_request_id = None
    with get_connection() as conn:
        cursor = conn.cursor()
        params = (
            community_id,
            campaign_id,
            user_id,
            telegram_user_id,
            invite_link_id,
            request_key,
            str(status or 'pending').strip(),
            decision_type,
            str(decision_reason or '').strip(),
            moderator_id,
            requested_at,
            decided_at,
        )
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO join_requests('
                'community_id, campaign_id, user_id, telegram_user_id, invite_link_id, request_key, '
                'status, decision_type, decision_reason, moderator_id, requested_at, decided_at'
                ') VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()), %s) '
                'ON CONFLICT (request_key) DO UPDATE SET '
                'campaign_id = EXCLUDED.campaign_id, user_id = EXCLUDED.user_id, invite_link_id = EXCLUDED.invite_link_id, '
                'status = EXCLUDED.status, decision_type = EXCLUDED.decision_type, '
                'decision_reason = EXCLUDED.decision_reason, moderator_id = EXCLUDED.moderator_id, '
                'requested_at = COALESCE(EXCLUDED.requested_at, join_requests.requested_at), '
                'decided_at = COALESCE(EXCLUDED.decided_at, join_requests.decided_at) '
                'RETURNING id',
                params,
            )
            join_request_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                'INSERT INTO join_requests('
                'community_id, campaign_id, user_id, telegram_user_id, invite_link_id, request_key, '
                'status, decision_type, decision_reason, moderator_id, requested_at, decided_at'
                ') VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), ?) '
                'ON CONFLICT(request_key) DO UPDATE SET '
                'campaign_id = excluded.campaign_id, user_id = excluded.user_id, invite_link_id = excluded.invite_link_id, '
                'status = excluded.status, decision_type = excluded.decision_type, '
                'decision_reason = excluded.decision_reason, moderator_id = excluded.moderator_id, '
                'requested_at = COALESCE(excluded.requested_at, join_requests.requested_at), '
                'decided_at = COALESCE(excluded.decided_at, join_requests.decided_at)',
                params,
            )
            join_request_id = cursor.lastrowid
            if not join_request_id:
                cursor.execute('SELECT id FROM join_requests WHERE request_key = ?', (request_key,))
                row = cursor.fetchone()
                join_request_id = row[0] if row else None
        conn.commit()
        cursor.close()
    return get_join_request(join_request_id)


def update_join_request_status(join_request_id, status, decision_type=None, decision_reason='', moderator_id=None, decided_at=None):
    with get_connection() as conn:
        cursor = conn.cursor()
        if str(status or '').strip() == 'pending' and decided_at is None:
            if IS_POSTGRES:
                cursor.execute(
                    'UPDATE join_requests SET status = %s, decision_type = %s, decision_reason = %s, '
                    'moderator_id = %s, decided_at = NULL WHERE id = %s',
                    (status, decision_type, str(decision_reason or '').strip(), moderator_id, join_request_id),
                )
            else:
                cursor.execute(
                    'UPDATE join_requests SET status = ?, decision_type = ?, decision_reason = ?, '
                    'moderator_id = ?, decided_at = NULL WHERE id = ?',
                    (status, decision_type, str(decision_reason or '').strip(), moderator_id, join_request_id),
                )
        elif decided_at is None:
            if IS_POSTGRES:
                cursor.execute(
                    'UPDATE join_requests SET status = %s, decision_type = %s, decision_reason = %s, '
                    'moderator_id = %s, decided_at = NOW() WHERE id = %s',
                    (status, decision_type, str(decision_reason or '').strip(), moderator_id, join_request_id),
                )
            else:
                cursor.execute(
                    'UPDATE join_requests SET status = ?, decision_type = ?, decision_reason = ?, '
                    'moderator_id = ?, decided_at = CURRENT_TIMESTAMP WHERE id = ?',
                    (status, decision_type, str(decision_reason or '').strip(), moderator_id, join_request_id),
                )
        else:
            if IS_POSTGRES:
                cursor.execute(
                    'UPDATE join_requests SET status = %s, decision_type = %s, decision_reason = %s, '
                    'moderator_id = %s, decided_at = %s WHERE id = %s',
                    (status, decision_type, str(decision_reason or '').strip(), moderator_id, decided_at, join_request_id),
                )
            else:
                cursor.execute(
                    'UPDATE join_requests SET status = ?, decision_type = ?, decision_reason = ?, '
                    'moderator_id = ?, decided_at = ? WHERE id = ?',
                    (status, decision_type, str(decision_reason or '').strip(), moderator_id, decided_at, join_request_id),
                )
        conn.commit()
        cursor.close()
    return get_join_request(join_request_id)


def list_join_requests(status=None, limit=None):
    params = []
    sql_text = 'SELECT * FROM join_requests'
    if status is not None:
        sql_text += f' WHERE status = {"%s" if IS_POSTGRES else "?"}'
        params.append(status)
    sql_text += ' ORDER BY requested_at ASC'
    if limit:
        sql_text += f' LIMIT {"%s" if IS_POSTGRES else "?"}'
        params.append(int(limit))
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_text, tuple(params))
        rows = cursor.fetchall()
        result = _rows_to_dicts(cursor, rows)
        cursor.close()
        return result


def create_audit_log(actor_id, action, entity_type, entity_id, payload_json=None):
    log_id = None
    payload_text = _json_text(payload_json, default='')
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute(
                'INSERT INTO audit_log(actor_id, action, entity_type, entity_id, payload_json) '
                'VALUES (%s, %s, %s, %s, %s) RETURNING id',
                (actor_id, action, entity_type, entity_id, payload_text),
            )
            log_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                'INSERT INTO audit_log(actor_id, action, entity_type, entity_id, payload_json) '
                'VALUES (?, ?, ?, ?, ?)',
                (actor_id, action, entity_type, entity_id, payload_text),
            )
            log_id = cursor.lastrowid
        conn.commit()
        cursor.close()
    return log_id


def get_campaign_stats(campaign_id):
    stats = {
        'total': 0,
        'queued': 0,
        'pending': 0,
        'sent': 0,
        'delivered': 0,
        'failed': 0,
        'suppressed': 0,
        'clicked': 0,
        'joined': 0,
        'join_requested': 0,
        'approved': 0,
        'declined': 0,
        'unsubscribed': 0,
    }
    with get_connection() as conn:
        cursor = conn.cursor()
        if IS_POSTGRES:
            cursor.execute('SELECT COUNT(*) FROM campaign_recipients WHERE campaign_id = %s', (campaign_id,))
        else:
            cursor.execute('SELECT COUNT(*) FROM campaign_recipients WHERE campaign_id = ?', (campaign_id,))
        stats['total'] = int((cursor.fetchone() or [0])[0] or 0)
        if IS_POSTGRES:
            cursor.execute(
                'SELECT delivery_status, COUNT(*) FROM campaign_recipients WHERE campaign_id = %s GROUP BY delivery_status',
                (campaign_id,),
            )
        else:
            cursor.execute(
                'SELECT delivery_status, COUNT(*) FROM campaign_recipients WHERE campaign_id = ? GROUP BY delivery_status',
                (campaign_id,),
            )
        for status, count in cursor.fetchall():
            if status in stats:
                stats[status] = int(count or 0)
        if IS_POSTGRES:
            cursor.execute(
                'SELECT join_status, COUNT(*) FROM campaign_recipients WHERE campaign_id = %s GROUP BY join_status',
                (campaign_id,),
            )
        else:
            cursor.execute(
                'SELECT join_status, COUNT(*) FROM campaign_recipients WHERE campaign_id = ? GROUP BY join_status',
                (campaign_id,),
            )
        for status, count in cursor.fetchall():
            if status in stats:
                stats[status] = int(count or 0)
        if IS_POSTGRES:
            cursor.execute(
                'SELECT COUNT(*) '
                'FROM campaign_recipients r JOIN users u ON u.id = r.user_id '
                'WHERE r.campaign_id = %s AND u.unsubscribed_at IS NOT NULL',
                (campaign_id,),
            )
        else:
            cursor.execute(
                'SELECT COUNT(*) '
                'FROM campaign_recipients r JOIN users u ON u.id = r.user_id '
                'WHERE r.campaign_id = ? AND u.unsubscribed_at IS NOT NULL',
                (campaign_id,),
            )
        stats['unsubscribed'] = int((cursor.fetchone() or [0])[0] or 0)
        cursor.close()
    return stats
