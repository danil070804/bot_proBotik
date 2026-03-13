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


init_db()


def get_main_connection():
    if not IS_POSTGRES:
        raise RuntimeError('Set DATABASE_URL for main bot database')
    return psycopg2.connect(DATABASE_URL)
