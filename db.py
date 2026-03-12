import sqlite3 as sql
import functions
from config import full_chats
conn = sql.connect('base.db')
cursor = conn.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS chats(acc TEXT, chat TEXT UNIQUE)')


def insert_chat_db(acc, chat):
    conn = sql.connect('base.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO chats VALUES(?, ?)', (acc, chat))
    conn.commit()
    cursor.close()


def get_all_chats():
    conn = sql.connect('base.db')
    cursor = conn.cursor()
    all_c = cursor.execute('SELECT chat FROM chats').fetchall()
    c = []
    try:
        for i in all_c:
            c.append(i[0])
    except:
        pass
    return c


def is_full(acc):
    conn = sql.connect('base.db')
    cursor = conn.cursor()
    val = cursor.execute(
        f'SELECT * FROM chats WHERE acc = "a{acc}"').fetchall()
    try:
        if len(val) > full_chats:
            return True
        else:
            return False
    except:
        return False
    conn.commit()
    cursor.close()


def check_all_full():
    conn = sql.connect('base.db')
    cursor = conn.cursor()
    a = 0
    for sess in functions.get_sessions():
        try:
            all = len(cursor.execute(
                'SELECT * FROM chats WHERE acc = "%s"' % sess).fetchall())
            if all >= full_chats:
                a += 1
        except Exception as e:
            print(e)
    return a


def get_sess_chats(session):
    conn = sql.connect('base.db')
    cursor = conn.cursor()
    c = []
    chats = cursor.execute(
        'SELECT chat FROM chats WHERE acc = %s' % session).fetchall()
    for chat in chats:
        c.append(chat)
    return chat
