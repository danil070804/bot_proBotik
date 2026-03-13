from decimal import *
import telebot
import datetime
from telebot import types, apihelper
import sqlite3
import random, string
import time
import os,random,shutil,subprocess
import sys
import glob
import threading
from queue import Queue
import json
import keyboards
import requests
from datetime import datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
import secrets
import hashlib
import config
from getpass import getpass
import pytz
from db import get_main_connection, init_db, get_app_setting, set_app_setting

from pyqiwip2p import QiwiP2P
from pyqiwip2p.p2p_types import QiwiCustomer, QiwiDatetime


TOKEN = config.bot_invite_token
bot = telebot.TeleBot(TOKEN)
admin = config.admin
init_db()
USER_STATE = {}
RUNNING_TASKS = {}
TASK_QUEUE = Queue()
TASK_LOCK = threading.Lock()
DEFAULT_APP_SETTINGS = {
	'parser_posts_limit': '100',
	'parser_comments_limit': '200',
	'parser_use_all_sessions': '1',
	'inviter_limit': '100',
	'inviter_sleep': '15',
	'inviter_per_account_limit': str(config.invite_per_account_limit),
	'inviter_max_flood_wait': str(config.invite_max_flood_wait),
	'inviter_use_all_sessions': '1',
}


def build_new_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='🧩 Аккаунты • Session', callback_data='accounts_menu'),
		types.InlineKeyboardButton(text='🔎 Парсинг • Аудитория', callback_data='parser_start'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='📨 Инвайт • Добавление', callback_data='inviter_start'),
		types.InlineKeyboardButton(text='📊 Статус • Live', callback_data='task_status'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='⚙️ Управление • Процессы', callback_data='manage_menu'),
		types.InlineKeyboardButton(text='📈 Аналитика • Отчёты', callback_data='stats_overview'),
	)
	keyboard.add(types.InlineKeyboardButton(text='🛠 Настройки • Парсинг/Инвайт', callback_data='settings_menu'))
	keyboard.add(types.InlineKeyboardButton(text='ℹ️ Помощь • Гайд', callback_data='help_new'))
	return keyboard


def list_sessions():
	return glob.glob('*.session')


def _save_targets_file(user_id, text_value, prefix):
	filename = f'{prefix}_{user_id}.txt'
	rows = []
	for raw in str(text_value).replace('\r', '\n').split('\n'):
		for part in raw.split(','):
			item = part.strip()
			if item != '':
				rows.append(item)
	with open(filename, 'w', encoding='utf-8') as f:
		for row in rows:
			f.write(row + '\n')
	return filename, len(rows)


def _progress_file(user_id, task_type):
	return f'progress_{task_type}_{user_id}_{int(time.time())}.json'


def _format_progress_text(item, progress):
	mode = progress.get('mode', '')
	if mode == 'parser':
		return (
			f'🔎 Парсинг в процессе\n'
			f'Источник: {progress.get("current_source", "-")}\n'
			f'Источники: {progress.get("sources_done", 0)}/{progress.get("sources_total", 0)}\n'
			f'Пользователей: {progress.get("users_parsed", 0)}\n'
			f'Комментариев: {progress.get("comments_parsed", 0)}'
		)
	if mode == 'inviter':
		return (
			f'📨 Инвайт в процессе\n'
			f'Цель: {progress.get("invite_target", "-")}\n'
			f'Источники: {progress.get("sources_done", 0)}/{progress.get("sources_total", 0)}\n'
			f'Кандидатов: {progress.get("total_candidates", 0)}\n'
			f'Обработано: {progress.get("processed", 0)}\n'
			f'✅ Добавлено: {progress.get("invited", 0)} | ⛔ privacy: {progress.get("privacy", 0)}\n'
			f'⚠️ already: {progress.get("already", 0)} | errors: {progress.get("error", 0)}'
		)
	return f'Задача: {item["title"]} выполняется...'


def _start_progress_monitor(item):
	progress_file = item.get('progress_file')
	if not progress_file:
		return

	def _watch():
		last_snapshot = ''
		while item.get('status') == 'running':
			try:
				if os.path.exists(progress_file):
					with open(progress_file, 'r', encoding='utf-8') as f:
						progress = json.load(f)
					text = _format_progress_text(item, progress)
					if text != last_snapshot:
						bot.send_message(item['user_id'], text)
						last_snapshot = text
			except Exception:
				pass
			time.sleep(4)
		try:
			if os.path.exists(progress_file):
				os.remove(progress_file)
		except Exception:
			pass

	threading.Thread(target=_watch, daemon=True).start()


def _queue_worker():
	while True:
		item = TASK_QUEUE.get()
		with TASK_LOCK:
			item['status'] = 'running'
		proc = subprocess.Popen(item['command'])
		item['pid'] = proc.pid
		item['proc'] = proc
		try:
			bot.send_message(item['user_id'], f'▶️ Запущено: {item["title"]} (PID {proc.pid})')
		except Exception:
			pass
		_start_progress_monitor(item)
		code = proc.wait()
		with TASK_LOCK:
			item['status'] = f'finished ({code})'
		try:
			bot.send_message(item['user_id'], f'✅ Завершено: {item["title"]} (code {code})')
		except Exception:
			pass
		TASK_QUEUE.task_done()


def _ensure_worker():
	if getattr(_ensure_worker, 'started', False):
		return
	t = threading.Thread(target=_queue_worker, daemon=True)
	t.start()
	_ensure_worker.started = True


def _enqueue_process(user_id, title, command, progress_file=''):
	_ensure_worker()
	item = {
		'user_id': user_id,
		'title': title,
		'command': command,
		'status': 'queued',
		'pid': None,
		'proc': None,
		'progress_file': progress_file,
	}
	with TASK_LOCK:
		RUNNING_TASKS.setdefault(user_id, []).append(item)
	TASK_QUEUE.put(item)
	return TASK_QUEUE.qsize()


def _is_cancel(text):
	return str(text).strip().lower() in ['отмена', '❌ отмена', 'cancel', 'меню', 'menu', 'назад', 'back']


def _step_keyboard():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='❌ Отменить сценарий', callback_data='cancel_flow'),
		types.InlineKeyboardButton(text='⬅️ Назад в меню', callback_data='main_menu'),
	)
	return keyboard


def _is_admin(user_id):
	try:
		return int(user_id) == int(admin)
	except Exception:
		return False


def _deny_access(chat_id):
	bot.send_message(chat_id, '⛔ Доступ закрыт. Этим ботом может пользоваться только администратор.')


def _get_setting(key):
	return get_app_setting(key, DEFAULT_APP_SETTINGS.get(key, ''))


def _set_setting(key, value):
	set_app_setting(key, value)


def _setting_int(key):
	try:
		return int(_get_setting(key))
	except Exception:
		return int(DEFAULT_APP_SETTINGS.get(key, '0') or 0)


def _setting_bool(key):
	return str(_get_setting(key)).strip() in ['1', 'true', 'True', 'yes']


def _setting_title(key):
	titles = {
		'parser_posts_limit': 'Лимит постов для парсинга',
		'parser_comments_limit': 'Лимит комментариев для парсинга',
		'parser_use_all_sessions': 'Использовать все аккаунты в парсинге',
		'inviter_limit': 'Лимит пользователей за запуск инвайта',
		'inviter_sleep': 'Пауза между инвайтами (сек)',
		'inviter_per_account_limit': 'Лимит добавлений на 1 аккаунт',
		'inviter_max_flood_wait': 'Максимальный FloodWait (сек)',
		'inviter_use_all_sessions': 'Использовать все аккаунты в инвайте',
	}
	return titles.get(key, key)


def _settings_text():
	return (
		'⚙️ Настройки парсинга и инвайта\n'
		f'• Парсинг: постов={_setting_int("parser_posts_limit")}, комментариев={_setting_int("parser_comments_limit")}, '
		f'все аккаунты={"ВКЛ" if _setting_bool("parser_use_all_sessions") else "ВЫКЛ"}\n'
		f'• Инвайт: лимит={_setting_int("inviter_limit")}, пауза={_setting_int("inviter_sleep")}с, '
		f'на аккаунт={_setting_int("inviter_per_account_limit")}, flood={_setting_int("inviter_max_flood_wait")}с, '
		f'все аккаунты={"ВКЛ" if _setting_bool("inviter_use_all_sessions") else "ВЫКЛ"}'
	)


def _build_settings_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='✏️ Посты (парсинг)', callback_data='settings_edit|parser_posts_limit'),
		types.InlineKeyboardButton(text='✏️ Комментарии (парсинг)', callback_data='settings_edit|parser_comments_limit'),
	)
	keyboard.add(types.InlineKeyboardButton(text='🔁 Все аккаунты (парсинг)', callback_data='settings_toggle|parser_use_all_sessions'))
	keyboard.add(
		types.InlineKeyboardButton(text='✏️ Лимит (инвайт)', callback_data='settings_edit|inviter_limit'),
		types.InlineKeyboardButton(text='✏️ Пауза (инвайт)', callback_data='settings_edit|inviter_sleep'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='✏️ Лимит на аккаунт', callback_data='settings_edit|inviter_per_account_limit'),
		types.InlineKeyboardButton(text='✏️ Макс. FloodWait', callback_data='settings_edit|inviter_max_flood_wait'),
	)
	keyboard.add(types.InlineKeyboardButton(text='🔁 Все аккаунты (инвайт)', callback_data='settings_toggle|inviter_use_all_sessions'))
	keyboard.add(types.InlineKeyboardButton(text='♻️ Сброс по умолчанию', callback_data='settings_reset'))
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _build_manage_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(types.InlineKeyboardButton(text='🛑 Остановить задачу', callback_data='task_stop_menu'))
	keyboard.add(types.InlineKeyboardButton(text='🧹 Очистить завершенные', callback_data='task_clear_done'))
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _build_accounts_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='📄 Список аккаунтов', callback_data='accounts_list'),
		types.InlineKeyboardButton(text='🗑 Удалить аккаунт', callback_data='accounts_delete_menu'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _stats_text():
	try:
		connection = get_main_connection()
		q = connection.cursor()
		q.execute('SELECT COUNT(*) FROM parsed_usernames')
		parsed_users = q.fetchone()[0] or 0
		q.execute('SELECT COUNT(*) FROM parsed_comments')
		parsed_comments = q.fetchone()[0] or 0
		q.execute('SELECT status, COUNT(*) FROM invited_users GROUP BY status')
		rows = q.fetchall()
		status_map = {r[0]: r[1] for r in rows}
		connection.close()
		return (
			f'📈 Аналитика:\n'
			f'Юзернеймов в базе: {parsed_users}\n'
			f'Комментариев в базе: {parsed_comments}\n'
			f'Успешно добавлено: {status_map.get("invited", 0)}\n'
			f'Уже в чате/канале: {status_map.get("already", 0)}\n'
			f'Приватность (запрет): {status_map.get("privacy", 0)}\n'
			f'Flood/PeerFlood: {status_map.get("flood_wait", 0) + status_map.get("peer_flood", 0)}\n'
			f'Ошибки: {status_map.get("error", 0)}'
		)
	except Exception as e:
		return f'Не удалось собрать аналитику: {e}'


@bot.message_handler(commands=['start'])
def start_message(message):
	if message.chat.type == 'private':
		if not _is_admin(message.chat.id):
			_deny_access(message.chat.id)
			return
		userid = str(message.chat.id)
		username = str(message.from_user.username)
		connection = get_main_connection()
		q = connection.cursor()
		q.execute('SELECT * FROM ugc_users WHERE id = %s', (userid,))
		row = q.fetchall()
		if not row:
			q.execute('INSERT INTO ugc_users (id,data) VALUES (%s,%s)', (userid, 'Нет'))
			connection.commit()
			q.execute(
				'INSERT INTO invite (id, son_akk, time_vst, random_son, akk, chat_akk) VALUES (%s, %s, %s, %s, %s, %s)',
				(userid, 'Нет', 'Нет', 'Нет', 'Нет', 'Нет'),
			)
			connection.commit()
			if message.text[7:] != '':
				if message.text[7:] != userid:
					q.execute('UPDATE ugc_users SET ref = %s WHERE id = %s', (message.text[7:], userid))
					connection.commit()
					q.execute('UPDATE ugc_users SET ref_colvo = ref_colvo + 1 WHERE id = %s', (message.text[7:],))
					connection.commit()
					bot.send_message(message.text[7:], f'➕ Новый партнер: @{message.from_user.username}',reply_markup=keyboards.main)

			keyboard = types.InlineKeyboardMarkup()
			keyboard.add(types.InlineKeyboardButton(text=f'''💢 Как работает сервис ?!''',url=f'https://telegra.ph/Informaciya-po-proektu-10-29'))
			bot.send_message(message.chat.id,f'💡 Перед началом использования сервиса, пожалуйста, ознакомьтесь со статьей: https://telegra.ph/Informaciya-po-proektu-10-29',parse_mode='HTML',reply_markup=keyboard, disable_web_page_preview=True)

		bot.send_message(message.chat.id, '👑 Добро пожаловать в <b>Teddy Invite Pro</b>.', parse_mode='HTML', reply_markup=keyboards.main)
		bot.send_message(message.chat.id, '✨ Панель управления готова. Выберите действие ниже:', parse_mode='HTML', reply_markup=build_new_menu())

@bot.message_handler(content_types=['text'])
def send_text(message):
	if message.chat.type == 'private':
		if not _is_admin(message.chat.id):
			if message.text.lower() in ['/start', '🎛 меню', 'меню', 'menu']:
				_deny_access(message.chat.id)
			return
		if message.text.lower() == '/admin':
			if message.chat.id == admin:
				connection = get_main_connection()
				q = connection.cursor()
				q.execute('SELECT COUNT(id) FROM ugc_users')
				all_user_count = q.fetchone()[0]

				q.execute("SELECT COUNT(id) FROM ugc_users WHERE data != 'Нет'")
				all_user_podpiska = q.fetchone()[0]

				q.execute('SELECT COUNT(id) FROM akk')
				akkakk = q.fetchone()[0]

				q.execute('SELECT COUNT(id) FROM list_chat')
				chat = q.fetchone()[0]

				q.execute('SELECT COUNT(id) FROM logi')
				colvo_send_1 = q.fetchone()[0]

				q.execute('SELECT SUM(colvo_send) FROM list_chat')
				colvo_sends = q.fetchone()[0]

				q.execute("SELECT COUNT(id) FROM list_chat WHERE status = 'Send'")
				chat_no_send = q.fetchone()[0]


				keyboard = types.InlineKeyboardMarkup()
				keyboard.add(types.InlineKeyboardButton(text='Пользователи',callback_data=f'admin_search_user'),types.InlineKeyboardButton(text='Чаты',callback_data=f'admin_search_chat'))
				keyboard.add(types.InlineKeyboardButton(text='Рассылка',callback_data='send_sms_bot'),types.InlineKeyboardButton(text='Удалить аккаунт',callback_data='del_akkss'))
				keyboard.add(types.InlineKeyboardButton(text='Обновить время',callback_data='timeupdate'),types.InlineKeyboardButton(text='Перезагрузка',callback_data='restartsssss'))
				keyboard.add(types.InlineKeyboardButton(text='Реклама чата',callback_data='рекламачата'),types.InlineKeyboardButton(text='Смена прайса',callback_data='сменапрайса'))
				bot.send_message(message.chat.id, f'''▪️Всего пользователей: {all_user_count}
🦋Подписок {all_user_podpiska}
👥Аккаунтов: {akkakk}
💬Чатов: {chat} 
♾Отправлено: {colvo_sends}
✅Успешно: {colvo_send_1}
👣Очередь: {chat_no_send}''',parse_mode='HTML', reply_markup=keyboard)

		elif message.text.lower() == '🎛 меню':
			bot.send_message(message.chat.id, '✨ Выберите действие:', parse_mode='HTML', reply_markup=build_new_menu())
			return


@bot.message_handler(content_types=['document'])
def receive_session_file(message):
	if message.chat.type != 'private':
		return
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	doc = message.document
	if not doc or not str(doc.file_name).lower().endswith('.session'):
		bot.send_message(message.chat.id, 'Нужен файл формата .session (как документ).')
		return
	file_info = bot.get_file(doc.file_id)
	data = bot.download_file(file_info.file_path)
	filename = os.path.basename(doc.file_name)
	with open(filename, 'wb') as f:
		f.write(data)
	bot.send_message(message.chat.id, f'✅ Аккаунт добавлен: {filename}\nВсего аккаунтов: {len(list_sessions())}')


def parser_step_sources(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	USER_STATE[message.chat.id] = {'flow': 'parser', 'sources': message.text}
	default_posts = _setting_int('parser_posts_limit')
	msg = bot.send_message(
		message.chat.id,
		f'🔎 Парсинг • Шаг 2/3\nУкажи количество постов для анализа.\nТекущее значение по умолчанию: <b>{default_posts}</b>.',
		parse_mode='HTML',
		reply_markup=_step_keyboard()
	)
	bot.register_next_step_handler(msg, parser_step_posts)


def parser_step_posts(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		state['posts_limit'] = int(message.text.strip())
	except (TypeError, ValueError):
		state['posts_limit'] = _setting_int('parser_posts_limit')
	USER_STATE[message.chat.id] = state
	msg = bot.send_message(
		message.chat.id,
		f'🔎 Парсинг • Шаг 3/3\nУкажи лимит комментариев.\nТекущее значение по умолчанию: <b>{_setting_int("parser_comments_limit")}</b>.',
		parse_mode='HTML',
		reply_markup=_step_keyboard()
	)
	bot.register_next_step_handler(msg, parser_step_comments)


def parser_step_comments(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		comments_limit = int(message.text.strip())
	except (TypeError, ValueError):
		comments_limit = _setting_int('parser_comments_limit')
	file_name, sources_count = _save_targets_file(message.chat.id, state.get('sources', ''), 'parser_sources')
	if sources_count == 0:
		bot.send_message(message.chat.id, '⚠️ Не нашёл источники. Запусти парсинг заново.', reply_markup=build_new_menu())
		return
	command = [
		sys.executable, 'parser.py',
		'--targets-file', file_name,
		'--posts-limit', str(state.get('posts_limit', 100)),
		'--comments-limit', str(comments_limit)
	]
	if _setting_bool('parser_use_all_sessions'):
		command.append('--use-all-sessions')
	progress_file = _progress_file(message.chat.id, 'parser')
	command += ['--progress-file', progress_file]
	queue_pos = _enqueue_process(message.chat.id, f'parser ({sources_count} sources)', command, progress_file=progress_file)
	USER_STATE.pop(message.chat.id, None)
	bot.send_message(message.chat.id, f'✅ Парсинг добавлен в очередь.\nИсточников: {sources_count}\nПозиция: ~{queue_pos}', reply_markup=build_new_menu())


def inviter_step_sources(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	USER_STATE[message.chat.id] = {'flow': 'inviter', 'sources': message.text}
	msg = bot.send_message(
		message.chat.id,
		'📨 Инвайт • Шаг 2/4\nУкажи цель инвайта (@chat или @channel).',
		reply_markup=_step_keyboard()
	)
	bot.register_next_step_handler(msg, inviter_step_target)


def inviter_step_target(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	target = message.text.strip()
	if target == '':
		bot.send_message(message.chat.id, '⚠️ Цель пустая. Введи @chat или @channel.', reply_markup=_step_keyboard())
		return
	state['invite_target'] = target
	USER_STATE[message.chat.id] = state
	msg = bot.send_message(
		message.chat.id,
		f'📨 Инвайт • Шаг 3/4\nЛимит пользователей за запуск.\nПо умолчанию: <b>{_setting_int("inviter_limit")}</b>.',
		parse_mode='HTML',
		reply_markup=_step_keyboard()
	)
	bot.register_next_step_handler(msg, inviter_step_limit)


def inviter_step_limit(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		state['limit'] = int(message.text.strip())
	except (TypeError, ValueError):
		state['limit'] = _setting_int('inviter_limit')
	USER_STATE[message.chat.id] = state
	msg = bot.send_message(
		message.chat.id,
		f'📨 Инвайт • Шаг 4/4\nПауза между инвайтами (секунды).\nПо умолчанию: <b>{_setting_int("inviter_sleep")}</b>.',
		parse_mode='HTML',
		reply_markup=_step_keyboard()
	)
	bot.register_next_step_handler(msg, inviter_step_sleep)


def inviter_step_sleep(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		sleep_sec = int(message.text.strip())
	except (TypeError, ValueError):
		sleep_sec = _setting_int('inviter_sleep')
	file_name, sources_count = _save_targets_file(message.chat.id, state.get('sources', ''), 'inviter_sources')
	if sources_count == 0:
		bot.send_message(message.chat.id, '⚠️ Не нашёл источники. Запусти инвайт заново.', reply_markup=build_new_menu())
		return
	command = [
		sys.executable, 'inviter.py',
		'--sources-file', file_name,
		'--invite-target', state.get('invite_target', ''),
		'--limit', str(state.get('limit', _setting_int('inviter_limit'))),
		'--sleep', str(sleep_sec),
		'--per-account-limit', str(_setting_int('inviter_per_account_limit')),
		'--max-flood-wait', str(_setting_int('inviter_max_flood_wait'))
	]
	if _setting_bool('inviter_use_all_sessions'):
		command.append('--use-all-sessions')
	progress_file = _progress_file(message.chat.id, 'inviter')
	command += ['--progress-file', progress_file]
	queue_pos = _enqueue_process(message.chat.id, f'inviter ({sources_count} sources)', command, progress_file=progress_file)
	USER_STATE.pop(message.chat.id, None)
	bot.send_message(message.chat.id, f'✅ Инвайт добавлен в очередь.\nИсточников: {sources_count}\nПозиция: ~{queue_pos}', reply_markup=build_new_menu())


def settings_value_step(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Изменение настройки отменено.', reply_markup=_build_settings_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	key = state.get('settings_key')
	if not key:
		bot.send_message(message.chat.id, 'Сессия настройки потеряна. Открой настройки заново.', reply_markup=build_new_menu())
		return
	val = str(message.text).strip()
	if not val.isdigit():
		bot.send_message(message.chat.id, 'Нужно ввести целое число. Попробуйте ещё раз.', reply_markup=_step_keyboard())
		return
	_set_setting(key, val)
	USER_STATE.pop(message.chat.id, None)
	bot.send_message(
		message.chat.id,
		f'✅ Настройка обновлена\n<b>{_setting_title(key)}</b>: <b>{val}</b>',
		parse_mode='HTML',
		reply_markup=_build_settings_menu()
	)

@bot.callback_query_handler(func=lambda call:True)
def podcategors(call):
	if not _is_admin(call.from_user.id):
		try:
			bot.answer_callback_query(call.id, 'Нет доступа')
		except Exception:
			pass
		_deny_access(call.message.chat.id)
		return

	if call.data == 'cancel_flow':
		USER_STATE.pop(call.message.chat.id, None)
		bot.send_message(call.message.chat.id, '❌ Текущий сценарий отменен.', reply_markup=build_new_menu())
		return

	if call.data == 'main_menu':
		bot.send_message(call.message.chat.id, '✨ Главное меню:', reply_markup=build_new_menu())
		return

	if call.data == 'settings_menu':
		bot.send_message(call.message.chat.id, _settings_text(), reply_markup=_build_settings_menu())
		return

	if call.data == 'settings_reset':
		for k, v in DEFAULT_APP_SETTINGS.items():
			_set_setting(k, v)
		bot.send_message(call.message.chat.id, '♻️ Настройки сброшены к значениям по умолчанию.', reply_markup=_build_settings_menu())
		return

	if call.data.startswith('settings_toggle|'):
		key = call.data.split('|', 1)[1]
		cur = _setting_bool(key)
		_set_setting(key, '0' if cur else '1')
		bot.send_message(call.message.chat.id, f'✅ {_setting_title(key)}: {"ВЫКЛ" if cur else "ВКЛ"}', reply_markup=_build_settings_menu())
		return

	if call.data.startswith('settings_edit|'):
		key = call.data.split('|', 1)[1]
		USER_STATE[call.message.chat.id] = {'flow': 'settings', 'settings_key': key}
		msg = bot.send_message(
			call.message.chat.id,
			f'🛠 Введите новое числовое значение для:\n<b>{_setting_title(key)}</b>\n\nТекущее значение: <b>{_get_setting(key)}</b>',
			parse_mode='HTML',
			reply_markup=_step_keyboard()
		)
		bot.register_next_step_handler(msg, settings_value_step)
		return

	if call.data == 'accounts_menu':
		sessions = list_sessions()
		bot.send_message(
			call.message.chat.id,
			f'📂 Управление аккаунтами\nЗагружено аккаунтов: {len(sessions)}\n\nОтправь .session файл документом, чтобы добавить аккаунт.',
			reply_markup=_build_accounts_menu()
		)
		return

	if call.data == 'accounts_list':
		sessions = list_sessions()
		if len(sessions) == 0:
			bot.send_message(call.message.chat.id, 'Аккаунты не добавлены.')
			return
		text = '📄 Аккаунты:\n' + '\n'.join([f'{idx + 1}. {s}' for idx, s in enumerate(sessions)])
		bot.send_message(call.message.chat.id, text)
		return

	if call.data == 'accounts_delete_menu':
		sessions = list_sessions()
		if len(sessions) == 0:
			bot.send_message(call.message.chat.id, 'Удалять нечего: аккаунтов нет.')
			return
		keyboard = types.InlineKeyboardMarkup()
		for s in sessions[:20]:
			keyboard.add(types.InlineKeyboardButton(text=f'🗑 {s}', callback_data=f'del_session|{s}'))
		keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='accounts_menu'))
		bot.send_message(call.message.chat.id, 'Выберите аккаунт для удаления:', reply_markup=keyboard)
		return

	if call.data.startswith('del_session|'):
		filename = call.data.split('|', 1)[1]
		try:
			if os.path.exists(filename):
				os.remove(filename)
				bot.send_message(call.message.chat.id, f'Удален аккаунт: {filename}')
			else:
				bot.send_message(call.message.chat.id, 'Файл уже отсутствует.')
		except Exception as e:
			bot.send_message(call.message.chat.id, f'Ошибка удаления: {e}')
		return

	if call.data == 'parser_start':
		msg = bot.send_message(
			call.message.chat.id,
			'🔎 Парсинг\nШаг 1/3: отправь источники (через запятую или с новой строки).\n'
			f'Текущие настройки: постов={_setting_int("parser_posts_limit")}, комментариев={_setting_int("parser_comments_limit")}, '
			f'все аккаунты={"ВКЛ" if _setting_bool("parser_use_all_sessions") else "ВЫКЛ"}.\n'
			'Пример:\n@chat1\n@chat2',
			reply_markup=_step_keyboard()
		)
		bot.register_next_step_handler(msg, parser_step_sources)
		return

	if call.data == 'inviter_start':
		msg = bot.send_message(
			call.message.chat.id,
			'📨 Инвайт\nШаг 1/4: отправь источники, из которых брать пользователей.\n'
			f'Текущие настройки: лимит={_setting_int("inviter_limit")}, пауза={_setting_int("inviter_sleep")}с, '
			f'на аккаунт={_setting_int("inviter_per_account_limit")}, flood={_setting_int("inviter_max_flood_wait")}с, '
			f'все аккаунты={"ВКЛ" if _setting_bool("inviter_use_all_sessions") else "ВЫКЛ"}.',
			reply_markup=_step_keyboard()
		)
		bot.register_next_step_handler(msg, inviter_step_sources)
		return

	if call.data == 'task_status':
		items = RUNNING_TASKS.get(call.message.chat.id, [])
		if len(items) == 0:
			bot.send_message(call.message.chat.id, 'Сейчас активных задач нет ✅')
			return
		lines = []
		for idx, item in enumerate(items, start=1):
			status = item.get('status', 'queued')
			pid = item.get('pid')
			pid_text = f'PID {pid}' if pid else 'PID -'
			extra = ''
			pf = item.get('progress_file')
			if pf and os.path.exists(pf):
				try:
					with open(pf, 'r', encoding='utf-8') as f:
						p = json.load(f)
					if p.get('mode') == 'parser':
						extra = f' | parsed={p.get("users_parsed", 0)} users'
					elif p.get('mode') == 'inviter':
						extra = f' | invited={p.get("invited", 0)} processed={p.get("processed", 0)}'
				except Exception:
					extra = ''
			lines.append(f'{idx}. {item["title"]} - {status} - {pid_text}{extra}')
		lines.append(f'Очередь (глобально): {TASK_QUEUE.qsize()}')
		bot.send_message(call.message.chat.id, '📊 Статус задач:\n' + '\n'.join(lines))
		return

	if call.data == 'manage_menu':
		bot.send_message(call.message.chat.id, '⚙️ Управление задачами:', reply_markup=_build_manage_menu())
		return

	if call.data == 'task_stop_menu':
		items = RUNNING_TASKS.get(call.message.chat.id, [])
		running = [i for i in items if i.get('status') == 'running' and i.get('pid')]
		if len(running) == 0:
			bot.send_message(call.message.chat.id, 'Сейчас нет запущенных задач.')
			return
		keyboard = types.InlineKeyboardMarkup()
		for idx, item in enumerate(items):
			if item.get('status') == 'running' and item.get('pid'):
				keyboard.add(types.InlineKeyboardButton(text=f'🛑 {item["title"]} (PID {item["pid"]})', callback_data=f'task_stop|{idx}'))
		keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='manage_menu'))
		bot.send_message(call.message.chat.id, 'Выберите задачу для остановки:', reply_markup=keyboard)
		return

	if call.data.startswith('task_stop|'):
		try:
			idx = int(call.data.split('|', 1)[1])
		except Exception:
			bot.send_message(call.message.chat.id, 'Некорректный индекс задачи.')
			return
		items = RUNNING_TASKS.get(call.message.chat.id, [])
		if idx < 0 or idx >= len(items):
			bot.send_message(call.message.chat.id, 'Задача не найдена.')
			return
		item = items[idx]
		pid = item.get('pid')
		proc = item.get('proc')
		if not pid:
			bot.send_message(call.message.chat.id, 'Эта задача не запущена.')
			return
		try:
			if proc and proc.poll() is None:
				proc.terminate()
			item['status'] = 'stopped'
			bot.send_message(call.message.chat.id, f'Задача остановлена: {item["title"]}')
		except Exception as e:
			bot.send_message(call.message.chat.id, f'Ошибка остановки: {e}')
		return

	if call.data == 'task_clear_done':
		items = RUNNING_TASKS.get(call.message.chat.id, [])
		left = [i for i in items if i.get('status') in ['queued', 'running']]
		removed = len(items) - len(left)
		RUNNING_TASKS[call.message.chat.id] = left
		bot.send_message(call.message.chat.id, f'Очищено завершенных задач: {removed}')
		return

	if call.data == 'stats_overview':
		bot.send_message(call.message.chat.id, _stats_text())
		return

	if call.data == 'help_new':
		bot.send_message(
			call.message.chat.id,
			'🤝 Как пользоваться:\n1) Добавь .session аккаунты в разделе «Аккаунты».\n2) Запусти «Парсинг» и укажи источники.\n3) Запусти «Инвайт» и укажи цель.\n4) В «Управление» можно остановить задачу и очистить список.\n5) В «Аналитика» видно результаты.\n\nВо время выполнения бот отправляет живые апдейты прогресса.\nНа любом шаге нажми «❌ Отмена» или «🎛 Меню».'
		)
		return
#	if call.data == 'Multi':
#			bot.delete_message(chat_id=call.message.chat.id,message_id=call.message.message_id)
#			keyboard = types.InlineKeyboardMarkup()
#			keyboard.add(types.InlineKeyboardButton(text='Количество чатов на аккаунт',callback_data=f'настройка{1}'))
#			keyboard.add(types.InlineKeyboardButton(text='Сон между вступлением',callback_data=f'настройка{2}'))
#			keyboard.add(types.InlineKeyboardButton(text='Время сна между вступлениями',callback_data=f'настройка{3}'))
#			keyboard.add(types.InlineKeyboardButton(text='Смена прокси',callback_data=f'настройка{5}'))
#			keyboard.add(types.InlineKeyboardButton(text='Назад',callback_data=f'akks'))
#			bot.send_message(call.from_user.id,  f'''▪️Смена информации по всем чатам аккаунта:''',parse_mode='HTML', reply_markup=keyboard)

	if call.data == 'info':
		bot.send_message(call.from_user.id, f'''asdasdasdas''')
	
	if call.data == 'akks':
		bot.delete_message(chat_id=call.message.chat.id,message_id=call.message.message_id)
		connection = get_main_connection()
		q = connection.cursor()
		q.execute('SELECT * FROM akk where user_id = %s', (call.message.chat.id,))
		row = q.fetchall()
		keyboard = types.InlineKeyboardMarkup()
		for i in row:
			keyboard.add(types.InlineKeyboardButton(text=i[2],callback_data=f'список{i[0]}'))
		keyboard.add(types.InlineKeyboardButton(text='➕ Добавить аккаунт',callback_data=f'добавитьаккаунт'))
		keyboard.add(types.InlineKeyboardButton(text=f'''⬅️ Назад''',callback_data=f'Главное'))
		bot.send_message(call.from_user.id,  f'''◾️ Выберите нужный аккаунт или добавьте новый:''',parse_mode='HTML', reply_markup=keyboard)

	if call.data[:6] == 'список':
		bot.delete_message(chat_id=call.message.chat.id,message_id=call.message.message_id)
		connection = get_main_connection()
		q = connection.cursor()
		q.execute('SELECT data FROM ugc_users where id = %s', (call.message.chat.id,))
		datas = q.fetchone()[0]
		if str(datas) != str('Нет'):

			q.execute('UPDATE ugc_users SET akk = %s WHERE id = %s', (call.data[6:], call.message.chat.id))
			connection.commit()

			print(call.data[6:])

			q.execute('SELECT akk FROM ugc_users where id = %s', (call.message.chat.id,))
			akk_akk = q.fetchone()[0]
			print(akk_akk)
			
			q.execute('SELECT proxi FROM akk where id = %s', (akk_akk,))
			proxi = q.fetchone()[0]

			keyboard = types.InlineKeyboardMarkup()
			
			keyboard = types.InlineKeyboardMarkup()
			keyboard.add(types.InlineKeyboardButton(text='Количество чатов на аккаунт',callback_data=f'настройка{1}'),types.InlineKeyboardButton(text='Сон между вступлением',callback_data=f'настройка{2}'))
			keyboard.add(types.InlineKeyboardButton(text='Время сна между вступлениями',callback_data=f'настройка{3}'),types.InlineKeyboardButton(text='Рандомный сон',callback_data=f'настройка{4}'))
			keyboard.add(types.InlineKeyboardButton(text='Смена прокси',callback_data=f'настройка{5}'))
			keyboard.add(types.InlineKeyboardButton(text='Запустить инвайт',callback_data=f'настройка{9}'))
			keyboard.add(types.InlineKeyboardButton(text='Назад',callback_data=f'akks'))
			bot.send_message(call.message.chat.id, f'''🌐 Прокси: {proxi}''',parse_mode='HTML', reply_markup=keyboard)
		else:
			keyboard = types.InlineKeyboardMarkup()
			keyboard.add(types.InlineKeyboardButton(text=f'''⬅️ Назад''',callback_data=f'akks'),)
			bot.send_message(call.message.chat.id, f'''✖️ Подписка отсутствует''',parse_mode='HTML', reply_markup=keyboard)


	elif call.data[:9] == 'настройка':
			bot.delete_message(chat_id=call.message.chat.id,message_id=call.message.message_id)
			global tipsend
			tipsend = call.data[9:]
			print(tipsend)
			connection = get_main_connection()
			q = connection.cursor()
			if int(tipsend) == 1:
				msg= bot.send_message(call.message.chat.id, "Введи новое значение: (Можно использовать формат разметки 'html') | Не указывайте в тексте знаки ' ",parse_mode='HTML')
				bot.register_next_step_handler(msg, new_data)
			if int(tipsend) == 2:
				msg= bot.send_message(call.message.chat.id, "<b>Введи новое значение:</b>",parse_mode='HTML')
				bot.register_next_step_handler(msg, new_data)
			if int(tipsend) == 3:
				msg= bot.send_message(call.message.chat.id, "<b>Введи новое значение:</b>",parse_mode='HTML')
				bot.register_next_step_handler(msg, new_data)
			if int(tipsend) == 4:
				msg= bot.send_message(call.message.chat.id, "<b>Введите ссылку на фото:</b>",parse_mode='HTML')
				bot.register_next_step_handler(msg, new_data)
			if int(tipsend) == 5:
				msg= bot.send_message(call.message.chat.id, "<b>Введи новое значение:</b>",parse_mode='HTML')
				bot.register_next_step_handler(msg, new_data)
			if int(tipsend) == 6:
				msg= bot.send_message(call.message.chat.id, "<b>Введи новое значение:</b>",parse_mode='HTML')
				bot.register_next_step_handler(msg, new_data)

	elif call.data == 'Главное':
		bot.delete_message(chat_id=call.message.chat.id,message_id=call.message.message_id)
		keyboard = types.InlineKeyboardMarkup()
		keyboard = types.InlineKeyboardMarkup()
		keyboard.add(types.InlineKeyboardButton(text=f'''⏳ Автопостинг''',callback_data=f'akks'),types.InlineKeyboardButton(text=f'''💬 Автоответчик''',callback_data=f'Автоответчик'))
		keyboard.add(types.InlineKeyboardButton(text=f'''🖥 Профиль''',callback_data=f'profale'),types.InlineKeyboardButton(text=f'''📖 Информация''',callback_data=f'info'))
		bot.send_message(call.message.chat.id, f'''◾️ Выберите нужный пункт меню:''',parse_mode='HTML', reply_markup=keyboard)

def new_data(message):
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(types.InlineKeyboardButton(text=f'''⬅️ Назад''',callback_data=f'akks'))
	if message.text != '🎛 Меню':
		connection = get_main_connection()
		q = connection.cursor()
		q.execute('SELECT akk FROM ugc_users where id = %s', (message.chat.id,))
		chat_chat = q.fetchone()[0]
		q = connection.cursor()
		if int(tipsend) == 1:
			q.execute('UPDATE invite SET chat_akk = %s WHERE id = %s', (message.text, message.chat.id))
			connection.commit()
			bot.send_message(message.chat.id, "Успешно",parse_mode='HTML')
		if int(tipsend) == 2:
			q.execute('UPDATE invite SET son_akk = %s WHERE akk = %s', (message.text, chat_chat))
			connection.commit()
			bot.send_message(message.chat.id, "Успешно",parse_mode='HTML')
		if int(tipsend) == 3:
			q.execute('UPDATE invite SET time_vst = %s WHERE akk = %s', (message.text, chat_chat))
			connection.commit()
			bot.send_message(message.chat.id, "Успешно",parse_mode='HTML')
		if int(tipsend) == 4:
			q.execute('UPDATE invite SET random_son = %s WHERE akk = %s', (message.text, chat_chat))
			connection.commit()
			bot.send_message(message.chat.id, "Успешно",parse_mode='HTML')
		if int(tipsend) == 9:
			bot.send_message(message.chat.id, 'запущено',parse_mode='HTML')
	else:
		bot.send_message(message.chat.id, 'Отменили',parse_mode='HTML', reply_markup=keyboard)

bot.polling(True)
