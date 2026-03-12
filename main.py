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
from db import get_main_connection, init_db

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


def build_new_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='📂 Аккаунты (.session)', callback_data='accounts_menu'),
		types.InlineKeyboardButton(text='🔎 Парсинг', callback_data='parser_start'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='📨 Инвайт', callback_data='inviter_start'),
		types.InlineKeyboardButton(text='📊 Статус задач', callback_data='task_status'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='⚙️ Управление', callback_data='manage_menu'),
		types.InlineKeyboardButton(text='📈 Аналитика', callback_data='stats_overview'),
	)
	keyboard.add(types.InlineKeyboardButton(text='ℹ️ Помощь', callback_data='help_new'))
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


def _enqueue_process(user_id, title, command):
	_ensure_worker()
	item = {
		'user_id': user_id,
		'title': title,
		'command': command,
		'status': 'queued',
		'pid': None,
		'proc': None,
	}
	with TASK_LOCK:
		RUNNING_TASKS.setdefault(user_id, []).append(item)
	TASK_QUEUE.put(item)
	return TASK_QUEUE.qsize()


def _is_cancel(text):
	return str(text).strip().lower() in ['отмена', 'cancel', 'меню', 'menu']


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
			f'Invited: {status_map.get("invited", 0)}\n'
			f'Already: {status_map.get("already", 0)}\n'
			f'Privacy: {status_map.get("privacy", 0)}\n'
			f'Flood/PeerFlood: {status_map.get("flood_wait", 0) + status_map.get("peer_flood", 0)}\n'
			f'Errors: {status_map.get("error", 0)}'
		)
	except Exception as e:
		return f'Не удалось собрать аналитику: {e}'


@bot.message_handler(commands=['start'])
def start_message(message):
	if message.chat.type == 'private':
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

		bot.send_message(message.chat.id, '👑 Добро пожаловать! Управление через кнопки ниже.', parse_mode='HTML', reply_markup=keyboards.main)
		bot.send_message(message.chat.id, '◾️ Основное меню:', parse_mode='HTML', reply_markup=build_new_menu())

@bot.message_handler(content_types=['text'])
def send_text(message):
	if message.chat.type == 'private':
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
			bot.send_message(message.chat.id, '◾️ Выберите нужный пункт меню:', parse_mode='HTML', reply_markup=build_new_menu())
			return


@bot.message_handler(content_types=['document'])
def receive_session_file(message):
	if message.chat.type != 'private':
		return
	doc = message.document
	if not doc or not str(doc.file_name).lower().endswith('.session'):
		bot.send_message(message.chat.id, 'Пришлите файл формата .session')
		return
	file_info = bot.get_file(doc.file_id)
	data = bot.download_file(file_info.file_path)
	filename = os.path.basename(doc.file_name)
	with open(filename, 'wb') as f:
		f.write(data)
	bot.send_message(message.chat.id, f'Аккаунт добавлен: {filename}\nВсего аккаунтов: {len(list_sessions())}')


def parser_step_sources(message):
	if _is_cancel(message.text):
		bot.send_message(message.chat.id, 'Отменено.', reply_markup=build_new_menu())
		return
	USER_STATE[message.chat.id] = {'flow': 'parser', 'sources': message.text}
	msg = bot.send_message(message.chat.id, 'Введите posts-limit (пример: 100):')
	bot.register_next_step_handler(msg, parser_step_posts)


def parser_step_posts(message):
	if _is_cancel(message.text):
		bot.send_message(message.chat.id, 'Отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		state['posts_limit'] = int(message.text.strip())
	except (TypeError, ValueError):
		state['posts_limit'] = 100
	USER_STATE[message.chat.id] = state
	msg = bot.send_message(message.chat.id, 'Введите comments-limit (пример: 200):')
	bot.register_next_step_handler(msg, parser_step_comments)


def parser_step_comments(message):
	if _is_cancel(message.text):
		bot.send_message(message.chat.id, 'Отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		comments_limit = int(message.text.strip())
	except (TypeError, ValueError):
		comments_limit = 200
	file_name, sources_count = _save_targets_file(message.chat.id, state.get('sources', ''), 'parser_sources')
	if sources_count == 0:
		bot.send_message(message.chat.id, 'Не найдено источников. Запустите заново.', reply_markup=build_new_menu())
		return
	command = [
		sys.executable, 'parser.py',
		'--targets-file', file_name,
		'--posts-limit', str(state.get('posts_limit', 100)),
		'--comments-limit', str(comments_limit),
		'--use-all-sessions'
	]
	queue_pos = _enqueue_process(message.chat.id, f'parser ({sources_count} sources)', command)
	bot.send_message(message.chat.id, f'Парсинг добавлен в очередь. Источников: {sources_count}. Позиция в очереди: ~{queue_pos}')


def inviter_step_sources(message):
	if _is_cancel(message.text):
		bot.send_message(message.chat.id, 'Отменено.', reply_markup=build_new_menu())
		return
	USER_STATE[message.chat.id] = {'flow': 'inviter', 'sources': message.text}
	msg = bot.send_message(message.chat.id, 'Введите @username целевого чата/канала для инвайта:')
	bot.register_next_step_handler(msg, inviter_step_target)


def inviter_step_target(message):
	if _is_cancel(message.text):
		bot.send_message(message.chat.id, 'Отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	target = message.text.strip()
	if target == '':
		bot.send_message(message.chat.id, 'Цель инвайта пустая, попробуйте снова.')
		return
	state['invite_target'] = target
	USER_STATE[message.chat.id] = state
	msg = bot.send_message(message.chat.id, 'Введите лимит пользователей за запуск (пример: 100):')
	bot.register_next_step_handler(msg, inviter_step_limit)


def inviter_step_limit(message):
	if _is_cancel(message.text):
		bot.send_message(message.chat.id, 'Отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		state['limit'] = int(message.text.strip())
	except (TypeError, ValueError):
		state['limit'] = 100
	USER_STATE[message.chat.id] = state
	msg = bot.send_message(message.chat.id, 'Введите паузу между инвайтами в секундах (пример: 15):')
	bot.register_next_step_handler(msg, inviter_step_sleep)


def inviter_step_sleep(message):
	if _is_cancel(message.text):
		bot.send_message(message.chat.id, 'Отменено.', reply_markup=build_new_menu())
		return
	state = USER_STATE.get(message.chat.id, {})
	try:
		sleep_sec = int(message.text.strip())
	except (TypeError, ValueError):
		sleep_sec = 15
	file_name, sources_count = _save_targets_file(message.chat.id, state.get('sources', ''), 'inviter_sources')
	if sources_count == 0:
		bot.send_message(message.chat.id, 'Не найдено источников. Запустите заново.', reply_markup=build_new_menu())
		return
	command = [
		sys.executable, 'inviter.py',
		'--sources-file', file_name,
		'--invite-target', state.get('invite_target', ''),
		'--limit', str(state.get('limit', 100)),
		'--sleep', str(sleep_sec),
		'--per-account-limit', str(config.invite_per_account_limit),
		'--max-flood-wait', str(config.invite_max_flood_wait),
		'--use-all-sessions'
	]
	queue_pos = _enqueue_process(message.chat.id, f'inviter ({sources_count} sources)', command)
	bot.send_message(message.chat.id, f'Инвайт добавлен в очередь. Источников: {sources_count}. Позиция в очереди: ~{queue_pos}')

@bot.callback_query_handler(func=lambda call:True)
def podcategors(call):
	if call.data == 'main_menu':
		bot.send_message(call.message.chat.id, '◾️ Основное меню:', reply_markup=build_new_menu())
		return

	if call.data == 'accounts_menu':
		sessions = list_sessions()
		bot.send_message(
			call.message.chat.id,
			f'Аккаунтов загружено: {len(sessions)}\nПришлите .session файлы сюда документом.',
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
			'Отправьте источники для парса (через запятую или с новой строки).\nПример:\n@chat1\n@chat2'
		)
		bot.register_next_step_handler(msg, parser_step_sources)
		return

	if call.data == 'inviter_start':
		msg = bot.send_message(
			call.message.chat.id,
			'Отправьте источники, из которых брать пользователей для инвайта (через запятую или с новой строки).'
		)
		bot.register_next_step_handler(msg, inviter_step_sources)
		return

	if call.data == 'task_status':
		items = RUNNING_TASKS.get(call.message.chat.id, [])
		if len(items) == 0:
			bot.send_message(call.message.chat.id, 'Активных задач нет.')
			return
		lines = []
		for idx, item in enumerate(items, start=1):
			status = item.get('status', 'queued')
			pid = item.get('pid')
			pid_text = f'PID {pid}' if pid else 'PID -'
			lines.append(f'{idx}. {item["title"]} - {status} - {pid_text}')
		lines.append(f'Очередь (глобально): {TASK_QUEUE.qsize()}')
		bot.send_message(call.message.chat.id, '\n'.join(lines))
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
			'Как пользоваться:\n1) Загрузите .session аккаунты.\n2) "Парсинг" — источники + лимиты.\n3) "Инвайт" — источники + цель.\n4) "Управление" — стоп/очистка задач.\n5) "Аналитика" — результаты.\nНа шаге ввода можно написать: Отмена.'
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
