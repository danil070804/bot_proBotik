from decimal import *
import telebot
import datetime
from telebot import types, apihelper
from telebot.apihelper import ApiTelegramException
import sqlite3
import random, string
import time
import os,random,shutil,subprocess
import sys
import glob
import threading
from queue import Queue
import json
import asyncio
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
from db import (
	get_main_connection, init_db, get_app_setting, set_app_setting,
	add_source_filter, remove_source_filter, get_source_filters,
	add_user_filter, remove_user_filter, get_user_filters,
	get_account_health, set_account_health, set_account_warmup, get_account_warmup_remaining,
	save_session_file, delete_session_file, get_session_files
)
from telethon.errors import UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from functions import get_proxy, get_sessions, build_telegram_client

from pyqiwip2p import QiwiP2P
from pyqiwip2p.p2p_types import QiwiCustomer, QiwiDatetime


TOKEN = config.bot_invite_token
bot = telebot.TeleBot(TOKEN)
admin = config.admin
ADMINS = set(getattr(config, 'admins', [admin]))
init_db()


def _restore_session_files():
	try:
		for name, content in get_session_files():
			if not str(name).lower().endswith(('.session', '.json')):
				continue
			with open(name, 'wb') as f:
				f.write(content)
	except Exception:
		pass


_restore_session_files()
USER_STATE = {}
RUNNING_TASKS = {}
TASK_QUEUE = Queue()
TASK_LOCK = threading.Lock()
UPLOAD_QUEUE = Queue()
DEFAULT_APP_SETTINGS = {
	'parser_posts_limit': '100',
	'parser_comments_limit': '200',
	'parser_use_all_sessions': '1',
	'inviter_limit': '100',
	'inviter_sleep': '15',
	'inviter_per_account_limit': str(config.invite_per_account_limit),
	'inviter_max_flood_wait': str(config.invite_max_flood_wait),
	'inviter_use_all_sessions': '1',
	'account_warmup_days': '2',
	'active_preset': 'standard',
}
PRESET_CONFIGS = {
	'super_safe': {
		'parser_posts_limit': '40',
		'parser_comments_limit': '60',
		'inviter_limit': '15',
		'inviter_sleep': '45',
		'inviter_per_account_limit': '5',
		'inviter_max_flood_wait': '1800',
		'parser_use_all_sessions': '1',
		'inviter_use_all_sessions': '1',
		'account_warmup_days': '3',
		'active_preset': 'super_safe',
	},
	'soft': {
		'parser_posts_limit': '80',
		'parser_comments_limit': '120',
		'inviter_limit': '40',
		'inviter_sleep': '25',
		'inviter_per_account_limit': '10',
		'inviter_max_flood_wait': '300',
		'parser_use_all_sessions': '1',
		'inviter_use_all_sessions': '1',
		'account_warmup_days': '2',
		'active_preset': 'soft',
	},
	'standard': {
		'parser_posts_limit': '100',
		'parser_comments_limit': '200',
		'inviter_limit': '100',
		'inviter_sleep': '15',
		'inviter_per_account_limit': str(config.invite_per_account_limit),
		'inviter_max_flood_wait': str(config.invite_max_flood_wait),
		'parser_use_all_sessions': '1',
		'inviter_use_all_sessions': '1',
		'account_warmup_days': '2',
		'active_preset': 'standard',
	},
	'aggressive': {
		'parser_posts_limit': '200',
		'parser_comments_limit': '350',
		'inviter_limit': '180',
		'inviter_sleep': '8',
		'inviter_per_account_limit': '45',
		'inviter_max_flood_wait': '900',
		'parser_use_all_sessions': '1',
		'inviter_use_all_sessions': '1',
		'account_warmup_days': '1',
		'active_preset': 'aggressive',
	},
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
	keyboard.add(types.InlineKeyboardButton(text='🛡 Фильтры • White/Black', callback_data='filters_menu'))
	keyboard.add(types.InlineKeyboardButton(text='🛠 Настройки • Парсинг/Инвайт', callback_data='settings_menu'))
	keyboard.add(types.InlineKeyboardButton(text='ℹ️ Помощь • Гайд', callback_data='help_new'))
	return keyboard


def list_sessions():
	return get_sessions()


def _account_status_emoji(status):
	return {
		'active': '🟢',
		'limited': '🟠',
		'dead': '🔴',
		'unknown': '⚪',
	}.get(status, '⚪')


def _account_status_title(status):
	return {
		'active': 'АКТИВЕН',
		'limited': 'ОГРАНИЧЕН',
		'dead': 'НЕРАБОЧИЙ',
		'unknown': 'НЕИЗВЕСТНО',
	}.get(status, 'НЕИЗВЕСТНО')


def _fmt_seconds_ru(total_seconds):
	try:
		sec = max(0, int(total_seconds))
	except Exception:
		sec = 0
	days = sec // 86400
	hours = (sec % 86400) // 3600
	minutes = (sec % 3600) // 60
	parts = []
	if days:
		parts.append(f'{days}д')
	if hours:
		parts.append(f'{hours}ч')
	if minutes or not parts:
		parts.append(f'{minutes}м')
	return ' '.join(parts)


def _health_details_ru(details):
	text = str(details or '').strip()
	if text == '':
		return '-'
	if 'Session not authorized' in text:
		return 'Сессия не авторизована'
	if 'TG_API_ID/TG_API_HASH not configured' in text:
		return 'Не настроены API ID / API HASH'
	if 'Authorization ok' in text:
		return 'Авторизация успешна'
	if 'Cannot get entity from a channel' in text:
		return 'Нет доступа к тестовому чату: аккаунт не состоит в нём'
	if 'Не удалось вступить в тестовый чат' in text:
		return text
	if 'Ограничения обнаружены через @SpamBot' in text:
		return text
	if 'Ограничений через @SpamBot не обнаружено' in text:
		return text
	if 'PeerFloodError' in text:
		return 'Спам-ограничение Telegram (PeerFlood)'
	if 'FloodWaitError' in text:
		return text.replace('FloodWaitError', 'Ожидание из-за лимитов')
	if 'AuthKeyUnregisteredError' in text:
		return 'Сессия недействительна (ключ авторизации удалён)'
	if 'SessionRevokedError' in text:
		return 'Сессия отозвана в Telegram'
	if 'UserDeactivatedError' in text or 'UserDeactivatedBanError' in text:
		return 'Аккаунт деактивирован или заблокирован'
	if 'PhoneNumberBannedError' in text:
		return 'Номер аккаунта заблокирован Telegram'
	return text


def _invite_hash_from_link(link):
	s = str(link or '').strip()
	if 'joinchat/' in s:
		return s.split('joinchat/', 1)[1].strip().strip('/')
	if 't.me/+' in s:
		return s.split('t.me/+', 1)[1].strip().strip('/')
	if s.startswith('+'):
		return s[1:].strip()
	return ''


async def _join_test_chat_if_needed(client):
	target = str(config.account_check_chat or '').strip()
	if not target:
		return
	join_hash = _invite_hash_from_link(target)
	try:
		if join_hash:
			await client(ImportChatInviteRequest(join_hash))
			return
		entity = await client.get_entity(target)
		try:
			await client(JoinChannelRequest(entity))
		except UserAlreadyParticipantError:
			return
	except UserAlreadyParticipantError:
		return
	except Exception as e:
		raise RuntimeError(f'Не удалось вступить в тестовый чат: {e}')


async def _probe_spam_block(client):
	try:
		spam_bot = await client.get_entity('SpamBot')
		await client.send_message(spam_bot, '/start')
		await asyncio.sleep(1)
		msgs = await client.get_messages(spam_bot, limit=1)
		text = (msgs[0].message or '') if msgs else ''
		lower = text.lower()
		bad = ['limited', 'огранич', 'cannot', "can't", 'спам', 'spam']
		ok = ['no limits', 'good news', 'нет ограничений']
		if any(x in lower for x in ok):
			return 'active', 'Ограничений через @SpamBot не обнаружено'
		if any(x in lower for x in bad):
			return 'limited', 'Ограничения обнаружены через @SpamBot'
		return 'active', 'Базовая проверка пройдена'
	except Exception:
		return 'active', 'Базовая проверка пройдена'


def _detect_health_status_by_error(exc):
	name = exc.__class__.__name__
	text = _health_details_ru(f'{name}: {exc}')
	if 'JSON не содержит StringSession' in text:
		return 'dead', text
	dead_errors = {
		'AuthKeyUnregisteredError',
		'SessionRevokedError',
		'UserDeactivatedError',
		'UserDeactivatedBanError',
		'PhoneNumberBannedError',
	}
	limited_errors = {
		'PeerFloodError',
		'FloodWaitError',
	}
	if name in dead_errors:
		return 'dead', text
	if name in limited_errors:
		return 'limited', text
	return 'limited', text


def _check_account_health(session, deep_check=False):
	async def _run():
		client = None
		try:
			client = build_telegram_client(session, config.API_ID, config.API_HASH, proxy=get_proxy())
			await client.connect()
			if not await client.is_user_authorized():
				return 'dead', 'Сессия не авторизована'
			me = await client.get_me()
			if deep_check:
				await _join_test_chat_if_needed(client)
				probe_status, probe_reason = await _probe_spam_block(client)
				if probe_status == 'limited':
					return 'limited', probe_reason
			user_label = f'@{me.username}' if getattr(me, 'username', None) else f'id={getattr(me, "id", "n/a")}'
			return 'active', f'Аккаунт рабочий: {user_label}'
		except Exception as e:
			return _detect_health_status_by_error(e)
		finally:
			if client:
				try:
					await client.disconnect()
				except Exception:
					pass

	if not config.API_ID or not config.API_HASH:
		return 'limited', 'Не настроены API ID / API HASH'
	return asyncio.run(_run())


def _notify_health_change_if_needed(session, prev_status, new_status, details):
	if prev_status == new_status:
		return
	if new_status not in ('dead', 'limited', 'active'):
		return
	if len(ADMINS) == 0:
		return
	text = (
		f'🔔 <b>Статус аккаунта изменился</b>\n'
		f'• Аккаунт: <code>{session}</code>\n'
		f'• Статус: {_account_status_emoji(new_status)} <b>{_account_status_title(new_status)}</b>\n'
		f'• Детали: <code>{_health_details_ru(details)[:500]}</code>'
	)
	for admin_id in sorted(ADMINS):
		try:
			bot.send_message(admin_id, text, parse_mode='HTML')
		except Exception:
			pass


def _process_uploaded_session(chat_id, filename):
	status, details = _check_account_health(filename, deep_check=False)
	prev = set_account_health(filename, status, details)
	_notify_health_change_if_needed(filename, prev, status, details)
	if status == 'dead':
		try:
			if os.path.exists(filename):
				os.remove(filename)
		except Exception:
			pass
		delete_session_file(filename)
		bot.send_message(
			chat_id,
			f'🗑 <b>Аккаунт удалён автоматически</b>\n'
			f'Файл: <code>{filename}</code>\n'
			f'Причина: <code>{_health_details_ru(details)[:300]}</code>',
			parse_mode='HTML'
		)
		return
	if status == 'active':
		warmup_days = max(0, _setting_int('account_warmup_days'))
		set_account_warmup(filename, warmup_days * 86400)
		warmup_text = f'\nПрогрев до инвайта: <b>{warmup_days} дн.</b>' if warmup_days > 0 else ''
	else:
		warmup_text = ''
	bot.send_message(
		chat_id,
		f'✅ Аккаунт добавлен: <code>{filename}</code>\n'
		f'Статус: {_account_status_emoji(status)} <b>{_account_status_title(status)}</b>\n'
		f'Детали: <code>{_health_details_ru(details)[:300]}</code>\n'
		f'Всего аккаунтов: <b>{len(list_sessions())}</b>{warmup_text}',
		parse_mode='HTML'
	)


def _upload_worker():
	while True:
		item = UPLOAD_QUEUE.get()
		try:
			_process_uploaded_session(item['chat_id'], item['filename'])
		finally:
			UPLOAD_QUEUE.task_done()


def _ensure_upload_worker():
	if getattr(_ensure_upload_worker, 'started', False):
		return
	t = threading.Thread(target=_upload_worker, daemon=True)
	t.start()
	_ensure_upload_worker.started = True


def _enqueue_uploaded_session(chat_id, filename):
	_ensure_upload_worker()
	UPLOAD_QUEUE.put({'chat_id': chat_id, 'filename': filename})
	return UPLOAD_QUEUE.qsize()


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
			f'Аккаунт: {progress.get("active_session", "-")}\n'
			f'Источники: {progress.get("sources_done", 0)}/{progress.get("sources_total", 0)}\n'
			f'Кандидатов: {progress.get("total_candidates", 0)}\n'
			f'Обработано: {progress.get("processed", 0)}\n'
			f'✅ Добавлено: {progress.get("invited", 0)} | ⛔ privacy: {progress.get("privacy", 0)}\n'
			f'⚠️ already: {progress.get("already", 0)} | errors: {progress.get("error", 0)}\n'
			f'🛡 фильтр источников: {progress.get("filtered_sources", 0)} | фильтр users: {progress.get("filtered_users", 0)}'
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
						msg_id = item.get('progress_msg_id')
						item['progress_msg_id'] = _render_inline(item['user_id'], msg_id, text, parse_mode=None)
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
			msg = bot.send_message(item['user_id'], f'▶️ Запущено: {item["title"]} (PID {proc.pid})')
			item['progress_msg_id'] = msg.message_id
		except Exception:
			pass
		_start_progress_monitor(item)
		code = proc.wait()
		with TASK_LOCK:
			item['status'] = f'finished ({code})'
		try:
			msg_id = item.get('progress_msg_id')
			_render_inline(item['user_id'], msg_id, f'✅ Завершено: {item["title"]}\nКод: {code}', parse_mode=None)
		except Exception:
			pass
		TASK_QUEUE.task_done()


def _ensure_worker():
	if getattr(_ensure_worker, 'started', False):
		return
	t = threading.Thread(target=_queue_worker, daemon=True)
	t.start()
	_ensure_worker.started = True


def _account_health_monitor():
	while True:
		try:
			for session in list_sessions():
				status, details = _check_account_health(session)
				prev = set_account_health(session, status, details)
				_notify_health_change_if_needed(session, prev, status, details)
		except Exception:
			pass
		time.sleep(max(60, int(config.account_monitor_interval)))


def _ensure_account_monitor():
	if getattr(_ensure_account_monitor, 'started', False):
		return
	t = threading.Thread(target=_account_health_monitor, daemon=True)
	t.start()
	_ensure_account_monitor.started = True


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
		'progress_msg_id': None,
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
		return int(user_id) in ADMINS
	except Exception:
		return False


def _deny_access(chat_id):
	bot.send_message(chat_id, '⛔ Доступ закрыт. Этим ботом может пользоваться только администратор.')


def _handle_filter_command(message):
	text = str(message.text or '').strip()
	parts = text.split(maxsplit=1)
	if len(parts) != 2:
		return False
	cmd = parts[0].lower()
	value = parts[1].strip()
	commands = {
		'+srcwl': ('source', 'whitelist', 'add'),
		'-srcwl': ('source', 'whitelist', 'remove'),
		'+srcbl': ('source', 'blacklist', 'add'),
		'-srcbl': ('source', 'blacklist', 'remove'),
		'+usrwl': ('user', 'whitelist', 'add'),
		'-usrwl': ('user', 'whitelist', 'remove'),
		'+usrbl': ('user', 'blacklist', 'add'),
		'-usrbl': ('user', 'blacklist', 'remove'),
	}
	if cmd not in commands:
		return False
	target_type, mode, action = commands[cmd]
	if target_type == 'source':
		if action == 'add':
			add_source_filter(mode, value)
		else:
			remove_source_filter(mode, value)
	else:
		if action == 'add':
			add_user_filter(mode, value)
		else:
			remove_user_filter(mode, value)
	bot.send_message(
		message.chat.id,
		f'✅ Фильтр обновлен: <code>{cmd} {value}</code>\n\n' + _filters_text(),
		parse_mode='HTML',
		reply_markup=build_new_menu()
	)
	return True


def _render_inline(chat_id, message_id, text, reply_markup=None, parse_mode='HTML'):
	try:
		bot.edit_message_text(
			chat_id=chat_id,
			message_id=message_id,
			text=text,
			reply_markup=reply_markup,
			parse_mode=parse_mode
		)
		return message_id
	except Exception:
		msg = bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
		return msg.message_id


def _send_html_chunks(chat_id, header, lines, chunk_size=3500):
	base = str(header or '')
	items = list(lines or [])
	current = base
	for line in items:
		add = ('\n' if current else '') + line
		if len(current) + len(add) > chunk_size:
			bot.send_message(chat_id, current, parse_mode='HTML')
			current = base + '\n' + line if base else line
		else:
			current += add
	if current.strip():
		bot.send_message(chat_id, current, parse_mode='HTML')


def _guide_text():
	return (
		'📘 <b>Гайд по функциям Teddy Invite Pro</b>\n\n'
		'🧩 <b>Аккаунты • Session</b>\n'
		'• Загружай <code>.session</code> или <code>.json</code> как документ.\n'
		'• Сразу после загрузки бот делает health-check через Telethon:\n'
		'  1) подключение к сессии,\n'
		'  2) проверка авторизации (<code>is_user_authorized()</code>),\n'
		'  3) получение профиля (<code>get_me()</code>),\n'
		'  4) при включенной проверке тест-чата — проверка доступа к тест-чату.\n'
		'• Статусы аккаунта:\n'
		'  • 🟢 <b>ACTIVE</b> — сессия валидна и готова к работе;\n'
		'  • 🟠 <b>LIMITED</b> — есть ограничения (Flood/PeerFlood/временные ошибки);\n'
		'  • 🔴 <b>DEAD</b> — сессия невалидна/разлогин/бан/ревок.\n'
		'• После успешной загрузки аккаунт ставится на прогрев и не участвует в инвайте до окончания таймера.\n'
		'• Все статусы хранятся в БД (таблица <code>account_health</code>), есть ручная кнопка «Проверить аккаунты» и фоновый монитор.\n'
		'• При смене статуса бот отправляет уведомление админу.\n\n'
		'🔎 <b>Парсинг • Аудитория</b>\n'
		'• Сбор пользователей из участников и комментариев.\n'
		'• Работает по одному или нескольким источникам.\n'
		'• Прогресс показывается в live-режиме без спама.\n\n'
		'📨 <b>Инвайт • Добавление</b>\n'
		'• Добавляет пользователей из общей базы парсинга (без повторов по цели).\n'
		'• Результат по каждому юзеру сохраняется: invited/already/privacy/flood_wait/peer_flood/error.\n'
		'• При проблемах аккаунта во время инвайта его статус в health автоматически обновляется.\n'
		'• Показывает live-отчет: сколько обработано и добавлено.\n\n'
		'🛠 <b>Настройки • Парсинг/Инвайт</b>\n'
		'• Тонкая настройка лимитов и пауз.\n'
		'• Пресеты: Мягкий / Стандарт / Агрессивный.\n'
		'• Все значения сохраняются в базе.\n\n'
		'⚙️ <b>Управление • Процессы</b>\n'
		'• Остановить задачу.\n'
		'• Очистить завершенные задачи.\n\n'
		'📊 <b>Статус • Live</b>\n'
		'• Очередь, PID, и текущий прогресс задач.\n\n'
		'📈 <b>Аналитика • Отчёты</b>\n'
		'• Сводка по парсингу и результатам инвайта.\n\n'
		'🛡 <b>Фильтры • White/Black</b>\n'
		'• Ограничивают источники и пользователей для безопасности.\n'
		'• Работают и в парсинге, и в инвайте.\n\n'
		'💡 <b>Подсказка:</b> на любом шаге нажми «❌ Отменить сценарий» или «⬅️ Назад в меню».'
	)


def _flow_title(flow):
	return {
		'parser': 'Парсинг',
		'inviter': 'Инвайт',
		'settings': 'Настройки',
	}.get(flow, 'Сценарий')


def _has_active_flow(user_id):
	state = USER_STATE.get(user_id, {})
	return bool(state.get('flow'))


def _ensure_sessions_or_warn(chat_id, message_id=None):
	count = len(list_sessions())
	if count > 0:
		return True
	text = (
		'⛔ <b>Невозможно запустить сценарий</b>\n'
		'Сначала добавьте хотя бы <b>1 .session</b> аккаунт в разделе «Аккаунты • Session».'
	)
	if message_id:
		_render_inline(chat_id, message_id, text, parse_mode='HTML', reply_markup=build_new_menu())
	else:
		bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=build_new_menu())
	return False


def _prompt_step(chat_id, state, text, handler):
	panel_id = state.get('panel_msg_id')
	state['panel_msg_id'] = _render_inline(chat_id, panel_id, text, parse_mode='HTML', reply_markup=_step_keyboard())
	USER_STATE[chat_id] = state
	if hasattr(bot, 'register_next_step_handler_by_chat_id'):
		bot.register_next_step_handler_by_chat_id(chat_id, handler)
	else:
		msg = bot.send_message(chat_id, '✍️ Введи ответ следующим сообщением:', reply_markup=_step_keyboard())
		bot.register_next_step_handler(msg, handler)


def _get_setting(key):
	return get_app_setting(key, DEFAULT_APP_SETTINGS.get(key, ''))


def _filters_text():
	src_wl, src_bl = get_source_filters()
	usr_wl, usr_bl = get_user_filters()
	return (
		'🛡 <b>Фильтры источников и пользователей</b>\n\n'
		f'Источники • whitelist: <b>{len(src_wl)}</b> | blacklist: <b>{len(src_bl)}</b>\n'
		f'Пользователи • whitelist: <b>{len(usr_wl)}</b> | blacklist: <b>{len(usr_bl)}</b>\n\n'
		'Формат команд:\n'
		'<code>+srcwl @chat</code> / <code>-srcwl @chat</code>\n'
		'<code>+srcbl @chat</code> / <code>-srcbl @chat</code>\n'
		'<code>+usrwl @username</code> / <code>-usrwl @username</code>\n'
		'<code>+usrbl @username</code> / <code>-usrbl @username</code>\n\n'
		'После изменения фильтры применяются сразу.'
	)


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
		'account_warmup_days': 'Дней прогрева после загрузки',
	}
	return titles.get(key, key)


def _preset_title(name):
	return {
		'super_safe': 'Супер-щадящий',
		'soft': 'Мягкий',
		'standard': 'Стандарт',
		'aggressive': 'Агрессивный',
	}.get(name, name)


def _apply_preset(name):
	data = PRESET_CONFIGS.get(name)
	if not data:
		return False
	for k, v in data.items():
		_set_setting(k, v)
	return True


def _settings_text():
	return (
		'⚙️ Настройки парсинга и инвайта\n'
		f'• Активный пресет: <b>{_preset_title(_get_setting("active_preset"))}</b>\n'
		f'• Парсинг: постов={_setting_int("parser_posts_limit")}, комментариев={_setting_int("parser_comments_limit")}, '
		f'все аккаунты={"ВКЛ" if _setting_bool("parser_use_all_sessions") else "ВЫКЛ"}\n'
		f'• Инвайт: лимит={_setting_int("inviter_limit")}, пауза={_setting_int("inviter_sleep")}с, '
		f'на аккаунт={_setting_int("inviter_per_account_limit")}, flood={_setting_int("inviter_max_flood_wait")}с, '
		f'все аккаунты={"ВКЛ" if _setting_bool("inviter_use_all_sessions") else "ВЫКЛ"}\n'
		f'• Прогрев аккаунтов: {_setting_int("account_warmup_days")} дн.'
	)


def _build_settings_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(types.InlineKeyboardButton(text='🛡 Пресет: Супер-щадящий', callback_data='settings_preset|super_safe'))
	keyboard.add(
		types.InlineKeyboardButton(text='🟢 Пресет: Мягкий', callback_data='settings_preset|soft'),
		types.InlineKeyboardButton(text='🟡 Пресет: Стандарт', callback_data='settings_preset|standard'),
	)
	keyboard.add(types.InlineKeyboardButton(text='🔴 Пресет: Агрессивный', callback_data='settings_preset|aggressive'))
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
	keyboard.add(types.InlineKeyboardButton(text='🕒 Дни прогрева аккаунтов', callback_data='settings_edit|account_warmup_days'))
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
		types.InlineKeyboardButton(text='🔍 Проверить аккаунты', callback_data='accounts_check_all'),
	)
	keyboard.add(
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
			if _is_admin(message.chat.id):
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
		elif message.text.lower() in ['фильтры', 'filters']:
			bot.send_message(message.chat.id, _filters_text(), parse_mode='HTML', reply_markup=build_new_menu())
			return
		elif _handle_filter_command(message):
			return


@bot.message_handler(content_types=['document'])
def receive_session_file(message):
	if message.chat.type != 'private':
		return
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	doc = message.document
	if not doc or not str(doc.file_name).lower().endswith(('.session', '.json')):
		bot.send_message(message.chat.id, 'Нужен файл формата .session или .json (как документ).')
		return
	file_info = bot.get_file(doc.file_id)
	data = bot.download_file(file_info.file_path)
	filename = os.path.basename(doc.file_name)
	save_session_file(filename, data)
	with open(filename, 'wb') as f:
		f.write(data)
	queue_pos = _enqueue_uploaded_session(message.chat.id, filename)
	bot.send_message(
		message.chat.id,
		f'⏳ Файл <code>{filename}</code> загружен.\n'
		f'Добавлен в очередь обработки: <b>~{queue_pos}</b>.\n'
		'Можно отправлять следующие файлы — обработаю по очереди.',
		parse_mode='HTML'
	)


def parser_step_sources(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий парсинга. Нажми кнопку «Парсинг • Аудитория» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	state['sources'] = message.text
	state['stage'] = 'posts'
	USER_STATE[message.chat.id] = state
	default_posts = _setting_int('parser_posts_limit')
	_prompt_step(
		message.chat.id,
		state,
		f'🔎 <b>Парсинг • Шаг 2/3</b>\n'
		f'Выбери глубину анализа по постам.\n'
		f'Рекомендация: <b>{default_posts}</b>.',
		parser_step_posts
	)


def parser_step_posts(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий парсинга. Нажми кнопку «Парсинг • Аудитория» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	try:
		state['posts_limit'] = int(message.text.strip())
	except (TypeError, ValueError):
		state['posts_limit'] = _setting_int('parser_posts_limit')
	state['stage'] = 'comments'
	USER_STATE[message.chat.id] = state
	_prompt_step(
		message.chat.id,
		state,
		f'🔎 <b>Парсинг • Шаг 3/3</b>\n'
		f'Укажи лимит комментариев на источник.\n'
		f'Рекомендация: <b>{_setting_int("parser_comments_limit")}</b>.',
		parser_step_comments
	)


def parser_step_comments(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий парсинга. Нажми кнопку «Парсинг • Аудитория» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
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
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		f'✅ <b>Парсинг поставлен в очередь</b>\n'
		f'• Источников: <b>{sources_count}</b>\n'
		f'• Позиция: <b>~{queue_pos}</b>\n'
		'Как только задача стартует — покажу живой прогресс.',
		parse_mode='HTML',
		reply_markup=build_new_menu()
	)


def inviter_step_target(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'inviter':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий инвайта. Нажми кнопку «Инвайт • Добавление» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	target = message.text.strip()
	if target == '':
		bot.send_message(message.chat.id, '⚠️ Цель пустая. Введи @chat или @channel.', reply_markup=_step_keyboard())
		return
	state['invite_target'] = target
	state['stage'] = 'limit'
	USER_STATE[message.chat.id] = state
	_prompt_step(
		message.chat.id,
		state,
		f'📨 <b>Инвайт • Шаг 2/3</b>\n'
		f'Укажи лимит пользователей за один запуск.\n'
		f'Рекомендация: <b>{_setting_int("inviter_limit")}</b>.',
		inviter_step_limit
	)


def inviter_step_limit(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'inviter':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий инвайта. Нажми кнопку «Инвайт • Добавление» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	try:
		state['limit'] = int(message.text.strip())
	except (TypeError, ValueError):
		state['limit'] = _setting_int('inviter_limit')
	state['stage'] = 'sleep'
	USER_STATE[message.chat.id] = state
	_prompt_step(
		message.chat.id,
		state,
		f'📨 <b>Инвайт • Шаг 3/3</b>\n'
		f'Укажи паузу между инвайтами в секундах.\n'
		f'Рекомендация: <b>{_setting_int("inviter_sleep")}</b>.',
		inviter_step_sleep
	)


def inviter_step_sleep(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'inviter':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий инвайта. Нажми кнопку «Инвайт • Добавление» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	try:
		sleep_sec = int(message.text.strip())
	except (TypeError, ValueError):
		sleep_sec = _setting_int('inviter_sleep')
	command = [
		sys.executable, 'inviter.py',
		'--all-parsed',
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
	queue_pos = _enqueue_process(message.chat.id, 'inviter (all parsed)', command, progress_file=progress_file)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		f'✅ <b>Инвайт поставлен в очередь</b>\n'
		'• Режим: <b>вся база парсинга</b>\n'
		f'• Позиция: <b>~{queue_pos}</b>\n'
		'При старте покажу live-отчет по добавлениям.',
		parse_mode='HTML',
		reply_markup=build_new_menu()
	)


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
		_render_inline(call.message.chat.id, call.message.message_id, '❌ Текущий сценарий отменен.', reply_markup=build_new_menu(), parse_mode=None)
		return

	if call.data == 'main_menu':
		USER_STATE.pop(call.message.chat.id, None)
		_render_inline(call.message.chat.id, call.message.message_id, '✨ Главное меню:', reply_markup=build_new_menu(), parse_mode=None)
		return

	if call.data == 'settings_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _settings_text(), parse_mode='HTML', reply_markup=_build_settings_menu())
		return

	if call.data == 'filters_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _filters_text(), parse_mode='HTML', reply_markup=build_new_menu())
		return

	if call.data == 'settings_reset':
		for k, v in DEFAULT_APP_SETTINGS.items():
			_set_setting(k, v)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			'♻️ Настройки сброшены к значениям по умолчанию.\n\n' + _settings_text(),
			parse_mode='HTML',
			reply_markup=_build_settings_menu()
		)
		return

	if call.data.startswith('settings_preset|'):
		name = call.data.split('|', 1)[1]
		if not _apply_preset(name):
			bot.send_message(call.message.chat.id, 'Неизвестный пресет.')
			return
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			f'🚀 Применен пресет: <b>{_preset_title(name)}</b>\nЛимиты и паузы обновлены.\n\n' + _settings_text(),
			parse_mode='HTML',
			reply_markup=_build_settings_menu()
		)
		return

	if call.data.startswith('settings_toggle|'):
		key = call.data.split('|', 1)[1]
		cur = _setting_bool(key)
		_set_setting(key, '0' if cur else '1')
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			f'✅ {_setting_title(key)}: {"ВЫКЛ" if cur else "ВКЛ"}\n\n' + _settings_text(),
			parse_mode='HTML',
			reply_markup=_build_settings_menu()
		)
		return

	if call.data.startswith('settings_edit|'):
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Сначала заверши текущий сценарий: <b>{_flow_title(cur)}</b>.\n'
				'Нажми «❌ Отменить сценарий» или закончи текущий шаг.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
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
		active = 0
		limited = 0
		dead = 0
		warming = 0
		for s in sessions:
			st, _, _ = get_account_health(s)
			if st == 'active':
				active += 1
			elif st == 'limited':
				limited += 1
			elif st == 'dead':
				dead += 1
			if get_account_warmup_remaining(s) > 0:
				warming += 1
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			f'📂 <b>Управление аккаунтами</b>\n'
			f'Загружено: <b>{len(sessions)}</b>\n'
			f'🟢 active: <b>{active}</b> | 🟠 limited: <b>{limited}</b> | 🔴 dead: <b>{dead}</b>\n'
			f'🕒 На прогреве: <b>{warming}</b>\n\n'
			'Отправь <code>.session</code> или <code>.json</code> файлом — проверка выполнится автоматически.',
			reply_markup=_build_accounts_menu(),
			parse_mode='HTML'
		)
		return

	if call.data == 'accounts_list':
		sessions = list_sessions()
		if len(sessions) == 0:
			bot.send_message(call.message.chat.id, 'Аккаунты не добавлены.')
			return
		lines = []
		for idx, s in enumerate(sessions, start=1):
			status, details, _ = get_account_health(s)
			lines.append(f'{idx}. {_account_status_emoji(status)} <code>{s}</code> — <b>{_account_status_title(status)}</b>')
			if details:
				lines.append(f'   <code>{_health_details_ru(details)[:120]}</code>')
			rem = get_account_warmup_remaining(s)
			if rem > 0:
				lines.append(f'   🕒 Прогрев: <b>{_fmt_seconds_ru(rem)}</b>')
		_send_html_chunks(call.message.chat.id, '📄 <b>Список аккаунтов</b>\n', lines)
		return

	if call.data == 'accounts_check_all':
		sessions = list_sessions()
		if len(sessions) == 0:
			bot.send_message(call.message.chat.id, 'Аккаунты не добавлены.')
			return
		bot.send_message(call.message.chat.id, '🔍 Запускаю проверку аккаунтов...')
		lines = []
		for s in sessions:
			status, details = _check_account_health(s, deep_check=False)
			prev = set_account_health(s, status, details)
			_notify_health_change_if_needed(s, prev, status, details)
			lines.append(f'{_account_status_emoji(status)} <code>{s}</code> — <b>{_account_status_title(status)}</b>')
		_send_html_chunks(call.message.chat.id, '✅ Проверка завершена\n', lines)
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
				delete_session_file(filename)
				bot.send_message(call.message.chat.id, f'Удален аккаунт: {filename}')
			else:
				delete_session_file(filename)
				bot.send_message(call.message.chat.id, 'Файл уже отсутствует.')
		except Exception as e:
			bot.send_message(call.message.chat.id, f'Ошибка удаления: {e}')
		return

	if call.data == 'parser_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\n'
				'Чтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		if not _ensure_sessions_or_warn(call.message.chat.id, call.message.message_id):
			return
		state = {'flow': 'parser', 'stage': 'sources', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'🔎 <b>Парсинг аудитории</b>\n'
			'Шаг 1/3: отправь источники (через запятую или с новой строки).\n\n'
			'Пример:\n<code>@chat1\n@chat2</code>\n\n'
			f'Текущие настройки: постов={_setting_int("parser_posts_limit")}, комментариев={_setting_int("parser_comments_limit")}, '
			f'все аккаунты={"ВКЛ" if _setting_bool("parser_use_all_sessions") else "ВЫКЛ"}.\n'
			'После этого я попрошу лимиты и поставлю задачу в очередь.',
			parser_step_sources
		)
		return

	if call.data == 'inviter_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\n'
				'Чтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		if not _ensure_sessions_or_warn(call.message.chat.id, call.message.message_id):
			return
		state = {'flow': 'inviter', 'stage': 'target', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'📨 <b>Инвайт пользователей</b>\n'
			'Шаг 1/3: укажи цель инвайта: <code>@chat</code> или <code>@channel</code>.\n\n'
			'Источник пользователей: <b>вся база пропарсенных</b>.\n\n'
			f'Текущие настройки: лимит={_setting_int("inviter_limit")}, пауза={_setting_int("inviter_sleep")}с, '
			f'на аккаунт={_setting_int("inviter_per_account_limit")}, flood={_setting_int("inviter_max_flood_wait")}с, '
			f'все аккаунты={"ВКЛ" if _setting_bool("inviter_use_all_sessions") else "ВЫКЛ"}.\n'
			'Дальше я попрошу лимит и паузу.',
			inviter_step_target
		)
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
		_render_inline(call.message.chat.id, call.message.message_id, '📊 Статус задач:\n' + '\n'.join(lines), reply_markup=build_new_menu(), parse_mode=None)
		return

	if call.data == 'manage_menu':
		_render_inline(call.message.chat.id, call.message.message_id, '⚙️ Управление задачами:', reply_markup=_build_manage_menu(), parse_mode=None)
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
		_render_inline(call.message.chat.id, call.message.message_id, _stats_text(), reply_markup=build_new_menu(), parse_mode=None)
		return

	if call.data == 'help_new':
		_render_inline(call.message.chat.id, call.message.message_id, _guide_text(), reply_markup=build_new_menu(), parse_mode='HTML')
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

def run_bot_polling():
	# Reset webhook mode to avoid clashes between webhook and long polling.
	_ensure_account_monitor()
	try:
		bot.remove_webhook()
	except Exception:
		pass

	while True:
		try:
			bot.infinity_polling(
				timeout=20,
				long_polling_timeout=20,
				skip_pending=True,
				allowed_updates=['message', 'callback_query'],
			)
		except ApiTelegramException as e:
			# 409 = another process is already calling getUpdates.
			if getattr(e, 'error_code', None) == 409:
				print('Telegram 409 conflict: another bot instance is running. Retry in 8s...')
				time.sleep(8)
				continue
			print(f'Telegram API error: {e}. Retry in 5s...')
			time.sleep(5)
		except Exception as e:
			print(f'Polling crashed: {e}. Retry in 5s...')
			time.sleep(5)


run_bot_polling()
