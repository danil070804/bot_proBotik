from decimal import *
import telebot
import datetime
from telebot import types, apihelper
from telebot.apihelper import ApiTelegramException
import csv
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
	get_account_health_record, set_account_health, set_account_warmup, get_account_warmup_remaining,
	save_session_file, delete_session_file, get_session_files
)
from telethon.errors import UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from functions import get_proxy, get_sessions, get_usable_sessions, build_telegram_client
from repositories.audience import AudienceRepository
from repositories.campaigns import CampaignRepository
from repositories.communities import CommunityRepository
from repositories.join_requests import JoinRequestRepository
from repositories.parse_tasks import ParseTaskRepository
from repositories.segments import SegmentRepository
from repositories.users import UserRepository
from services.campaign_service import CampaignService
from services.account_health_service import AccountHealthService
from services.join_request_service import JoinRequestService
from services.target_normalizer import INVITE_TYPE, is_invite_target_type, parse_target
from telegram.webhook_handlers import (
	handle_chat_join_request as process_chat_join_request_update,
	handle_chat_member_update as process_chat_member_update,
	handle_message as process_message_update,
)
from ui.builders import (
	build_confirm_screen,
	build_entity_card,
	build_inline_keyboard,
	build_list_page,
	build_section_screen,
	build_status_screen,
)

from pyqiwip2p import QiwiP2P
from pyqiwip2p.p2p_types import QiwiCustomer, QiwiDatetime


TOKEN = config.bot_invite_token
bot = telebot.TeleBot(TOKEN)
admin = config.admin
ADMINS = set(getattr(config, 'admins', [admin]))
init_db()
USER_REPO = UserRepository()
AUDIENCE_REPO = AudienceRepository()
COMMUNITY_REPO = CommunityRepository()
CAMPAIGN_REPO = CampaignRepository()
JOIN_REQUEST_REPO = JoinRequestRepository()
PARSE_TASK_REPO = ParseTaskRepository()
SEGMENT_REPO = SegmentRepository()
CAMPAIGN_SERVICE = CampaignService(campaigns=CAMPAIGN_REPO, communities=COMMUNITY_REPO)
JOIN_REQUEST_SERVICE = JoinRequestService(
	communities=COMMUNITY_REPO,
	campaigns=CAMPAIGN_REPO,
	users=USER_REPO,
	join_requests=JOIN_REQUEST_REPO,
)
ACCOUNT_HEALTH_SERVICE = AccountHealthService()


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
UPLOAD_BATCHES = {}
UPLOAD_BATCH_LOCK = threading.Lock()
UPLOAD_BATCH_WINDOW = 8
RECENT_UPLOAD_DOCS = {}
RECENT_UPLOAD_DOCS_LOCK = threading.Lock()
RECENT_UPLOAD_DOCS_TTL = 180
AUDIENCE_FILTERS = {}
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
	'campaign_rate_limit_per_minute': '20',
	'campaign_rate_limit_per_hour': '300',
	'campaign_max_attempts': '1',
	'campaign_stop_on_error_rate': '0.30',
	'accounts_allow_limited': '0',
	'interface_mode': 'pro',
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
		'campaign_rate_limit_per_minute': '10',
		'campaign_rate_limit_per_hour': '120',
		'campaign_max_attempts': '1',
		'campaign_stop_on_error_rate': '0.15',
		'accounts_allow_limited': '0',
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
		'campaign_rate_limit_per_minute': '15',
		'campaign_rate_limit_per_hour': '200',
		'campaign_max_attempts': '1',
		'campaign_stop_on_error_rate': '0.20',
		'accounts_allow_limited': '0',
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
		'campaign_rate_limit_per_minute': '20',
		'campaign_rate_limit_per_hour': '300',
		'campaign_max_attempts': '1',
		'campaign_stop_on_error_rate': '0.30',
		'accounts_allow_limited': '0',
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
		'campaign_rate_limit_per_minute': '40',
		'campaign_rate_limit_per_hour': '600',
		'campaign_max_attempts': '2',
		'campaign_stop_on_error_rate': '0.40',
		'accounts_allow_limited': '1',
		'active_preset': 'aggressive',
	},
}


def build_new_menu():
	return build_inline_keyboard([
		[('🔑 Аккаунты', 'accounts_menu'), ('📡 Парсинг', 'parsing_menu')],
		[('👥 Аудитория', 'audience_menu'), ('📂 Сообщества', 'communities_menu')],
		[('🚀 Кампании', 'campaigns_menu'), ('📨 Заявки', 'join_requests_menu')],
		[('📊 Аналитика', 'stats_overview'), ('📈 Статус', 'task_status')],
		[('⚡ Быстрый парсинг', 'quick_parse'), ('🔄 Повторить', 'quick_repeat_last')],
		[('🩺 Проверить', 'quick_accounts_check'), ('📌 Избранное', 'favorites_menu')],
		[('⚙️ Настройки', 'settings_menu'), ('ℹ️ Помощь', 'help_new')],
	])


def _bool_title(value):
	return 'Да' if str(value).strip().lower() in ['1', 'true', 'yes', 'on'] else 'Нет'


def _community_mode_title(mode):
	return {
		'invite_link': 'Ссылка',
		'join_request': 'Заявка',
		'auto': 'Авто',
	}.get(str(mode or '').strip(), str(mode or '-'))


def _compact_dt(value):
	if not value:
		return '—'
	text = str(value).replace('T', ' ').strip()
	return text[:16] if len(text) > 16 else text


def _interface_mode():
	return str(_get_setting('interface_mode') or 'pro').strip().lower() or 'pro'


def _is_compact_mode():
	return _interface_mode() == 'compact'


def _favorite_setting_key(entity_type):
	return f'favorites_{str(entity_type or "").strip()}'


def _get_favorites(entity_type):
	try:
		payload = json.loads(_get_setting(_favorite_setting_key(entity_type)) or '[]')
		if isinstance(payload, list):
			return payload
	except Exception:
		pass
	return []


def _set_favorites(entity_type, items):
	_set_setting(_favorite_setting_key(entity_type), json.dumps(list(items), ensure_ascii=False))


def _toggle_favorite(entity_type, entity_id):
	items = [str(item) for item in _get_favorites(entity_type)]
	value = str(entity_id)
	if value in items:
		items = [item for item in items if item != value]
		added = False
	else:
		items.insert(0, value)
		items = items[:20]
		added = True
	_set_favorites(entity_type, items)
	return added


def _is_favorite(entity_type, entity_id):
	return str(entity_id) in [str(item) for item in _get_favorites(entity_type)]


def _recent_platform_highlights():
	items = []
	last_task = next(iter(PARSE_TASK_REPO.list(limit=1)), None)
	if last_task:
		status = {
			'finished': 'завершено',
			'failed': 'ошибка',
			'running': 'в работе',
			'queued': 'в очереди',
			'paused': 'пауза',
			'cancelled': 'отменено',
		}.get(str(last_task.get('status') or '').strip(), last_task.get('status') or '—')
		items.append(
			f'{_parser_mode_title(last_task.get("mode"))} / {_parse_task_sources_count(last_task)} источника — {status}'
		)
	account_rows = _account_rows()
	problem_count = len([row for row in account_rows if row.get('status') in {'limited', 'flooded', 'dead', 'invalid'}])
	if problem_count:
		items.append(f'Проверка аккаунтов — {problem_count} проблемных')
	last_campaign = next(iter(CAMPAIGN_REPO.list()), None)
	if last_campaign:
		items.append(f'Кампания {last_campaign.get("name") or "без названия"} — {last_campaign.get("status") or "draft"}')
	return items[:3]


def _active_runtime_tasks_count():
	total = 0
	for items in RUNNING_TASKS.values():
		total += len([item for item in items if item.get('status') in {'queued', 'running'}])
	return total


def _build_error_entries(chat_id):
	entries = []
	for row in _account_rows():
		if row.get('status') not in {'limited', 'flooded', 'dead', 'invalid'}:
			continue
		entries.append(
			{
				'kind': 'account',
				'title': f'Аккаунт {row.get("phone") or row.get("session")}',
				'reason': row.get('reason_code') or row.get('status') or 'unknown_error',
				'detail': _health_details_ru(row.get('details') or ''),
				'time': _compact_dt(row.get('last_check')),
				'open_callback': f'account_open|{row.get("index")}|0',
				'retry_callback': f'account_check|{row.get("index")}|0',
			}
		)
	for task in PARSE_TASK_REPO.list(limit=20):
		if task.get('status') != 'failed' and not task.get('last_error'):
			continue
		entries.append(
			{
				'kind': 'parse_task',
				'title': f'Парсинг #{task.get("id")} · {_parser_mode_title(task.get("mode"))}',
				'reason': (task.get('last_error') or task.get('status') or 'parse_failed')[:140],
				'detail': task.get('last_error') or '',
				'time': _compact_dt(task.get('finished_at') or task.get('started_at') or task.get('created_at')),
				'open_callback': f'parse_task_view|{task.get("id")}|0',
				'retry_callback': f'parse_task_repeat|{task.get("id")}|0',
			}
		)
	for campaign in CAMPAIGN_REPO.list():
		if campaign.get('status') != 'failed':
			continue
		entries.append(
			{
				'kind': 'campaign',
				'title': f'Кампания {campaign.get("name") or "#" + str(campaign.get("id"))}',
				'reason': 'campaign_failed',
				'detail': campaign.get('status') or 'failed',
				'time': _compact_dt(campaign.get('updated_at') or campaign.get('created_at')),
				'open_callback': f'campaign_view|{campaign.get("id")}|0',
				'retry_callback': f'campaign_start|{campaign.get("id")}|0',
			}
		)
	entries.sort(key=lambda item: str(item.get('time') or ''), reverse=True)
	return entries


def _errors_overview_text(chat_id):
	entries = _build_error_entries(chat_id)
	account_errors = len([item for item in entries if item.get('kind') == 'account'])
	parse_errors = len([item for item in entries if item.get('kind') == 'parse_task'])
	campaign_errors = len([item for item in entries if item.get('kind') == 'campaign'])
	highlights = []
	if entries:
		latest = entries[0]
		highlights.append(f'Последняя: {latest.get("reason")}')
		for item in entries[:3]:
			highlights.append(f'{item.get("title")} — {item.get("reason")} · {item.get("time") or "—"}')
	else:
		highlights.append('Ошибок нет.')
	return build_status_screen(
		'🧯 Ошибки',
		stats=[
			('Аккаунты', account_errors),
			('Парсинг', parse_errors),
			('Кампании', campaign_errors),
		],
		highlights=highlights,
	)


def _errors_list_text(chat_id, page=0):
	entries = _build_error_entries(chat_id)
	items, page, total_pages = _slice_page(entries, page, 6)
	rows = []
	for item in items:
		rows.append(
			{
				'primary': item.get('title'),
				'secondary': item.get('reason'),
				'meta': item.get('time') or '—',
			}
		)
	return build_list_page('🧯 Ошибки', page, total_pages, rows)


def _build_errors_list_keyboard(chat_id, page=0):
	entries = _build_error_entries(chat_id)
	items, page, total_pages = _slice_page(entries, page, 6)
	rows_spec = []
	if items:
		open_buttons = [(str(idx), f'error_open|{page}|{idx - 1}') for idx, _ in enumerate(items, start=1)]
		rows_spec.append(open_buttons[:3])
		if len(open_buttons) > 3:
			rows_spec.append(open_buttons[3:6])
	rows_spec.append([('⬅️', f'errors_list|{max(0, page - 1)}'), ('➡️', f'errors_list|{min(total_pages - 1, page + 1)}')])
	rows_spec.append([('⬅️ Назад', 'status_errors')])
	return build_inline_keyboard(rows_spec)


def _error_detail_text(chat_id, page, local_index):
	entries = _build_error_entries(chat_id)
	items, page, _ = _slice_page(entries, page, 6)
	try:
		entry = items[int(local_index)]
	except Exception:
		return 'Ошибка не найдена.'
	return build_entity_card(
		'🧯 Ошибка',
		[
			('Объект', entry.get('title')),
			('Причина', entry.get('reason')),
			('Время', entry.get('time') or '—'),
			('Детали', (entry.get('detail') or '—')[:280]),
		],
	)


def _build_error_detail_keyboard(chat_id, page, local_index):
	entries = _build_error_entries(chat_id)
	items, page, _ = _slice_page(entries, page, 6)
	try:
		entry = items[int(local_index)]
	except Exception:
		return _build_errors_list_keyboard(chat_id, page)
	rows = []
	if entry.get('retry_callback'):
		rows.append([('🔄 Повторить', entry.get('retry_callback'))])
	if entry.get('open_callback'):
		rows.append([('🔍 Открыть', entry.get('open_callback'))])
	rows.append([('⬅️ Назад', f'errors_list|{page}')])
	return build_inline_keyboard(rows)


def _main_dashboard_text():
	try:
		audience = AUDIENCE_REPO.summary()
		parse_summary = PARSE_TASK_REPO.summary()
		campaigns = CAMPAIGN_REPO.list()
		total_errors = len(_build_error_entries(admin))
		active_tasks = _active_runtime_tasks_count()
		stats = [
			('Аккаунты', len(list_sessions()), ' 🟢'),
			('Аудитория', audience.get('total', 0)),
			('Сегодня найдено', audience.get('today_found', 0)),
			('Активных задач', active_tasks or int(parse_summary.get('running', 0) or 0) + int(parse_summary.get('queued', 0) or 0)),
			('Ошибки', total_errors),
		]
		highlights = _recent_platform_highlights()
		if total_errors:
			highlights.append(f'Есть проблемные зоны: {total_errors}. Рекомендуем открыть центр ошибок.')
		footer = 'Выбери раздел:' if _is_compact_mode() else 'Выбери раздел или быстрый сценарий ниже.'
		return build_status_screen('🏠 Teddy Invite', stats=stats, highlights=highlights, footer=footer)
	except Exception as e:
		return build_section_screen('🏠 Teddy Invite', description=f'Не удалось собрать экран: {e}')


def _communities_text():
	items = COMMUNITY_REPO.list()
	active = len([item for item in items if item.get('is_active')])
	return build_section_screen(
		'📂 Сообщества',
		stats=[('Всего', len(items)), ('Активные', active)],
		description='Управление целевыми чатами.'
	)


def _audience_text():
	summary = AUDIENCE_REPO.summary()
	return build_section_screen(
		'👥 Аудитория',
		stats=[
			('Всего', summary.get('total', 0)),
			('Сегменты', len(SEGMENT_REPO.list(limit=1000))),
			('Blacklisted', summary.get('blacklisted', 0)),
		],
		description='Управление базой пользователей.'
	)


def _campaigns_text():
	items = CAMPAIGN_REPO.list()
	active = len([item for item in items if item.get('status') in ['running', 'scheduled']])
	return build_section_screen(
		'🚀 Кампании',
		stats=[('Активные', active), ('Всего', len(items))],
		description='Запуск приглашений и сценариев.'
	)


def _join_requests_text(status='pending'):
	pending = len(JOIN_REQUEST_REPO.list(status='pending', limit=1000))
	approved = len(JOIN_REQUEST_REPO.list(status='approved', limit=1000))
	declined = len(JOIN_REQUEST_REPO.list(status='declined', limit=1000))
	return build_section_screen(
		'📨 Заявки',
		stats=[('Ожидают', pending), ('Одобрено', approved), ('Отклонено', declined)],
		description=f'Фильтр: {status}'
	)


def _parsing_text():
	parse_summary = PARSE_TASK_REPO.summary()
	recent_tasks = PARSE_TASK_REPO.list(limit=5)
	last_task = recent_tasks[0] if recent_tasks else None
	active_tasks = int(parse_summary.get('running', 0) or 0) + int(parse_summary.get('queued', 0) or 0)
	highlights = []
	if last_task and last_task.get('last_error'):
		highlights.append(f'Последняя ошибка: {(last_task.get("last_error") or "")[:80]}')
	if recent_tasks:
		last_done = recent_tasks[0]
		highlights.append(f'Рекомендуем: открыть задачу #{last_done.get("id")} или повторить последний запуск')
	return build_status_screen(
		'📡 Парсинг',
		stats=[
			('Активных задач', active_tasks),
			('Последний запуск', _compact_dt((last_task or {}).get('created_at') or (last_task or {}).get('started_at'))),
		],
		highlights=[] if _is_compact_mode() else highlights,
		footer='Выбери режим:'
	)


def list_sessions():
	return get_sessions()


ACCOUNTS_PAGE_SIZE = 5
AUDIENCE_PAGE_SIZE = 6
CAMPAIGNS_PAGE_SIZE = 6
PARSE_TASKS_PAGE_SIZE = 6


def _page_total(total_items, page_size):
	if total_items <= 0:
		return 1
	return max(1, (int(total_items) + int(page_size) - 1) // int(page_size))


def _normalize_page(page, total_items, page_size):
	try:
		page = int(page)
	except Exception:
		page = 0
	total_pages = _page_total(total_items, page_size)
	if page < 0:
		page = 0
	if page >= total_pages:
		page = total_pages - 1
	return page, total_pages


def _slice_page(items, page, page_size):
	page, total_pages = _normalize_page(page, len(items), page_size)
	start = page * page_size
	end = start + page_size
	return items[start:end], page, total_pages


def _get_audience_filters(user_id):
	return dict(AUDIENCE_FILTERS.get(int(user_id), {}))


def _set_audience_filters(user_id, filters=None):
	user_id = int(user_id)
	if filters:
		AUDIENCE_FILTERS[user_id] = dict(filters)
	else:
		AUDIENCE_FILTERS.pop(user_id, None)


def _audience_filter_summary(filters):
	filters = dict(filters or {})
	parts = []
	if filters.get('source_value'):
		parts.append(f'source={filters.get("source_value")}')
	if filters.get('discovered_after'):
		parts.append(f'from={str(filters.get("discovered_after"))[:10]}')
	if filters.get('search'):
		parts.append(f'search={filters.get("search")}')
	return ', '.join(parts) if parts else 'без фильтра'


def _build_page_nav(prev_callback, next_callback, back_callback):
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='⬅️', callback_data=prev_callback),
		types.InlineKeyboardButton(text='➡️', callback_data=next_callback),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data=back_callback))
	return keyboard


def _format_username(username):
	value = str(username or '').strip().lstrip('@')
	if value in ['', '—', '-']:
		return '—'
	return f'@{value}' if value else '—'


def _session_phone(session_name):
	base = os.path.splitext(os.path.basename(str(session_name or '')))[0]
	digits = ''
	for ch in base:
		if ch.isdigit():
			digits += ch
		elif digits:
			break
	return ('+' + digits) if digits else '—'


def _session_username(details):
	text = str(details or '').strip()
	for part in text.replace(',', ' ').split():
		if part.startswith('@') and len(part) > 1:
			return part
	return '—'


def _account_rows():
	rows = []
	for index, session in enumerate(list_sessions()):
		record = get_account_health_record(session)
		rows.append(
			{
				'index': index,
				'session': session,
				'phone': _session_phone(session),
				'username': _format_username(record.get('me_username') or _session_username(record.get('details'))),
				'status': record.get('status'),
				'details': record.get('reason_text') or record.get('details'),
				'reason_code': record.get('reason_code'),
				'last_check': record.get('last_check'),
				'is_deleted': record.get('is_deleted'),
				'dialogs_check_ok': record.get('dialogs_check_ok'),
				'entity_check_ok': record.get('entity_check_ok'),
				'warmup': get_account_warmup_remaining(session),
			}
		)
	return rows


def _campaign_status_emoji(status):
	return {
		'running': '🟢',
		'scheduled': '🟡',
		'paused': '⏸',
		'finished': '✅',
		'failed': '🔴',
		'cancelled': '⚫',
		'draft': '⚪',
	}.get(str(status or '').strip(), '⚪')


def _audience_rows(filters=None):
	return AUDIENCE_REPO.list(filters=filters, limit=500)


def _campaign_rows():
	return CAMPAIGN_REPO.list()


def _community_rows():
	return COMMUNITY_REPO.list()


def _accounts_text():
	rows = _account_rows()
	active = len([row for row in rows if row.get('status') == 'working'])
	problems = len([row for row in rows if row.get('status') in {'limited', 'flooded', 'dead', 'invalid'}])
	return build_section_screen(
		'🔑 Аккаунты',
		stats=[('Всего', len(rows)), ('Активны', active, ' 🟢'), ('Проблемы', problems, ' 🔴')],
		description='Управление аккаунтами и сессиями.'
	)


def _accounts_list_text(page=0):
	rows = _account_rows()
	items, page, total_pages = _slice_page(rows, page, ACCOUNTS_PAGE_SIZE)
	return build_list_page(
		'📋 Аккаунты',
		page,
		total_pages,
		[
			{
				'badge': _account_status_emoji(item.get('status')),
				'primary': item.get('phone'),
				'secondary': item.get('username'),
			}
			for item in items
		]
	)


def _build_accounts_list_keyboard(page=0):
	rows = _account_rows()
	items, page, total_pages = _slice_page(rows, page, ACCOUNTS_PAGE_SIZE)
	rows_spec = []
	if items:
		open_buttons = [
			(str(local_idx), f'account_open|{item.get("index")}|{page}')
			for local_idx, item in enumerate(items, start=1)
		]
		rows_spec.append(open_buttons[:4])
		if len(open_buttons) > 4:
			rows_spec.append(open_buttons[4:8])
	rows_spec.append([('⬅️', f'accounts_list|{max(0, page - 1)}'), ('➡️', f'accounts_list|{min(total_pages - 1, page + 1)}')])
	rows_spec.append([('🔎 Фильтр', 'accounts_filter_menu')])
	rows_spec.append([('⬅️ Назад', 'accounts_menu')])
	return build_inline_keyboard(rows_spec)


def _account_card_text(account_index):
	rows = _account_rows()
	try:
		item = rows[int(account_index)]
	except Exception:
		return 'Аккаунт не найден.'
	last_check = item.get('last_check') or '—'
	warmup = _fmt_seconds_ru(item.get('warmup')) if item.get('warmup') else '—'
	proxy_state = 'включён' if get_proxy() else 'выключен'
	return build_entity_card(
		'👤 Аккаунт',
		[
			('Телефон', item.get('phone'), 'code'),
			('Username', item.get('username'), 'code'),
			('Статус', f'{_account_status_emoji(item.get("status"))} {_account_status_title(item.get("status"))}'),
			('Причина', item.get('reason_code') or 'ok', 'code'),
			('Сессия', item.get('session'), 'code'),
			('Прокси', proxy_state),
			('Последняя проверка', last_check, 'code'),
			('Диалоги', 'ok' if item.get('dialogs_check_ok') else 'fail', 'code'),
			('Entity', 'ok' if item.get('entity_check_ok') else 'fail', 'code'),
			('Группа', '—'),
			('Прогрев', warmup, 'code'),
		],
		footer=item.get('details') or '—'
	)


def _build_account_card_keyboard(account_index, page=0):
	return build_inline_keyboard([
		[('🩺 Проверить', f'account_check|{account_index}|{page}')],
		[('✏️ Изменить', f'account_edit|{account_index}|{page}')],
		[('⏸ Отключить', f'account_disable|{account_index}|{page}')],
		[('🗑 Удалить', f'account_delete|{account_index}|{page}')],
		[('⬅️ Назад', f'accounts_list|{page}')],
	])


def _audience_list_text(page=0, filters=None):
	rows = _audience_rows(filters=filters)
	items, page, total_pages = _slice_page(rows, page, AUDIENCE_PAGE_SIZE)
	text = build_list_page(
		'👥 Аудитория',
		page,
		total_pages,
		[
			{
				'badge': '🚫' if item.get('is_blacklisted') else '🟢',
				'primary': item.get('first_name') or item.get('username') or item.get('telegram_user_id'),
				'secondary': f'{_format_username(item.get("username"))} · {item.get("source_value") or "—"}',
			}
			for item in items
		]
	)
	if filters:
		text += f'\n\nФильтр: <code>{_audience_filter_summary(filters)}</code>'
	return text


def _build_audience_list_keyboard(page=0, filters=None):
	rows = _audience_rows(filters=filters)
	items, page, total_pages = _slice_page(rows, page, AUDIENCE_PAGE_SIZE)
	rows_spec = []
	if items:
		open_buttons = [
			(str(local_idx), f'audience_user|{item.get("id")}|{page}')
			for local_idx, item in enumerate(items, start=1)
		]
		rows_spec.append(open_buttons[:4])
		if len(open_buttons) > 4:
			rows_spec.append(open_buttons[4:8])
	rows_spec.append([('⬅️', f'audience_list|{max(0, page - 1)}'), ('➡️', f'audience_list|{min(total_pages - 1, page + 1)}')])
	rows_spec.append([('🧭 Фильтр', 'audience_filters_menu'), ('🧹 Сброс', 'audience_filters_clear')])
	rows_spec.append([('⬅️ Назад', 'audience_menu')])
	return build_inline_keyboard(rows_spec)


def _campaigns_list_text(page=0):
	rows = _campaign_rows()
	items, page, total_pages = _slice_page(rows, page, CAMPAIGNS_PAGE_SIZE)
	return build_list_page(
		'🚀 Кампании',
		page,
		total_pages,
		[
			{
				'badge': _campaign_status_emoji(item.get('status')),
				'primary': item.get('name') or 'Без названия',
				'secondary': f'{item.get("status") or "draft"} · {_community_mode_title(item.get("invite_mode"))}',
			}
			for item in items
		]
	)


def _parse_task_status_emoji(status):
	return {
		'draft': '⚪',
		'queued': '⏳',
		'running': '🔵',
		'paused': '🟠',
		'finished': '✅',
		'failed': '🔴',
		'cancelled': '⚫',
	}.get(str(status or '').strip(), '⚪')


def _parse_task_rows():
	return PARSE_TASK_REPO.list(limit=200)


def _parse_tasks_list_text(page=0):
	rows = _parse_task_rows()
	items, page, total_pages = _slice_page(rows, page, PARSE_TASKS_PAGE_SIZE)
	return build_list_page(
		'📋 Задачи парсинга',
		page,
		total_pages,
		[
			{
				'badge': _parse_task_status_emoji(item.get('status')),
				'primary': f'#{item.get("id")} · {_parser_mode_title(item.get("mode"))}',
				'secondary': f'{item.get("status") or "draft"} · saved {item.get("total_saved", 0)}',
				'meta': (item.get('source_value') or '—')[:80],
			}
			for item in items
		],
	)


def _build_parse_tasks_list_keyboard(page=0):
	rows = _parse_task_rows()
	items, page, total_pages = _slice_page(rows, page, PARSE_TASKS_PAGE_SIZE)
	rows_spec = []
	if items:
		open_buttons = [
			(str(local_idx), f'parse_task_view|{item.get("id")}|{page}')
			for local_idx, item in enumerate(items, start=1)
		]
		rows_spec.append(open_buttons[:4])
		if len(open_buttons) > 4:
			rows_spec.append(open_buttons[4:8])
	rows_spec.append([('⬅️', f'parse_tasks_list|{max(0, page - 1)}'), ('➡️', f'parse_tasks_list|{min(total_pages - 1, page + 1)}')])
	rows_spec.append([('⬅️ Назад', 'parsing_menu')])
	return build_inline_keyboard(rows_spec)


def _parse_task_sources_count(task):
	task = task or {}
	source_reports = task.get('source_report_json') or []
	if source_reports:
		return len(source_reports)
	sources = str((task.get('meta_json') or {}).get('sources') or task.get('source_value') or '').strip()
	return len([row for row in sources.splitlines() if str(row).strip()])


def _parse_task_rollup(task):
	task = task or {}
	source_reports = task.get('source_report_json') or []
	mode = str(task.get('mode') or '').strip()
	rollup = {
		'authors_saved': 0,
		'commenters_saved': 0,
		'unique_users': 0,
		'duplicates': 0,
	}
	if mode != 'engaged_users':
		return rollup
	meta_stats = ((task.get('meta_json') or {}).get('stats') or {})
	if meta_stats:
		rollup['authors_saved'] = int(meta_stats.get('authors_saved') or 0)
		rollup['commenters_saved'] = int(meta_stats.get('commenters_saved') or 0)
		rollup['unique_users'] = int(meta_stats.get('unique_users') or 0)
		rollup['duplicates'] = int(meta_stats.get('duplicates') or 0)
		return rollup
	unique_total = 0
	for row in source_reports:
		rollup['authors_saved'] += int(row.get('authors_saved') or 0)
		rollup['commenters_saved'] += int(row.get('commenters_saved') or 0)
		unique_total += int(row.get('unique_users') or row.get('saved') or 0)
	rollup['unique_users'] = unique_total
	rollup['duplicates'] = max(0, rollup['authors_saved'] + rollup['commenters_saved'] - unique_total)
	return rollup


def _parse_task_detail_text(task_id):
	task = PARSE_TASK_REPO.get(task_id)
	if not task:
		return 'Задача парсинга не найдена.'
	source_reports = task.get('source_report_json') or []
	rollup = _parse_task_rollup(task)
	highlights = []
	for row in source_reports[:3]:
		status = row.get('status') or row.get('error_code') or '—'
		highlights.append(
			f'{row.get("source_title") or row.get("source")} — {status}, {_format_parse_source_metrics(row)}'
		)
	if int(task.get('total_saved') or 0) <= 3:
		highlights.append('Найдено мало пользователей. Проверь, доступны ли комментарии или участники в источнике.')
	if task.get('last_error'):
		highlights.append('Рекомендуем открыть список источников и проверить проблемные цели.')
	return build_status_screen(
		'📄 Задача парсинга',
		stats=[
			('ID', task.get('id')),
			('Режим', _parser_mode_title(task.get('mode'))),
			('Источники', _parse_task_sources_count(task)),
			('Статус', f'{_parse_task_status_emoji(task.get("status"))} {task.get("status") or "draft"}'),
			('Сохранено', task.get('total_saved', 0)),
			('Ошибки', task.get('total_errors', 0)),
			('Уникальные', rollup.get('unique_users', 0) if task.get('mode') == 'engaged_users' else task.get('total_saved', 0)),
			('Дубликаты', rollup.get('duplicates', 0) if task.get('mode') == 'engaged_users' else task.get('total_skipped', 0)),
			('Запуск', _compact_dt(task.get('started_at') or task.get('created_at'))),
		],
		highlights=[] if _is_compact_mode() else highlights,
		footer=(task.get('last_error') or '')[:140] if task.get('last_error') else None,
	)


def _parse_task_sources_text(task_id):
	task = PARSE_TASK_REPO.get(task_id)
	if not task:
		return 'Задача парсинга не найдена.'
	source_reports = task.get('source_report_json') or []
	lines = [f'📋 <b>Источники задачи #{task_id}</b>']
	if not source_reports:
		lines.append('')
		lines.append('Отчёт по источникам пока пуст.')
		return '\n'.join(lines)
	lines.append('')
	for idx, row in enumerate(source_reports[:20], start=1):
		status = row.get('status') or row.get('error_code') or '—'
		lines.append(f'{idx}. <b>{row.get("source_title") or row.get("source")}</b>')
		lines.append(f'Статус: <code>{status}</code>')
		if task.get('mode') == 'engaged_users':
			lines.append(f'Авторы: <b>{int(row.get("authors_saved") or 0)}</b>')
			lines.append(f'Комментаторы: <b>{int(row.get("commenters_saved") or 0)}</b>')
			lines.append(f'Уникальных: <b>{int(row.get("unique_users") or row.get("saved") or 0)}</b>')
			lines.append(f'Дубликатов: <b>{int(row.get("duplicates") or 0)}</b>')
		else:
			lines.append(f'Пользователи: <b>{int(row.get("saved") or 0)}</b>')
			lines.append(f'Комментарии: <b>{int(row.get("comments") or 0)}</b>')
		if row.get('error_text'):
			lines.append(f'   {str(row.get("error_text"))[:140]}')
		lines.append('')
	return '\n'.join(lines)


def _build_parse_task_actions(task_id, page=0):
	return build_inline_keyboard([
		[('📋 Источники', f'parse_task_sources|{task_id}|{page}'), ('🔄 Повторить', f'parse_task_repeat|{task_id}|{page}')],
		[('👥 Аудитория', 'audience_list|0')],
		[('⬅️ Назад', f'parse_tasks_list|{page}')],
	])


def _build_campaigns_list_keyboard(page=0):
	rows = _campaign_rows()
	items, page, total_pages = _slice_page(rows, page, CAMPAIGNS_PAGE_SIZE)
	rows_spec = []
	if items:
		open_buttons = [
			(str(local_idx), f'campaign_view|{item.get("id")}|{page}')
			for local_idx, item in enumerate(items, start=1)
		]
		rows_spec.append(open_buttons[:4])
		if len(open_buttons) > 4:
			rows_spec.append(open_buttons[4:8])
	rows_spec.append([('⬅️', f'campaigns_list|{max(0, page - 1)}'), ('➡️', f'campaigns_list|{min(total_pages - 1, page + 1)}')])
	rows_spec.append([('⬅️ Назад', 'campaigns_menu')])
	return build_inline_keyboard(rows_spec)


def _communities_list_text(page=0):
	rows = _community_rows()
	items, page, total_pages = _slice_page(rows, page, CAMPAIGNS_PAGE_SIZE)
	return build_list_page(
		'📂 Сообщества',
		page,
		total_pages,
		[
			{
				'badge': '🟢' if item.get('is_active') else '⚫',
				'primary': item.get('title') or item.get('chat_id') or 'Без названия',
				'secondary': f'{item.get("type") or "group"} · {_community_mode_title(item.get("default_invite_mode"))}',
			}
			for item in items
		]
	)


def _build_communities_list_keyboard(page=0):
	rows = _community_rows()
	items, page, total_pages = _slice_page(rows, page, CAMPAIGNS_PAGE_SIZE)
	rows_spec = []
	if items:
		open_buttons = [
			(str(local_idx), f'community_view|{item.get("id")}|{page}')
			for local_idx, item in enumerate(items, start=1)
		]
		rows_spec.append(open_buttons[:4])
		if len(open_buttons) > 4:
			rows_spec.append(open_buttons[4:8])
	rows_spec.append([('⬅️', f'communities_list|{max(0, page - 1)}'), ('➡️', f'communities_list|{min(total_pages - 1, page + 1)}')])
	rows_spec.append([('⬅️ Назад', 'communities_menu')])
	return build_inline_keyboard(rows_spec)


def _account_status_emoji(status):
	return {
		'working': '🟢',
		'limited': '🟡',
		'flooded': '🟠',
		'dead': '🔴',
		'invalid': '⚫',
		'active': '🟢',
		'unknown': '⚫',
	}.get(status, '⚫')


def _account_status_title(status):
	return {
		'working': 'Рабочий',
		'limited': 'Ограничен',
		'flooded': 'Flood',
		'dead': 'Мёртвый',
		'invalid': 'Не авторизован',
		'active': 'Рабочий',
		'unknown': 'Не авторизован',
	}.get(status, 'Не авторизован')


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
	code_map = {
		'ok': 'Аккаунт пригоден для рабочих операций',
		'deleted_account': 'Аккаунт недоступен для рабочих операций',
		'unauthorized': 'Сессия не авторизована',
		'session_invalid': 'Сессия недействительна или требует повторного входа',
		'flood_wait': 'Аккаунт временно ограничен flood-лимитами',
		'entity_resolve_failed': 'Не удалось разрешить Telegram-сущность',
		'dialogs_failed': 'Не удалось получить список диалогов',
		'rpc_error': 'Аккаунт ограничен для части операций',
		'unknown_error': 'Неизвестная ошибка проверки аккаунта',
		'functional_checks_failed': 'Функциональная проверка не пройдена',
		'api_credentials_missing': 'Не настроены API ID / API HASH',
		'not_checked': 'Проверка аккаунта ещё не выполнялась',
	}
	if text in code_map:
		return code_map[text]
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
			return 'working', 'Ограничений через @SpamBot не обнаружено'
		if any(x in lower for x in bad):
			return 'limited', 'Ограничения обнаружены через @SpamBot'
		return 'working', 'Базовая проверка пройдена'
	except Exception:
		return 'working', 'Базовая проверка пройдена'


def _detect_health_status_by_error(exc):
	name = exc.__class__.__name__
	text = _health_details_ru(f'{name}: {exc}')
	if 'JSON не содержит StringSession' in text:
		return 'invalid', text
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
		return 'flooded', text
	return 'limited', text


def _check_account_health(session, deep_check=False):
	return ACCOUNT_HEALTH_SERVICE.check_account_health(session, deep_check=deep_check)


def _store_account_health_result(session, result):
	return set_account_health(
		session,
		result.status,
		result.reason_text,
		reason_code=result.reason_code,
		reason_text=result.reason_text,
		me_username=result.me_username,
		me_id=result.me_id,
		is_deleted=result.is_deleted,
		dialogs_check_ok=result.dialogs_ok,
		entity_check_ok=result.entity_ok,
	)


def _notify_health_change_if_needed(session, prev_status, result):
	new_status = result.status
	if prev_status == new_status:
		return
	if new_status not in ('working', 'limited', 'flooded', 'dead', 'invalid'):
		return
	if len(ADMINS) == 0:
		return
	text = (
		f'🔔 <b>Статус аккаунта изменился</b>\n'
		f'• Аккаунт: <code>{session}</code>\n'
		f'• Статус: {_account_status_emoji(new_status)} <b>{_account_status_title(new_status)}</b>\n'
		f'• Причина: <code>{result.reason_code or "-"}</code>\n'
		f'• Детали: <code>{_health_details_ru(result.reason_text)[:500]}</code>'
	)
	for admin_id in sorted(ADMINS):
		try:
			bot.send_message(admin_id, text, parse_mode='HTML')
		except Exception:
			pass


def _process_uploaded_session(filename):
	result = _check_account_health(filename, deep_check=False)
	prev = _store_account_health_result(filename, result)
	_notify_health_change_if_needed(filename, prev, result)
	warmup_days = 0
	if result.status == 'working':
		warmup_days = max(0, _setting_int('account_warmup_days'))
		set_account_warmup(filename, warmup_days * 86400)
	return {
		'status': result.status,
		'reason_code': result.reason_code,
		'reason_text': result.reason_text,
		'warmup_days': warmup_days,
	}


def _upload_worker():
	while True:
		item = UPLOAD_QUEUE.get()
		try:
			try:
				result = _process_uploaded_session(item['filename'])
			except Exception as e:
				result = {
					'status': 'invalid',
					'reason_code': 'upload_processing_failed',
					'reason_text': str(e),
					'warmup_days': 0,
				}
			_finalize_upload_batch(item['chat_id'], item.get('batch_id'), item['filename'], result)
		finally:
			UPLOAD_QUEUE.task_done()


def _ensure_upload_worker():
	if getattr(_ensure_upload_worker, 'started', False):
		return
	t = threading.Thread(target=_upload_worker, daemon=True)
	t.start()
	_ensure_upload_worker.started = True


def _cleanup_recent_upload_docs(now_ts=None):
	now_ts = float(now_ts or time.time())
	with RECENT_UPLOAD_DOCS_LOCK:
		for key, ts in list(RECENT_UPLOAD_DOCS.items()):
			if now_ts - float(ts or 0) > RECENT_UPLOAD_DOCS_TTL:
				RECENT_UPLOAD_DOCS.pop(key, None)


def _is_duplicate_upload(chat_id, document):
	if not document:
		return False
	now_ts = time.time()
	_cleanup_recent_upload_docs(now_ts)
	key = f'{int(chat_id)}:{getattr(document, "file_unique_id", "") or getattr(document, "file_id", "")}'
	with RECENT_UPLOAD_DOCS_LOCK:
		if key in RECENT_UPLOAD_DOCS:
			return True
		RECENT_UPLOAD_DOCS[key] = now_ts
	return False


def _new_upload_batch(chat_id):
	batch_id = f'{int(chat_id)}:{int(time.time() * 1000)}'
	return {
		'batch_id': batch_id,
		'chat_id': int(chat_id),
		'created_at': time.time(),
		'updated_at': time.time(),
		'queued': 0,
		'processed': 0,
		'counts': {'working': 0, 'limited': 0, 'flooded': 0, 'dead': 0, 'invalid': 0},
		'files': [],
		'errors': [],
		'message_id': None,
		'closed': False,
	}


def _get_or_create_upload_batch(chat_id):
	with UPLOAD_BATCH_LOCK:
		batch = UPLOAD_BATCHES.get(int(chat_id))
		if batch and not batch.get('closed') and time.time() - float(batch.get('updated_at') or 0) <= UPLOAD_BATCH_WINDOW:
			return batch.get('batch_id')
		batch = _new_upload_batch(chat_id)
		UPLOAD_BATCHES[int(chat_id)] = batch
		return batch.get('batch_id')


def _queue_uploaded_file(chat_id, filename):
	batch_id = _get_or_create_upload_batch(chat_id)
	with UPLOAD_BATCH_LOCK:
		batch = UPLOAD_BATCHES.get(int(chat_id))
		if not batch:
			batch = _new_upload_batch(chat_id)
			UPLOAD_BATCHES[int(chat_id)] = batch
			batch_id = batch.get('batch_id')
		batch['updated_at'] = time.time()
		batch['queued'] = int(batch.get('queued') or 0) + 1
		files = list(batch.get('files') or [])
		files.append(str(filename))
		batch['files'] = files[-8:]
	_ensure_upload_worker()
	UPLOAD_QUEUE.put({'chat_id': chat_id, 'filename': filename, 'batch_id': batch_id})
	return batch_id, UPLOAD_QUEUE.qsize()


def _upload_batch_text(batch):
	batch = dict(batch or {})
	counts = dict(batch.get('counts') or {})
	queued = int(batch.get('queued') or 0)
	processed = int(batch.get('processed') or 0)
	pending = max(0, queued - processed)
	files = list(batch.get('files') or [])
	status = '✅ Завершено' if queued > 0 and processed >= queued else '⏳ Загрузка аккаунтов'
	lines = [
		f'{status}',
		'',
		f'Файлов: <b>{queued}</b>',
		f'Обработано: <b>{processed}</b>',
		f'В очереди: <b>{pending}</b>',
	]
	if processed > 0:
		lines.extend(
			[
				f'🟢 Рабочие: <b>{int(counts.get("working") or 0)}</b>',
				f'🟡 Ограничены: <b>{int(counts.get("limited") or 0)}</b>',
				f'🟠 Flood: <b>{int(counts.get("flooded") or 0)}</b>',
				f'🔴 Мёртвые: <b>{int(counts.get("dead") or 0)}</b>',
				f'⚫ Не авторизованы: <b>{int(counts.get("invalid") or 0)}</b>',
			]
		)
	if files:
		lines.append('')
		lines.append('Последние файлы:')
		for name in files[-4:]:
			lines.append(f'• <code>{name}</code>')
	errors = list(batch.get('errors') or [])
	if errors:
		lines.append('')
		lines.append('Последние проблемы:')
		for row in errors[-2:]:
			lines.append(f'• <code>{row}</code>')
	return '\n'.join(lines)


def _render_upload_batch(chat_id, batch_id):
	with UPLOAD_BATCH_LOCK:
		batch = dict((UPLOAD_BATCHES.get(int(chat_id)) or {}))
	if not batch or batch.get('batch_id') != batch_id:
		return None
	message_id = batch.get('message_id')
	new_message_id = _render_inline(chat_id, message_id, _upload_batch_text(batch), parse_mode='HTML')
	with UPLOAD_BATCH_LOCK:
		current = UPLOAD_BATCHES.get(int(chat_id))
		if current and current.get('batch_id') == batch_id:
			current['message_id'] = new_message_id
	return new_message_id


def _finalize_upload_batch(chat_id, batch_id, filename, result):
	with UPLOAD_BATCH_LOCK:
		batch = UPLOAD_BATCHES.get(int(chat_id))
		if not batch or batch.get('batch_id') != batch_id:
			return
		batch['updated_at'] = time.time()
		batch['processed'] = int(batch.get('processed') or 0) + 1
		counts = dict(batch.get('counts') or {})
		status = str((result or {}).get('status') or 'invalid').strip().lower()
		counts[status] = int(counts.get(status) or 0) + 1
		batch['counts'] = counts
		if status in {'dead', 'invalid'}:
			errors = list(batch.get('errors') or [])
			reason = str((result or {}).get('reason_code') or status)
			errors.append(f'{filename}: {reason}')
			batch['errors'] = errors[-6:]
		if int(batch.get('processed') or 0) >= int(batch.get('queued') or 0):
			batch['closed'] = True
	_render_upload_batch(chat_id, batch_id)


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


def _format_parse_source_metrics(row):
	row = row or {}
	saved = int(row.get('saved') or 0)
	comments = int(row.get('comments') or 0)
	commenters_saved = int(row.get('commenters_saved') or 0)
	authors_saved = int(row.get('authors_saved') or 0)
	unique_users = int(row.get('unique_users') or 0)
	duplicates = int(row.get('duplicates') or 0)
	if 'unique_users' in row or 'commenters_saved' in row or 'authors_saved' in row:
		return (
			f'authors {authors_saved}, commenters {commenters_saved}, '
			f'unique {unique_users or saved}, duplicates {duplicates}'
		)
	return f'users {saved}, comments {comments}'


def _format_parse_totals(progress):
	progress = progress or {}
	if str(progress.get('parser_mode') or '') == 'engaged_users':
		return (
			f'Авторы сообщений: {progress.get("authors_saved", 0)}\n'
			f'Комментаторы: {progress.get("commenters_saved", 0)}\n'
			f'Уникальных пользователей: {progress.get("unique_users", progress.get("users_parsed", 0))}\n'
			f'Дубликатов: {progress.get("duplicates", 0)}'
		)
	return (
		f'Пользователей: {progress.get("users_parsed", 0)}\n'
		f'Комментариев: {progress.get("comments_parsed", 0)}'
	)


def _format_progress_text(item, progress):
	mode = progress.get('mode', '')
	if mode == 'parser':
		text = (
			f'🔎 Парсинг в процессе\n'
			f'Источник: {progress.get("current_source", "-")}\n'
			f'Аккаунт: {progress.get("active_session", "-")}\n'
			f'Источники: {progress.get("sources_done", 0)}/{progress.get("sources_total", 0)}\n'
			f'{_format_parse_totals(progress)}\n'
			f'Ошибок: {progress.get("errors", 0)}'
		)
		source_results = progress.get('source_results') or []
		if source_results and not _is_compact_mode():
			lines = []
			for row in source_results[-3:]:
				status = 'success' if row.get('status') == 'success' else row.get('error_code') or row.get('status') or '—'
				lines.append(
					f'{row.get("source_title") or row.get("source")} — {status}, '
					f'{_format_parse_source_metrics(row)}'
				)
			text += '\n\nПоследние источники:\n' + '\n'.join(lines[:3])
		last_error = str(progress.get('last_error', '') or '').strip()
		if last_error:
			text += f'\nПоследняя ошибка: {last_error[:300]}'
		return text
	if mode == 'join_target':
		target_type = 'private invite' if is_invite_target_type(progress.get('target_type')) else 'public username'
		text = (
			f'➕ Вход аккаунтов в цель\n'
			f'Цель: {progress.get("target", "-")}\n'
			f'Тип: {target_type}\n'
			f'Метод: {progress.get("join_method", "-")}\n'
			f'Аккаунт: {progress.get("active_session", "-")}\n'
			f'Готово: {progress.get("done_accounts", 0)}/{progress.get("total_accounts", 0)}\n'
			f'Вошли: {progress.get("joined", 0)}\n'
			f'Уже были: {progress.get("already_in", progress.get("already", 0))}\n'
			f'Заявка: {progress.get("join_request_sent", 0)}\n'
			f'Invite ошибки: {progress.get("invite_errors", 0)} | Аккаунты: {progress.get("account_errors", 0)}'
		)
		last_code = str(progress.get('last_error_code', '') or '').strip()
		last_text = str(progress.get('last_error_text', '') or '').strip()
		if last_code:
			text += f'\nПоследняя ошибка: {last_code}'
			if last_text:
				text += f'\n{last_text[:220]}'
		return text
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
					item['last_progress'] = progress
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
		proc = subprocess.Popen(
			item['command'],
			cwd=os.path.dirname(os.path.abspath(__file__)),
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			text=True,
			encoding='utf-8',
			errors='replace'
		)
		item['pid'] = proc.pid
		item['proc'] = proc
		try:
			msg = bot.send_message(item['user_id'], f'▶️ Запущено: {item["title"]} (PID {proc.pid})')
			item['progress_msg_id'] = msg.message_id
		except Exception:
			pass
		_start_progress_monitor(item)
		output, _ = proc.communicate()
		code = proc.returncode
		item['output'] = (output or '').strip()
		with TASK_LOCK:
			item['status'] = f'finished ({code})'
		try:
			msg_id = item.get('progress_msg_id')
			if code == 0:
				progress = item.get('last_progress') or {}
				if progress.get('mode') == 'parser':
					task_id = progress.get('parse_task_id')
					text = (
						f'✅ Завершено: {item["title"]}\n'
						f'Код: {code}\n'
						f'Режим: {_parser_mode_title(progress.get("parser_mode"))}\n'
						f'Источники: {progress.get("sources_done", 0)}/{progress.get("sources_total", 0)}\n'
						f'Успешно: {max(0, progress.get("sources_done", 0) - progress.get("sources_failed", 0))}\n'
						f'С ошибками: {progress.get("sources_failed", 0)}\n'
						f'{_format_parse_totals(progress)}'
					)
					source_results = progress.get('source_results') or []
					if source_results:
						lines = []
						for row in source_results[:6]:
							status = 'success' if row.get('status') == 'success' else row.get('error_code') or row.get('status') or '—'
							lines.append(
								f'{row.get("source_title") or row.get("source")} — {status}, '
								f'{_format_parse_source_metrics(row)}'
							)
						text += '\n\nПо источникам:\n' + '\n'.join(lines)
					last_error = str(progress.get('last_error', '') or '').strip()
					if last_error:
						text += f'\n\nПоследняя ошибка:\n{last_error[:800]}'
					reply_markup = None
					if task_id:
						reply_markup = build_inline_keyboard([
							[('📄 Детали', f'parse_task_view|{task_id}|0'), ('🔄 Повторить', f'parse_task_repeat|{task_id}|0')],
							[('👥 Открыть аудиторию', 'audience_list|0')],
						])
					_render_inline(item['user_id'], msg_id, text, parse_mode=None, reply_markup=reply_markup)
				elif progress.get('mode') == 'join_target':
					target_type = 'private invite' if is_invite_target_type(progress.get('target_type')) else 'public username'
					text = (
						f'✅ Завершено: {item["title"]}\n'
						f'Код: {code}\n'
						f'Цель: {progress.get("target", "-")}\n'
						f'Тип: {target_type}\n'
						f'Метод: {progress.get("join_method", "-")}\n'
						f'Аккаунтов обработано: {progress.get("done_accounts", 0)}/{progress.get("total_accounts", 0)}\n'
						f'Вошли: {progress.get("joined", 0)}\n'
						f'Уже внутри: {progress.get("already_in", progress.get("already", 0))}\n'
						f'Заявка отправлена: {progress.get("join_request_sent", 0)}\n'
						f'Ошибки invite: {progress.get("invite_errors", 0)}\n'
						f'Ошибки аккаунтов: {progress.get("account_errors", 0)}'
					)
					last_code = str(progress.get('last_error_code', '') or '').strip()
					last_text = str(progress.get('last_error_text', '') or '').strip()
					if last_code:
						text += f'\n\nПоследняя ошибка: {last_code}'
						if last_text:
							text += f'\n{last_text[:600]}'
					_render_inline(item['user_id'], msg_id, text, parse_mode=None)
				else:
					_render_inline(item['user_id'], msg_id, f'✅ Завершено: {item["title"]}\nКод: {code}', parse_mode=None)
			else:
				error_text = item.get('output', '')[-1500:].strip()
				if not error_text:
					error_text = 'Процесс завершился с ошибкой, но stderr пустой.'
				_render_inline(
					item['user_id'],
					msg_id,
					f'❌ Ошибка: {item["title"]}\nКод: {code}\n\n{error_text}',
					parse_mode=None
				)
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
				result = _check_account_health(session)
				prev = _store_account_health_result(session, result)
				_notify_health_change_if_needed(session, prev, result)
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
		'ℹ️ <b>Помощь</b>\n\n'
		'<b>Teddy Invite</b> — это Telegram control panel для аккаунтов, парсинга, аудитории и кампаний.\n\n'
		'<b>Основной сценарий работы</b>\n'
		'1. Загрузи и проверь аккаунты.\n'
		'2. Собери аудиторию через парсинг или импорт.\n'
		'3. Добавь сообщество и выбери режим приглашения.\n'
		'4. Создай кампанию и запусти её.\n'
		'5. Следи за заявками, статусом и аналитикой.\n\n'
		'<b>Что где находится</b>\n'
		'🔑 Аккаунты — сессии, проверка, вход в цель.\n'
		'📡 Парсинг — сбор участников, комментаторов и авторов.\n'
		'👥 Аудитория — база пользователей, поиск, сегменты, blacklist.\n'
		'📂 Сообщества — цели для invite link и join request.\n'
		'🚀 Кампании — запуск сценариев приглашения.\n'
		'📨 Заявки — одобрение и отклонение join requests.\n\n'
		'<b>Подсказка</b>\n'
		'Используй карточки объектов для деталей, а раздел <b>Статус</b> — для быстрого контроля очередей и ошибок.'
	)


def _help_detail_text(topic):
	data = {
		'quick': (
			'🚀 <b>Быстрый старт</b>\n\n'
			'<b>Шаг 1. Аккаунты</b>\n'
			'Загрузи <code>.session</code> или <code>.json</code> и запусти проверку.\n\n'
			'<b>Шаг 2. Аудитория</b>\n'
			'Собери пользователей через парсинг или импортируй свою базу.\n\n'
			'<b>Шаг 3. Сообщество</b>\n'
			'Добавь чат или канал, укажи режим: ссылка, заявка или авто.\n\n'
			'<b>Шаг 4. Кампания</b>\n'
			'Выбери сегмент, шаблон сообщения и запусти приглашение.\n\n'
			'<b>Шаг 5. Контроль</b>\n'
			'Открывай <b>Заявки</b>, <b>Статус</b> и <b>Аналитику</b> для контроля результата.'
		),
		'accounts': (
			'🔑 <b>Аккаунты</b>\n\n'
			'В этом разделе хранятся все Telegram-сессии, которые используются для парсинга и технических действий.\n\n'
			'<b>Что можно делать</b>\n'
			'• загружать новые сессии;\n'
			'• проверять реальную работоспособность аккаунта;\n'
			'• открывать карточку аккаунта;\n'
			'• запускать вход в цель;\n'
			'• удалять нерабочие сессии.\n\n'
			'<b>Важно</b>\n'
			'В задачи по умолчанию попадают только аккаунты со статусом <b>Рабочий</b>. '
			'Ограниченные аккаунты можно включать отдельно через настройки.'
		),
		'parsing': (
			'📡 <b>Как парсить</b>\n\n'
			'Парсинг нужен для наполнения общей аудитории, а не для прямого инвайта.\n\n'
			'<b>Доступные режимы</b>\n'
			'• <b>Участники</b> — собирает список из чата или группы.\n'
			'• <b>Комментаторы</b> — ищет пользователей по комментариям.\n'
			'• <b>Авторы</b> — собирает авторов сообщений.\n'
			'• <b>Импорт</b> — загружает CSV/JSON в аудиторию.\n\n'
			'<b>Как проходит запуск</b>\n'
			'1. Выбираешь режим.\n'
			'2. Вводишь источник.\n'
			'3. Указываешь лимит.\n'
			'4. Подтверждаешь задачу.\n\n'
			'После завершения пользователи появляются в разделе <b>Аудитория</b>.'
		),
		'audience': (
			'👥 <b>Аудитория</b>\n\n'
			'Здесь хранится единая база пользователей для сегментов и кампаний.\n\n'
			'<b>Что доступно</b>\n'
			'• список и карточки пользователей;\n'
			'• поиск по username и Telegram ID;\n'
			'• сегменты под кампании;\n'
			'• blacklist и unsubscribe;\n'
			'• импорт и будущий экспорт выборок.\n\n'
			'<b>Практика</b>\n'
			'Сначала проверь, что парсинг сохранил пользователей корректно, потом собирай из них сегменты для рассылки.'
		),
		'campaigns': (
			'🚀 <b>Как запускать кампании</b>\n\n'
			'Кампания — это сценарий приглашения аудитории в сообщество через <b>invite link</b> или <b>join request</b>.\n\n'
			'<b>Стандартный flow</b>\n'
			'1. Выбери сообщество.\n'
			'2. Выбери сегмент аудитории.\n'
			'3. Укажи режим: <code>invite_link</code>, <code>join_request</code> или <code>auto</code>.\n'
			'4. Введи текст сообщения.\n'
			'5. Проверь лимиты и запусти кампанию.\n\n'
			'<b>После запуска</b>\n'
			'Следи за статусом отправки, заявками на вступление и итоговой аналитикой по воронке.'
		),
		'faq': (
			'❓ <b>FAQ</b>\n\n'
			'<b>Почему задача не стартует?</b>\n'
			'Обычно причина в том, что нет ни одного рабочего аккаунта.\n\n'
			'<b>Куда попадает парсинг?</b>\n'
			'Всегда в раздел <b>Аудитория</b>, а не в старый direct-invite pipeline.\n\n'
			'<b>Почему аккаунт не используется?</b>\n'
			'Если статус не <b>Рабочий</b>, он исключается из задач по умолчанию.\n\n'
			'<b>Где смотреть ошибки?</b>\n'
			'В разделах <b>Статус</b>, <b>Аккаунты</b> и в карточках конкретных объектов.'
		),
	}
	return data.get(
		topic,
		'ℹ️ <b>Помощь</b>\n\nРаздел в разработке.'
	)


def _build_help_menu():
	return build_inline_keyboard([
		[('🚀 Быстрый старт', 'help_topic|quick'), ('🔑 Аккаунты', 'help_topic|accounts')],
		[('📡 Парсинг', 'help_topic|parsing'), ('👥 Аудитория', 'help_topic|audience')],
		[('🚀 Кампании', 'help_topic|campaigns'), ('❓ FAQ', 'help_topic|faq')],
		[('⬅️ Назад', 'main_menu')],
	])


def _flow_title(flow):
	return {
		'parser': 'Парсинг',
		'community_add': 'Сообщество',
		'audience_add': 'Аудитория',
		'audience_import': 'Импорт аудитории',
		'audience_search': 'Поиск аудитории',
		'audience_filter_source': 'Фильтр аудитории',
		'segment_create': 'Сегмент',
		'campaign_create': 'Кампания',
		'join_target': 'Вход в цель',
		'settings': 'Настройки',
	}.get(flow, 'Сценарий')


def _has_active_flow(user_id):
	state = USER_STATE.get(user_id, {})
	return bool(state.get('flow'))


def _ensure_sessions_or_warn(chat_id, message_id=None):
	all_count = len(list_sessions())
	usable_count = len(get_usable_sessions())
	if usable_count > 0:
		return True
	if all_count > 0:
		text = (
			'⛔ <b>Нет рабочих аккаунтов</b>\n'
			'Запусти проверку в разделе «Аккаунты», чтобы получить хотя бы 1 статус <b>Рабочий</b>.'
		)
		if message_id:
			_render_inline(chat_id, message_id, text, parse_mode='HTML', reply_markup=build_new_menu())
		else:
			bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=build_new_menu())
		return False
	text = (
		'⛔ <b>Невозможно запустить сценарий</b>\n'
		'Сначала добавьте хотя бы <b>1 .session</b> аккаунт в разделе «Аккаунты».'
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


def _parse_bool_value(value):
	return str(value or '').strip().lower() in ['1', 'true', 'yes', 'on', 'y']


def _import_users_from_bytes(filename, data):
	name = str(filename or '').lower()
	if name.endswith('.csv'):
		text = bytes(data).decode('utf-8-sig', errors='replace')
		rows = list(csv.DictReader(text.splitlines()))
	elif name.endswith('.json'):
		payload = json.loads(bytes(data).decode('utf-8-sig', errors='replace'))
		if isinstance(payload, dict):
			rows = payload.get('users', [])
		elif isinstance(payload, list):
			rows = payload
		else:
			raise RuntimeError('JSON должен содержать список пользователей')
	else:
		raise RuntimeError('Поддерживаются только .csv и .json')
	imported = 0
	for row in rows:
		if not isinstance(row, dict):
			continue
		telegram_user_id = row.get('telegram_user_id') or row.get('user_id') or row.get('id')
		username = str(row.get('username') or '').strip().lstrip('@')
		if telegram_user_id not in [None, '']:
			try:
				telegram_user_id = int(str(telegram_user_id).strip())
			except Exception:
				telegram_user_id = None
		else:
			telegram_user_id = None
		if telegram_user_id in [None, ''] and username == '':
			continue
		tags = row.get('tags') or []
		if isinstance(tags, str):
			tags = [part.strip() for part in tags.split(',') if part.strip()]
		source_value = str(row.get('source') or '').strip() or str(filename or 'import').strip()
		source = AUDIENCE_REPO.get_or_create_source(
			source_type='file',
			source_value=source_value,
			title=source_value,
			meta_json={'filename': filename},
		)
		AUDIENCE_REPO.upsert(
			telegram_user_id=telegram_user_id,
			username=username,
			first_name=str(row.get('first_name') or '').strip(),
			last_name=str(row.get('last_name') or '').strip(),
			source_id=(source or {}).get('id'),
			discovered_via='import_file',
			consent_status=str(row.get('consent_status') or 'imported').strip(),
			tags_json=tags,
			is_blacklisted=_parse_bool_value(row.get('is_blacklisted')),
			unsubscribed_at=row.get('unsubscribed_at') or None,
		)
		imported += 1
	return imported


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
		'parser_posts_limit': 'Лимит постов парсинга',
		'parser_comments_limit': 'Лимит комментариев парсинга',
		'parser_use_all_sessions': 'Использовать все аккаунты в парсинге',
		'accounts_allow_limited': 'Разрешить ограниченные аккаунты',
		'interface_mode': 'Режим интерфейса',
		'campaign_rate_limit_per_minute': 'Rate limit в минуту',
		'campaign_rate_limit_per_hour': 'Rate limit в час',
		'campaign_max_attempts': 'Максимум попыток доставки',
		'campaign_stop_on_error_rate': 'Стоп по error-rate',
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
	return build_section_screen(
		'⚙️ Настройки',
		description='Системные и рабочие параметры.'
	)


def _settings_category_text(category):
	data = {
		'logic': ('🧠 Логика', f'Пресет: {_preset_title(_get_setting("active_preset"))}. Max attempts: {_setting_int("campaign_max_attempts")}.'),
		'limits': ('⏱ Лимиты', f'Парсинг: {_setting_int("parser_posts_limit")} / {_setting_int("parser_comments_limit")}. Rate: {_setting_int("campaign_rate_limit_per_minute")}/мин.'),
		'notifications': ('🔔 Уведомления', 'Системные уведомления и служебные сообщения будут вынесены сюда.'),
		'interface': ('🎨 Интерфейс', f'Режим: {"Compact" if _is_compact_mode() else "Pro"}. Compact показывает только ключевые метрики, Pro — расширенные summary.'),
		'security': ('🛡 Безопасность', f'Прогрев аккаунтов: {_setting_int("account_warmup_days")} дн. Ограниченные аккаунты: {"вкл" if _setting_bool("accounts_allow_limited") else "выкл"}.'),
	}
	title, description = data.get(category, ('⚙️ Настройки', 'Раздел в разработке.'))
	return build_section_screen(title, description=description)


def _build_settings_detail_menu(category):
	if category == 'logic':
		return build_inline_keyboard([
			[('🛡 Safe', 'settings_preset|super_safe'), ('🟢 Soft', 'settings_preset|soft')],
			[('🟡 Standard', 'settings_preset|standard'), ('🔴 Aggro', 'settings_preset|aggressive')],
			[('✏️ Попытки', 'settings_edit|campaign_max_attempts'), ('✏️ Error-rate', 'settings_edit|campaign_stop_on_error_rate')],
			[('⬅️ Назад', 'settings_menu')],
		])
	if category == 'limits':
		return build_inline_keyboard([
			[('✏️ Посты', 'settings_edit|parser_posts_limit'), ('✏️ Комменты', 'settings_edit|parser_comments_limit')],
			[('✏️ Лимит/мин', 'settings_edit|campaign_rate_limit_per_minute'), ('✏️ Лимит/час', 'settings_edit|campaign_rate_limit_per_hour')],
			[('⬅️ Назад', 'settings_menu')],
		])
	if category == 'security':
		return build_inline_keyboard([
			[('🕒 Прогрев', 'settings_edit|account_warmup_days')],
			[('🟡 Ограниченные', 'settings_toggle|accounts_allow_limited')],
			[('⬅️ Назад', 'settings_menu')],
		])
	if category == 'interface':
		return build_inline_keyboard([
			[('📦 Compact', 'settings_interface_mode|compact'), ('🧠 Pro', 'settings_interface_mode|pro')],
			[('⬅️ Назад', 'settings_menu')],
		])
	return build_inline_keyboard([[('⬅️ Назад', 'settings_menu')]])


def _stub_text(title, description):
	return build_section_screen(title, description=description)


def _build_settings_menu():
	return build_inline_keyboard([
		[('🧠 Логика', 'settings_logic'), ('⏱ Лимиты', 'settings_limits')],
		[('🔔 Уведомления', 'settings_notifications'), ('🎨 Интерфейс', 'settings_interface')],
		[('🛡 Безопасность', 'settings_security')],
		[('⬅️ Назад', 'main_menu')],
	])


def _build_communities_menu():
	return build_inline_keyboard([
		[('➕ Добавить', 'community_add_start'), ('📋 Список', 'communities_list')],
		[('🔗 Ссылки', 'communities_links'), ('⚙️ Режимы', 'communities_modes')],
		[('⬅️ Назад', 'main_menu')],
	])


def _build_audience_menu():
	return build_inline_keyboard([
		[('📋 Список', 'audience_list'), ('🔎 Поиск', 'audience_search_start')],
		[('🧭 Фильтр', 'audience_filters_menu'), ('🏷 Сегменты', 'segments_menu')],
		[('🚫 Blacklist', 'audience_blacklist_list'), ('📤 Экспорт', 'audience_export')],
		[('📂 Импорт', 'audience_import_start')],
		[('⬅️ Назад', 'main_menu')],
	])


def _build_parser_menu():
	return build_inline_keyboard([
		[('👥 Участники', 'parser_mode|members'), ('💬 Комментаторы', 'parser_mode|commenters')],
		[('📝 Авторы', 'parser_mode|message_authors'), ('⚡ Активность', 'parser_mode|engaged_users')],
		[('📂 Импорт', 'parser_mode|import_file'), ('✍️ Вручную', 'parser_mode|manual_add')],
		[('📋 Задачи', 'parse_tasks_list|0'), ('🔁 Повторить', 'parser_repeat_last')],
		[('⬅️ Назад', 'main_menu')],
	])


def _build_segments_menu():
	return build_inline_keyboard([
		[('➕ Создать', 'segment_create_start'), ('📋 Список', 'segments_list')],
		[('⬅️ Назад', 'audience_menu')],
	])


def _build_campaigns_menu():
	return build_inline_keyboard([
		[('➕ Создать', 'campaign_create_start'), ('📋 Список', 'campaigns_list')],
		[('▶️ Запустить', 'campaigns_quick_start'), ('⏸ Пауза', 'campaigns_quick_pause')],
		[('📦 Шаблоны', 'campaign_templates')],
		[('⬅️ Назад', 'main_menu')],
	])


def _build_join_requests_menu():
	return build_inline_keyboard([
		[('🕒 Ожидают', 'join_requests_view|pending'), ('✅ Одобрено', 'join_requests_view|approved')],
		[('❌ Отклонено', 'join_requests_view|declined'), ('⚡ Авто-режим', 'join_requests_auto_mode')],
		[('⬅️ Назад', 'main_menu')],
	])


def _parse_yes_no(value):
	text = str(value or '').strip().lower()
	if text in ['да', 'yes', 'y', '1', 'true', 'on']:
		return True
	if text in ['нет', 'no', 'n', '0', 'false', 'off']:
		return False
	raise ValueError('Ожидается да/нет')


def _parser_mode_title(mode):
	return {
		'members': 'Участники',
		'commenters': 'Комментаторы',
		'message_authors': 'Авторы',
		'engaged_users': 'Активность',
		'import_file': 'Импорт',
		'manual_add': 'Вручную',
	}.get(str(mode or '').strip(), str(mode or '-'))


def _parse_segment_filter_text(text):
	raw = str(text or '').strip()
	if raw == '':
		return {'exclude_unsubscribed': True}
	if raw.lower() == 'all_active':
		return {'exclude_unsubscribed': True, 'is_blacklisted': False}
	result = {'exclude_unsubscribed': True}
	for part in [item.strip() for item in raw.split(',') if item.strip()]:
		lower = part.lower()
		if lower == 'blacklisted':
			result['is_blacklisted'] = True
		elif lower == 'not_blacklisted':
			result['is_blacklisted'] = False
		elif lower == 'username_missing':
			result['has_username'] = False
		elif lower == 'username_exists':
			result['has_username'] = True
		elif lower.startswith('source='):
			result['source_value'] = part.split('=', 1)[1].strip()
		elif lower.startswith('tag='):
			result['tag'] = part.split('=', 1)[1].strip()
		elif lower.startswith('search='):
			result['search'] = part.split('=', 1)[1].strip()
	return result


def _segment_detail_text(segment_id):
	segment = SEGMENT_REPO.get(segment_id)
	if not segment:
		return 'Сегмент не найден.'
	users = SEGMENT_REPO.get_users(segment_id, limit=1000)
	star = '⭐ Да' if _is_favorite('segments', segment_id) else '☆ Нет'
	return (
		f'🧩 <b>{segment.get("name")}</b>\n'
		f'ID: <code>{segment.get("id")}</code>\n'
		f'Пользователей (preview ≤1000): <b>{len(users)}</b>\n'
		f'Избранное: <b>{star}</b>\n'
		f'Фильтр: <code>{json.dumps(segment.get("filter_json") or {}, ensure_ascii=False)}</code>'
	)


def _build_segment_actions(segment_id):
	label = '⭐ Убрать' if _is_favorite('segments', segment_id) else '⭐ В избранное'
	return build_inline_keyboard([
		[(label, f'favorite_toggle|segments|{segment_id}|0')],
		[('⬅️ Назад', 'segments_menu')],
	])


def _build_campaign_actions(campaign_id, page=0):
	favorite_label = '⭐ Убрать' if _is_favorite('campaigns', campaign_id) else '⭐ В избранное'
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='▶️ Запустить', callback_data=f'campaign_start|{campaign_id}|{page}'),
		types.InlineKeyboardButton(text='⏸ Пауза', callback_data=f'campaign_pause|{campaign_id}|{page}'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='🔄 Возобновить', callback_data=f'campaign_resume|{campaign_id}|{page}'),
		types.InlineKeyboardButton(text='🛑 Отменить', callback_data=f'campaign_cancel|{campaign_id}|{page}'),
	)
	keyboard.add(types.InlineKeyboardButton(text=favorite_label, callback_data=f'favorite_toggle|campaigns|{campaign_id}|{page}'))
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data=f'campaigns_list|{page}'))
	return keyboard


def _campaign_detail_text(campaign_id):
	campaign = CAMPAIGN_REPO.get(campaign_id)
	if not campaign:
		return 'Кампания не найдена.'
	stats = CAMPAIGN_REPO.get_stats(campaign_id)
	community = COMMUNITY_REPO.get(campaign.get('community_id'))
	star = '⭐ Да' if _is_favorite('campaigns', campaign_id) else '☆ Нет'
	return (
		f'🚀 <b>Кампания</b>\n\n'
		f'Название: <b>{campaign.get("name")}</b>\n'
		f'Статус: {_campaign_status_emoji(campaign.get("status"))} <b>{campaign.get("status")}</b>\n'
		f'Сообщество: <b>{(community or {}).get("title", "-")}</b>\n'
		f'Режим: <b>{_community_mode_title(campaign.get("invite_mode"))}</b>\n'
		f'Избранное: <b>{star}</b>\n'
		f'Rate: <code>{campaign.get("rate_limit_per_minute")}/мин</code>\n\n'
		f'Получатели: <b>{stats.get("total", 0)}</b>\n'
		f'Отправлено: <b>{stats.get("sent", 0)}</b>\n'
		f'Одобрено: <b>{stats.get("approved", 0)}</b>\n'
		f'Вступили: <b>{stats.get("joined", 0)}</b>\n'
		f'Ошибки: <b>{stats.get("failed", 0)}</b>'
	)


def _build_user_actions(user_id, page=0):
	user = AUDIENCE_REPO.get(user_id)
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(
			text='✅ Убрать blacklist' if user and user.get('is_blacklisted') else '🚫 В blacklist',
			callback_data=f'audience_toggle_blacklist|{user_id}|{page}'
		),
		types.InlineKeyboardButton(text='✉️ Unsubscribe', callback_data=f'audience_unsubscribe|{user_id}|{page}'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data=f'audience_list|{page}'))
	return keyboard


def _user_detail_text(user_id):
	user = AUDIENCE_REPO.get(user_id)
	if not user:
		return 'Пользователь не найден.'
	return (
		'👤 <b>Пользователь</b>\n\n'
		f'Имя: <b>{user.get("first_name") or "-"}</b>\n'
		f'TG ID: <code>{user.get("telegram_user_id")}</code>\n'
		f'Username: <code>{_format_username(user.get("username"))}</code>\n'
		f'Источник: <code>{user.get("source_value") or "-"}</code>\n'
		f'Режим: <code>{user.get("discovered_via") or "-"}</code>\n'
		f'Согласие: <b>{user.get("consent_status") or "-"}</b>\n'
		f'В blacklist: <b>{_bool_title(user.get("is_blacklisted"))}</b>\n'
		f'Отписка: <b>{"Да" if user.get("unsubscribed_at") else "Нет"}</b>'
	)


def _segments_text():
	items = SEGMENT_REPO.list(limit=20)
	lines = [
		'🏷 <b>Сегменты</b>',
		f'Всего: <b>{len(items)}</b>',
		'Группы аудитории для кампаний.',
	]
	for item in items[:5]:
		user_count = len(SEGMENT_REPO.get_users(item.get('id'), limit=500))
		lines.append(f'• <b>{item.get("name")}</b> — <b>{user_count}</b>')
	return '\n'.join(lines)


def _audience_filters_text(user_id):
	filters = _get_audience_filters(user_id)
	highlights = [f'Текущий фильтр: {_audience_filter_summary(filters)}']
	if filters.get('source_value'):
		highlights.append(f'Источник: {filters.get("source_value")}')
	if filters.get('discovered_after'):
		highlights.append(f'С даты: {str(filters.get("discovered_after"))[:10]}')
	return build_status_screen(
		'🧭 Фильтры аудитории',
		stats=[
			('Всего после фильтра', len(_audience_rows(filters=filters))),
			('Источники', len(AUDIENCE_REPO.list_sources(limit=1000))),
		],
		highlights=highlights,
	)


def _build_audience_filters_menu():
	return build_inline_keyboard([
		[('📂 Источник', 'audience_filter_source_start'), ('🕒 24ч', 'audience_filter_recent|1')],
		[('📅 7 дней', 'audience_filter_recent|7'), ('🗓 30 дней', 'audience_filter_recent|30')],
		[('🧹 Сброс', 'audience_filters_clear'), ('⬅️ Назад', 'audience_menu')],
	])


def _build_manage_menu():
	return build_inline_keyboard([
		[('🛑 Остановить', 'task_stop_menu')],
		[('🧹 Очистить', 'task_clear_done')],
		[('⬅️ Назад', 'main_menu')],
	])


def _build_accounts_menu():
	return build_inline_keyboard([
		[('➕ Добавить', 'accounts_upload_help'), ('📋 Список', 'accounts_list|0')],
		[('🩺 Проверка', 'accounts_check_all'), ('🎯 В цель', 'accounts_join_target_start')],
		[('🧩 Группы', 'accounts_groups'), ('🗑 Удалить', 'accounts_list|0')],
		[('⬅️ Назад', 'main_menu')],
	])


def _stats_text():
	try:
		campaigns = CAMPAIGN_REPO.list()
		join_requests = JOIN_REQUEST_REPO.list(limit=5000)
		parse_summary = PARSE_TASK_REPO.summary()
		total_stats = {
			'sent': 0,
			'delivered': 0,
			'failed': 0,
			'join_requested': 0,
			'approved': 0,
			'joined': 0,
			'declined': 0,
			'total': 0,
		}
		for campaign in campaigns:
			stats = CAMPAIGN_REPO.get_stats(campaign['id'])
			for key in total_stats:
				total_stats[key] += int(stats.get(key, 0) or 0)
		jr_status = {}
		for row in join_requests:
			status = str(row.get('status') or 'pending')
			jr_status[status] = jr_status.get(status, 0) + 1
		audience = AUDIENCE_REPO.summary()
		total_campaigns = len(campaigns)
		total_requests = len(join_requests)
		conversion = 0
		if total_stats.get('sent', 0):
			conversion = round((total_stats.get('approved', 0) + total_stats.get('joined', 0)) * 100 / max(1, total_stats.get('sent', 0)))
		highlights = [
			f'Парсинг: {parse_summary.get("total_saved", 0)} сохранено',
			f'Отправлено: {total_stats.get("sent", 0)}',
			f'Одобрено: {total_stats.get("approved", 0)}',
			f'Вступили: {total_stats.get("joined", 0)}',
			f'Ожидают: {jr_status.get("pending", 0)}',
		]
		return build_status_screen(
			'📊 Аналитика',
			stats=[
				('Аудитория', audience.get('total', 0)),
				('Кампании', total_campaigns),
				('Заявки', total_requests),
				('Конверсия', f'{conversion}%'),
			],
			highlights=[] if _is_compact_mode() else highlights,
		)
	except Exception as e:
		return f'Не удалось собрать аналитику: {e}'


def _status_text():
	parse_summary = PARSE_TASK_REPO.summary()
	campaigns = CAMPAIGN_REPO.list()
	active_campaigns = len([item for item in campaigns if item.get('status') in ['running', 'scheduled']])
	pending_requests = len(JOIN_REQUEST_REPO.list(status='pending', limit=1000))
	audience = AUDIENCE_REPO.summary()
	highlights = []
	last_task = next(iter(PARSE_TASK_REPO.list(limit=1)), None)
	if last_task and last_task.get('last_error'):
		highlights.append(f'Последняя ошибка парсинга: {(last_task.get("last_error") or "")[:90]}')
	highlights.append(f'База: {audience.get("total", 0)} пользователей')
	return build_status_screen(
		'📈 Статус системы',
		stats=[
			('Аккаунты', len(list_sessions()), ' 🟢'),
			('Парсинг', f'{parse_summary.get("running", 0) + parse_summary.get("queued", 0)} задачи'),
			('Кампании', f'{active_campaigns} активны'),
			('Заявки', f'{pending_requests} ожидают'),
		],
		highlights=[] if _is_compact_mode() else highlights,
	)


def _build_status_menu():
	return build_inline_keyboard([
		[('🔄 Обновить', 'task_status'), ('🧯 Ошибки', 'status_errors')],
		[('⏸ Остановить всё', 'status_stop_all')],
		[('⬅️ Назад', 'main_menu')],
	])


def _build_favorites_menu():
	return build_inline_keyboard([
		[('📂 Сообщества', 'favorites_view|communities'), ('🧩 Сегменты', 'favorites_view|segments')],
		[('🚀 Кампании', 'favorites_view|campaigns'), ('📡 Источники', 'favorites_view|sources')],
		[('⬅️ Назад', 'main_menu')],
	])


def _favorites_text(kind=None):
	community_ids = _get_favorites('communities')
	segment_ids = _get_favorites('segments')
	campaign_ids = _get_favorites('campaigns')
	source_values = _get_favorites('sources')
	if not kind:
		return build_status_screen(
			'📌 Избранное',
			stats=[
				('Источники', len(source_values)),
				('Сегменты', len(segment_ids)),
				('Сообщества', len(community_ids)),
				('Кампании', len(campaign_ids)),
			],
			highlights=[] if _is_compact_mode() else ['Открой раздел ниже, чтобы быстро перейти к сохранённым сущностям.'],
		)
	highlights = []
	if kind == 'communities':
		for item_id in community_ids[:6]:
			item = COMMUNITY_REPO.get(int(item_id))
			if item:
				highlights.append(f'{item.get("title") or item.get("chat_id")} · {_community_mode_title(item.get("default_invite_mode"))}')
	if kind == 'segments':
		for item_id in segment_ids[:6]:
			item = SEGMENT_REPO.get(int(item_id))
			if item:
				highlights.append(f'{item.get("name")} · id {item.get("id")}')
	if kind == 'campaigns':
		for item_id in campaign_ids[:6]:
			item = CAMPAIGN_REPO.get(int(item_id))
			if item:
				highlights.append(f'{item.get("name")} · {item.get("status")}')
	if kind == 'sources':
		for value in source_values[:6]:
			highlights.append(str(value))
	title_map = {
		'communities': '📌 Избранное · Сообщества',
		'segments': '📌 Избранное · Сегменты',
		'campaigns': '📌 Избранное · Кампании',
		'sources': '📌 Избранное · Источники',
	}
	return build_status_screen(title_map.get(kind, '📌 Избранное'), highlights=highlights or ['Пока пусто.'])


def _community_detail_text(community_id):
	item = COMMUNITY_REPO.get(community_id)
	if not item:
		return 'Сообщество не найдено.'
	star = '⭐ В избранном' if _is_favorite('communities', community_id) else '☆ Не в избранном'
	return build_entity_card(
		'📂 Сообщество',
		[
			('Название', item.get('title') or '—'),
			('Chat ID', item.get('chat_id'), 'code'),
			('Тип', item.get('type') or 'group'),
			('Режим', _community_mode_title(item.get('default_invite_mode'))),
			('Автоодобрение', 'Да' if item.get('auto_approve_join_requests') else 'Нет'),
			('Strict', 'Да' if item.get('strict_moderation') else 'Нет'),
			('Статус', '🟢 Активно' if item.get('is_active') else '⚫ Неактивно'),
			('Избранное', star),
		],
	)


def _build_community_actions(community_id, page=0):
	label = '⭐ Убрать' if _is_favorite('communities', community_id) else '⭐ В избранное'
	return build_inline_keyboard([
		[(label, f'favorite_toggle|communities|{community_id}|{page}')],
		[('⬅️ Назад', f'communities_list|{page}')],
	])


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
		bot.send_message(message.chat.id, _main_dashboard_text(), parse_mode='HTML', reply_markup=build_new_menu())

@bot.message_handler(content_types=['text'])
def send_text(message):
	if message.chat.type == 'private':
		if not _is_admin(message.chat.id):
			unsubscribe_result = process_message_update(message)
			if unsubscribe_result:
				bot.send_message(message.chat.id, 'Вы отписаны от приглашений.')
				return
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
			bot.send_message(message.chat.id, _main_dashboard_text(), parse_mode='HTML', reply_markup=build_new_menu())
			return
		elif message.text.lower() in ['фильтры', 'filters']:
			bot.send_message(message.chat.id, 'Фильтры выведены из основного продукта. Для ограничений используй blacklist/unsubscribe в разделе «Аудитория».', reply_markup=build_new_menu())
			return
		elif _handle_filter_command(message):
			return


@bot.chat_join_request_handler()
def on_chat_join_request(join_request):
	try:
		process_chat_join_request_update(join_request)
	except Exception as e:
		print(f'Join request handler error: {e}')


@bot.chat_member_handler()
def on_chat_member_update(chat_member_update):
	try:
		process_chat_member_update(chat_member_update)
	except Exception as e:
		print(f'Chat member handler error: {e}')


@bot.message_handler(content_types=['document'])
def receive_session_file(message):
	if message.chat.type != 'private':
		return
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	doc = message.document
	state = USER_STATE.get(message.chat.id, {})
	if not doc:
		return
	if _is_duplicate_upload(message.chat.id, doc):
		return
	file_info = bot.get_file(doc.file_id)
	data = bot.download_file(file_info.file_path)
	filename = os.path.basename(doc.file_name)
	if state.get('flow') == 'audience_import':
		if not str(filename).lower().endswith(('.csv', '.json')):
			bot.send_message(message.chat.id, 'Для импорта аудитории нужен файл .csv или .json.')
			return
		try:
			imported = _import_users_from_bytes(filename, data)
			USER_STATE.pop(message.chat.id, None)
			bot.send_message(
				message.chat.id,
				f'✅ Импорт аудитории завершён.\nЗагружено пользователей: <b>{imported}</b>',
				parse_mode='HTML',
				reply_markup=build_new_menu()
			)
		except Exception as e:
			bot.send_message(message.chat.id, f'Ошибка импорта аудитории: {e}', reply_markup=build_new_menu())
		return
	if not str(filename).lower().endswith(('.session', '.json')):
		bot.send_message(message.chat.id, 'Нужен файл формата .session или .json (как документ).')
		return
	save_session_file(filename, data)
	with open(filename, 'wb') as f:
		f.write(data)
	batch_id, queue_pos = _queue_uploaded_file(message.chat.id, filename)
	_render_upload_batch(message.chat.id, batch_id)


def community_add_step_title(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'community_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление сообщества отменено.', reply_markup=build_new_menu())
		return
	state['title'] = str(message.text or '').strip()
	state['stage'] = 'chat_id'
	_prompt_step(
		message.chat.id,
		state,
		'📂 <b>Сообщество • Шаг 2/6</b>\nВведи числовой <code>chat_id</code> Telegram.',
		community_add_step_chat_id
	)


def community_add_step_chat_id(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'community_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление сообщества отменено.', reply_markup=build_new_menu())
		return
	try:
		state['chat_id'] = int(str(message.text).strip())
	except Exception:
		bot.send_message(message.chat.id, 'Нужен числовой chat_id.')
		_prompt_step(message.chat.id, state, 'Повтори <code>chat_id</code> сообщества.', community_add_step_chat_id)
		return
	state['stage'] = 'type'
	_prompt_step(
		message.chat.id,
		state,
		'📂 <b>Сообщество • Шаг 3/6</b>\nУкажи тип: <code>group</code> или <code>channel</code>.',
		community_add_step_type
	)


def community_add_step_type(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'community_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление сообщества отменено.', reply_markup=build_new_menu())
		return
	community_type = str(message.text or '').strip().lower()
	if community_type not in ['group', 'channel']:
		bot.send_message(message.chat.id, 'Тип должен быть <code>group</code> или <code>channel</code>.', parse_mode='HTML')
		_prompt_step(message.chat.id, state, 'Повтори тип: <code>group</code> / <code>channel</code>.', community_add_step_type)
		return
	state['community_type'] = community_type
	state['stage'] = 'mode'
	_prompt_step(
		message.chat.id,
		state,
		'📂 <b>Сообщество • Шаг 4/6</b>\nУкажи режим по умолчанию: <code>invite_link</code>, <code>join_request</code> или <code>auto</code>.',
		community_add_step_mode
	)


def community_add_step_mode(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'community_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление сообщества отменено.', reply_markup=build_new_menu())
		return
	mode = str(message.text or '').strip().lower()
	if mode not in ['invite_link', 'join_request', 'auto']:
		bot.send_message(message.chat.id, 'Режим должен быть invite_link / join_request / auto.')
		_prompt_step(message.chat.id, state, 'Повтори режим по умолчанию.', community_add_step_mode)
		return
	state['default_invite_mode'] = mode
	state['stage'] = 'auto_approve'
	_prompt_step(
		message.chat.id,
		state,
		'📂 <b>Сообщество • Шаг 5/6</b>\nАвтоодобрение заявок? Ответь: <code>да</code> / <code>нет</code>.',
		community_add_step_auto
	)


def community_add_step_auto(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'community_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление сообщества отменено.', reply_markup=build_new_menu())
		return
	try:
		state['auto_approve_join_requests'] = _parse_yes_no(message.text)
	except Exception:
		bot.send_message(message.chat.id, 'Ответь: да / нет.')
		_prompt_step(message.chat.id, state, 'Автоодобрение заявок? <code>да</code> / <code>нет</code>.', community_add_step_auto)
		return
	state['stage'] = 'strict_moderation'
	_prompt_step(
		message.chat.id,
		state,
		'📂 <b>Сообщество • Шаг 6/6</b>\nСтрогая модерация? Ответь: <code>да</code> / <code>нет</code>.',
		community_add_step_strict
	)


def community_add_step_strict(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'community_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление сообщества отменено.', reply_markup=build_new_menu())
		return
	try:
		strict = _parse_yes_no(message.text)
	except Exception:
		bot.send_message(message.chat.id, 'Ответь: да / нет.')
		_prompt_step(message.chat.id, state, 'Строгая модерация? <code>да</code> / <code>нет</code>.', community_add_step_strict)
		return
	community = COMMUNITY_REPO.create(
		chat_id=state['chat_id'],
		title=state['title'],
		community_type=state['community_type'],
		default_invite_mode=state['default_invite_mode'],
		auto_approve_join_requests=state['auto_approve_join_requests'],
		strict_moderation=strict,
		is_active=True,
	)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		f'✅ <b>Сообщество добавлено</b>\n'
		f'ID: <code>{community.get("id")}</code>\n'
		f'Название: <b>{community.get("title")}</b>\n'
		f'chat_id: <code>{community.get("chat_id")}</code>\n'
		f'Режим: <b>{_community_mode_title(community.get("default_invite_mode"))}</b>',
		reply_markup=_build_communities_menu(),
		parse_mode='HTML'
	)


def audience_add_step_tg_id(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'audience_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление пользователя отменено.', reply_markup=build_new_menu())
		return
	try:
		state['telegram_user_id'] = int(str(message.text).strip())
	except Exception:
		bot.send_message(message.chat.id, 'Нужен числовой Telegram user id.')
		_prompt_step(message.chat.id, state, 'Повтори Telegram user id.', audience_add_step_tg_id)
		return
	state['stage'] = 'username'
	_prompt_step(
		message.chat.id,
		state,
		'👥 <b>Аудитория • Шаг 2/4</b>\nВведи username без <code>@</code> или <code>-</code>.',
		audience_add_step_username
	)


def audience_add_step_username(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'audience_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление пользователя отменено.', reply_markup=build_new_menu())
		return
	value = str(message.text or '').strip()
	state['username'] = '' if value == '-' else value.lstrip('@')
	state['stage'] = 'first_name'
	_prompt_step(
		message.chat.id,
		state,
		'👥 <b>Аудитория • Шаг 3/4</b>\nВведи first_name.',
		audience_add_step_first_name
	)


def audience_add_step_first_name(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'audience_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление пользователя отменено.', reply_markup=build_new_menu())
		return
	state['first_name'] = str(message.text or '').strip()
	state['stage'] = 'source'
	_prompt_step(
		message.chat.id,
		state,
		'👥 <b>Аудитория • Шаг 4/4</b>\nВведи source пользователя или <code>manual</code>.',
		audience_add_step_source
	)


def audience_add_step_source(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'audience_add':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Добавление пользователя отменено.', reply_markup=build_new_menu())
		return
	source_value = str(message.text or '').strip() or 'manual'
	source = AUDIENCE_REPO.get_or_create_source(
		source_type='manual',
		source_value=source_value,
		title=source_value,
		meta_json={'added_by': message.chat.id},
	)
	user = AUDIENCE_REPO.upsert(
		telegram_user_id=state['telegram_user_id'],
		username=state.get('username', ''),
		first_name=state.get('first_name', ''),
		source_id=(source or {}).get('id'),
		discovered_via='manual_add',
		consent_status='manual',
		tags_json=[],
	)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		f'✅ <b>Пользователь добавлен</b>\n'
		f'ID: <code>{user.get("id")}</code>\n'
		f'Telegram ID: <code>{user.get("telegram_user_id")}</code>\n'
		f'Username: <code>@{user.get("username") or "-"}</code>',
		reply_markup=_build_audience_menu(),
		parse_mode='HTML'
	)


def audience_search_step_query(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'audience_search':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Поиск отменён.', reply_markup=build_new_menu())
		return
	query = str(message.text or '').strip()
	results = AUDIENCE_REPO.search(query, limit=15)
	USER_STATE.pop(message.chat.id, None)
	keyboard = types.InlineKeyboardMarkup()
	for user in results:
		label = f'👤 @{user.get("username") or user.get("telegram_user_id")} ({user.get("id")})'
		keyboard.add(types.InlineKeyboardButton(text=label[:60], callback_data=f'audience_user|{user.get("id")}'))
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='audience_menu'))
	text = f'🔎 <b>Поиск аудитории</b>\nЗапрос: <code>{query}</code>\nНайдено: <b>{len(results)}</b>'
	_render_inline(message.chat.id, state.get('panel_msg_id'), text, parse_mode='HTML', reply_markup=keyboard)


def audience_filter_source_step(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'audience_filter_source':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Фильтр отменён.', reply_markup=_build_audience_filters_menu())
		return
	source_value = str(message.text or '').strip()
	filters = _get_audience_filters(message.chat.id)
	if source_value:
		filters['source_value'] = source_value
	else:
		filters.pop('source_value', None)
	_set_audience_filters(message.chat.id, filters)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		_audience_list_text(0, filters=filters),
		parse_mode='HTML',
		reply_markup=_build_audience_list_keyboard(0, filters=filters)
	)


def segment_create_step_name(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'segment_create':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Создание сегмента отменено.', reply_markup=build_new_menu())
		return
	state['name'] = str(message.text or '').strip()
	state['stage'] = 'filters'
	_prompt_step(
		message.chat.id,
		state,
		'🧩 <b>Сегмент • Шаг 2/2</b>\n'
		'Введи фильтр.\n\n'
		'Примеры:\n'
		'<code>all_active</code>\n'
		'<code>source=@chatname</code>\n'
		'<code>tag=vip</code>\n'
		'<code>source=@chatname,tag=vip</code>\n'
		'<code>search=john,username_exists</code>',
		segment_create_step_filters
	)


def segment_create_step_filters(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'segment_create':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Создание сегмента отменено.', reply_markup=build_new_menu())
		return
	filter_json = _parse_segment_filter_text(message.text)
	segment = SEGMENT_REPO.create(state.get('name') or 'Segment', filter_json=filter_json, created_by=message.chat.id)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		'✅ <b>Сегмент создан</b>\n\n' + _segment_detail_text(segment.get('id')),
		parse_mode='HTML',
		reply_markup=_build_segments_menu()
	)


def campaign_create_step_name(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'campaign_create':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Создание кампании отменено.', reply_markup=build_new_menu())
		return
	state['name'] = str(message.text or '').strip()
	state['stage'] = 'community_id'
	communities = COMMUNITY_REPO.list()
	if not communities:
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, 'Сначала добавь хотя бы одно сообщество.', reply_markup=build_new_menu())
		return
	lines = ['🚀 <b>Кампания • Шаг 2/4</b>', 'Выбери community_id из списка:']
	for item in communities[:15]:
		lines.append(f'• <code>{item.get("id")}</code> — {item.get("title")} ({item.get("chat_id")})')
	_prompt_step(message.chat.id, state, '\n'.join(lines), campaign_create_step_community)


def campaign_create_step_community(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'campaign_create':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Создание кампании отменено.', reply_markup=build_new_menu())
		return
	try:
		community_id = int(str(message.text).strip())
	except Exception:
		bot.send_message(message.chat.id, 'Нужен числовой community_id.')
		_prompt_step(message.chat.id, state, 'Повтори community_id.', campaign_create_step_community)
		return
	community = COMMUNITY_REPO.get(community_id)
	if not community:
		bot.send_message(message.chat.id, 'Сообщество не найдено.')
		_prompt_step(message.chat.id, state, 'Повтори community_id из списка.', campaign_create_step_community)
		return
	state['community_id'] = community_id
	state['stage'] = 'invite_mode'
	_prompt_step(
		message.chat.id,
		state,
		'🚀 <b>Кампания • Шаг 3/4</b>\n'
		'Укажи режим: <code>invite_link</code>, <code>join_request</code> или <code>auto</code>.\n'
		'Аудитория по умолчанию: вся активная база.',
		campaign_create_step_mode
	)


def campaign_create_step_mode(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'campaign_create':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Создание кампании отменено.', reply_markup=build_new_menu())
		return
	mode = str(message.text or '').strip().lower()
	if mode not in ['invite_link', 'join_request', 'auto']:
		bot.send_message(message.chat.id, 'Режим должен быть invite_link / join_request / auto.')
		_prompt_step(message.chat.id, state, 'Повтори режим кампании.', campaign_create_step_mode)
		return
	state['invite_mode'] = mode
	state['stage'] = 'message_template'
	_prompt_step(
		message.chat.id,
		state,
		'🚀 <b>Кампания • Шаг 4/4</b>\n'
		'Введи шаблон сообщения.\n'
		'Доступные переменные: <code>{first_name}</code>, <code>{username}</code>, <code>{community_title}</code>, <code>{invite_link}</code>, <code>{campaign_name}</code>.',
		campaign_create_step_message
	)


def campaign_create_step_message(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'campaign_create':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Создание кампании отменено.', reply_markup=build_new_menu())
		return
	recipients = [row['id'] for row in USER_REPO.list(include_blacklisted=False, include_unsubscribed=False)]
	if not recipients:
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, 'В активной аудитории нет пользователей для кампании.', reply_markup=build_new_menu())
		return
	campaign = CAMPAIGN_SERVICE.create_campaign(
		name=state['name'],
		community_id=state['community_id'],
		invite_mode=state['invite_mode'],
		message_template=str(message.text or '').strip(),
		rate_limit_per_minute=_setting_int('campaign_rate_limit_per_minute'),
		rate_limit_per_hour=_setting_int('campaign_rate_limit_per_hour'),
		max_attempts=_setting_int('campaign_max_attempts'),
		stop_on_error_rate=float(str(_get_setting('campaign_stop_on_error_rate') or '0.30').replace(',', '.')),
		created_by=message.chat.id,
		recipients=recipients,
	)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		'✅ <b>Кампания создана</b>\n\n' + _campaign_detail_text(campaign['id']),
		reply_markup=_build_campaign_actions(campaign['id']),
		parse_mode='HTML'
	)


def _queue_parser_job(message, state):
	file_name, sources_count = _save_targets_file(message.chat.id, state.get('sources', ''), 'parser_sources')
	if sources_count == 0:
		bot.send_message(message.chat.id, '⚠️ Не нашёл источники. Запусти парсинг заново.', reply_markup=build_new_menu())
		return
	parse_mode = state.get('parse_mode') or 'members'
	task_meta = {
		'sources': str(state.get('sources') or '').strip(),
		'parse_mode': parse_mode,
		'members_limit': int(state.get('members_limit') or 0),
		'posts_limit': int(state.get('posts_limit') or 0),
		'comments_limit': int(state.get('comments_limit') or 0),
		'messages_limit': int(state.get('messages_limit') or 0),
		'use_all_sessions': _setting_bool('parser_use_all_sessions'),
	}
	task = PARSE_TASK_REPO.create(
		mode=parse_mode,
		source_type='chat',
		source_value=str(state.get('sources') or '').strip(),
		status='queued',
		created_by=message.chat.id,
		meta_json=task_meta,
	)
	command = [
		sys.executable, os.path.join('workers', 'parser_worker.py'),
		'--mode', parse_mode,
		'--targets-file', file_name,
		'--task-id', str(task.get('id')),
	]
	if parse_mode == 'members':
		command += ['--members-limit', str(int(state.get('members_limit') or _setting_int('parser_posts_limit')))]
	elif parse_mode in ('commenters', 'engaged_users'):
		command += [
			'--posts-limit', str(int(state.get('posts_limit') or _setting_int('parser_posts_limit'))),
			'--comments-limit', str(int(state.get('comments_limit') or _setting_int('parser_comments_limit'))),
		]
	elif parse_mode == 'message_authors':
		command += ['--messages-limit', str(int(state.get('messages_limit') or _setting_int('parser_posts_limit')))]
	if _setting_bool('parser_use_all_sessions'):
		command.append('--use-all-sessions')
	progress_file = _progress_file(message.chat.id, 'parser')
	command += ['--progress-file', progress_file]
	queue_pos = _enqueue_process(
		message.chat.id,
		f'parser {parse_mode} ({sources_count} sources)',
		command,
		progress_file=progress_file
	)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		f'✅ <b>Парсинг поставлен в очередь</b>\n'
		f'• Task ID: <b>{task.get("id")}</b>\n'
		f'• Mode: <b>{_parser_mode_title(parse_mode)}</b>\n'
		f'• Источников: <b>{sources_count}</b>\n'
		f'• Позиция: <b>~{queue_pos}</b>\n'
		'Как только задача стартует — прогресс пойдёт в <code>parse_tasks</code> и live-status.',
		parse_mode='HTML',
		reply_markup=build_new_menu()
	)


def _queue_parser_task_from_record(chat_id, task, panel_msg_id=None):
	task = task or {}
	meta = task.get('meta_json') or {}
	sources = str(meta.get('sources') or task.get('source_value') or '').strip()
	file_name, sources_count = _save_targets_file(chat_id, sources, 'parser_sources_repeat')
	if sources_count == 0:
		_render_inline(
			chat_id,
			panel_msg_id,
			build_confirm_screen('🔄 Повтор запуска', summary='У последней задачи нет сохранённых источников.'),
			reply_markup=_build_parser_menu(),
			parse_mode='HTML'
		)
		return
	parse_mode = str(meta.get('parse_mode') or task.get('mode') or 'members').strip()
	repeat_task = PARSE_TASK_REPO.create(
		mode=parse_mode,
		source_type=task.get('source_type') or 'chat',
		source_value=sources,
		status='queued',
		created_by=chat_id,
		meta_json=meta,
	)
	command = [
		sys.executable, os.path.join('workers', 'parser_worker.py'),
		'--mode', parse_mode,
		'--targets-file', file_name,
		'--task-id', str(repeat_task.get('id')),
	]
	if parse_mode == 'members':
		command += ['--members-limit', str(int(meta.get('members_limit') or _setting_int('parser_posts_limit')))]
	elif parse_mode in ('commenters', 'engaged_users'):
		command += [
			'--posts-limit', str(int(meta.get('posts_limit') or _setting_int('parser_posts_limit'))),
			'--comments-limit', str(int(meta.get('comments_limit') or _setting_int('parser_comments_limit'))),
		]
	elif parse_mode == 'message_authors':
		command += ['--messages-limit', str(int(meta.get('messages_limit') or _setting_int('parser_posts_limit')))]
	if bool(meta.get('use_all_sessions')):
		command.append('--use-all-sessions')
	progress_file = _progress_file(chat_id, 'parser')
	command += ['--progress-file', progress_file]
	queue_pos = _enqueue_process(chat_id, f'parser {parse_mode} ({sources_count} sources)', command, progress_file=progress_file)
	_render_inline(
		chat_id,
		panel_msg_id,
		build_confirm_screen(
			'🔄 Последний запуск повторён',
			summary=[
				f'Task ID: {repeat_task.get("id")}',
				f'Режим: {_parser_mode_title(parse_mode)}',
				f'Источников: {sources_count}',
				f'Позиция: ~{queue_pos}',
			],
		),
		reply_markup=build_new_menu(),
		parse_mode='HTML'
	)


def parser_step_sources(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий парсинга. Открой раздел «📡 Парсинг» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	state['sources'] = message.text
	parse_mode = state.get('parse_mode') or 'members'
	if parse_mode == 'members':
		state['stage'] = 'members_limit'
		USER_STATE[message.chat.id] = state
		_prompt_step(
			message.chat.id,
			state,
			f'🔎 <b>Парсинг • Шаг 2/2</b>\n'
			f'Режим: <b>{_parser_mode_title(parse_mode)}</b>\n'
			f'Укажи лимит участников на источник.\n'
			f'Рекомендация: <b>{_setting_int("parser_posts_limit")}</b>.',
			parser_step_members_limit
		)
		return
	if parse_mode == 'message_authors':
		state['stage'] = 'messages_limit'
		USER_STATE[message.chat.id] = state
		_prompt_step(
			message.chat.id,
			state,
			f'🔎 <b>Парсинг • Шаг 2/2</b>\n'
			f'Режим: <b>{_parser_mode_title(parse_mode)}</b>\n'
			f'Укажи лимит сообщений на источник.\n'
			f'Рекомендация: <b>{_setting_int("parser_posts_limit")}</b>.',
			parser_step_messages_limit
		)
		return
	state['stage'] = 'posts'
	USER_STATE[message.chat.id] = state
	_prompt_step(
		message.chat.id,
		state,
		f'🔎 <b>Парсинг • Шаг 2/3</b>\n'
		f'Режим: <b>{_parser_mode_title(parse_mode)}</b>\n'
		f'{"Выбери глубину анализа по постам и сообщениям." if parse_mode == "engaged_users" else "Выбери глубину анализа по постам."}\n'
		f'Рекомендация: <b>{_setting_int("parser_posts_limit")}</b>.',
		parser_step_posts
	)


def parser_step_members_limit(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	try:
		state['members_limit'] = int(str(message.text).strip())
	except (TypeError, ValueError):
		state['members_limit'] = _setting_int('parser_posts_limit')
	_queue_parser_job(message, state)


def parser_step_posts(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий парсинга. Открой раздел «📡 Парсинг» заново.', reply_markup=build_new_menu())
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
		f'{"Укажи лимит комментариев на источник. Авторы сообщений будут собраны по лимиту из прошлого шага." if (state.get("parse_mode") or "members") == "engaged_users" else "Укажи лимит комментариев на источник."}\n'
		f'Рекомендация: <b>{_setting_int("parser_comments_limit")}</b>.',
		parser_step_comments
	)


def parser_step_comments(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий парсинга. Открой раздел «📡 Парсинг» заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	try:
		state['comments_limit'] = int(message.text.strip())
	except (TypeError, ValueError):
		state['comments_limit'] = _setting_int('parser_comments_limit')
	_queue_parser_job(message, state)


def parser_step_messages_limit(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'parser':
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	try:
		state['messages_limit'] = int(str(message.text).strip())
	except (TypeError, ValueError):
		state['messages_limit'] = _setting_int('parser_posts_limit')
	_queue_parser_job(message, state)


def join_target_step_target(message):
	if not _is_admin(message.chat.id):
		_deny_access(message.chat.id)
		return
	state = USER_STATE.get(message.chat.id, {})
	if state.get('flow') != 'join_target':
		bot.send_message(message.chat.id, '⚠️ Сейчас неактивен сценарий входа в цель. Нажми кнопку заново.', reply_markup=build_new_menu())
		return
	if _is_cancel(message.text):
		USER_STATE.pop(message.chat.id, None)
		bot.send_message(message.chat.id, '❌ Действие отменено.', reply_markup=build_new_menu())
		return
	target = str(message.text or '').strip()
	if target == '':
		bot.send_message(message.chat.id, '⚠️ Цель пустая. Введи @chat, @channel или ссылку-приглашение.', reply_markup=_step_keyboard())
		return
	try:
		parsed_target = parse_target(target)
	except Exception as e:
		bot.send_message(
			message.chat.id,
			f'⚠️ Не удалось разобрать цель.\n{e}\n\nПоддерживается:\n<code>@name</code>\n<code>https://t.me/name</code>\n<code>https://t.me/+HASH</code>\n<code>https://t.me/joinchat/HASH</code>',
			parse_mode='HTML',
			reply_markup=_step_keyboard()
		)
		return
	command = [
		sys.executable, 'target_joiner.py',
		'--target', target,
	]
	progress_file = _progress_file(message.chat.id, 'join_target')
	command += ['--progress-file', progress_file]
	queue_pos = _enqueue_process(message.chat.id, 'join target', command, progress_file=progress_file)
	USER_STATE.pop(message.chat.id, None)
	_render_inline(
		message.chat.id,
		state.get('panel_msg_id'),
		f'✅ <b>Вход аккаунтов поставлен в очередь</b>\n'
		f'• Цель: <code>{parsed_target.display_value}</code>\n'
		f'• Тип: <b>{"private invite" if is_invite_target_type(parsed_target.target_type) else "public username"}</b>\n'
		f'• Метод: <code>{parsed_target.join_method}</code>\n'
		f'• Аккаунтов: <b>{len(list_sessions())}</b>\n'
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
	try:
		if key == 'campaign_stop_on_error_rate':
			float(val.replace(',', '.'))
			val = val.replace(',', '.')
		else:
			int(val)
	except Exception:
		bot.send_message(message.chat.id, 'Некорректное значение. Попробуйте ещё раз.', reply_markup=_step_keyboard())
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
		_render_inline(call.message.chat.id, call.message.message_id, _main_dashboard_text(), reply_markup=build_new_menu(), parse_mode='HTML')
		return

	if call.data == 'quick_parse':
		_render_inline(call.message.chat.id, call.message.message_id, _parsing_text(), parse_mode='HTML', reply_markup=_build_parser_menu())
		return

	if call.data == 'quick_repeat_last':
		last_task = PARSE_TASK_REPO.list(limit=1)
		if not last_task:
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				build_confirm_screen('🔄 Повтор запуска', summary='Нет предыдущей задачи парсинга для повтора.'),
				parse_mode='HTML',
				reply_markup=build_new_menu()
			)
			return
		_queue_parser_task_from_record(call.message.chat.id, last_task[0], panel_msg_id=call.message.message_id)
		return

	if call.data == 'quick_accounts_check':
		call.data = 'accounts_check_all'

	if call.data == 'favorites_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _favorites_text(), parse_mode='HTML', reply_markup=_build_favorites_menu())
		return

	if call.data.startswith('favorites_view|'):
		kind = call.data.split('|', 1)[1]
		_render_inline(call.message.chat.id, call.message.message_id, _favorites_text(kind), parse_mode='HTML', reply_markup=_build_favorites_menu())
		return

	if call.data.startswith('favorite_toggle|'):
		parts = call.data.split('|')
		entity_type = parts[1]
		entity_id = parts[2]
		page = parts[3] if len(parts) > 3 else '0'
		_toggle_favorite(entity_type, entity_id)
		if entity_type == 'campaigns':
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				_campaign_detail_text(int(entity_id)),
				parse_mode='HTML',
				reply_markup=_build_campaign_actions(int(entity_id), page=page)
			)
			return
		if entity_type == 'segments':
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				_segment_detail_text(int(entity_id)),
				parse_mode='HTML',
				reply_markup=_build_segment_actions(int(entity_id))
			)
			return
		if entity_type == 'communities':
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				_community_detail_text(int(entity_id)),
				parse_mode='HTML',
				reply_markup=_build_community_actions(int(entity_id), page=page)
			)
			return
		return

	if call.data == 'settings_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _settings_text(), parse_mode='HTML', reply_markup=_build_settings_menu())
		return

	if call.data in ['settings_logic', 'settings_limits', 'settings_notifications', 'settings_interface', 'settings_security']:
		category = call.data.split('_', 1)[1]
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_settings_category_text(category),
			parse_mode='HTML',
			reply_markup=_build_settings_detail_menu(category)
		)
		return

	if call.data == 'parsing_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _parsing_text(), parse_mode='HTML', reply_markup=_build_parser_menu())
		return

	if call.data == 'parse_tasks_list' or call.data.startswith('parse_tasks_list|'):
		page = 0
		if '|' in call.data:
			page = call.data.split('|', 1)[1]
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_parse_tasks_list_text(page),
			parse_mode='HTML',
			reply_markup=_build_parse_tasks_list_keyboard(page)
		)
		return

	if call.data.startswith('parse_task_view|'):
		parts = call.data.split('|')
		task_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_parse_task_detail_text(task_id),
			parse_mode='HTML',
			reply_markup=_build_parse_task_actions(task_id, page=page)
		)
		return

	if call.data.startswith('parse_task_sources|'):
		parts = call.data.split('|')
		task_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_parse_task_sources_text(task_id),
			parse_mode='HTML',
			reply_markup=_build_parse_task_actions(task_id, page=page)
		)
		return

	if call.data.startswith('parse_task_repeat|'):
		parts = call.data.split('|')
		task_id = int(parts[1])
		task = PARSE_TASK_REPO.get(task_id)
		if not task:
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				build_confirm_screen('🔄 Повтор задачи', summary='Задача не найдена.'),
				parse_mode='HTML',
				reply_markup=_build_parser_menu()
			)
			return
		_queue_parser_task_from_record(call.message.chat.id, task, panel_msg_id=call.message.message_id)
		return

	if call.data == 'parser_repeat_last':
		last_task = PARSE_TASK_REPO.list(limit=1)
		if not last_task:
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				build_confirm_screen('🔄 Повтор запуска', summary='Нет предыдущей задачи для повтора.'),
				parse_mode='HTML',
				reply_markup=_build_parser_menu()
			)
			return
		_queue_parser_task_from_record(call.message.chat.id, last_task[0], panel_msg_id=call.message.message_id)
		return

	if call.data == 'communities_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _communities_text(), parse_mode='HTML', reply_markup=_build_communities_menu())
		return

	if call.data == 'communities_links':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('🔗 Invite Links', 'Управление ссылками будет вынесено в отдельный экран.'), parse_mode='HTML', reply_markup=_build_communities_menu())
		return

	if call.data == 'communities_modes':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('⚙️ Режимы', 'Здесь будут быстрые настройки invite_link / join_request / auto.'), parse_mode='HTML', reply_markup=_build_communities_menu())
		return

	if call.data == 'communities_list' or call.data.startswith('communities_list|'):
		page = 0
		if '|' in call.data:
			page = call.data.split('|', 1)[1]
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_communities_list_text(page),
			parse_mode='HTML',
			reply_markup=_build_communities_list_keyboard(page)
		)
		return

	if call.data.startswith('community_view|'):
		parts = call.data.split('|')
		community_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_community_detail_text(community_id),
			parse_mode='HTML',
			reply_markup=_build_community_actions(community_id, page=page)
		)
		return

	if call.data == 'community_add_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		state = {'flow': 'community_add', 'stage': 'title', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'📂 <b>Сообщество • Шаг 1/6</b>\nВведи название сообщества.',
			community_add_step_title
		)
		return

	if call.data == 'audience_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _audience_text(), parse_mode='HTML', reply_markup=_build_audience_menu())
		return

	if call.data == 'audience_filters_menu':
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_audience_filters_text(call.message.chat.id),
			parse_mode='HTML',
			reply_markup=_build_audience_filters_menu()
		)
		return

	if call.data == 'audience_filter_source_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		state = {'flow': 'audience_filter_source', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'🧭 <b>Фильтр аудитории</b>\nВведи source_value источника, например <code>@chatname</code>.',
			audience_filter_source_step
		)
		return

	if call.data.startswith('audience_filter_recent|'):
		days = int(call.data.split('|', 1)[1])
		since = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
		filters = _get_audience_filters(call.message.chat.id)
		filters['discovered_after'] = since
		_set_audience_filters(call.message.chat.id, filters)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_audience_list_text(0, filters=filters),
			parse_mode='HTML',
			reply_markup=_build_audience_list_keyboard(0, filters=filters)
		)
		return

	if call.data == 'audience_filters_clear':
		_set_audience_filters(call.message.chat.id, {})
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_audience_list_text(0, filters={}),
			parse_mode='HTML',
			reply_markup=_build_audience_list_keyboard(0, filters={})
		)
		return

	if call.data == 'audience_export':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('📤 Экспорт', 'Экспорт сегментов и выборок будет добавлен отдельным мастером.'), parse_mode='HTML', reply_markup=_build_audience_menu())
		return

	if call.data == 'audience_add_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		state = {'flow': 'audience_add', 'stage': 'telegram_user_id', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'👥 <b>Аудитория • Шаг 1/4</b>\nВведи числовой Telegram user id.',
			audience_add_step_tg_id
		)
		return

	if call.data == 'audience_import_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		USER_STATE[call.message.chat.id] = {'flow': 'audience_import', 'panel_msg_id': call.message.message_id}
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			'⬆️ <b>Импорт аудитории</b>\nОтправь файл <code>.csv</code> или <code>.json</code> документом.\n\n'
			'Поддерживаемые поля:\n'
			'<code>telegram_user_id,username,first_name,last_name,source,consent_status,tags,is_blacklisted,unsubscribed_at</code>',
			parse_mode='HTML',
			reply_markup=_step_keyboard()
		)
		return

	if call.data == 'audience_list' or call.data.startswith('audience_list|'):
		page = 0
		if '|' in call.data:
			page = call.data.split('|', 1)[1]
		filters = _get_audience_filters(call.message.chat.id)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_audience_list_text(page, filters=filters),
			parse_mode='HTML',
			reply_markup=_build_audience_list_keyboard(page, filters=filters)
		)
		return

	if call.data == 'audience_search_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		state = {'flow': 'audience_search', 'stage': 'query', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'🔎 <b>Поиск аудитории</b>\nВведи username, first_name или Telegram user id.',
			audience_search_step_query
		)
		return

	if call.data == 'audience_blacklist_list':
		users = AUDIENCE_REPO.list(limit=12)
		keyboard = types.InlineKeyboardMarkup()
		for user in users:
			marker = '✅' if user.get('is_blacklisted') else '🚫'
			keyboard.add(
				types.InlineKeyboardButton(
					text=f'{marker} @{user.get("username") or user.get("telegram_user_id")} ({user.get("id")})'[:60],
					callback_data=f'audience_toggle_blacklist|{user.get("id")}'
				)
			)
		keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='audience_menu'))
		_render_inline(call.message.chat.id, call.message.message_id, '🚫 <b>Blacklist аудитории</b>\nНажми на пользователя, чтобы переключить статус.', parse_mode='HTML', reply_markup=keyboard)
		return

	if call.data.startswith('audience_user|'):
		parts = call.data.split('|')
		user_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_user_detail_text(user_id),
			parse_mode='HTML',
			reply_markup=_build_user_actions(user_id, page=page)
		)
		return

	if call.data.startswith('audience_toggle_blacklist|'):
		parts = call.data.split('|')
		user_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		user = AUDIENCE_REPO.get(user_id)
		if not user:
			bot.send_message(call.message.chat.id, 'Пользователь не найден.')
			return
		AUDIENCE_REPO.blacklist(user_id, is_blacklisted=not bool(user.get('is_blacklisted')))
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_user_detail_text(user_id),
			parse_mode='HTML',
			reply_markup=_build_user_actions(user_id, page=page)
		)
		return

	if call.data.startswith('audience_unsubscribe|'):
		parts = call.data.split('|')
		user_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		AUDIENCE_REPO.unsubscribe(user_id)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_user_detail_text(user_id),
			parse_mode='HTML',
			reply_markup=_build_user_actions(user_id, page=page)
		)
		return

	if call.data == 'segments_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _segments_text(), parse_mode='HTML', reply_markup=_build_segments_menu())
		return

	if call.data == 'segments_list':
		keyboard = types.InlineKeyboardMarkup()
		for item in SEGMENT_REPO.list(limit=12):
			keyboard.add(
				types.InlineKeyboardButton(
					text=f'🧩 {item.get("name")} ({item.get("id")})'[:60],
					callback_data=f'segment_view|{item.get("id")}'
				)
			)
		keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='segments_menu'))
		_render_inline(call.message.chat.id, call.message.message_id, _segments_text(), parse_mode='HTML', reply_markup=keyboard)
		return

	if call.data == 'segment_create_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		state = {'flow': 'segment_create', 'stage': 'name', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'🧩 <b>Сегмент • Шаг 1/2</b>\nВведи название сегмента.',
			segment_create_step_name
		)
		return

	if call.data.startswith('segment_view|'):
		segment_id = int(call.data.split('|', 1)[1])
		_render_inline(call.message.chat.id, call.message.message_id, _segment_detail_text(segment_id), parse_mode='HTML', reply_markup=_build_segment_actions(segment_id))
		return

	if call.data == 'campaigns_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _campaigns_text(), parse_mode='HTML', reply_markup=_build_campaigns_menu())
		return

	if call.data == 'campaigns_quick_start':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('▶️ Быстрый запуск', 'Быстрый запуск появится после шаблонов кампаний.'), parse_mode='HTML', reply_markup=_build_campaigns_menu())
		return

	if call.data == 'campaigns_quick_pause':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('⏸ Быстрая пауза', 'Массовая пауза кампаний будет доступна позже.'), parse_mode='HTML', reply_markup=_build_campaigns_menu())
		return

	if call.data == 'campaign_templates':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('📦 Шаблоны', 'Шаблоны кампаний вынесены в следующий этап.'), parse_mode='HTML', reply_markup=_build_campaigns_menu())
		return

	if call.data == 'campaigns_list' or call.data.startswith('campaigns_list|'):
		page = 0
		if '|' in call.data:
			page = call.data.split('|', 1)[1]
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_campaigns_list_text(page),
			parse_mode='HTML',
			reply_markup=_build_campaigns_list_keyboard(page)
		)
		return

	if call.data == 'campaign_create_start':
		if _has_active_flow(call.message.chat.id):
			cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		if not COMMUNITY_REPO.list():
			bot.send_message(call.message.chat.id, 'Сначала добавь хотя бы одно сообщество.', reply_markup=build_new_menu())
			return
		if USER_REPO.summary().get('active', 0) <= 0:
			bot.send_message(call.message.chat.id, 'Сначала добавь аудиторию.', reply_markup=build_new_menu())
			return
		state = {'flow': 'campaign_create', 'stage': 'name', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'🚀 <b>Кампания • Шаг 1/4</b>\nВведи название кампании.',
			campaign_create_step_name
		)
		return

	if call.data.startswith('campaign_view|'):
		parts = call.data.split('|')
		campaign_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_campaign_detail_text(campaign_id),
			parse_mode='HTML',
			reply_markup=_build_campaign_actions(campaign_id, page=page)
		)
		return

	if call.data.startswith('campaign_start|'):
		parts = call.data.split('|')
		campaign_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		CAMPAIGN_SERVICE.start_campaign(campaign_id)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_campaign_detail_text(campaign_id) + '\n\n🕒 Кампания переведена в scheduled.',
			parse_mode='HTML',
			reply_markup=_build_campaign_actions(campaign_id, page=page)
		)
		return

	if call.data.startswith('campaign_pause|'):
		parts = call.data.split('|')
		campaign_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		CAMPAIGN_SERVICE.pause_campaign(campaign_id)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_campaign_detail_text(campaign_id),
			parse_mode='HTML',
			reply_markup=_build_campaign_actions(campaign_id, page=page)
		)
		return

	if call.data.startswith('campaign_resume|'):
		parts = call.data.split('|')
		campaign_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		CAMPAIGN_SERVICE.resume_campaign(campaign_id)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_campaign_detail_text(campaign_id),
			parse_mode='HTML',
			reply_markup=_build_campaign_actions(campaign_id, page=page)
		)
		return

	if call.data.startswith('campaign_cancel|'):
		parts = call.data.split('|')
		campaign_id = int(parts[1])
		page = parts[2] if len(parts) > 2 else 0
		CAMPAIGN_SERVICE.cancel_campaign(campaign_id)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_campaign_detail_text(campaign_id),
			parse_mode='HTML',
			reply_markup=_build_campaign_actions(campaign_id, page=page)
		)
		return

	if call.data == 'join_requests_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _join_requests_text('pending'), parse_mode='HTML', reply_markup=_build_join_requests_menu())
		return

	if call.data == 'join_requests_auto_mode':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('⚡ Авто-режим', 'Правила автоодобрения будут на отдельном экране.'), parse_mode='HTML', reply_markup=_build_join_requests_menu())
		return

	if call.data.startswith('join_requests_view|'):
		status = call.data.split('|', 1)[1]
		keyboard = types.InlineKeyboardMarkup()
		for item in JOIN_REQUEST_REPO.list(status=status, limit=10):
			if status == 'pending':
				keyboard.add(
					types.InlineKeyboardButton(text=f'✅ {item.get("id")}', callback_data=f'join_request_approve|{item.get("id")}'),
					types.InlineKeyboardButton(text=f'❌ {item.get("id")}', callback_data=f'join_request_decline|{item.get("id")}')
				)
			else:
				keyboard.add(types.InlineKeyboardButton(text=f'ID {item.get("id")} | tg {item.get("telegram_user_id")}', callback_data='join_requests_menu'))
		keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='join_requests_menu'))
		_render_inline(call.message.chat.id, call.message.message_id, _join_requests_text(status), parse_mode='HTML', reply_markup=keyboard)
		return

	if call.data.startswith('join_request_approve|'):
		join_request_id = int(call.data.split('|', 1)[1])
		try:
			JOIN_REQUEST_SERVICE.approve_join_request(join_request_id, moderator_id=call.from_user.id, reason='manual_approve')
			bot.answer_callback_query(call.id, 'Заявка одобрена')
		except Exception as e:
			bot.answer_callback_query(call.id, f'Ошибка: {e}')
		_render_inline(call.message.chat.id, call.message.message_id, _join_requests_text('pending'), parse_mode='HTML', reply_markup=_build_join_requests_menu())
		return

	if call.data.startswith('join_request_decline|'):
		join_request_id = int(call.data.split('|', 1)[1])
		try:
			JOIN_REQUEST_SERVICE.decline_join_request(join_request_id, moderator_id=call.from_user.id, reason='manual_decline')
			bot.answer_callback_query(call.id, 'Заявка отклонена')
		except Exception as e:
			bot.answer_callback_query(call.id, f'Ошибка: {e}')
		_render_inline(call.message.chat.id, call.message.message_id, _join_requests_text('pending'), parse_mode='HTML', reply_markup=_build_join_requests_menu())
		return

	if call.data == 'filters_menu':
		_render_inline(call.message.chat.id, call.message.message_id, 'Фильтры выведены из основного меню. Используй blacklist/unsubscribe в разделе «Аудитория».', parse_mode='HTML', reply_markup=build_new_menu())
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

	if call.data.startswith('settings_interface_mode|'):
		mode = call.data.split('|', 1)[1]
		if mode not in ['compact', 'pro']:
			bot.answer_callback_query(call.id, 'Неизвестный режим')
			return
		_set_setting('interface_mode', mode)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			f'✅ Режим интерфейса: <b>{mode.capitalize()}</b>\n\n' + _settings_category_text('interface'),
			parse_mode='HTML',
			reply_markup=_build_settings_detail_menu('interface')
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
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_accounts_text(),
			reply_markup=_build_accounts_menu(),
			parse_mode='HTML'
		)
		return

	if call.data == 'accounts_upload_help':
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			build_section_screen('🔑 Добавление аккаунтов', description='Отправь .session или .json файлом. Проверка начнется автоматически после загрузки.'),
			parse_mode='HTML',
			reply_markup=_build_accounts_menu()
		)
		return

	if call.data == 'accounts_groups':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('🧩 Группы аккаунтов', 'Группы и теги аккаунтов будут добавлены отдельным экраном.'), parse_mode='HTML', reply_markup=_build_accounts_menu())
		return

	if call.data == 'accounts_filter_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('🔎 Фильтр аккаунтов', 'Фильтры по статусу, проверке и группе будут добавлены следующим этапом.'), parse_mode='HTML', reply_markup=_build_accounts_list_keyboard(0))
		return

	if call.data == 'accounts_list' or call.data.startswith('accounts_list|'):
		page = 0
		if '|' in call.data:
			page = call.data.split('|', 1)[1]
		if len(list_sessions()) == 0:
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				'📋 <b>Аккаунты</b>\n\nСписок пока пуст.',
				parse_mode='HTML',
				reply_markup=_build_accounts_menu()
			)
			return
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_accounts_list_text(page),
			parse_mode='HTML',
			reply_markup=_build_accounts_list_keyboard(page)
		)
		return

	if call.data.startswith('account_open|'):
		parts = call.data.split('|')
		account_index = parts[1]
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_account_card_text(account_index),
			parse_mode='HTML',
			reply_markup=_build_account_card_keyboard(account_index, page=page)
		)
		return

	if call.data.startswith('account_check|'):
		parts = call.data.split('|')
		account_index = parts[1]
		page = parts[2] if len(parts) > 2 else 0
		rows = _account_rows()
		try:
			item = rows[int(account_index)]
		except Exception:
			bot.answer_callback_query(call.id, 'Аккаунт не найден')
			return
		result = _check_account_health(item.get('session'), deep_check=False)
		prev = _store_account_health_result(item.get('session'), result)
		_notify_health_change_if_needed(item.get('session'), prev, result)
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_account_card_text(account_index),
			parse_mode='HTML',
			reply_markup=_build_account_card_keyboard(account_index, page=page)
		)
		return

	if call.data.startswith('account_edit|'):
		parts = call.data.split('|')
		account_index = parts[1]
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('✏️ Изменение аккаунта', 'Редактирование метаданных аккаунта будет добавлено следующим этапом.'), parse_mode='HTML', reply_markup=_build_account_card_keyboard(account_index, page=page))
		return

	if call.data.startswith('account_disable|'):
		parts = call.data.split('|')
		account_index = parts[1]
		page = parts[2] if len(parts) > 2 else 0
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('⏸ Отключение аккаунта', 'Массовое включение и отключение аккаунтов будет вынесено в отдельный модуль.'), parse_mode='HTML', reply_markup=_build_account_card_keyboard(account_index, page=page))
		return

	if call.data.startswith('account_delete|'):
		parts = call.data.split('|')
		account_index = parts[1]
		page = parts[2] if len(parts) > 2 else 0
		rows = _account_rows()
		try:
			item = rows[int(account_index)]
			filename = item.get('session')
		except Exception:
			bot.answer_callback_query(call.id, 'Аккаунт не найден')
			return
		try:
			if os.path.exists(filename):
				os.remove(filename)
			delete_session_file(filename)
			bot.answer_callback_query(call.id, 'Аккаунт удален')
		except Exception as e:
			bot.answer_callback_query(call.id, f'Ошибка: {e}')
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_accounts_list_text(page),
			parse_mode='HTML',
			reply_markup=_build_accounts_list_keyboard(page)
		)
		return

	if call.data == 'accounts_check_all':
		sessions = list_sessions()
		if len(sessions) == 0:
			bot.send_message(call.message.chat.id, 'Аккаунты не добавлены.')
			return
		bot.send_message(call.message.chat.id, '🔍 Запускаю проверку аккаунтов...')
		counts = {
			'working': 0,
			'limited': 0,
			'flooded': 0,
			'dead': 0,
			'invalid': 0,
		}
		problem_lines = []
		for s in sessions:
			result = _check_account_health(s, deep_check=False)
			prev = _store_account_health_result(s, result)
			_notify_health_change_if_needed(s, prev, result)
			counts[result.status] = counts.get(result.status, 0) + 1
			if result.status != 'working':
				problem_lines.append(
					f'{_account_status_emoji(result.status)} <code>{s}</code> — <code>{result.reason_code}</code>'
				)
		text = (
			'🩺 <b>Проверка аккаунтов завершена</b>\n\n'
			f'Всего: <b>{len(sessions)}</b>\n'
			f'🟢 Рабочие: <b>{counts.get("working", 0)}</b>\n'
			f'🟡 Ограничены: <b>{counts.get("limited", 0)}</b>\n'
			f'🟠 Flood: <b>{counts.get("flooded", 0)}</b>\n'
			f'🔴 Мёртвые: <b>{counts.get("dead", 0)}</b>\n'
			f'⚫ Не авторизованы: <b>{counts.get("invalid", 0)}</b>'
		)
		if problem_lines:
			text += '\n\n' + '\n'.join(problem_lines[:10])
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			text,
			parse_mode='HTML',
			reply_markup=_build_accounts_menu()
		)
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

	if call.data == 'accounts_join_target_start':
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
		state = {'flow': 'join_target', 'stage': 'target', 'panel_msg_id': call.message.message_id}
		USER_STATE[call.message.chat.id] = state
		_prompt_step(
			call.message.chat.id,
			state,
			'➕ <b>Вход аккаунтов в цель</b>\n'
			'Отправь цель, куда нужно завести все загруженные аккаунты.\n\n'
			'Поддерживается:\n'
			'<code>@channel</code>\n'
			'<code>@group</code>\n'
			'<code>https://t.me/+HASH</code>\n'
			'<code>https://t.me/...</code>\n'
			'<code>https://t.me/joinchat/...</code>',
			join_target_step_target
		)
		return

	if call.data.startswith('parser_mode|'):
		parse_mode = call.data.split('|', 1)[1]
		if parse_mode == 'import_file':
			if _has_active_flow(call.message.chat.id):
				cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
				_render_inline(
					call.message.chat.id,
					call.message.message_id,
					f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
					parse_mode='HTML',
					reply_markup=_step_keyboard()
				)
				return
			USER_STATE[call.message.chat.id] = {'flow': 'audience_import', 'panel_msg_id': call.message.message_id}
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				'⬆️ <b>Импорт аудитории</b>\nОтправь файл <code>.csv</code> или <code>.json</code> документом.\n\n'
				'Данные попадут в <code>audience_users</code> и будут доступны для сегментов.',
				parse_mode='HTML',
				reply_markup=_step_keyboard()
			)
			return
		if parse_mode == 'manual_add':
			if _has_active_flow(call.message.chat.id):
				cur = USER_STATE.get(call.message.chat.id, {}).get('flow')
				_render_inline(
					call.message.chat.id,
					call.message.message_id,
					f'⚠️ Уже активен сценарий: <b>{_flow_title(cur)}</b>.\nЧтобы начать новый, сначала отмени текущий.',
					parse_mode='HTML',
					reply_markup=_step_keyboard()
				)
				return
			state = {'flow': 'audience_add', 'stage': 'telegram_user_id', 'panel_msg_id': call.message.message_id}
			USER_STATE[call.message.chat.id] = state
			_prompt_step(
				call.message.chat.id,
				state,
				'👥 <b>Аудитория • Шаг 1/4</b>\nВведи числовой Telegram user id.',
				audience_add_step_tg_id
			)
			return
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
		state = {'flow': 'parser', 'stage': 'sources', 'panel_msg_id': call.message.message_id, 'parse_mode': parse_mode}
		USER_STATE[call.message.chat.id] = state
		mode_text = 'Шаг 1/2' if parse_mode in ['members', 'message_authors'] else 'Шаг 1/3'
		_prompt_step(
			call.message.chat.id,
			state,
			f'📡 <b>Парсинг</b>\n\n'
			f'Режим: <b>{_parser_mode_title(parse_mode)}</b>\n'
			'Введи ссылку или username:\n\n'
			'<code>@chat1\n@chat2</code>',
			parser_step_sources
		)
		return

	if call.data == 'parser_start':
		_render_inline(call.message.chat.id, call.message.message_id, _parsing_text(), parse_mode='HTML', reply_markup=_build_parser_menu())
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
			_render_inline(
				call.message.chat.id,
				call.message.message_id,
				_status_text(),
				reply_markup=_build_status_menu(),
				parse_mode='HTML'
			)
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
					elif p.get('mode') == 'join_target':
						extra = (
							f' | joined={p.get("joined", 0)} '
							f'already={p.get("already_in", p.get("already", 0))} '
							f'failed={p.get("failed", 0)}'
						)
					elif p.get('mode') == 'inviter':
						extra = f' | invited={p.get("invited", 0)} processed={p.get("processed", 0)}'
				except Exception:
					extra = ''
			lines.append(f'{idx}. {item["title"]} - {status} - {pid_text}{extra}')
		lines.append(f'Очередь: {TASK_QUEUE.qsize()}')
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_status_text() + '\n\n' + '<b>Активные процессы</b>\n' + '\n'.join(lines),
			reply_markup=_build_status_menu(),
			parse_mode='HTML'
		)
		return

	if call.data == 'status_errors':
		reply_markup = build_inline_keyboard([
			[('📋 Список', 'errors_list|0')],
			[('⬅️ Назад', 'task_status')],
		])
		_render_inline(call.message.chat.id, call.message.message_id, _errors_overview_text(call.message.chat.id), reply_markup=reply_markup, parse_mode='HTML')
		return

	if call.data == 'errors_list' or call.data.startswith('errors_list|'):
		page = 0
		if '|' in call.data:
			page = call.data.split('|', 1)[1]
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_errors_list_text(call.message.chat.id, page),
			reply_markup=_build_errors_list_keyboard(call.message.chat.id, page),
			parse_mode='HTML'
		)
		return

	if call.data.startswith('error_open|'):
		parts = call.data.split('|')
		page = int(parts[1])
		local_index = int(parts[2])
		_render_inline(
			call.message.chat.id,
			call.message.message_id,
			_error_detail_text(call.message.chat.id, page, local_index),
			reply_markup=_build_error_detail_keyboard(call.message.chat.id, page, local_index),
			parse_mode='HTML'
		)
		return

	if call.data == 'status_stop_all':
		_render_inline(call.message.chat.id, call.message.message_id, _stub_text('⏸ Остановить всё', 'Массовая остановка задач будет добавлена отдельным безопасным действием.'), reply_markup=_build_status_menu(), parse_mode='HTML')
		return

	if call.data == 'manage_menu':
		_render_inline(call.message.chat.id, call.message.message_id, '⚙️ <b>Процессы</b>\n\nУправление очередью и запущенными задачами.', reply_markup=_build_manage_menu(), parse_mode='HTML')
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
		_render_inline(call.message.chat.id, call.message.message_id, _stats_text(), reply_markup=build_new_menu(), parse_mode='HTML')
		return

	if call.data == 'help_new':
		_render_inline(call.message.chat.id, call.message.message_id, _guide_text(), reply_markup=_build_help_menu(), parse_mode='HTML')
		return

	if call.data.startswith('help_topic|'):
		topic = call.data.split('|', 1)[1]
		_render_inline(call.message.chat.id, call.message.message_id, _help_detail_text(topic), reply_markup=_build_help_menu(), parse_mode='HTML')
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
	transport = str(getattr(config, 'bot_transport', 'polling') or 'polling').strip().lower()
	if transport != 'polling':
		print(f'Polling skipped: BOT_TRANSPORT={transport}. Start webhook.py for Telegram updates.')
		return

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
				allowed_updates=['message', 'callback_query', 'chat_join_request', 'chat_member', 'my_chat_member'],
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


if __name__ == '__main__':
	run_bot_polling()
