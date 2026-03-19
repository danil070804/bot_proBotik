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
	get_account_health, set_account_health, set_account_warmup, get_account_warmup_remaining,
	save_session_file, delete_session_file, get_session_files
)
from telethon.errors import UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from functions import get_proxy, get_sessions, build_telegram_client
from repositories.audience import AudienceRepository
from repositories.campaigns import CampaignRepository
from repositories.communities import CommunityRepository
from repositories.join_requests import JoinRequestRepository
from repositories.parse_tasks import ParseTaskRepository
from repositories.segments import SegmentRepository
from repositories.users import UserRepository
from services.campaign_service import CampaignService
from services.join_request_service import JoinRequestService
from services.target_normalizer import INVITE_TYPE, parse_target
from telegram.webhook_handlers import (
	handle_chat_join_request as process_chat_join_request_update,
	handle_chat_member_update as process_chat_member_update,
	handle_message as process_message_update,
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
	'campaign_rate_limit_per_minute': '20',
	'campaign_rate_limit_per_hour': '300',
	'campaign_max_attempts': '1',
	'campaign_stop_on_error_rate': '0.30',
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
		'active_preset': 'aggressive',
	},
}


def build_new_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='🔑 Аккаунты', callback_data='accounts_menu'),
		types.InlineKeyboardButton(text='📡 Парсинг', callback_data='parsing_menu'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='👥 Аудитория', callback_data='audience_menu'),
		types.InlineKeyboardButton(text='📂 Сообщества', callback_data='communities_menu'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='🚀 Кампании', callback_data='campaigns_menu'),
		types.InlineKeyboardButton(text='📨 Заявки', callback_data='join_requests_menu'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='📊 Аналитика', callback_data='stats_overview'),
		types.InlineKeyboardButton(text='📈 Статус', callback_data='task_status'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='⚙️ Настройки', callback_data='settings_menu'),
		types.InlineKeyboardButton(text='ℹ️ Помощь', callback_data='help_new'),
	)
	return keyboard


def _bool_title(value):
	return 'Да' if str(value).strip().lower() in ['1', 'true', 'yes', 'on'] else 'Нет'


def _community_mode_title(mode):
	return {
		'invite_link': 'Invite Link',
		'join_request': 'Join Request',
		'auto': 'Auto',
	}.get(str(mode or '').strip(), str(mode or '-'))


def _main_dashboard_text():
	try:
		communities = COMMUNITY_REPO.list()
		audience = AUDIENCE_REPO.summary()
		parse_summary = PARSE_TASK_REPO.summary()
		campaigns = CAMPAIGN_REPO.list()
		join_pending = JOIN_REQUEST_REPO.list(status='pending', limit=1000)
		running = len([c for c in campaigns if c.get('status') == 'running'])
		scheduled = len([c for c in campaigns if c.get('status') == 'scheduled'])
		return (
			'🏠 <b>Invite Platform</b>\n\n'
			f'🔑 Аккаунты: <b>{len(list_sessions())}</b>\n'
			f'📡 Парсинг: задач=<b>{parse_summary.get("total", 0)}</b>, running=<b>{parse_summary.get("running", 0)}</b>, '
			f'saved=<b>{parse_summary.get("total_saved", 0)}</b>\n'
			f'📂 Сообщества: <b>{len(communities)}</b>\n'
			f'👥 Аудитория: <b>{audience.get("total", 0)}</b> '
			f'(active={audience.get("active", 0)}, blacklist={audience.get("blacklisted", 0)}, unsubscribed={audience.get("unsubscribed", 0)})\n'
			f'🚀 Кампании: <b>{len(campaigns)}</b> '
			f'(running={running}, scheduled={scheduled})\n'
			f'📨 Заявки в ожидании: <b>{len(join_pending)}</b>\n\n'
			'Новый основной pipeline:\n'
			'<code>Парсинг -> Аудитория -> Кампания -> Invite Link -> Join Request -> Approval</code>'
		)
	except Exception as e:
		return f'🏠 <b>Invite Platform</b>\n\nНе удалось собрать dashboard: <code>{e}</code>'


def _communities_text():
	items = COMMUNITY_REPO.list()
	lines = [
		'📂 <b>Сообщества</b>',
		f'Всего: <b>{len(items)}</b>',
		'Добавляй чаты/каналы для кампаний, Invite Link и Join Request.',
	]
	if items:
		lines.append('')
		for item in items[:10]:
			lines.append(
				f'• <b>{item.get("title")}</b> '
				f'(<code>{item.get("chat_id")}</code>) | тип={item.get("type")} | '
				f'режим={_community_mode_title(item.get("default_invite_mode"))} | '
				f'auto-approve={_bool_title(item.get("auto_approve_join_requests"))}'
			)
	return '\n'.join(lines)


def _audience_text():
	summary = AUDIENCE_REPO.summary()
	lines = [
		'👥 <b>Аудитория</b>',
		f'Всего пользователей: <b>{summary.get("total", 0)}</b>',
		f'Активных: <b>{summary.get("active", 0)}</b>',
		f'Blacklisted: <b>{summary.get("blacklisted", 0)}</b>',
		f'Unsubscribed: <b>{summary.get("unsubscribed", 0)}</b>',
		f'С username: <b>{summary.get("with_username", 0)}</b> | без username: <b>{summary.get("without_username", 0)}</b>',
		f'Источников: <b>{summary.get("sources", 0)}</b>',
		'Поддерживается парсинг, импорт CSV/JSON, поиск и сегменты.',
	]
	for item in AUDIENCE_REPO.list(limit=5):
		lines.append(
			f'• ID <code>{item.get("id")}</code> | tg=<code>{item.get("telegram_user_id")}</code> | '
			f'@{item.get("username") or "-"} | {item.get("first_name") or "-"} | src={item.get("source_value") or "-"}'
		)
	return '\n'.join(lines)


def _campaigns_text():
	items = CAMPAIGN_REPO.list()
	status_map = {}
	for item in items:
		status = str(item.get('status') or 'draft')
		status_map[status] = status_map.get(status, 0) + 1
	lines = [
		'🚀 <b>Кампании</b>',
		f'Всего: <b>{len(items)}</b>',
		f'draft={status_map.get("draft", 0)} | scheduled={status_map.get("scheduled", 0)} | running={status_map.get("running", 0)}',
		f'paused={status_map.get("paused", 0)} | finished={status_map.get("finished", 0)} | failed={status_map.get("failed", 0)}',
	]
	for item in items[:8]:
		lines.append(
			f'• <b>{item.get("name")}</b> '
			f'(ID {item.get("id")}) | status={item.get("status")} | '
			f'mode={_community_mode_title(item.get("invite_mode"))}'
		)
	return '\n'.join(lines)


def _join_requests_text(status='pending'):
	items = JOIN_REQUEST_REPO.list(status=status, limit=20)
	pending = len(JOIN_REQUEST_REPO.list(status='pending', limit=1000))
	approved = len(JOIN_REQUEST_REPO.list(status='approved', limit=1000))
	declined = len(JOIN_REQUEST_REPO.list(status='declined', limit=1000))
	lines = [
		'📨 <b>Заявки</b>',
		f'pending=<b>{pending}</b> | approved=<b>{approved}</b> | declined=<b>{declined}</b>',
		f'Текущий фильтр: <b>{status}</b>',
	]
	for item in items:
		lines.append(
			f'• ID <code>{item.get("id")}</code> | tg=<code>{item.get("telegram_user_id")}</code> | '
			f'campaign={item.get("campaign_id") or "-"} | community={item.get("community_id") or "-"} | '
			f'status={item.get("status")}'
		)
	return '\n'.join(lines)


def _parsing_text():
	parse_summary = PARSE_TASK_REPO.summary()
	recent_tasks = PARSE_TASK_REPO.list(limit=5)
	lines = [
		'📡 <b>Парсинг аудитории</b>',
		'Модуль парсинга работает как источник для сегментов и кампаний.',
		'',
		f'Задач: <b>{parse_summary.get("total", 0)}</b> | running=<b>{parse_summary.get("running", 0)}</b> | failed=<b>{parse_summary.get("failed", 0)}</b>',
		f'Найдено: <b>{parse_summary.get("total_found", 0)}</b> | сохранено: <b>{parse_summary.get("total_saved", 0)}</b> | skipped=<b>{parse_summary.get("total_skipped", 0)}</b>',
		f'Текущие лимиты: posts={_setting_int("parser_posts_limit")}, comments={_setting_int("parser_comments_limit")}, '
		f'all_sessions={"ВКЛ" if _setting_bool("parser_use_all_sessions") else "ВЫКЛ"}',
		'',
		'Режимы: members / commenters / message_authors / import_file / manual_add',
	]
	for item in recent_tasks:
		lines.append(
			f'• #{item.get("id")} | mode={item.get("mode")} | status={item.get("status")} | '
			f'saved={item.get("total_saved", 0)} / found={item.get("total_found", 0)}'
		)
	return '\n'.join(lines)


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
		bot.send_message(
			chat_id,
			f'⚠️ <b>Аккаунт не прошёл проверку</b>\n'
			f'Файл сохранён: <code>{filename}</code>\n'
			f'Причина: <code>{_health_details_ru(details)[:300]}</code>\n\n'
			'Автоудаление отключено: файл оставлен как есть, удалить его можно вручную из меню аккаунтов.',
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
		text = (
			f'🔎 Парсинг в процессе\n'
			f'Источник: {progress.get("current_source", "-")}\n'
			f'Аккаунт: {progress.get("active_session", "-")}\n'
			f'Источники: {progress.get("sources_done", 0)}/{progress.get("sources_total", 0)}\n'
			f'Пользователей: {progress.get("users_parsed", 0)}\n'
			f'Комментариев: {progress.get("comments_parsed", 0)}\n'
			f'Ошибок: {progress.get("errors", 0)}'
		)
		last_error = str(progress.get('last_error', '') or '').strip()
		if last_error:
			text += f'\nПоследняя ошибка: {last_error[:300]}'
		return text
	if mode == 'join_target':
		target_type = 'private invite' if progress.get('target_type') == INVITE_TYPE else 'public username'
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
					text = (
						f'✅ Завершено: {item["title"]}\n'
						f'Код: {code}\n'
						f'Источники: {progress.get("sources_done", 0)}/{progress.get("sources_total", 0)}\n'
						f'Успешно: {max(0, progress.get("sources_done", 0) - progress.get("sources_failed", 0))}\n'
						f'С ошибками: {progress.get("sources_failed", 0)}\n'
						f'Пользователей: {progress.get("users_parsed", 0)}\n'
						f'Комментариев: {progress.get("comments_parsed", 0)}'
					)
					last_error = str(progress.get('last_error', '') or '').strip()
					if last_error:
						text += f'\n\nПоследняя ошибка:\n{last_error[:800]}'
					_render_inline(item['user_id'], msg_id, text, parse_mode=None)
				elif progress.get('mode') == 'join_target':
					target_type = 'private invite' if progress.get('target_type') == INVITE_TYPE else 'public username'
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
		'ℹ️ <b>Invite Platform</b>\n\n'
		'🔑 <b>Аккаунты</b>\n'
		'• Загружай <code>.session</code> или <code>.json</code> как документ.\n'
		'• Аккаунты используются для парсинга и технических проверок доступности источников.\n'
		'• Health-check и прогрев остаются отдельным служебным модулем.\n\n'
		'📡 <b>Парсинг</b>\n'
		'• Сбор аудитории из участников, комментариев и авторов сообщений.\n'
		'• Парсинг не ведёт в direct add member, а пополняет аудиторию для кампаний.\n'
		'• Поддерживаются live-progress и повторный запуск задач.\n\n'
		'👥 <b>Аудитория</b>\n'
		'• Ручное добавление пользователей.\n'
		'• Импорт своей базы через CSV/JSON.\n'
		'• Хранение результатов парсинга, blacklist и unsubscribe.\n\n'
		'📂 <b>Сообщества</b>\n'
		'• Хранят Telegram chat_id, тип сообщества, default mode и auto-approve.\n'
		'• Используются как цель для invite-link и join-request кампаний.\n\n'
		'🚀 <b>Кампании</b>\n'
		'• Создаются поверх аудитории и сообщества.\n'
		'• Режимы: <code>invite_link</code>, <code>join_request</code>, <code>auto</code>.\n'
		'• Worker отправляет сообщения со ссылкой, а не делает direct add member.\n\n'
		'📨 <b>Заявки</b>\n'
		'• Telegram webhook принимает <code>chat_join_request</code>.\n'
		'• Заявки сохраняются в БД и могут быть auto-approve или manual approve/decline.\n\n'
		'📊 <b>Аналитика</b>\n'
		'• Данные берутся из <code>campaign_recipients</code> и <code>join_requests</code>.\n'
		'• Основные метрики: sent, failed, join_requested, approved, joined, declined.\n\n'
		'⚙️ <b>Настройки</b>\n'
		'• Rate limit per minute/hour.\n'
		'• Max attempts.\n'
		'• Stop on error rate.\n'
		'• Отдельно доступны управление сессиями и процессами как служебные инструменты.\n\n'
		'💡 <b>Подсказка:</b> на любом шаге нажми «❌ Отменить сценарий» или «⬅️ Назад в меню».'
	)


def _flow_title(flow):
	return {
		'parser': 'Парсинг',
		'community_add': 'Сообщество',
		'audience_add': 'Аудитория',
		'audience_import': 'Импорт аудитории',
		'audience_search': 'Поиск аудитории',
		'segment_create': 'Сегмент',
		'campaign_create': 'Кампания',
		'join_target': 'Вход в цель',
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
		if telegram_user_id in [None, '']:
			continue
		try:
			telegram_user_id = int(str(telegram_user_id).strip())
		except Exception:
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
			username=str(row.get('username') or '').strip().lstrip('@'),
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
	return (
		'⚙️ Настройки платформы\n'
		f'• Активный пресет: <b>{_preset_title(_get_setting("active_preset"))}</b>\n'
		f'• Парсинг: posts={_setting_int("parser_posts_limit")}, comments={_setting_int("parser_comments_limit")}, '
		f'all_sessions={"ВКЛ" if _setting_bool("parser_use_all_sessions") else "ВЫКЛ"}\n'
		f'• Rate limit: {_setting_int("campaign_rate_limit_per_minute")}/мин, {_setting_int("campaign_rate_limit_per_hour")}/час\n'
		f'• Max attempts: {_setting_int("campaign_max_attempts")}\n'
		f'• Stop on error rate: {_get_setting("campaign_stop_on_error_rate")}\n'
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
		types.InlineKeyboardButton(text='✏️ Посты парсинга', callback_data='settings_edit|parser_posts_limit'),
		types.InlineKeyboardButton(text='✏️ Комментарии', callback_data='settings_edit|parser_comments_limit'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='🔁 Все аккаунты парсинга', callback_data='settings_toggle|parser_use_all_sessions'),
		types.InlineKeyboardButton(text='✏️ Лимит / мин', callback_data='settings_edit|campaign_rate_limit_per_minute'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='✏️ Лимит / час', callback_data='settings_edit|campaign_rate_limit_per_hour'),
		types.InlineKeyboardButton(text='✏️ Max attempts', callback_data='settings_edit|campaign_max_attempts'),
	)
	keyboard.add(types.InlineKeyboardButton(text='✏️ Error-rate', callback_data='settings_edit|campaign_stop_on_error_rate'))
	keyboard.add(types.InlineKeyboardButton(text='🕒 Дни прогрева аккаунтов', callback_data='settings_edit|account_warmup_days'))
	keyboard.add(
		types.InlineKeyboardButton(text='🧩 Аккаунты', callback_data='accounts_menu'),
		types.InlineKeyboardButton(text='⚙️ Процессы', callback_data='manage_menu'),
	)
	keyboard.add(types.InlineKeyboardButton(text='♻️ Сброс по умолчанию', callback_data='settings_reset'))
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _build_communities_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='➕ Добавить', callback_data='community_add_start'),
		types.InlineKeyboardButton(text='📋 Список', callback_data='communities_list'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _build_audience_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='➕ Добавить', callback_data='audience_add_start'),
		types.InlineKeyboardButton(text='⬆️ Импорт CSV/JSON', callback_data='audience_import_start'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='📋 Список', callback_data='audience_list'),
		types.InlineKeyboardButton(text='🔎 Поиск', callback_data='audience_search_start'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='🚫 Blacklist', callback_data='audience_blacklist_list'),
		types.InlineKeyboardButton(text='🧩 Сегменты', callback_data='segments_menu'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _build_parser_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='👥 Members', callback_data='parser_mode|members'),
		types.InlineKeyboardButton(text='💬 Commenters', callback_data='parser_mode|commenters'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='✍️ Authors', callback_data='parser_mode|message_authors'),
		types.InlineKeyboardButton(text='⬆️ Import File', callback_data='parser_mode|import_file'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='➕ Manual Add', callback_data='parser_mode|manual_add'),
		types.InlineKeyboardButton(text='📈 Статус', callback_data='task_status'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='⚙️ Настройки парсинга', callback_data='settings_menu'),
		types.InlineKeyboardButton(text='👥 В аудиторию', callback_data='audience_menu'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _build_segments_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='➕ Создать сегмент', callback_data='segment_create_start'),
		types.InlineKeyboardButton(text='📋 Список сегментов', callback_data='segments_list'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='audience_menu'))
	return keyboard


def _build_campaigns_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='➕ Создать', callback_data='campaign_create_start'),
		types.InlineKeyboardButton(text='📋 Список', callback_data='campaigns_list'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _build_join_requests_menu():
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='🕒 Pending', callback_data='join_requests_view|pending'),
		types.InlineKeyboardButton(text='✅ Approved', callback_data='join_requests_view|approved'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='❌ Declined', callback_data='join_requests_view|declined'),
		types.InlineKeyboardButton(text='🔁 Обновить', callback_data='join_requests_menu'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


def _parse_yes_no(value):
	text = str(value or '').strip().lower()
	if text in ['да', 'yes', 'y', '1', 'true', 'on']:
		return True
	if text in ['нет', 'no', 'n', '0', 'false', 'off']:
		return False
	raise ValueError('Ожидается да/нет')


def _parser_mode_title(mode):
	return {
		'members': 'Members',
		'commenters': 'Commenters',
		'message_authors': 'Message Authors',
		'import_file': 'Import File',
		'manual_add': 'Manual Add',
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
	return (
		f'🧩 <b>{segment.get("name")}</b>\n'
		f'ID: <code>{segment.get("id")}</code>\n'
		f'Пользователей (preview ≤1000): <b>{len(users)}</b>\n'
		f'Фильтр: <code>{json.dumps(segment.get("filter_json") or {}, ensure_ascii=False)}</code>'
	)


def _build_campaign_actions(campaign_id):
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(text='▶️ Запланировать', callback_data=f'campaign_start|{campaign_id}'),
		types.InlineKeyboardButton(text='⏸ Пауза', callback_data=f'campaign_pause|{campaign_id}'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='🔄 Возобновить', callback_data=f'campaign_resume|{campaign_id}'),
		types.InlineKeyboardButton(text='🛑 Отменить', callback_data=f'campaign_cancel|{campaign_id}'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='campaigns_menu'))
	return keyboard


def _campaign_detail_text(campaign_id):
	campaign = CAMPAIGN_REPO.get(campaign_id)
	if not campaign:
		return 'Кампания не найдена.'
	stats = CAMPAIGN_REPO.get_stats(campaign_id)
	community = COMMUNITY_REPO.get(campaign.get('community_id'))
	return (
		f'🚀 <b>{campaign.get("name")}</b>\n'
		f'ID: <code>{campaign_id}</code>\n'
		f'Статус: <b>{campaign.get("status")}</b>\n'
		f'Сообщество: <b>{(community or {}).get("title", "-")}</b>\n'
		f'Режим: <b>{_community_mode_title(campaign.get("invite_mode"))}</b>\n'
		f'Rate: {campaign.get("rate_limit_per_minute")}/мин, {campaign.get("rate_limit_per_hour")}/час\n'
		f'Max attempts: {campaign.get("max_attempts")}\n\n'
		f'Получателей: <b>{stats.get("total", 0)}</b>\n'
		f'sent={stats.get("sent", 0)} | failed={stats.get("failed", 0)} | '
		f'join_requested={stats.get("join_requested", 0)}\n'
		f'approved={stats.get("approved", 0)} | joined={stats.get("joined", 0)} | declined={stats.get("declined", 0)}'
	)


def _build_user_actions(user_id):
	user = AUDIENCE_REPO.get(user_id)
	keyboard = types.InlineKeyboardMarkup()
	keyboard.add(
		types.InlineKeyboardButton(
			text='✅ Убрать blacklist' if user and user.get('is_blacklisted') else '🚫 В blacklist',
			callback_data=f'audience_toggle_blacklist|{user_id}'
		),
		types.InlineKeyboardButton(text='✉️ Unsubscribe', callback_data=f'audience_unsubscribe|{user_id}'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='audience_menu'))
	return keyboard


def _user_detail_text(user_id):
	user = AUDIENCE_REPO.get(user_id)
	if not user:
		return 'Пользователь не найден.'
	return (
		f'👤 <b>{user.get("first_name") or "-"}</b>\n'
		f'ID: <code>{user.get("id")}</code>\n'
		f'Telegram ID: <code>{user.get("telegram_user_id")}</code>\n'
		f'Username: <code>@{user.get("username") or "-"}</code>\n'
		f'Source: <code>{user.get("source_value") or "-"}</code>\n'
		f'Discovered via: <code>{user.get("discovered_via") or "-"}</code>\n'
		f'Consent: <b>{user.get("consent_status") or "-"}</b>\n'
		f'Blacklisted: <b>{_bool_title(user.get("is_blacklisted"))}</b>\n'
		f'Unsubscribed: <b>{"Да" if user.get("unsubscribed_at") else "Нет"}</b>'
	)


def _segments_text():
	items = SEGMENT_REPO.list(limit=20)
	lines = [
		'🧩 <b>Сегменты</b>',
		f'Всего: <b>{len(items)}</b>',
		'Фильтры сегментов сейчас строятся поверх аудитории: search/source/tag/blacklist/active.',
	]
	for item in items[:8]:
		user_count = len(SEGMENT_REPO.get_users(item.get('id'), limit=500))
		lines.append(
			f'• <b>{item.get("name")}</b> | id=<code>{item.get("id")}</code> | preview≤500=<b>{user_count}</b>'
		)
	return '\n'.join(lines)


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
		types.InlineKeyboardButton(text='➕ Ввести аккаунты в цель', callback_data='accounts_join_target_start'),
	)
	keyboard.add(
		types.InlineKeyboardButton(text='🗑 Удалить аккаунт', callback_data='accounts_delete_menu'),
	)
	keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='main_menu'))
	return keyboard


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
		return (
			f'📊 <b>Аналитика платформы</b>\n'
			f'Аккаунтов: <b>{len(list_sessions())}</b>\n'
			f'Парсинг: tasks=<b>{parse_summary.get("total", 0)}</b>, running=<b>{parse_summary.get("running", 0)}</b>, '
			f'found=<b>{parse_summary.get("total_found", 0)}</b>, saved=<b>{parse_summary.get("total_saved", 0)}</b>, '
			f'errors=<b>{parse_summary.get("total_errors", 0)}</b>\n'
			f'Сообществ: <b>{len(COMMUNITY_REPO.list())}</b>\n'
			f'Пользователей: <b>{audience.get("total", 0)}</b>\n'
			f'Кампаний: <b>{len(campaigns)}</b>\n'
			f'Получателей: <b>{total_stats.get("total", 0)}</b>\n\n'
			f'✉️ sent: <b>{total_stats.get("sent", 0)}</b>\n'
			f'📬 delivered: <b>{total_stats.get("delivered", 0)}</b>\n'
			f'🕒 join_requested: <b>{total_stats.get("join_requested", 0)}</b>\n'
			f'✅ approved: <b>{total_stats.get("approved", 0)}</b>\n'
			f'👤 joined: <b>{total_stats.get("joined", 0)}</b>\n'
			f'❌ declined: <b>{total_stats.get("declined", 0)}</b>\n'
			f'⚠️ failed: <b>{total_stats.get("failed", 0)}</b>\n\n'
			f'Join requests: pending=<b>{jr_status.get("pending", 0)}</b>, '
			f'approved=<b>{jr_status.get("approved", 0)}</b>, '
			f'declined=<b>{jr_status.get("declined", 0)}</b>'
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
	queue_pos = _enqueue_uploaded_session(message.chat.id, filename)
	bot.send_message(
		message.chat.id,
		f'⏳ Файл <code>{filename}</code> загружен.\n'
		f'Добавлен в очередь обработки: <b>~{queue_pos}</b>.\n'
		'Можно отправлять следующие файлы — обработаю по очереди.',
		parse_mode='HTML'
	)


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
	task = PARSE_TASK_REPO.create(
		mode=parse_mode,
		source_type='chat',
		source_value=str(state.get('sources') or '').strip(),
		status='queued',
		created_by=message.chat.id,
	)
	command = [
		sys.executable, os.path.join('workers', 'parser_worker.py'),
		'--mode', parse_mode,
		'--targets-file', file_name,
		'--task-id', str(task.get('id')),
	]
	if parse_mode == 'members':
		command += ['--members-limit', str(int(state.get('members_limit') or _setting_int('parser_posts_limit')))]
	elif parse_mode == 'commenters':
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
		f'Выбери глубину анализа по постам.\n'
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
		f'• Тип: <b>{"private invite" if parsed_target.target_type == INVITE_TYPE else "public username"}</b>\n'
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

	if call.data == 'settings_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _settings_text(), parse_mode='HTML', reply_markup=_build_settings_menu())
		return

	if call.data == 'parsing_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _parsing_text(), parse_mode='HTML', reply_markup=_build_parser_menu())
		return

	if call.data == 'communities_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _communities_text(), parse_mode='HTML', reply_markup=_build_communities_menu())
		return

	if call.data == 'communities_list':
		_render_inline(call.message.chat.id, call.message.message_id, _communities_text(), parse_mode='HTML', reply_markup=_build_communities_menu())
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

	if call.data == 'audience_list':
		users = AUDIENCE_REPO.list(limit=12)
		keyboard = types.InlineKeyboardMarkup()
		for user in users:
			label = f'👤 @{user.get("username") or user.get("telegram_user_id")} ({user.get("id")})'
			keyboard.add(types.InlineKeyboardButton(text=label[:60], callback_data=f'audience_user|{user.get("id")}'))
		keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='audience_menu'))
		_render_inline(call.message.chat.id, call.message.message_id, _audience_text(), parse_mode='HTML', reply_markup=keyboard)
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
		user_id = int(call.data.split('|', 1)[1])
		_render_inline(call.message.chat.id, call.message.message_id, _user_detail_text(user_id), parse_mode='HTML', reply_markup=_build_user_actions(user_id))
		return

	if call.data.startswith('audience_toggle_blacklist|'):
		user_id = int(call.data.split('|', 1)[1])
		user = AUDIENCE_REPO.get(user_id)
		if not user:
			bot.send_message(call.message.chat.id, 'Пользователь не найден.')
			return
		AUDIENCE_REPO.blacklist(user_id, is_blacklisted=not bool(user.get('is_blacklisted')))
		_render_inline(call.message.chat.id, call.message.message_id, _user_detail_text(user_id), parse_mode='HTML', reply_markup=_build_user_actions(user_id))
		return

	if call.data.startswith('audience_unsubscribe|'):
		user_id = int(call.data.split('|', 1)[1])
		AUDIENCE_REPO.unsubscribe(user_id)
		_render_inline(call.message.chat.id, call.message.message_id, _user_detail_text(user_id), parse_mode='HTML', reply_markup=_build_user_actions(user_id))
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
		_render_inline(call.message.chat.id, call.message.message_id, _segment_detail_text(segment_id), parse_mode='HTML', reply_markup=_build_segments_menu())
		return

	if call.data == 'campaigns_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _campaigns_text(), parse_mode='HTML', reply_markup=_build_campaigns_menu())
		return

	if call.data == 'campaigns_list':
		items = CAMPAIGN_REPO.list()
		keyboard = types.InlineKeyboardMarkup()
		for item in items[:12]:
			keyboard.add(
				types.InlineKeyboardButton(
					text=f'🚀 {item.get("name")} [{item.get("status")}]'[:60],
					callback_data=f'campaign_view|{item.get("id")}'
				)
			)
		keyboard.add(types.InlineKeyboardButton(text='⬅️ Назад', callback_data='campaigns_menu'))
		_render_inline(call.message.chat.id, call.message.message_id, _campaigns_text(), parse_mode='HTML', reply_markup=keyboard)
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
		campaign_id = int(call.data.split('|', 1)[1])
		_render_inline(call.message.chat.id, call.message.message_id, _campaign_detail_text(campaign_id), parse_mode='HTML', reply_markup=_build_campaign_actions(campaign_id))
		return

	if call.data.startswith('campaign_start|'):
		campaign_id = int(call.data.split('|', 1)[1])
		CAMPAIGN_SERVICE.start_campaign(campaign_id)
		_render_inline(call.message.chat.id, call.message.message_id, _campaign_detail_text(campaign_id) + '\n\n🕒 Кампания переведена в scheduled. Worker подхватит её автоматически.', parse_mode='HTML', reply_markup=_build_campaign_actions(campaign_id))
		return

	if call.data.startswith('campaign_pause|'):
		campaign_id = int(call.data.split('|', 1)[1])
		CAMPAIGN_SERVICE.pause_campaign(campaign_id)
		_render_inline(call.message.chat.id, call.message.message_id, _campaign_detail_text(campaign_id), parse_mode='HTML', reply_markup=_build_campaign_actions(campaign_id))
		return

	if call.data.startswith('campaign_resume|'):
		campaign_id = int(call.data.split('|', 1)[1])
		CAMPAIGN_SERVICE.resume_campaign(campaign_id)
		_render_inline(call.message.chat.id, call.message.message_id, _campaign_detail_text(campaign_id), parse_mode='HTML', reply_markup=_build_campaign_actions(campaign_id))
		return

	if call.data.startswith('campaign_cancel|'):
		campaign_id = int(call.data.split('|', 1)[1])
		CAMPAIGN_SERVICE.cancel_campaign(campaign_id)
		_render_inline(call.message.chat.id, call.message.message_id, _campaign_detail_text(campaign_id), parse_mode='HTML', reply_markup=_build_campaign_actions(campaign_id))
		return

	if call.data == 'join_requests_menu':
		_render_inline(call.message.chat.id, call.message.message_id, _join_requests_text('pending'), parse_mode='HTML', reply_markup=_build_join_requests_menu())
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
			f'🔎 <b>Парсинг аудитории</b>\n'
			f'Режим: <b>{_parser_mode_title(parse_mode)}</b>\n'
			f'{mode_text}: отправь источники (через запятую или с новой строки).\n\n'
			'Пример:\n<code>@chat1\n@chat2</code>',
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
			parse_summary = PARSE_TASK_REPO.summary()
			if parse_summary.get('running', 0) or parse_summary.get('queued', 0):
				recent = PARSE_TASK_REPO.list(limit=5)
				lines = [
					'📈 Статус задач',
					f'Parse queued: {parse_summary.get("queued", 0)}',
					f'Parse running: {parse_summary.get("running", 0)}',
				]
				for item in recent:
					lines.append(
						f'• parse #{item.get("id")} | {item.get("mode")} | {item.get("status")} | '
						f'saved={item.get("total_saved", 0)}'
					)
				_render_inline(call.message.chat.id, call.message.message_id, '\n'.join(lines), reply_markup=build_new_menu(), parse_mode=None)
				return
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
