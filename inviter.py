import argparse
import asyncio
import json

from loguru import logger
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserAlreadyParticipantError,
    UserPrivacyRestrictedError,
)
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.messages import AddChatUserRequest
from telethon.tl.types import InputPeerChannel, InputPeerChat
from telethon.tl.functions.channels import JoinChannelRequest

from config import API_ID, API_HASH, invite_per_account_limit, invite_max_flood_wait
from db import (
    get_usernames_for_invite,
    get_usernames_for_invite_all,
    mark_invite_result,
    is_source_allowed,
    is_username_allowed,
    set_account_cooldown,
    get_account_cooldown_remaining,
    set_account_health,
)
from functions import get_proxy, get_sessions, build_telegram_client


logger.add('logging.log', rotation='1 MB', encoding='utf-8')


def _write_progress(progress_file, data):
    if not progress_file:
        return
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def _normalize_target(target):
    t = (target or '').strip()
    if not t:
        return ''
    if t.startswith('https://t.me/'):
        return '@' + t.replace('https://t.me/', '').strip('@')
    if t.startswith('@'):
        return t
    return '@' + t


def _read_sources(single_source, sources_file):
    sources = []
    if single_source:
        sources.append(_normalize_target(single_source))
    if sources_file:
        with open(sources_file, 'r', encoding='utf-8') as f:
            for line in f:
                item = _normalize_target(line.strip())
                if item:
                    sources.append(item)
    uniq = []
    seen = set()
    for s in sources:
        if s and s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


async def invite_one(client, invite_target, username):
    user_ref = username if username.startswith('@') else f'@{username}'
    entity = await client.get_input_entity(invite_target)
    user_entity = await client.get_input_entity(user_ref)
    if isinstance(entity, InputPeerChannel):
        await client(InviteToChannelRequest(entity, [user_entity]))
    elif isinstance(entity, InputPeerChat):
        await client(AddChatUserRequest(entity.chat_id, user_entity, fwd_limit=10))
    else:
        raise RuntimeError('Unsupported invite target type')


async def ensure_join_target(client, invite_target):
    try:
        entity = await client.get_entity(invite_target)
        if getattr(entity, 'username', None):
            await client(JoinChannelRequest(entity.username))
    except UserAlreadyParticipantError:
        return
    except Exception as e:
        logger.warning(f'Не удалось вступить в целевой чат {invite_target}: {e}')


def _pick_next_session(sessions, start_idx):
    total = len(sessions)
    for offset in range(total):
        i = (start_idx + offset) % total
        sess = sessions[i]
        if get_account_cooldown_remaining(sess) == 0:
            return i, sess
    return None, None


async def _get_or_create_client(session, clients, proxy, invite_target):
    if session in clients:
        return clients[session]
    client = build_telegram_client(session, API_ID, API_HASH, proxy=proxy)
    await client.start()
    set_account_health(session, 'active', 'Авторизация успешна')
    await ensure_join_target(client, invite_target)
    clients[session] = client
    return client


async def run_inviter(
    source_target, sources_file, invite_target, limit, sleep_seconds, session_index, use_all_sessions, per_account_limit, max_flood_wait, progress_file, all_parsed
):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    invite_target = _normalize_target(invite_target)
    sources = _read_sources(source_target, sources_file)
    if not all_parsed and not sources:
        raise RuntimeError('Set --source-target or --sources-file')
    if all_parsed:
        sources = ['__all__']

    sessions = get_sessions()
    if not sessions:
        raise RuntimeError('No session files found')
    if not use_all_sessions and (session_index < 0 or session_index >= len(sessions)):
        raise RuntimeError(f'session-index out of range: 0..{len(sessions) - 1}')

    proxy = get_proxy()
    selected_sessions = sessions if use_all_sessions else [sessions[session_index]]
    total_limit_per_source = max(1, limit)
    progress = {
        'mode': 'inviter',
        'status': 'running',
        'sources_total': len(sources),
        'sources_done': 0,
        'invite_target': invite_target,
        'total_candidates': 0,
        'processed': 0,
        'invited': 0,
        'already': 0,
        'privacy': 0,
        'flood_wait': 0,
        'peer_flood': 0,
        'error': 0,
        'last_user': '',
        'message': 'Inviter started',
        'filtered_sources': 0,
        'filtered_users': 0,
    }
    _write_progress(progress_file, progress)
    clients = {}
    session_stats = {s: {'extra_sleep': 0, 'success': 0, 'errors': 0} for s in selected_sessions}
    rr_index = 0

    for src in sources:
        if not is_source_allowed(src):
            progress['filtered_sources'] += 1
            progress['sources_done'] += 1
            progress['message'] = f'Источник заблокирован фильтром: {src}'
            _write_progress(progress_file, progress)
            continue
        if src == '__all__':
            all_rows = get_usernames_for_invite_all(invite_target, total_limit_per_source)
            rows = [(source, u, uid) for (source, u, uid) in all_rows if is_source_allowed(source) and is_username_allowed(u)]
            progress['filtered_users'] += max(0, total_limit_per_source - len(rows))
        else:
            rows_raw = get_usernames_for_invite(src, invite_target, total_limit_per_source)
            rows = [(src, u, uid) for (u, uid) in rows_raw if is_username_allowed(u)]
            progress['filtered_users'] += max(0, total_limit_per_source - len(rows))
        if not rows:
            logger.info(f'Нет пользователей для инвайта из {src}')
            progress['sources_done'] += 1
            progress['message'] = f'Нет пользователей в {src}'
            _write_progress(progress_file, progress)
            continue
        progress['total_candidates'] += len(rows)
        progress['message'] = f'Инвайт из {src}'
        _write_progress(progress_file, progress)

        for source_row, username, user_id in rows:
            idx, session = _pick_next_session(selected_sessions, rr_index)
            if session is None:
                min_wait = min([get_account_cooldown_remaining(s) for s in selected_sessions] or [10])
                await asyncio.sleep(max(3, min_wait))
                idx, session = _pick_next_session(selected_sessions, 0)
                if session is None:
                    mark_invite_result(source_row, invite_target, username, user_id, 'cooldown_skip', 'All accounts cooldown')
                    progress['error'] += 1
                    progress['processed'] += 1
                    progress['last_user'] = username
                    _write_progress(progress_file, progress)
                    continue

            rr_index = (idx + 1) % len(selected_sessions)
            stats = session_stats[session]
            if stats['success'] >= per_account_limit:
                set_account_cooldown(session, 300, 'per-account-limit')
                continue
            try:
                client = await _get_or_create_client(session, clients, proxy, invite_target)
                await invite_one(client, invite_target, username)
                mark_invite_result(source_row, invite_target, username, user_id, 'invited', '')
                stats['success'] += 1
                stats['errors'] = max(0, stats['errors'] - 1)
                stats['extra_sleep'] = max(0, stats['extra_sleep'] - 1)
                progress['invited'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                progress['active_session'] = session
            except UserAlreadyParticipantError:
                mark_invite_result(source_row, invite_target, username, user_id, 'already', '')
                progress['already'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                progress['active_session'] = session
            except UserPrivacyRestrictedError:
                msg = f'Пользователь @{username} не добавлен: запретил приглашения'
                mark_invite_result(source_row, invite_target, username, user_id, 'privacy', msg)
                progress['privacy'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                progress['active_session'] = session
            except PeerFloodError:
                set_account_cooldown(session, max_flood_wait * 2, 'peer-flood')
                set_account_health(session, 'limited', 'Спам-ограничение Telegram (PeerFlood)')
                mark_invite_result(source_row, invite_target, username, user_id, 'peer_flood', 'PeerFloodError')
                stats['errors'] += 2
                stats['extra_sleep'] += 4
                progress['peer_flood'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                progress['active_session'] = session
            except FloodWaitError as e:
                wait_s = int(e.seconds)
                set_account_cooldown(session, wait_s, 'flood-wait')
                set_account_health(session, 'limited', f'Ожидание из-за лимитов: {wait_s} сек')
                mark_invite_result(source_row, invite_target, username, user_id, 'flood_wait', str(wait_s))
                stats['errors'] += 1
                stats['extra_sleep'] += 2
                progress['flood_wait'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                progress['active_session'] = session
                if wait_s <= max_flood_wait:
                    await asyncio.sleep(wait_s)
            except Exception as e:
                err_name = e.__class__.__name__
                if err_name in {'AuthKeyUnregisteredError', 'SessionRevokedError', 'UserDeactivatedError', 'UserDeactivatedBanError', 'PhoneNumberBannedError'}:
                    set_account_health(session, 'dead', 'Сессия/аккаунт недействительны или заблокированы')
                else:
                    set_account_health(session, 'limited', f'Ошибка работы аккаунта: {err_name}')
                mark_invite_result(source_row, invite_target, username, user_id, 'error', str(e))
                stats['errors'] += 1
                stats['extra_sleep'] += 1
                progress['error'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                progress['active_session'] = session
                if stats['errors'] >= 3:
                    set_account_cooldown(session, 120, 'error-burst')
            _write_progress(progress_file, progress)
            await asyncio.sleep(max(0, sleep_seconds + session_stats[session]['extra_sleep']))

        progress['sources_done'] += 1
        progress['message'] = f'Готово: {src}'
        _write_progress(progress_file, progress)

    for client in clients.values():
        try:
            await client.disconnect()
        except Exception:
            pass
    progress['status'] = 'finished'
    progress['message'] = 'Inviter finished'
    _write_progress(progress_file, progress)
    logger.info('Инвайт завершен')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Invite parsed usernames to target chat/channel')
    p.add_argument('--source-target', default='', help='Single source parsed target')
    p.add_argument('--sources-file', default='', help='File with source targets (one per line)')
    p.add_argument('--invite-target', required=True, help='Target where users will be invited')
    p.add_argument('--limit', type=int, default=50, help='Max users per run')
    p.add_argument('--sleep', type=int, default=15, help='Sleep between invites in seconds')
    p.add_argument('--session-index', type=int, default=0, help='Use one account by index')
    p.add_argument('--use-all-sessions', action='store_true', help='Distribute invites across all added accounts')
    p.add_argument('--per-account-limit', type=int, default=invite_per_account_limit, help='Max successful invites per account')
    p.add_argument('--max-flood-wait', type=int, default=invite_max_flood_wait, help='Stop account if FloodWait is higher')
    p.add_argument('--progress-file', default='', help='Path to JSON progress file')
    p.add_argument('--all-parsed', action='store_true', help='Invite from full parsed base (all sources)')
    args = p.parse_args()
    asyncio.run(
        run_inviter(
            source_target=args.source_target,
            sources_file=args.sources_file,
            invite_target=args.invite_target,
            limit=args.limit,
            sleep_seconds=args.sleep,
            session_index=args.session_index,
            use_all_sessions=args.use_all_sessions,
            per_account_limit=max(1, args.per_account_limit),
            max_flood_wait=max(1, args.max_flood_wait),
            progress_file=args.progress_file,
            all_parsed=args.all_parsed,
        )
    )
