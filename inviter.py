import argparse
import asyncio
import json

from loguru import logger
from telethon import TelegramClient
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
from db import get_usernames_for_invite, mark_invite_result
from functions import get_proxy, get_sessions


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


async def invite_batch_with_session(
    session, source_target, invite_target, rows, sleep_seconds, proxy, per_account_limit, max_flood_wait, progress, progress_lock, progress_file
):
    client = TelegramClient(session, API_ID, API_HASH, proxy=proxy)
    await client.start()
    await ensure_join_target(client, invite_target)
    logger.info(f'Инвайтер запущен: session={session}, source={source_target}, users={len(rows)}')

    invited_count = 0
    for username, user_id in rows:
        if invited_count >= per_account_limit:
            logger.warning(f'{session}: достигнут лимит на аккаунт ({per_account_limit})')
            break
        try:
            await invite_one(client, invite_target, username)
            mark_invite_result(source_target, invite_target, username, user_id, 'invited', '')
            logger.info(f'Добавлен: @{username}')
            invited_count += 1
            async with progress_lock:
                progress['invited'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                _write_progress(progress_file, progress)
        except UserAlreadyParticipantError:
            mark_invite_result(source_target, invite_target, username, user_id, 'already', '')
            async with progress_lock:
                progress['already'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                _write_progress(progress_file, progress)
        except UserPrivacyRestrictedError:
            msg = f'Пользователь @{username} не добавлен: запретил приглашения'
            mark_invite_result(source_target, invite_target, username, user_id, 'privacy', msg)
            logger.warning(msg)
            async with progress_lock:
                progress['privacy'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                _write_progress(progress_file, progress)
        except PeerFloodError:
            mark_invite_result(source_target, invite_target, username, user_id, 'peer_flood', 'PeerFloodError')
            logger.error('PeerFloodError: остановка инвайта для защиты аккаунта')
            async with progress_lock:
                progress['peer_flood'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                _write_progress(progress_file, progress)
            break
        except FloodWaitError as e:
            mark_invite_result(source_target, invite_target, username, user_id, 'flood_wait', str(e.seconds))
            async with progress_lock:
                progress['flood_wait'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                _write_progress(progress_file, progress)
            if int(e.seconds) > max_flood_wait:
                logger.error(f'FloodWait {e.seconds}s > лимита {max_flood_wait}s, остановка аккаунта {session}')
                break
            await asyncio.sleep(int(e.seconds))
        except Exception as e:
            mark_invite_result(source_target, invite_target, username, user_id, 'error', str(e))
            async with progress_lock:
                progress['error'] += 1
                progress['processed'] += 1
                progress['last_user'] = username
                _write_progress(progress_file, progress)
        await asyncio.sleep(max(0, sleep_seconds))

    await client.disconnect()


def _split_rows(rows, buckets):
    if buckets <= 1:
        return [rows]
    chunks = [[] for _ in range(buckets)]
    for idx, row in enumerate(rows):
        chunks[idx % buckets].append(row)
    return chunks


async def run_inviter(
    source_target, sources_file, invite_target, limit, sleep_seconds, session_index, use_all_sessions, per_account_limit, max_flood_wait, progress_file
):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    invite_target = _normalize_target(invite_target)
    sources = _read_sources(source_target, sources_file)
    if not sources:
        raise RuntimeError('Set --source-target or --sources-file')

    sessions = get_sessions()
    if not sessions:
        raise RuntimeError('No .session files found')
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
    }
    progress_lock = asyncio.Lock()
    _write_progress(progress_file, progress)

    for src in sources:
        rows = get_usernames_for_invite(src, invite_target, total_limit_per_source)
        if not rows:
            logger.info(f'Нет пользователей для инвайта из {src}')
            async with progress_lock:
                progress['sources_done'] += 1
                progress['message'] = f'No users in {src}'
                _write_progress(progress_file, progress)
            continue
        async with progress_lock:
            progress['total_candidates'] += len(rows)
            progress['message'] = f'Inviting from {src}'
            _write_progress(progress_file, progress)
        chunks = _split_rows(rows, len(selected_sessions))
        tasks = []
        for idx, batch in enumerate(chunks):
            if not batch:
                continue
            tasks.append(
                asyncio.create_task(
                    invite_batch_with_session(
                        selected_sessions[idx],
                        src,
                        invite_target,
                        batch,
                        sleep_seconds,
                        proxy,
                        per_account_limit,
                        max_flood_wait,
                        progress,
                        progress_lock,
                        progress_file,
                    )
                )
            )
        if tasks:
            await asyncio.gather(*tasks)
        async with progress_lock:
            progress['sources_done'] += 1
            progress['message'] = f'Done source {src}'
            _write_progress(progress_file, progress)
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
        )
    )
