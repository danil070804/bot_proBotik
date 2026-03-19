import argparse
import asyncio
import json

from loguru import logger
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest

from config import API_ID, API_HASH
from db import save_parsed_user, save_parsed_comment, is_source_allowed
from functions import get_sessions, get_proxy, build_telegram_client
from repositories.audience import AudienceRepository
from repositories.parse_tasks import ParseTaskRepository


logger.add('logging.log', rotation='1 MB', encoding='utf-8')
AUDIENCE_REPO = AudienceRepository()
PARSE_TASK_REPO = ParseTaskRepository()


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


def _read_targets(single_target, targets_file):
    targets = []
    if single_target:
        targets.append(_normalize_target(single_target))
    if targets_file:
        with open(targets_file, 'r', encoding='utf-8') as f:
            for line in f:
                item = _normalize_target(line.strip())
                if item:
                    targets.append(item)
    uniq = []
    seen = set()
    for t in targets:
        if t and t not in seen:
            seen.add(t)
            uniq.append(t)
    allowed = []
    for t in uniq:
        if is_source_allowed(t):
            allowed.append(t)
    return allowed


async def ensure_join(client, target):
    try:
        entity = await client.get_entity(target)
        if getattr(entity, 'username', None):
            await client(JoinChannelRequest(entity.username))
        logger.info(f'Аккаунт в источнике: {target}')
    except UserAlreadyParticipantError:
        logger.info(f'Уже в источнике: {target}')
    except Exception as e:
        logger.warning(f'Не удалось вступить в {target}: {e}')


def normalize_user(user):
    if not user or not getattr(user, 'id', None):
        return None
    return {
        'telegram_user_id': int(user.id),
        'username': getattr(user, 'username', '') or '',
        'first_name': getattr(user, 'first_name', '') or '',
        'last_name': getattr(user, 'last_name', '') or '',
    }


def resolve_source(target, source_type='chat'):
    return AUDIENCE_REPO.get_or_create_source(
        source_type=source_type,
        source_value=target,
        title=target,
        meta_json={'target': target},
    )


def save_parsed_audience_user(raw_user, target, discovered_via, parse_task_id=None, source_type='chat', source_id=None):
    user = normalize_user(raw_user)
    if not user:
        return {'found': 0, 'saved': 0, 'skipped': 0, 'record': None}
    source = {'id': source_id} if source_id else resolve_source(target, source_type=source_type)
    existing = AUDIENCE_REPO.get_by_telegram_user_id(user['telegram_user_id'])
    audience_user = AUDIENCE_REPO.upsert(
        telegram_user_id=user['telegram_user_id'],
        username=user['username'],
        first_name=user['first_name'],
        last_name=user['last_name'],
        source_id=(source or {}).get('id'),
        parse_task_id=parse_task_id,
        discovered_via=discovered_via,
        tags_json=[f'source:{target}', 'parsed', discovered_via],
        consent_status='parsed',
        sync_user=True,
    )
    return {
        'found': 1,
        'saved': 0 if existing else 1,
        'skipped': 1 if existing else 0,
        'record': audience_user,
    }


async def parse_members(client, target, members_limit=None, parse_task_id=None):
    stats = {'found': 0, 'saved': 0, 'skipped': 0, 'legacy_saved': 0}
    source = resolve_source(target)
    try:
        async for user in client.iter_participants(target, limit=members_limit or None):
            result = save_parsed_audience_user(
                user,
                target,
                'members',
                parse_task_id=parse_task_id,
                source_id=(source or {}).get('id'),
            )
            stats['found'] += result['found']
            stats['saved'] += result['saved']
            stats['skipped'] += result['skipped']
            if getattr(user, 'username', None):
                save_parsed_user(target, user.username, user.id)
                stats['legacy_saved'] += 1
    except Exception as e:
        logger.warning(f'Не удалось собрать участников {target}: {e}')
    return stats


async def parse_comments(client, target, posts_limit, comments_limit, parse_task_id=None):
    stats = {'found': 0, 'saved': 0, 'skipped': 0, 'legacy_saved': 0}
    source = resolve_source(target)
    try:
        async for post in client.iter_messages(target, limit=posts_limit):
            if not post:
                continue
            try:
                async for comment in client.iter_messages(target, reply_to=post.id, limit=comments_limit):
                    if comment and comment.sender_id:
                        sender = await comment.get_sender()
                        result = save_parsed_audience_user(
                            sender,
                            target,
                            'commenters',
                            parse_task_id=parse_task_id,
                            source_id=(source or {}).get('id'),
                        )
                        stats['found'] += result['found']
                        stats['saved'] += result['saved']
                        stats['skipped'] += result['skipped']
                        if sender and getattr(sender, 'username', None):
                            save_parsed_comment(
                                target=target,
                                message_id=post.id,
                                username=sender.username,
                                user_id=sender.id,
                                text=comment.message or '',
                            )
                            stats['legacy_saved'] += 1
            except FloodWaitError as e:
                await asyncio.sleep(int(e.seconds))
            except Exception:
                continue
    except Exception as e:
        logger.warning(f'Не удалось собрать посты/комментарии {target}: {e}')
    return stats


async def parse_message_authors(client, target, messages_limit, parse_task_id=None):
    stats = {'found': 0, 'saved': 0, 'skipped': 0, 'legacy_saved': 0}
    source = resolve_source(target)
    try:
        async for item in client.iter_messages(target, limit=messages_limit):
            if not item or not getattr(item, 'sender_id', None):
                continue
            try:
                sender = await item.get_sender()
            except Exception:
                sender = None
            result = save_parsed_audience_user(
                sender,
                target,
                'message_authors',
                parse_task_id=parse_task_id,
                source_id=(source or {}).get('id'),
            )
            stats['found'] += result['found']
            stats['saved'] += result['saved']
            stats['skipped'] += result['skipped']
    except FloodWaitError as e:
        await asyncio.sleep(int(e.seconds))
    except Exception as e:
        logger.warning(f'Не удалось собрать авторов сообщений {target}: {e}')
    return stats


async def parse_target_with_client(client, target, mode, posts_limit, comments_limit, members_limit=None, messages_limit=None, parse_task_id=None):
    await ensure_join(client, target)
    if mode == 'members':
        stats = await parse_members(client, target, members_limit=members_limit, parse_task_id=parse_task_id)
    elif mode == 'commenters':
        stats = await parse_comments(client, target, posts_limit, comments_limit, parse_task_id=parse_task_id)
    elif mode == 'message_authors':
        stats = await parse_message_authors(client, target, messages_limit=messages_limit or posts_limit, parse_task_id=parse_task_id)
    else:
        raise RuntimeError(f'Unsupported parse mode: {mode}')
    logger.info(
        f'Источник {target}: mode={mode}, found={stats["found"]}, '
        f'saved={stats["saved"]}, skipped={stats["skipped"]}'
    )
    return stats


async def run_parser(
    target,
    targets_file,
    posts_limit,
    comments_limit,
    session_index,
    use_all_sessions,
    progress_file,
    mode='members',
    members_limit=None,
    messages_limit=None,
    task_id=None,
):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    targets = _read_targets(target, targets_file)
    if not targets:
        raise RuntimeError('Set --target or --targets-file with source chats/channels')
    sessions = get_sessions()
    if not sessions:
        raise RuntimeError('No session files found')
    if not use_all_sessions and (session_index < 0 or session_index >= len(sessions)):
        raise RuntimeError(f'session-index out of range: 0..{len(sessions) - 1}')
    proxy = get_proxy()
    selected_sessions = sessions if use_all_sessions else [sessions[session_index]]

    progress = {
        'mode': 'parser',
        'parser_mode': mode,
        'parse_task_id': task_id,
        'status': 'running',
        'sources_total': len(targets),
        'sources_done': 0,
        'sources_failed': 0,
        'users_parsed': 0,
        'comments_parsed': 0,
        'total_found': 0,
        'total_saved': 0,
        'total_skipped': 0,
        'errors': 0,
        'current_source': '',
        'active_session': '',
        'last_error': '',
        'message': f'Parser started ({mode})',
    }
    _write_progress(progress_file, progress)
    if task_id:
        PARSE_TASK_REPO.update_status(task_id, 'running')

    total_found = 0
    total_saved = 0
    total_skipped = 0
    total_legacy_saved = 0
    for idx, src in enumerate(targets):
        progress['current_source'] = src
        progress['message'] = f'Parsing {src}'
        _write_progress(progress_file, progress)
        sess = selected_sessions[idx % len(selected_sessions)]
        progress['active_session'] = sess
        client = None
        try:
            client = build_telegram_client(sess, API_ID, API_HASH, proxy=proxy)
            await client.start()
            stats = await parse_target_with_client(
                client,
                src,
                mode,
                posts_limit,
                comments_limit,
                members_limit=members_limit,
                messages_limit=messages_limit,
                parse_task_id=task_id,
            )
            total_found += int(stats.get('found') or 0)
            total_saved += int(stats.get('saved') or 0)
            total_skipped += int(stats.get('skipped') or 0)
            total_legacy_saved += int(stats.get('legacy_saved') or 0)
            progress['message'] = f'Done {src}'
        except Exception as e:
            progress['sources_failed'] += 1
            progress['errors'] += 1
            progress['last_error'] = f'{src}: {e.__class__.__name__}: {e}'
            progress['message'] = f'Ошибка в {src}'
            logger.exception(f'Ошибка парсинга источника {src}')
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        progress['sources_done'] += 1
        progress['users_parsed'] = total_saved
        progress['comments_parsed'] = total_legacy_saved
        progress['total_found'] = total_found
        progress['total_saved'] = total_saved
        progress['total_skipped'] = total_skipped
        if task_id:
            PARSE_TASK_REPO.update_progress(
                task_id,
                found=total_found,
                saved=total_saved,
                skipped=total_skipped,
                errors=progress['errors'],
            )
        _write_progress(progress_file, progress)

    progress['status'] = 'finished_with_errors' if progress['errors'] else 'finished'
    progress['message'] = 'Parser finished with errors' if progress['errors'] else 'Parser finished'
    if task_id:
        PARSE_TASK_REPO.finish(task_id, status='failed' if progress['errors'] else 'finished')
    _write_progress(progress_file, progress)
    logger.info(
        f'Парсинг завершен: found={total_found}, saved={total_saved}, skipped={total_skipped}, legacy={total_legacy_saved}, '
        f'sources={len(targets)}, errors={progress["errors"]}'
    )


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Telegram parser: audience normalization worker')
    p.add_argument('--mode', default='members', choices=['members', 'commenters', 'message_authors'])
    p.add_argument('--target', default='', help='Single source chat/channel (@name or name)')
    p.add_argument('--targets-file', default='', help='File with source chats/channels (one per line)')
    p.add_argument('--members-limit', type=int, default=0)
    p.add_argument('--posts-limit', type=int, default=100)
    p.add_argument('--comments-limit', type=int, default=200)
    p.add_argument('--messages-limit', type=int, default=0)
    p.add_argument('--session-index', type=int, default=0, help='Use one account by index')
    p.add_argument('--use-all-sessions', action='store_true', help='Distribute parsing across all added accounts')
    p.add_argument('--progress-file', default='', help='Path to JSON progress file')
    p.add_argument('--task-id', type=int, default=0)
    args = p.parse_args()
    asyncio.run(
        run_parser(
            target=args.target,
            targets_file=args.targets_file,
            posts_limit=args.posts_limit,
            comments_limit=args.comments_limit,
            session_index=args.session_index,
            use_all_sessions=args.use_all_sessions,
            progress_file=args.progress_file,
            mode=args.mode,
            members_limit=args.members_limit or None,
            messages_limit=args.messages_limit or None,
            task_id=args.task_id or None,
        )
    )
