import argparse
import asyncio
import json

from loguru import logger
from telethon.errors import FloodWaitError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest

from config import API_ID, API_HASH
from db import save_parsed_user, save_parsed_comment, is_source_allowed
from functions import get_sessions, get_proxy, build_telegram_client


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


async def parse_members(client, target):
    count = 0
    async for user in client.iter_participants(target):
        if user and user.username:
            save_parsed_user(target, user.username, user.id)
            count += 1
    return count


async def parse_comments(client, target, posts_limit, comments_limit):
    comments_saved = 0
    async for post in client.iter_messages(target, limit=posts_limit):
        if not post:
            continue
        try:
            async for comment in client.iter_messages(target, reply_to=post.id, limit=comments_limit):
                if comment and comment.sender_id:
                    sender = await comment.get_sender()
                    if sender and getattr(sender, 'username', None):
                        save_parsed_comment(
                            target=target,
                            message_id=post.id,
                            username=sender.username,
                            user_id=sender.id,
                            text=comment.message or '',
                        )
                        comments_saved += 1
        except FloodWaitError as e:
            await asyncio.sleep(int(e.seconds))
        except Exception:
            continue
    return comments_saved


async def parse_target_with_client(client, target, posts_limit, comments_limit):
    await ensure_join(client, target)
    users = await parse_members(client, target)
    comments = await parse_comments(client, target, posts_limit, comments_limit)
    logger.info(f'Источник {target}: users={users}, comments={comments}')
    return users, comments


async def run_parser(target, targets_file, posts_limit, comments_limit, session_index, use_all_sessions, progress_file):
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
        'status': 'running',
        'sources_total': len(targets),
        'sources_done': 0,
        'users_parsed': 0,
        'comments_parsed': 0,
        'current_source': '',
        'message': 'Parser started',
    }
    _write_progress(progress_file, progress)

    total_users = 0
    total_comments = 0
    for idx, src in enumerate(targets):
        progress['current_source'] = src
        progress['message'] = f'Parsing {src}'
        _write_progress(progress_file, progress)
        sess = selected_sessions[idx % len(selected_sessions)]
        client = build_telegram_client(sess, API_ID, API_HASH, proxy=proxy)
        await client.start()
        users, comments = await parse_target_with_client(client, src, posts_limit, comments_limit)
        await client.disconnect()
        total_users += users
        total_comments += comments
        progress['sources_done'] += 1
        progress['users_parsed'] = total_users
        progress['comments_parsed'] = total_comments
        progress['message'] = f'Done {src}'
        _write_progress(progress_file, progress)

    progress['status'] = 'finished'
    progress['message'] = 'Parser finished'
    _write_progress(progress_file, progress)
    logger.info(f'Парсинг завершен: users={total_users}, comments={total_comments}, sources={len(targets)}')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Telegram parser: usernames from members/comments')
    p.add_argument('--target', default='', help='Single source chat/channel (@name or name)')
    p.add_argument('--targets-file', default='', help='File with source chats/channels (one per line)')
    p.add_argument('--posts-limit', type=int, default=100)
    p.add_argument('--comments-limit', type=int, default=200)
    p.add_argument('--session-index', type=int, default=0, help='Use one account by index')
    p.add_argument('--use-all-sessions', action='store_true', help='Distribute parsing across all added accounts')
    p.add_argument('--progress-file', default='', help='Path to JSON progress file')
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
        )
    )
