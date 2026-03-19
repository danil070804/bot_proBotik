import argparse
import asyncio
import json

from loguru import logger
from telethon.errors import ChatAdminRequiredError, ChannelPrivateError, FloodWaitError

from config import API_ID, API_HASH
from db import save_parsed_user, save_parsed_comment, is_source_allowed
from functions import get_usable_sessions, get_proxy, build_telegram_client
from repositories.audience import AudienceRepository
from repositories.parse_tasks import ParseTaskRepository
from services.source_access_service import SourceAccessService
from services.target_normalizer import detect_target_type, parse_target


logger.add('logging.log', rotation='1 MB', encoding='utf-8')
AUDIENCE_REPO = AudienceRepository()
PARSE_TASK_REPO = ParseTaskRepository()
SOURCE_ACCESS_SERVICE = SourceAccessService()


def _write_progress(progress_file, data):
    if not progress_file:
        return
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def _read_targets(single_target, targets_file):
    targets = []
    if single_target:
        targets.append(str(single_target).strip())
    if targets_file:
        with open(targets_file, 'r', encoding='utf-8') as f:
            for line in f:
                item = str(line or '').strip()
                if item:
                    targets.append(item)
    uniq = []
    seen = set()
    for raw_target in targets:
        if not raw_target:
            continue
        try:
            parsed = parse_target(raw_target)
        except Exception as exc:
            logger.warning(f'Пропускаю источник {raw_target}: {exc}')
            continue
        dedupe_key = f'{parsed.target_type}:{parsed.normalized_value}'
        if dedupe_key in seen:
            continue
        if not is_source_allowed(parsed.raw_target) and not is_source_allowed(parsed.display_value):
            continue
        seen.add(dedupe_key)
        uniq.append(parsed)
    return uniq


def normalize_user(user):
    if not user:
        return None
    telegram_user_id = getattr(user, 'id', None)
    username = getattr(user, 'username', '') or ''
    if not telegram_user_id and not username:
        return None
    return {
        'telegram_user_id': int(telegram_user_id) if telegram_user_id else None,
        'username': username,
        'first_name': getattr(user, 'first_name', '') or '',
        'last_name': getattr(user, 'last_name', '') or '',
    }


def resolve_source(target, source_type='chat', title=None, meta_json=None):
    return AUDIENCE_REPO.get_or_create_source(
        source_type=source_type,
        source_value=target,
        title=title or target,
        meta_json=meta_json or {'target': target},
    )


def save_parsed_audience_user(raw_user, target, discovered_via, parse_task_id=None, source_type='chat', source_id=None):
    user = normalize_user(raw_user)
    if not user:
        return {'found': 0, 'saved': 0, 'skipped': 0, 'record': None}
    source = {'id': source_id} if source_id else resolve_source(target, source_type=source_type)
    existing = None
    if user.get('telegram_user_id'):
        existing = AUDIENCE_REPO.get_by_telegram_user_id(user['telegram_user_id'])
    audience_user = AUDIENCE_REPO.upsert(
        telegram_user_id=user.get('telegram_user_id'),
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


async def parse_members(client, target_entity, target_label, members_limit=None, parse_task_id=None, source_id=None):
    stats = {'found': 0, 'saved': 0, 'skipped': 0, 'legacy_saved': 0}
    try:
        async for user in client.iter_participants(target_entity, limit=members_limit or None):
            result = save_parsed_audience_user(
                user,
                target_label,
                'members',
                parse_task_id=parse_task_id,
                source_id=source_id,
            )
            stats['found'] += result['found']
            stats['saved'] += result['saved']
            stats['skipped'] += result['skipped']
            if getattr(user, 'username', None):
                save_parsed_user(target_label, user.username, user.id)
                stats['legacy_saved'] += 1
    except (ChatAdminRequiredError, ChannelPrivateError) as e:
        raise RuntimeError(f'participants_unavailable: {e}')
    except Exception as e:
        logger.warning(f'Не удалось собрать участников {target_label}: {e}')
    return stats


async def parse_comments(client, target_entity, target_label, posts_limit, comments_limit, parse_task_id=None, source_id=None):
    stats = {'found': 0, 'saved': 0, 'skipped': 0, 'legacy_saved': 0}
    try:
        async for post in client.iter_messages(target_entity, limit=posts_limit):
            if not post:
                continue
            try:
                async for comment in client.iter_messages(target_entity, reply_to=post.id, limit=comments_limit):
                    if comment and comment.sender_id:
                        sender = await comment.get_sender()
                        result = save_parsed_audience_user(
                            sender,
                            target_label,
                            'commenters',
                            parse_task_id=parse_task_id,
                            source_id=source_id,
                        )
                        stats['found'] += result['found']
                        stats['saved'] += result['saved']
                        stats['skipped'] += result['skipped']
                        if sender and getattr(sender, 'username', None):
                            save_parsed_comment(
                                target=target_label,
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
        logger.warning(f'Не удалось собрать посты/комментарии {target_label}: {e}')
    return stats


async def parse_message_authors(client, target_entity, target_label, messages_limit, parse_task_id=None, source_id=None):
    stats = {'found': 0, 'saved': 0, 'skipped': 0, 'legacy_saved': 0}
    try:
        async for item in client.iter_messages(target_entity, limit=messages_limit):
            if not item or not getattr(item, 'sender_id', None):
                continue
            try:
                sender = await item.get_sender()
            except Exception:
                sender = None
            result = save_parsed_audience_user(
                sender,
                target_label,
                'message_authors',
                parse_task_id=parse_task_id,
                source_id=source_id,
            )
            stats['found'] += result['found']
            stats['saved'] += result['saved']
            stats['skipped'] += result['skipped']
    except FloodWaitError as e:
        await asyncio.sleep(int(e.seconds))
    except Exception as e:
        logger.warning(f'Не удалось собрать авторов сообщений {target_label}: {e}')
    return stats


def _merge_parse_stats(base, child):
    for key in ['found', 'saved', 'skipped', 'legacy_saved']:
        base[key] = int(base.get(key, 0) or 0) + int(child.get(key, 0) or 0)
    return base


async def parse_engaged_users(client, target_entity, target_label, posts_limit, comments_limit, messages_limit=None, parse_task_id=None, source_id=None):
    stats = {
        'found': 0,
        'saved': 0,
        'skipped': 0,
        'legacy_saved': 0,
        'commenters_found': 0,
        'commenters_saved': 0,
        'commenters_skipped': 0,
        'comments_count': 0,
        'authors_found': 0,
        'authors_saved': 0,
        'authors_skipped': 0,
    }
    comment_stats = await parse_comments(
        client,
        target_entity,
        target_label,
        posts_limit,
        comments_limit,
        parse_task_id=parse_task_id,
        source_id=source_id,
    )
    _merge_parse_stats(stats, comment_stats)
    stats['commenters_found'] = int(comment_stats.get('found') or 0)
    stats['commenters_saved'] = int(comment_stats.get('saved') or 0)
    stats['commenters_skipped'] = int(comment_stats.get('skipped') or 0)
    stats['comments_count'] = int(comment_stats.get('legacy_saved') or 0)

    author_stats = await parse_message_authors(
        client,
        target_entity,
        target_label,
        messages_limit=messages_limit or posts_limit,
        parse_task_id=parse_task_id,
        source_id=source_id,
    )
    _merge_parse_stats(stats, author_stats)
    stats['authors_found'] = int(author_stats.get('found') or 0)
    stats['authors_saved'] = int(author_stats.get('saved') or 0)
    stats['authors_skipped'] = int(author_stats.get('skipped') or 0)
    return stats


def _source_target_type(parsed_target):
    target_type = str(getattr(parsed_target, 'target_type', '') or '')
    if target_type in {'private_invite', 'joinchat_invite'}:
        return 'invite'
    if target_type in {'public_link', 'public_username'}:
        return 'chat'
    return target_type or 'chat'


def _classify_parser_error(exc):
    text = str(exc or '').strip()
    lowered = text.lower()
    if lowered.startswith('participants_unavailable:'):
        return 'participants_unavailable', text.split(':', 1)[1].strip() or text
    if 'invalid_private_access' in lowered:
        return 'invalid_private_access', text
    if 'expired_private_access' in lowered:
        return 'expired_private_access', text
    if 'private_target_unresolved' in lowered:
        return 'private_target_unresolved', text
    if 'resolve_failed' in lowered:
        return 'resolve_failed', text
    if isinstance(exc, FloodWaitError):
        return 'flood_wait', f'Нужно подождать {int(getattr(exc, "seconds", 0) or 0)} сек'
    if isinstance(exc, ChannelPrivateError):
        return 'private_target_unresolved', 'Цель приватна и недоступна'
    return exc.__class__.__name__.replace('Error', '').lower() or 'unknown_error', text or 'Неизвестная ошибка'


async def parse_target_with_client(client, parsed_target, mode, posts_limit, comments_limit, members_limit=None, messages_limit=None, parse_task_id=None):
    access = await SOURCE_ACCESS_SERVICE.ensure_source_access(client, parsed_target)
    if not access.get('ok'):
        raise RuntimeError(f'{access.get("error_code")}: {access.get("error_text")}')
    entity = access.get('entity')
    target_label = access.get('title') or parsed_target.display_value
    source = resolve_source(
        parsed_target.display_value,
        source_type=_source_target_type(parsed_target),
        title=target_label,
        meta_json={
            'target_type': parsed_target.target_type,
            'normalized_value': parsed_target.normalized_value,
            'display_value': parsed_target.display_value,
        },
    )
    if mode == 'members':
        stats = await parse_members(
            client,
            entity,
            target_label,
            members_limit=members_limit,
            parse_task_id=parse_task_id,
            source_id=(source or {}).get('id'),
        )
    elif mode == 'commenters':
        stats = await parse_comments(
            client,
            entity,
            target_label,
            posts_limit,
            comments_limit,
            parse_task_id=parse_task_id,
            source_id=(source or {}).get('id'),
        )
    elif mode == 'message_authors':
        stats = await parse_message_authors(
            client,
            entity,
            target_label,
            messages_limit=messages_limit or posts_limit,
            parse_task_id=parse_task_id,
            source_id=(source or {}).get('id'),
        )
    elif mode == 'engaged_users':
        stats = await parse_engaged_users(
            client,
            entity,
            target_label,
            posts_limit,
            comments_limit,
            messages_limit=messages_limit or posts_limit,
            parse_task_id=parse_task_id,
            source_id=(source or {}).get('id'),
        )
    else:
        raise RuntimeError(f'Unsupported parse mode: {mode}')
    logger.info(
        f'Источник {target_label}: mode={mode}, found={stats["found"]}, '
        f'saved={stats["saved"]}, skipped={stats["skipped"]}'
    )
    stats['source_title'] = target_label
    stats['source_id'] = (source or {}).get('id')
    stats['join_status'] = access.get('join_status')
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
    sessions = get_usable_sessions()
    if not sessions:
        raise RuntimeError('No working session files found')
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
        'commenters_saved': 0,
        'authors_saved': 0,
        'errors': 0,
        'current_source': '',
        'active_session': '',
        'last_error': '',
        'source_results': [],
        'message': f'Parser started ({mode})',
    }
    _write_progress(progress_file, progress)
    if task_id:
        PARSE_TASK_REPO.update_status(task_id, 'running')

    total_found = 0
    total_saved = 0
    total_skipped = 0
    total_legacy_saved = 0
    total_commenters_saved = 0
    total_authors_saved = 0
    source_results = []
    for idx, parsed_target in enumerate(targets):
        source_label = parsed_target.display_value
        progress['current_source'] = source_label
        progress['message'] = f'Parsing {source_label}'
        _write_progress(progress_file, progress)
        sess = selected_sessions[idx % len(selected_sessions)]
        progress['active_session'] = sess
        client = None
        source_result = {
            'source': source_label,
            'target_type': parsed_target.target_type,
            'status': 'running',
            'saved': 0,
            'skipped': 0,
            'found': 0,
            'error_code': '',
            'error_text': '',
            'source_title': source_label,
        }
        try:
            client = build_telegram_client(sess, API_ID, API_HASH, proxy=proxy)
            await client.start()
            stats = await parse_target_with_client(
                client,
                parsed_target,
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
            total_commenters_saved += int(stats.get('commenters_saved') or 0)
            total_authors_saved += int(stats.get('authors_saved') or 0)
            source_result.update(
                {
                    'status': 'success',
                    'saved': int(stats.get('saved') or 0),
                    'skipped': int(stats.get('skipped') or 0),
                    'found': int(stats.get('found') or 0),
                    'comments': int(stats.get('legacy_saved') or 0),
                    'commenters_saved': int(stats.get('commenters_saved') or 0),
                    'authors_saved': int(stats.get('authors_saved') or 0),
                    'source_title': stats.get('source_title') or source_label,
                    'source_id': stats.get('source_id'),
                    'join_status': stats.get('join_status') or '',
                }
            )
            progress['message'] = f'Done {source_label}'
        except Exception as e:
            progress['sources_failed'] += 1
            progress['errors'] += 1
            error_code, error_text = _classify_parser_error(e)
            progress['last_error'] = f'{source_label}: {error_code}: {error_text}'
            progress['message'] = f'Ошибка в {source_label}'
            source_result.update({'status': 'failed', 'error_code': error_code, 'error_text': error_text})
            logger.exception(f'Ошибка парсинга источника {source_label}')
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
        progress['commenters_saved'] = total_commenters_saved
        progress['authors_saved'] = total_authors_saved
        source_results.append(source_result)
        progress['source_results'] = source_results[-8:]
        if task_id:
            PARSE_TASK_REPO.update_progress(
                task_id,
                found=total_found,
                saved=total_saved,
                skipped=total_skipped,
                errors=progress['errors'],
            )
            PARSE_TASK_REPO.update_details(
                task_id,
                source_report_json=source_results,
                last_error=progress.get('last_error') or '',
            )
        _write_progress(progress_file, progress)

    progress['status'] = 'finished_with_errors' if progress['errors'] else 'finished'
    progress['message'] = 'Parser finished with errors' if progress['errors'] else 'Parser finished'
    if task_id:
        PARSE_TASK_REPO.update_details(
            task_id,
            source_report_json=source_results,
            last_error=progress.get('last_error') or '',
        )
        PARSE_TASK_REPO.finish(task_id, status='failed' if progress['errors'] else 'finished')
    _write_progress(progress_file, progress)
    logger.info(
        f'Парсинг завершен: found={total_found}, saved={total_saved}, skipped={total_skipped}, legacy={total_legacy_saved}, '
        f'sources={len(targets)}, errors={progress["errors"]}'
    )


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Telegram parser: audience normalization worker')
    p.add_argument('--mode', default='members', choices=['members', 'commenters', 'message_authors', 'engaged_users'])
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
