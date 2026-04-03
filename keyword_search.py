import argparse
import asyncio
import json
import os
from typing import Iterable, List, Optional, Set, Tuple

from loguru import logger
from telethon import functions, types
from telethon.errors import FloodWaitError

from config import API_ID, API_HASH
from functions import build_telegram_client, get_proxy, get_usable_sessions
from db import is_source_allowed, set_account_health, delete_session_file
from repositories.audience import AudienceRepository
from repositories.parse_tasks import ParseTaskRepository
from services.join_service import JoinService


logger.add('logging.log', rotation='1 MB', encoding='utf-8')
AUDIENCE_REPO = AudienceRepository()
PARSE_TASK_REPO = ParseTaskRepository()
JOIN_SERVICE = JoinService()
FATAL_ACCOUNT_STATUSES = {'account_banned', 'session_invalid', 'invalid', 'dead'}


def _write_progress(progress_file: str, data: dict):
    if not progress_file:
        return
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def _read_keywords(single: str, keywords_file: str) -> List[str]:
    keywords = []
    if single:
        raw_single = str(single).replace('\r', '\n')
        for part in raw_single.replace(',', '\n').split('\n'):
            item = part.strip()
            if item:
                keywords.append(item)
    if keywords_file:
        with open(keywords_file, 'r', encoding='utf-8') as f:
            for line in f:
                item = (line or '').strip()
                if item:
                    keywords.append(item)
    uniq = []
    seen = set()
    for kw in keywords:
        if kw and kw.lower() not in seen:
            seen.add(kw.lower())
            uniq.append(kw)
    return uniq


def _chat_target(chat) -> Optional[Tuple[str, str]]:
    """
    Returns tuple (target, title) if chat has a public username, otherwise None.
    """
    username = getattr(chat, 'username', '') or ''
    if not username:
        return None
    target = '@' + username.lstrip('@')
    title = getattr(chat, 'title', '') or target
    return target, title


def _purge_dead_session(session, status):
    if status not in FATAL_ACCOUNT_STATUSES:
        return
    try:
        if os.path.exists(session):
            os.remove(session)
    except Exception:
        pass
    try:
        delete_session_file(session)
    except Exception:
        pass
    try:
        set_account_health(session, 'dead', f'auto_delete:{status}', reason_code=status, reason_text=f'{status}')
    except Exception:
        pass


async def _search_keyword(client, keyword: str, limit: int) -> List[dict]:
    try:
        result = await client(functions.contacts.SearchRequest(q=keyword, limit=limit))
    except FloodWaitError as e:
        await asyncio.sleep(int(e.seconds))
        return []
    except Exception as exc:
        logger.warning(f'Поиск "{keyword}" завершился ошибкой: {exc}')
        return []
    items = []
    for chat in result.chats or []:
        target_title = _chat_target(chat)
        if not target_title:
            continue
        target, title = target_title
        chat_type = 'channel' if getattr(chat, 'broadcast', False) else 'supergroup' if getattr(chat, 'megagroup', False) else 'group'
        items.append(
            {
                'target': target,
                'title': title,
                'chat_type': chat_type,
                'keyword': keyword,
                'telegram_id': int(getattr(chat, 'id', 0) or 0),
            }
        )
    return items


async def _join_and_post(client, target: str, post_text: str, post_image: str, delay: int) -> Tuple[bool, bool, str]:
    """
    Returns (joined, posted, error_text)
    """
    joined = False
    posted = False
    last_error = ''
    try:
        join_result = await JOIN_SERVICE.join_target(client, target)
        status = join_result.get('status')
        joined = status in {'joined', 'already_in', 'join_request_sent'}
        if not joined and status not in {'already_in', 'join_request_sent'}:
            last_error = join_result.get('error_text') or status or ''
        if status == 'flood_wait':
            await asyncio.sleep(int(getattr(join_result.get('error_text'), 'seconds', 0) or 0))
        if status in FATAL_ACCOUNT_STATUSES:
            raise RuntimeError(f'fatal_account:{status}')
    except FloodWaitError as e:
        last_error = f'flood_wait:{int(e.seconds)}'
        await asyncio.sleep(int(e.seconds))
    except Exception as exc:
        last_error = str(exc)

    if post_text or post_image:
        try:
            if delay and delay > 0:
                await asyncio.sleep(int(delay))
            if joined or post_image or post_text:
                if post_image:
                    await client.send_file(target, post_image, caption=post_text or '')
                else:
                    await client.send_message(target, post_text or '')
                posted = True
        except FloodWaitError as e:
            last_error = f'post_flood:{int(e.seconds)}'
            await asyncio.sleep(int(e.seconds))
        except Exception as exc:
            last_error = str(exc)
    return joined, posted, last_error


async def run_keyword_search(
    keywords: Iterable[str],
    search_limit: int,
    session_index: int,
    use_all_sessions: bool,
    progress_file: str,
    post_text: str = '',
    post_image: str = '',
    post_delay: int = 0,
    post_limit_per_account: int = 5,
    task_id: int = None,
):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')

    keywords = [kw for kw in keywords if str(kw or '').strip()]
    if not keywords:
        raise RuntimeError('Передай хотя бы одно ключевое слово')

    sessions = get_usable_sessions()
    if not sessions:
        raise RuntimeError('No working session files found')
    if not use_all_sessions and (session_index < 0 or session_index >= len(sessions)):
        raise RuntimeError(f'session-index out of range: 0..{len(sessions) - 1}')

    selected_sessions = sessions if use_all_sessions else [sessions[session_index]]
    proxy = get_proxy()
    seen_targets: Set[str] = set()
    progress = {
        'mode': 'keyword_search',
        'parser_mode': 'keyword_search',
        'parse_task_id': task_id,
        'status': 'running',
        'keywords_total': len(keywords),
        'keywords_done': 0,
        'found_total': 0,
        'saved_total': 0,
        'filtered_total': 0,
        'joined_total': 0,
        'posted_total': 0,
        'post_limit_per_account': post_limit_per_account,
        'skipped_posts': 0,
        'errors': 0,
        'current_keyword': '',
        'active_session': '',
        'last_error': '',
        'source_results': [],
        'message': 'Keyword search started',
    }
    _write_progress(progress_file, progress)
    if task_id:
        PARSE_TASK_REPO.update_status(task_id, 'running')

    session_post_counter = {s: 0 for s in selected_sessions}

    for idx, keyword in enumerate(keywords):
        session = selected_sessions[idx % len(selected_sessions)]
        progress['current_keyword'] = keyword
        progress['active_session'] = session
        progress['message'] = f'Поиск "{keyword}"'
        _write_progress(progress_file, progress)

        client = None
        try:
            client = build_telegram_client(session, API_ID, API_HASH, proxy=proxy)
            await client.start()
            results = await _search_keyword(client, keyword, search_limit)
            for item in results:
                target = item['target']
                if target.lower() in seen_targets:
                    continue
                seen_targets.add(target.lower())
                if not is_source_allowed(target):
                    progress['filtered_total'] += 1
                    continue
                source = AUDIENCE_REPO.get_or_create_source(
                    source_type='search',
                    source_value=target,
                    title=item.get('title') or target,
                    meta_json={
                        'keyword': keyword,
                        'chat_type': item.get('chat_type'),
                        'telegram_id': item.get('telegram_id'),
                    },
                )
                progress['found_total'] += 1
                progress['saved_total'] += 1 if source else 0

                joined = False
                posted = False
                error_text = ''
                try:
                    can_post = session_post_counter.get(session, 0) < post_limit_per_account if post_limit_per_account > 0 else True
                    if not can_post:
                        joined, posted, error_text = await _join_and_post(client, target, '', '', post_delay)
                        progress['skipped_posts'] += 1
                    else:
                        joined, posted, error_text = await _join_and_post(client, target, post_text, post_image, post_delay)
                        if posted:
                            session_post_counter[session] = session_post_counter.get(session, 0) + 1
                except Exception as exc:
                    error_text = str(exc)
                    if str(exc).startswith('fatal_account:'):
                        _purge_dead_session(session, str(exc).split(':', 1)[1])
                        break
                if joined:
                    progress['joined_total'] += 1
                if posted:
                    progress['posted_total'] += 1
                if error_text:
                    progress['errors'] += 1
                    progress['last_error'] = f'{target}: {error_text}'
                progress['source_results'].append(
                    {
                        'source': target,
                        'source_title': item.get('title') or target,
                        'status': 'success' if not error_text else 'failed',
                        'error_code': '' if not error_text else 'join_or_post_error',
                        'error_text': error_text,
                        'joined': joined,
                        'posted': posted,
                        'keyword': keyword,
                        'source_id': (source or {}).get('id') if source else None,
                    }
                )
        except FloodWaitError as e:
            progress['errors'] += 1
            progress['last_error'] = f'flood_wait:{int(e.seconds)}'
            await asyncio.sleep(int(e.seconds))
        except Exception as exc:
            progress['errors'] += 1
            progress['last_error'] = str(exc)
            logger.warning(f'Ошибка обработки ключа {keyword}: {exc}')
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

        progress['keywords_done'] += 1
        _write_progress(progress_file, progress)
        if task_id:
            PARSE_TASK_REPO.update_progress(
                task_id,
                found=progress.get('found_total'),
                saved=progress.get('saved_total'),
                skipped=progress.get('filtered_total'),
                errors=progress.get('errors'),
            )
            PARSE_TASK_REPO.update_details(
                task_id,
                source_report_json=progress.get('source_results'),
                last_error=progress.get('last_error'),
                meta_json={
                    'search_limit': search_limit,
                    'post_text': post_text,
                    'post_image': post_image,
                    'post_delay': post_delay,
                    'post_limit_per_account': post_limit_per_account,
                    'keywords': keywords,
                },
            )

    progress['status'] = 'finished_with_errors' if progress['errors'] else 'finished'
    progress['message'] = 'Keyword search finished with errors' if progress['errors'] else 'Keyword search finished'
    _write_progress(progress_file, progress)
    if task_id:
        PARSE_TASK_REPO.finish(task_id, status='failed' if progress['errors'] else 'finished')
    logger.info(
        'keyword_search.finish total_keywords={} found={} saved={} joined={} posted={} skipped_posts={} errors={}',
        len(keywords),
        progress.get('found_total', 0),
        progress.get('saved_total', 0),
        progress.get('joined_total', 0),
        progress.get('posted_total', 0),
        progress.get('skipped_posts', 0),
        progress.get('errors', 0),
    )


def main():
    parser = argparse.ArgumentParser(description='Telegram keyword search worker')
    parser.add_argument('--keywords', default='')
    parser.add_argument('--keywords-file', default='')
    parser.add_argument('--search-limit', type=int, default=20)
    parser.add_argument('--post-text', default='')
    parser.add_argument('--post-image', default='')
    parser.add_argument('--post-delay', type=int, default=0)
    parser.add_argument('--session-index', type=int, default=0)
    parser.add_argument('--use-all-sessions', action='store_true')
    parser.add_argument('--progress-file', default='')
    parser.add_argument('--task-id', type=int, default=0)
    args = parser.parse_args()

    keywords = _read_keywords(args.keywords, args.keywords_file)
    try:
        asyncio.run(
            run_keyword_search(
                keywords=keywords,
                search_limit=args.search_limit,
                session_index=args.session_index,
                use_all_sessions=args.use_all_sessions,
                progress_file=args.progress_file,
                post_text=args.post_text or '',
                post_image=args.post_image or '',
                post_delay=args.post_delay or 0,
                task_id=args.task_id or None,
            )
        )
    except Exception:
        if args.task_id:
            try:
                PARSE_TASK_REPO.finish(args.task_id, status='failed')
            except Exception:
                pass
        raise


if __name__ == '__main__':
    main()
