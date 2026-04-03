import argparse
import asyncio
import json
import os

from loguru import logger

from config import API_ID, API_HASH
from db import set_account_health, delete_session_file
from functions import get_proxy, get_usable_sessions, build_telegram_client
from services.join_service import JoinService


logger.add('logging.log', rotation='1 MB', encoding='utf-8')
JOIN_SERVICE = JoinService()
TARGET_FATAL_STATUSES = {'invalid_invite', 'expired_invite', 'private_target_unresolved'}
ACCOUNT_ERROR_STATUSES = {'flood_wait', 'account_limited', 'account_banned', 'session_invalid', 'unknown_error'}
FATAL_ACCOUNT_STATUSES = {'account_banned', 'session_invalid', 'invalid', 'dead'}


def _write_progress(progress_file, data):
    if not progress_file:
        return
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


def _base_progress(raw_target, parsed_target, sessions):
    return {
        'mode': 'join_target',
        'status': 'running',
        'target': parsed_target.display_value,
        'target_raw': raw_target,
        'target_type': parsed_target.target_type,
        'target_normalized': parsed_target.normalized_value,
        'join_method': parsed_target.join_method,
        'total_accounts': len(sessions),
        'done_accounts': 0,
        'joined': 0,
        'already_in': 0,
        'already': 0,
        'join_request_sent': 0,
        'invalid_invite': 0,
        'expired_invite': 0,
        'private_target_unresolved': 0,
        'flood_wait': 0,
        'account_limited': 0,
        'account_banned': 0,
        'session_invalid': 0,
        'unknown_error': 0,
        'invite_errors': 0,
        'account_errors': 0,
        'failed': 0,
        'active_session': '',
        'last_error_code': '',
        'last_error_text': '',
        'last_result_status': '',
        'last_join_method': parsed_target.join_method,
        'message': 'Join started',
    }


def _apply_result(progress, result):
    status = str(result.get('status') or 'unknown_error').strip()
    progress['last_result_status'] = status
    progress['last_join_method'] = result.get('join_method') or ''
    if status in progress:
        progress[status] += 1
    if status == 'already_in':
        progress['already'] += 1
    if status in {'invalid_invite', 'expired_invite'}:
        progress['invite_errors'] += 1
    elif status in ACCOUNT_ERROR_STATUSES:
        progress['account_errors'] += 1
    elif status == 'private_target_unresolved':
        progress['invite_errors'] += 1
    progress['failed'] = progress['invite_errors'] + progress['account_errors']
    if result.get('error_code'):
        progress['last_error_code'] = result.get('error_code') or ''
        progress['last_error_text'] = result.get('error_text') or ''


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


def _health_update_from_result(result, parsed_target):
    status = result.get('status')
    target_ref = parsed_target.display_value
    if status == 'joined':
        return 'working', 'ok', f'Вступил в цель {target_ref}'
    if status == 'already_in':
        return 'working', 'ok', f'Уже состоит в цели {target_ref}'
    if status == 'join_request_sent':
        return 'working', 'ok', f'Отправил join request в цель {target_ref}'
    if status in {'flood_wait', 'account_limited'}:
        return (
            'flooded' if status == 'flood_wait' else 'limited',
            result.get('error_code') or status,
            result.get('error_text') or '',
        )
    if status == 'account_banned':
        return 'dead', result.get('error_code') or status, result.get('error_text') or ''
    if status == 'session_invalid':
        return 'invalid', result.get('error_code') or status, result.get('error_text') or ''
    if status == 'unknown_error':
        return 'limited', result.get('error_code') or 'unknown_error', result.get('error_text') or ''
    return '', '', ''


async def run_target_joiner(target, progress_file):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    target = str(target or '').strip()
    if not target:
        raise RuntimeError('Set --target')

    parsed_target = JOIN_SERVICE.parse_target(target)
    sessions = get_usable_sessions()
    if not sessions:
        raise RuntimeError('No working session files found')

    progress = _base_progress(target, parsed_target, sessions)
    logger.info(
        'target_joiner.start raw_target={} normalized_target={} target_type={} join_method={} accounts={}',
        target,
        parsed_target.normalized_value,
        parsed_target.target_type,
        parsed_target.join_method,
        len(sessions),
    )
    _write_progress(progress_file, progress)

    proxy = get_proxy()
    for session in sessions:
        progress['active_session'] = session
        progress['message'] = f'Joining with {session}'
        _write_progress(progress_file, progress)
        client = None
        result = None
        try:
            client = build_telegram_client(session, API_ID, API_HASH, proxy=proxy)
            await client.start()
            result = await JOIN_SERVICE.join_target(client, parsed_target)
        except Exception as exc:
            result = {
                'status': 'unknown_error',
                'error_code': 'unknown_error',
                'error_text': str(exc),
                'target_raw': target,
                'target_type': parsed_target.target_type,
                'target_normalized': parsed_target.normalized_value,
                'target_display': parsed_target.display_value,
                'join_method': parsed_target.join_method,
                'exception_name': exc.__class__.__name__,
                'result_class': '',
            }
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

        _apply_result(progress, result)
        progress['message'] = f'{result.get("status")}:{session}'
        account_status, reason_code, account_details = _health_update_from_result(result, parsed_target)
        if account_status:
            set_account_health(session, account_status, account_details, reason_code=reason_code, reason_text=account_details)
        if result.get('status') in FATAL_ACCOUNT_STATUSES:
            _purge_dead_session(session, result.get('status'))
        logger.info(
            'target_joiner.account session={} target_type={} normalized_target={} join_method={} status={} error_code={} error_text={}',
            session,
            parsed_target.target_type,
            parsed_target.normalized_value,
            result.get('join_method'),
            result.get('status'),
            result.get('error_code') or '-',
            result.get('error_text') or '-',
        )
        progress['done_accounts'] += 1
        _write_progress(progress_file, progress)
        if result.get('status') in TARGET_FATAL_STATUSES:
            progress['message'] = f'fatal_target_error:{result.get("status")}'
            logger.warning(
                'target_joiner.stop reason={} normalized_target={} after_session={}',
                result.get('status'),
                parsed_target.normalized_value,
                session,
            )
            break

    progress['status'] = 'finished_with_errors' if progress['failed'] else 'finished'
    progress['message'] = 'Join finished with errors' if progress['failed'] else 'Join finished'
    _write_progress(progress_file, progress)
    logger.info(
        'target_joiner.finish target={} total={} joined={} already_in={} join_request_sent={} invite_errors={} account_errors={} flood_wait={} failed={}',
        parsed_target.display_value,
        progress.get('done_accounts', 0),
        progress.get('joined', 0),
        progress.get('already_in', 0),
        progress.get('join_request_sent', 0),
        progress.get('invite_errors', 0),
        progress.get('account_errors', 0),
        progress.get('flood_wait', 0),
        progress.get('failed', 0),
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Join all saved sessions to a target channel or group')
    parser.add_argument('--target', required=True, help='Target chat/channel or invite link')
    parser.add_argument('--progress-file', default='', help='Path to JSON progress file')
    args = parser.parse_args()
    asyncio.run(run_target_joiner(args.target, args.progress_file))
