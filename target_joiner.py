import argparse
import asyncio
import json

from telethon.errors import FloodWaitError, InviteHashExpiredError, UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from config import API_ID, API_HASH
from db import set_account_health
from functions import get_proxy, get_sessions, build_telegram_client, link_convert


def _write_progress(progress_file, data):
    if not progress_file:
        return
    with open(progress_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


async def _join_target(client, target):
    chat_type, converted_target = link_convert(target)
    if chat_type == 'close':
        await client(ImportChatInviteRequest(converted_target))
    else:
        await client(JoinChannelRequest(converted_target))


async def run_target_joiner(target, progress_file):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    target = str(target or '').strip()
    if not target:
        raise RuntimeError('Set --target')

    sessions = get_sessions()
    if not sessions:
        raise RuntimeError('No session files found')

    progress = {
        'mode': 'join_target',
        'status': 'running',
        'target': target,
        'total_accounts': len(sessions),
        'done_accounts': 0,
        'joined': 0,
        'already': 0,
        'failed': 0,
        'active_session': '',
        'last_error': '',
        'message': 'Join started',
    }
    _write_progress(progress_file, progress)

    proxy = get_proxy()
    for session in sessions:
        progress['active_session'] = session
        progress['message'] = f'Joining with {session}'
        _write_progress(progress_file, progress)
        client = None
        try:
            client = build_telegram_client(session, API_ID, API_HASH, proxy=proxy)
            await client.start()
            await _join_target(client, target)
            progress['joined'] += 1
            progress['message'] = f'Joined: {session}'
            set_account_health(session, 'active', f'Вступил в цель {target}')
        except UserAlreadyParticipantError:
            progress['already'] += 1
            progress['message'] = f'Already in target: {session}'
            set_account_health(session, 'active', f'Уже состоит в цели {target}')
        except InviteHashExpiredError as e:
            progress['failed'] += 1
            progress['last_error'] = f'{session}: InviteHashExpiredError: {e}'
            progress['message'] = f'Ссылка недействительна: {session}'
        except FloodWaitError as e:
            wait_s = int(e.seconds)
            progress['failed'] += 1
            progress['last_error'] = f'{session}: FloodWaitError: {wait_s}'
            progress['message'] = f'Flood wait: {session}'
            set_account_health(session, 'limited', f'Ожидание из-за лимитов: {wait_s} сек')
        except Exception as e:
            name = e.__class__.__name__
            progress['failed'] += 1
            progress['last_error'] = f'{session}: {name}: {e}'
            progress['message'] = f'Ошибка: {session}'
            if name in {'AuthKeyUnregisteredError', 'SessionRevokedError', 'UserDeactivatedError', 'UserDeactivatedBanError', 'PhoneNumberBannedError'}:
                set_account_health(session, 'dead', f'{name}: {e}')
            else:
                set_account_health(session, 'limited', f'{name}: {e}')
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        progress['done_accounts'] += 1
        _write_progress(progress_file, progress)

    progress['status'] = 'finished_with_errors' if progress['failed'] else 'finished'
    progress['message'] = 'Join finished with errors' if progress['failed'] else 'Join finished'
    _write_progress(progress_file, progress)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Join all saved sessions to a target channel or group')
    parser.add_argument('--target', required=True, help='Target chat/channel or invite link')
    parser.add_argument('--progress-file', default='', help='Path to JSON progress file')
    args = parser.parse_args()
    asyncio.run(run_target_joiner(args.target, args.progress_file))