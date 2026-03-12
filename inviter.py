import argparse
import asyncio

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

from config import API_ID, API_HASH
from db import get_usernames_for_invite, mark_invite_result
from functions import get_proxy, get_sessions


logger.add('logging.log', rotation='1 MB', encoding='utf-8')


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


async def run_inviter(source_target, invite_target, limit, sleep_seconds, session_index):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    sessions = get_sessions()
    if not sessions:
        raise RuntimeError('No .session files found')
    if session_index < 0 or session_index >= len(sessions):
        raise RuntimeError(f'session-index out of range: 0..{len(sessions) - 1}')

    rows = get_usernames_for_invite(source_target, invite_target, limit)
    if not rows:
        logger.info('Нет пользователей для инвайта')
        return

    proxy = get_proxy()
    session = sessions[session_index]
    client = TelegramClient(session, API_ID, API_HASH, proxy=proxy)
    await client.start()
    logger.info(f'Инвайтер запущен: session={session}, users={len(rows)}')

    for username, user_id in rows:
        try:
            await invite_one(client, invite_target, username)
            mark_invite_result(source_target, invite_target, username, user_id, 'invited', '')
            logger.info(f'Добавлен: @{username}')
        except UserAlreadyParticipantError:
            mark_invite_result(source_target, invite_target, username, user_id, 'already', '')
        except UserPrivacyRestrictedError:
            mark_invite_result(source_target, invite_target, username, user_id, 'privacy', 'User privacy restricted')
        except PeerFloodError:
            mark_invite_result(source_target, invite_target, username, user_id, 'peer_flood', 'PeerFloodError')
            logger.error('PeerFloodError: остановка инвайта для защиты аккаунта')
            break
        except FloodWaitError as e:
            mark_invite_result(source_target, invite_target, username, user_id, 'flood_wait', str(e.seconds))
            await asyncio.sleep(int(e.seconds))
        except Exception as e:
            mark_invite_result(source_target, invite_target, username, user_id, 'error', str(e))
        await asyncio.sleep(max(0, sleep_seconds))

    await client.disconnect()
    logger.info('Инвайт завершен')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Invite parsed usernames to target chat/channel')
    p.add_argument('--source-target', required=True, help='Source parsed target, e.g. @source_chat')
    p.add_argument('--invite-target', required=True, help='Target where users will be invited')
    p.add_argument('--limit', type=int, default=50, help='Max users per run')
    p.add_argument('--sleep', type=int, default=15, help='Sleep between invites in seconds')
    p.add_argument('--session-index', type=int, default=0, help='Which .session to use')
    args = p.parse_args()
    asyncio.run(
        run_inviter(
            source_target=args.source_target,
            invite_target=args.invite_target,
            limit=args.limit,
            sleep_seconds=args.sleep,
            session_index=args.session_index,
        )
    )
