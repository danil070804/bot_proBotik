from telethon.errors import FloodWaitError
from loguru import logger
from random import randint
from time import sleep
import asyncio
import os

from config import API_ID, API_HASH, slp, join_sleep, from_join_sleep, to_join_sleep
from functions import get_usable_sessions, get_proxy, generate_chats_list, build_telegram_client
from db import insert_chat_db, get_all_chats, is_full, set_account_health, delete_session_file
from services.join_service import JoinService


logger.add('logging.log', rotation='1 MB', encoding='utf-8')
JOIN_SERVICE = JoinService()
FATAL_ACCOUNT_STATUSES = {'account_banned', 'session_invalid', 'invalid', 'dead'}


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


async def join_to_chat(chat, client, session):
    if chat in get_all_chats():
        return
    parsed_target = None
    try:
        parsed_target = JOIN_SERVICE.parse_target(chat)
        insert_chat_db(session, parsed_target.normalized_value)
        result = await JOIN_SERVICE.join_target(client, parsed_target)
        status = result.get('status')
        if status in {'joined', 'join_request_sent'}:
            logger.info(f'{session} обработал цель {parsed_target.display_value}: {status}')
        elif status == 'already_in':
            logger.info(f'{session} уже состоит в чате: {parsed_target.display_value}')
        elif status in {'invalid_invite', 'expired_invite', 'private_target_unresolved'}:
            logger.error(f'{session} проблема с целью {parsed_target.display_value}: {status} | {result.get("error_text")}')
            return
        elif status in {'account_banned', 'session_invalid', 'account_limited', 'unknown_error'}:
            logger.error(f'{session} ошибка аккаунта для {parsed_target.display_value}: {status} | {result.get("error_text")}')
            _purge_dead_session(session, status)
            return
        if join_sleep != 0:
            await asyncio.sleep(join_sleep)
        else:
            await asyncio.sleep(randint(from_join_sleep, to_join_sleep))
    except FloodWaitError as e:
        logger.error(f'{session} flood wait: {e.seconds}')
        await asyncio.sleep(int(e.seconds))
    except Exception as e:
        target_ref = parsed_target.display_value if parsed_target else str(chat)
        logger.error(f'{session} ошибка для {target_ref}: {e}')
        await asyncio.sleep(randint(5, 20))


async def add_acc(session, chats, proxy):
    client = build_telegram_client(session, API_ID, API_HASH, proxy=proxy)
    await client.start()
    logger.info(f'Joiner started ({session})')
    for chat in chats:
        if is_full(session):
            logger.info(f'{session} достиг лимита {len(get_all_chats())} чатов')
            break
        await join_to_chat(chat, client, session)
    await client.disconnect()
    logger.info(f'Аккаунт {session} завершил работу')


async def main():
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    sessions = get_usable_sessions()
    if not sessions:
        raise RuntimeError('No working session files found')
    chats_list = generate_chats_list(sessions=sessions)
    proxy = get_proxy()
    logger.info('Script started')

    for sess in sessions:
        client = build_telegram_client(sess, API_ID, API_HASH, proxy=proxy)
        await client.start()
        await client.disconnect()
        logger.info(f'Проверка аккаунта {sess}, сон {slp} сек')
        sleep(slp)

    tasks = []
    for i, session in enumerate(sessions):
        chats = chats_list[i] if i < len(chats_list) else []
        tasks.append(asyncio.create_task(add_acc(session, chats, proxy)))
    if tasks:
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())
