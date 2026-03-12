from telethon import TelegramClient
from telethon.errors import InviteHashExpiredError, UserAlreadyParticipantError, ChannelsTooMuchError, FloodWaitError, UserBannedInChannelError
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from loguru import logger
from random import randint
from time import sleep
import asyncio

from config import API_ID, API_HASH, slp, join_sleep, from_join_sleep, to_join_sleep
from functions import get_sessions, get_proxy, generate_chats_list, link_convert
from db import insert_chat_db, get_all_chats, is_full


logger.add('logging.log', rotation='1 MB', encoding='utf-8')


async def join_to_chat(chat, client, session):
    if chat in get_all_chats():
        return
    chat_type, converted_chat = link_convert(chat)
    try:
        insert_chat_db(session, converted_chat)
        if chat_type == 'close':
            await client(ImportChatInviteRequest(converted_chat))
        else:
            await client(JoinChannelRequest(converted_chat))
        logger.info(f'{session} вступил в чат {converted_chat}')
        if join_sleep != 0:
            await asyncio.sleep(join_sleep)
        else:
            await asyncio.sleep(randint(from_join_sleep, to_join_sleep))
    except (InviteHashExpiredError, ValueError):
        logger.error(f'{session} ссылка не рабочая: {converted_chat}')
    except UserAlreadyParticipantError:
        logger.error(f'{session} уже состоит в чате: {converted_chat}')
    except ChannelsTooMuchError:
        logger.error(f'{session} достиг лимита чатов')
    except UserBannedInChannelError:
        logger.error(f'{session} заблокирован в чате: {converted_chat}')
    except FloodWaitError as e:
        logger.error(f'{session} flood wait: {e.seconds}')
        await asyncio.sleep(int(e.seconds))
    except Exception as e:
        logger.error(f'{session} ошибка: {e}')
        await asyncio.sleep(randint(5, 20))


async def add_acc(session, chats, proxy):
    client = TelegramClient(session, API_ID, API_HASH, proxy=proxy)
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
    sessions = get_sessions()
    chats_list = generate_chats_list()
    proxy = get_proxy()
    logger.info('Script started')

    for sess in sessions:
        client = TelegramClient(sess, API_ID, API_HASH, proxy=proxy)
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
