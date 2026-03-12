from telethon.errors import (InviteHashExpiredError, UserAlreadyParticipantError,
                             ChannelsTooMuchError, FloodWaitError, SessionPasswordNeededError, PeerFloodError, UserBannedInChannelError)
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest
from telethon import TelegramClient, connection


from random import randint
from loguru import logger
from time import sleep
import asyncio
import os


from functions import *
from config import *
from db import *

sessions = get_sessions()
prox = get_proxy()

logger.add('logging.log', rotation='1 MB', encoding='utf-8')
logger.info('Script started')
chats_list = generate_chats_list()

for sess in sessions:
    client = TelegramClient(str(i[0]), API_ID, API_HASH, device_model="Ids bot", system_version="6.12.0", app_version="10 P (28)", proxy=proxy)
    logger.info(f'Проверка аккаунта {sess}')
    client.start()
    client.disconnect()
    logger.info(f'Сон между проверкой {slp} секунд...')
    sleep(slp)


async def join_to_chat(chat, client, session):
    if not chat in get_all_chats():
        data = link_convert(chat)
        chat_type, chat = data[0], data[1]
        chat = chat
        try:
            insert_chat_db(sess, chat)
            if chat_type == 'close':
                await client(ImportChatInviteRequest(chat))
            elif chat_type == 'open':
                await client(JoinChannelRequest(chat))
            logger.info(f'{session} вступил в чат {chat}')
            if join_sleep != 0:
                logger.info(f'{session} сон {join_sleep} сек')
                await asyncio.sleep(join_sleep)
            else:
                s = randint(from_join_sleep, to_join_sleep)
                logger.info(f'{session} сон {s}сек')
                await asyncio.sleep(s)
        except InviteHashExpiredError:
            logger.error(f'{session} Ссылка не рабочая:{chat}')
        except ValueError:
            logger.error(f'{session} Ссылка не рабочая:{chat}')
        except UserAlreadyParticipantError:
            logger.error(
                f'{session} Пользователь уже состоит в чате!{chat}')
        except UserAlreadyParticipantError:
            logger.error(f'{session} Превышен лимит вступления в чаты')
        except FloodWaitError as e:
            logger.error(f'{session} {e}')
            await asyncio.sleep(int(e.seconds))
        except Exception as e:
            logger.error(f'{session} {e}')
            await asyncio.sleep(randint(5, 20))


async def add_acc(session, chats):
    client = TelegramClient(str(i[0]), API_ID, API_HASH, device_model="Ids bot", system_version="6.12.0", app_version="10 P (28)", proxy=proxy)
    await client.start()
    logger.info(f'Joiner started (account {session})')
    for chat in chats:
        if is_full(session):
            return
        await join_to_chat(chat, client, session)
    logger.info(f'Аккаунт {session} завершил свою работу!')
    await client.disconnect()

loop = asyncio.get_event_loop()

i = 0
for session in sessions:
    loop.create_task(add_acc(session, chats_list[i]))
    i += 1

loop.run_forever()
