import argparse
import asyncio

from loguru import logger
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from config import API_ID, API_HASH
from db import save_parsed_user, save_parsed_comment
from functions import get_sessions, get_proxy


logger.add('logging.log', rotation='1 MB', encoding='utf-8')


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


async def run_parser(target, posts_limit, comments_limit):
    if not API_ID or not API_HASH:
        raise RuntimeError('Set TG_API_ID and TG_API_HASH env vars')
    sessions = get_sessions()
    if not sessions:
        raise RuntimeError('No .session files found')
    proxy = get_proxy()
    client = TelegramClient(sessions[0], API_ID, API_HASH, proxy=proxy)
    await client.start()
    users = await parse_members(client, target)
    comments = await parse_comments(client, target, posts_limit, comments_limit)
    await client.disconnect()
    logger.info(f'Парсинг завершен: users={users}, comments={comments}')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description='Telegram parser: usernames from members/comments')
    p.add_argument('--target', required=True, help='@chat, @channel or invite link')
    p.add_argument('--posts-limit', type=int, default=100)
    p.add_argument('--comments-limit', type=int, default=200)
    args = p.parse_args()
    asyncio.run(run_parser(args.target, args.posts_limit, args.comments_limit))
