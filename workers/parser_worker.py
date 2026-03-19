import argparse
import asyncio
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from parser import run_parser
from repositories.parse_tasks import ParseTaskRepository


PARSE_TASK_REPO = ParseTaskRepository()


def main():
    parser = argparse.ArgumentParser(description='Telegram parser worker')
    parser.add_argument('--mode', default='members', choices=['members', 'commenters', 'message_authors'])
    parser.add_argument('--target', default='')
    parser.add_argument('--targets-file', default='')
    parser.add_argument('--members-limit', type=int, default=0)
    parser.add_argument('--posts-limit', type=int, default=100)
    parser.add_argument('--comments-limit', type=int, default=200)
    parser.add_argument('--messages-limit', type=int, default=0)
    parser.add_argument('--session-index', type=int, default=0)
    parser.add_argument('--use-all-sessions', action='store_true')
    parser.add_argument('--progress-file', default='')
    parser.add_argument('--task-id', type=int, default=0)
    args = parser.parse_args()

    try:
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
    except Exception:
        if args.task_id:
            try:
                PARSE_TASK_REPO.finish(args.task_id, status='failed')
            except Exception:
                pass
        raise


if __name__ == '__main__':
    main()
