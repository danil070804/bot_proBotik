import argparse
import asyncio
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from parser import run_parser


def main():
    parser = argparse.ArgumentParser(description='Telegram parser worker')
    parser.add_argument('--target', default='')
    parser.add_argument('--targets-file', default='')
    parser.add_argument('--posts-limit', type=int, default=100)
    parser.add_argument('--comments-limit', type=int, default=200)
    parser.add_argument('--session-index', type=int, default=0)
    parser.add_argument('--use-all-sessions', action='store_true')
    parser.add_argument('--progress-file', default='')
    args = parser.parse_args()

    asyncio.run(
        run_parser(
            target=args.target,
            targets_file=args.targets_file,
            posts_limit=args.posts_limit,
            comments_limit=args.comments_limit,
            session_index=args.session_index,
            use_all_sessions=args.use_all_sessions,
            progress_file=args.progress_file,
        )
    )


if __name__ == '__main__':
    main()
