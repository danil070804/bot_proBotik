import argparse
import asyncio
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from keyword_search import run_keyword_search, _read_keywords  # noqa: E402
from repositories.parse_tasks import ParseTaskRepository  # noqa: E402


PARSE_TASK_REPO = ParseTaskRepository()


def main():
    parser = argparse.ArgumentParser(description='Keyword search worker')
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
