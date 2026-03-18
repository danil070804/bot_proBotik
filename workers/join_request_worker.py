import argparse
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from repositories.join_requests import JoinRequestRepository
from services.join_request_service import JoinRequestService


def _write_progress(progress_file, data):
    if not progress_file:
        return
    with open(progress_file, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description='Process Telegram join requests')
    parser.add_argument('--join-request-id', type=int, default=0)
    parser.add_argument('--action', choices=['auto', 'approve', 'decline'], default='auto')
    parser.add_argument('--limit', type=int, default=50)
    parser.add_argument('--moderator-id', type=int, default=0)
    parser.add_argument('--reason', default='')
    parser.add_argument('--progress-file', default='')
    args = parser.parse_args()

    service = JoinRequestService()
    repo = JoinRequestRepository()

    try:
        if args.join_request_id:
            if args.action == 'approve':
                result = service.approve_join_request(
                    args.join_request_id,
                    moderator_id=args.moderator_id or None,
                    reason=args.reason or 'manual_approve',
                )
            elif args.action == 'decline':
                result = service.decline_join_request(
                    args.join_request_id,
                    moderator_id=args.moderator_id or None,
                    reason=args.reason or 'manual_decline',
                )
            else:
                result = service.process_existing_join_request(args.join_request_id)
            _write_progress(
                args.progress_file,
                {
                    'mode': 'join_request_worker',
                    'status': 'finished',
                    'processed': 1,
                    'result': result if args.action != 'auto' else repo.get(args.join_request_id),
                },
            )
            return

        results = service.process_pending_join_requests(limit=args.limit)
        summary = {'approved': 0, 'declined': 0, 'pending': 0}
        for item in results:
            status = str((item or {}).get('status') or 'pending')
            summary[status] = summary.get(status, 0) + 1
        _write_progress(
            args.progress_file,
            {
                'mode': 'join_request_worker',
                'status': 'finished',
                'processed': len(results),
                'summary': summary,
            },
        )
    except Exception as exc:
        _write_progress(
            args.progress_file,
            {
                'mode': 'join_request_worker',
                'status': 'failed',
                'error': str(exc),
            },
        )
        raise


if __name__ == '__main__':
    main()
