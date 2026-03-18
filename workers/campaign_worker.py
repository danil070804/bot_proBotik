import argparse
import json
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from services.campaign_service import CampaignService


def _write_progress(progress_file, data):
    if not progress_file:
        return
    with open(progress_file, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description='Run invite-link / join-request campaign')
    parser.add_argument('--campaign-id', type=int, required=True)
    parser.add_argument('--progress-file', default='')
    args = parser.parse_args()

    service = CampaignService()

    def progress_callback(payload):
        _write_progress(args.progress_file, payload)

    try:
        progress_callback(
            {
                'mode': 'campaign_worker',
                'status': 'starting',
                'campaign_id': args.campaign_id,
                'last_message': 'worker_started',
            }
        )
        campaign = service.run_campaign(args.campaign_id, progress_callback=progress_callback)
        progress_callback(
            {
                'mode': 'campaign_worker',
                'status': (campaign or {}).get('status', 'finished'),
                'campaign_id': args.campaign_id,
                'last_message': 'worker_finished',
                'stats': service.get_campaign_stats(args.campaign_id),
            }
        )
    except Exception as exc:
        progress_callback(
            {
                'mode': 'campaign_worker',
                'status': 'failed',
                'campaign_id': args.campaign_id,
                'last_message': 'worker_failed',
                'error': str(exc),
            }
        )
        raise


if __name__ == '__main__':
    main()
