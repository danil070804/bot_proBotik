import time

import config
from repositories.campaigns import CampaignRepository
from services.campaign_service import CampaignService
from services.join_request_service import JoinRequestService


def _iter_active_campaigns(repo):
    seen = set()
    for status in ('scheduled', 'running'):
        for campaign in repo.list(status=status):
            campaign_id = campaign.get('id')
            if campaign_id in seen:
                continue
            seen.add(campaign_id)
            yield campaign


def main():
    campaigns = CampaignRepository()
    campaign_service = CampaignService(campaigns=campaigns)
    join_request_service = JoinRequestService()
    poll_interval = max(2, int(config.worker_poll_interval or 15))
    batch_size = max(1, int(config.join_request_batch_size or 50))

    print(f'Worker started: poll_interval={poll_interval}s join_request_batch_size={batch_size}')
    while True:
        processed_any = False

        for campaign in _iter_active_campaigns(campaigns):
            processed_any = True
            campaign_id = campaign['id']
            try:
                print(f'Processing campaign #{campaign_id} ({campaign.get("status")})')
                campaign_service.run_campaign(campaign_id)
            except Exception as exc:
                campaigns.update(campaign_id, status='failed')
                campaign_service.audit.log(
                    actor_id=campaign.get('created_by'),
                    action='campaign_failed',
                    entity_type='campaign',
                    entity_id=campaign_id,
                    payload={'error': str(exc)},
                )
                print(f'Campaign #{campaign_id} failed: {exc}')

        try:
            join_results = join_request_service.process_pending_join_requests(limit=batch_size)
            if join_results:
                processed_any = True
                print(f'Processed join requests: {len(join_results)}')
        except Exception as exc:
            print(f'Join request processing failed: {exc}')

        if not processed_any:
            time.sleep(poll_interval)


if __name__ == '__main__':
    main()
