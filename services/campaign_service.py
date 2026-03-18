import time

from repositories.campaigns import CampaignRepository
from repositories.communities import CommunityRepository
from services.audit_service import AuditService
from services.delivery_service import DeliveryService
from services.invite_link_service import InviteLinkService


class SimpleRateLimiter:
    def __init__(self, per_minute=None, per_hour=None):
        self.per_minute = max(0, int(per_minute or 0))
        self.per_hour = max(0, int(per_hour or 0))
        self._minute_events = []
        self._hour_events = []

    def _trim(self, now):
        self._minute_events = [item for item in self._minute_events if now - item < 60]
        self._hour_events = [item for item in self._hour_events if now - item < 3600]

    def wait_for_slot(self):
        while True:
            now = time.time()
            self._trim(now)
            minute_wait = 0
            hour_wait = 0
            if self.per_minute and len(self._minute_events) >= self.per_minute:
                minute_wait = max(0.0, 60 - (now - self._minute_events[0]))
            if self.per_hour and len(self._hour_events) >= self.per_hour:
                hour_wait = max(0.0, 3600 - (now - self._hour_events[0]))
            delay = max(minute_wait, hour_wait)
            if delay <= 0:
                stamp = time.time()
                self._minute_events.append(stamp)
                self._hour_events.append(stamp)
                return
            time.sleep(min(delay, 5.0))


class CampaignService:
    ACTIVE_DELIVERY_STATUSES = ('pending', 'queued', 'failed')

    def __init__(self, campaigns=None, communities=None, invite_links=None, delivery=None, audit=None):
        self.campaigns = campaigns or CampaignRepository()
        self.communities = communities or CommunityRepository()
        self.invite_links = invite_links or InviteLinkService()
        self.delivery = delivery or DeliveryService(campaigns=self.campaigns)
        self.audit = audit or AuditService()

    def create_campaign(self, **payload):
        campaign = self.campaigns.create(**payload)
        self.audit.log(
            actor_id=campaign.get('created_by'),
            action='campaign_created',
            entity_type='campaign',
            entity_id=campaign['id'],
            payload={'community_id': campaign.get('community_id'), 'invite_mode': campaign.get('invite_mode')},
        )
        return campaign

    def start_campaign(self, campaign_id):
        campaign = self.campaigns.update_status(campaign_id, 'scheduled')
        self.audit.log(campaign.get('created_by'), 'campaign_scheduled', 'campaign', campaign_id, payload=None)
        return campaign

    def pause_campaign(self, campaign_id, reason='manual_pause'):
        campaign = self.campaigns.update_status(campaign_id, 'paused')
        self.audit.log(campaign.get('created_by'), 'campaign_paused', 'campaign', campaign_id, payload={'reason': reason})
        return campaign

    def resume_campaign(self, campaign_id):
        campaign = self.campaigns.update_status(campaign_id, 'scheduled')
        self.audit.log(campaign.get('created_by'), 'campaign_resumed', 'campaign', campaign_id, payload=None)
        return campaign

    def cancel_campaign(self, campaign_id, reason='cancelled'):
        campaign = self.campaigns.update_status(campaign_id, 'cancelled')
        self.audit.log(campaign.get('created_by'), 'campaign_cancelled', 'campaign', campaign_id, payload={'reason': reason})
        return campaign

    def get_campaign_stats(self, campaign_id):
        return self.campaigns.get_stats(campaign_id)

    def run_campaign(self, campaign_id, progress_callback=None):
        campaign = self.campaigns.get(campaign_id)
        if not campaign:
            raise RuntimeError(f'Campaign {campaign_id} not found')
        community = self.communities.get(campaign.get('community_id'))
        if not community:
            raise RuntimeError(f'Community {campaign.get("community_id")} not found')

        campaign = self.campaigns.update_status(campaign_id, 'running')
        invite_mode = self.invite_links.resolve_mode(campaign, community)
        invite_link = self.invite_links.get_or_create_active_link(campaign, community, invite_mode=invite_mode)
        recipients = self.campaigns.list_recipients(campaign_id, delivery_statuses=self.ACTIVE_DELIVERY_STATUSES)
        limiter = SimpleRateLimiter(
            per_minute=campaign.get('rate_limit_per_minute'),
            per_hour=campaign.get('rate_limit_per_hour'),
        )

        processed = 0
        sent = 0
        failed = 0
        suppressed = 0
        total = len(recipients)

        def emit(last_message):
            if progress_callback is None:
                return
            progress_callback(
                {
                    'mode': 'campaign_worker',
                    'status': campaign.get('status'),
                    'campaign_id': campaign_id,
                    'campaign_name': campaign.get('name'),
                    'invite_mode': invite_mode,
                    'community_id': community.get('id'),
                    'community_title': community.get('title'),
                    'total': total,
                    'processed': processed,
                    'sent': sent,
                    'failed': failed,
                    'suppressed': suppressed,
                    'last_message': last_message,
                    'stats': self.campaigns.get_stats(campaign_id),
                }
            )

        emit('campaign_started')
        for recipient in recipients:
            latest = self.campaigns.get(campaign_id)
            if not latest or latest.get('status') in {'paused', 'cancelled', 'finished'}:
                campaign = latest or campaign
                emit(f'campaign_stopped:{(campaign or {}).get("status", "unknown")}')
                return campaign

            limiter.wait_for_slot()
            result = self.delivery.send_campaign_message(campaign, community, invite_link, recipient)
            processed += 1
            if result['status'] == 'sent':
                sent += 1
            elif result['status'] == 'failed':
                failed += 1
            elif result['status'] == 'suppressed':
                suppressed += 1
            emit(result['status'])

            stop_on_error_rate = float(campaign.get('stop_on_error_rate') or 0)
            if stop_on_error_rate > 0 and processed >= 5 and (failed / max(processed, 1)) >= stop_on_error_rate:
                campaign = self.pause_campaign(campaign_id, reason='error_rate_threshold')
                emit('paused:error_rate_threshold')
                return campaign

        campaign = self.campaigns.update_status(campaign_id, 'finished')
        self.audit.log(
            actor_id=campaign.get('created_by'),
            action='campaign_finished',
            entity_type='campaign',
            entity_id=campaign_id,
            payload=self.campaigns.get_stats(campaign_id),
        )
        emit('campaign_finished')
        return campaign
