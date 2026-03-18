from db import (
    bulk_upsert_campaign_recipients,
    create_campaign,
    find_latest_campaign_recipient,
    get_campaign,
    get_campaign_recipient,
    get_campaign_recipient_by_key,
    get_campaign_stats,
    list_campaign_recipients,
    list_campaigns,
    update_campaign,
    update_campaign_recipient_status,
    update_campaign_status,
    upsert_campaign_recipient,
)


class CampaignRepository:
    def create(self, **payload):
        return create_campaign(**payload)

    def get(self, campaign_id):
        return get_campaign(campaign_id)

    def list(self, status=None):
        return list_campaigns(status=status)

    def update(self, campaign_id, **fields):
        return update_campaign(campaign_id, **fields)

    def update_status(self, campaign_id, status):
        return update_campaign_status(campaign_id, status)

    def upsert_recipient(self, **payload):
        return upsert_campaign_recipient(**payload)

    def bulk_upsert_recipients(self, campaign_id, community_id, user_ids):
        return bulk_upsert_campaign_recipients(campaign_id, community_id, user_ids)

    def get_recipient(self, recipient_id):
        return get_campaign_recipient(recipient_id)

    def get_recipient_by_key(self, campaign_id, user_id, community_id):
        return get_campaign_recipient_by_key(campaign_id, user_id, community_id)

    def list_recipients(self, campaign_id, delivery_statuses=None, join_statuses=None, limit=None):
        return list_campaign_recipients(
            campaign_id,
            delivery_statuses=delivery_statuses,
            join_statuses=join_statuses,
            limit=limit,
        )

    def update_recipient_status(self, recipient_id, **fields):
        return update_campaign_recipient_status(recipient_id, **fields)

    def find_latest_recipient(self, community_id, telegram_user_id, campaign_id=None):
        return find_latest_campaign_recipient(community_id, telegram_user_id, campaign_id=campaign_id)

    def get_stats(self, campaign_id):
        return get_campaign_stats(campaign_id)
