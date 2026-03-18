from db import create_invite_link, get_active_invite_link, get_invite_link, get_invite_link_by_url, revoke_invite_link


class InviteLinkRepository:
    def create(self, **payload):
        return create_invite_link(**payload)

    def get(self, invite_link_id):
        return get_invite_link(invite_link_id)

    def get_by_url(self, telegram_invite_link):
        return get_invite_link_by_url(telegram_invite_link)

    def get_active(self, community_id, campaign_id=None, invite_mode=None):
        return get_active_invite_link(community_id, campaign_id=campaign_id, invite_mode=invite_mode)

    def revoke(self, invite_link_id):
        return revoke_invite_link(invite_link_id)
