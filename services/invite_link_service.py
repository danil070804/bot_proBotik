from repositories.campaigns import CampaignRepository
from repositories.communities import CommunityRepository
from repositories.invite_links import InviteLinkRepository
from services.audit_service import AuditService
from telegram.bot_api import TelegramBotAPI


def _is_true(value):
    if isinstance(value, str):
        return value.strip().lower() in {'1', 'true', 'yes', 'on'}
    return bool(value)


class InviteLinkService:
    def __init__(self, bot_api=None, invite_links=None, campaigns=None, communities=None, audit=None):
        self.bot_api = bot_api or TelegramBotAPI()
        self.invite_links = invite_links or InviteLinkRepository()
        self.campaigns = campaigns or CampaignRepository()
        self.communities = communities or CommunityRepository()
        self.audit = audit or AuditService()

    def resolve_mode(self, campaign, community):
        explicit_mode = str((campaign or {}).get('invite_mode') or '').strip()
        if explicit_mode and explicit_mode != 'auto':
            return explicit_mode
        if _is_true((community or {}).get('strict_moderation')):
            return 'join_request'
        if _is_true((community or {}).get('auto_approve_join_requests')):
            return 'join_request'
        return 'invite_link'

    def get_or_create_active_link(self, campaign, community, invite_mode=None, expire_at=None, member_limit=None):
        campaign_id = (campaign or {}).get('id')
        community_id = (community or {}).get('id')
        if campaign_id is None or community_id is None:
            raise RuntimeError('Campaign and community are required to create invite links')
        invite_mode = invite_mode or self.resolve_mode(campaign, community)
        self.campaigns.update(campaign_id, resolved_invite_mode=invite_mode)
        existing = self.invite_links.get_active(community_id, campaign_id=campaign_id, invite_mode=invite_mode)
        if existing:
            return existing
        result = self.bot_api.create_chat_invite_link(
            chat_id=community['chat_id'],
            creates_join_request=invite_mode == 'join_request',
            expire_date=expire_at,
            member_limit=member_limit,
            name=f'campaign:{campaign_id}:{invite_mode}',
        )
        record = self.invite_links.create(
            community_id=community_id,
            campaign_id=campaign_id,
            invite_mode=invite_mode,
            telegram_invite_link=result.get('invite_link'),
            creates_join_request=result.get('creates_join_request', invite_mode == 'join_request'),
            expire_at=result.get('expire_date'),
            member_limit=result.get('member_limit'),
            is_revoked=False,
        )
        self.audit.log(
            actor_id=(campaign or {}).get('created_by'),
            action='invite_link_created',
            entity_type='invite_link',
            entity_id=record['id'],
            payload={'campaign_id': campaign_id, 'community_id': community_id, 'invite_mode': invite_mode},
        )
        return record

    def revoke_link(self, invite_link_id):
        record = self.invite_links.get(invite_link_id)
        if not record:
            return None
        community = self.communities.get(record['community_id'])
        self.bot_api.revoke_chat_invite_link(
            chat_id=community['chat_id'],
            invite_link=record['telegram_invite_link'],
        )
        updated = self.invite_links.revoke(invite_link_id)
        self.audit.log(
            actor_id=None,
            action='invite_link_revoked',
            entity_type='invite_link',
            entity_id=invite_link_id,
            payload={'telegram_invite_link': record['telegram_invite_link']},
        )
        return updated
