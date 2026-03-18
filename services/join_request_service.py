from repositories.campaigns import CampaignRepository
from repositories.communities import CommunityRepository
from repositories.invite_links import InviteLinkRepository
from repositories.join_requests import JoinRequestRepository
from repositories.users import UserRepository
from services.audit_service import AuditService
from telegram.bot_api import TelegramBotAPI


def _is_true(value):
    if isinstance(value, str):
        return value.strip().lower() in {'1', 'true', 'yes', 'on'}
    return bool(value)


class JoinRequestService:
    def __init__(self, bot_api=None, communities=None, campaigns=None, users=None, invite_links=None, join_requests=None, audit=None):
        self.bot_api = bot_api or TelegramBotAPI()
        self.communities = communities or CommunityRepository()
        self.campaigns = campaigns or CampaignRepository()
        self.users = users or UserRepository()
        self.invite_links = invite_links or InviteLinkRepository()
        self.join_requests = join_requests or JoinRequestRepository()
        self.audit = audit or AuditService()

    def _evaluate_request(self, community, campaign, user, recipient):
        if user and _is_true(user.get('is_blacklisted')):
            return 'decline', 'blacklisted'
        if user and user.get('unsubscribed_at'):
            return 'decline', 'unsubscribed'
        if not community:
            return 'manual', 'community_not_found'
        if campaign and not _is_true(campaign.get('allow_auto_approve')):
            return 'manual', 'campaign_auto_approve_disabled'
        if _is_true(community.get('strict_moderation')):
            return 'manual', 'strict_moderation'
        if not _is_true(community.get('auto_approve_join_requests')):
            return 'manual', 'community_auto_approve_disabled'
        if not user:
            return 'manual', 'user_not_found'
        if campaign and campaign.get('segment_id') and not recipient:
            return 'manual', 'segment_not_matched'
        return 'approve', 'auto_approved'

    def process_join_request(
        self,
        chat_id,
        telegram_user_id,
        username='',
        first_name='',
        invite_link='',
        requested_at=None,
        user_chat_id=None,
    ):
        community = self.communities.get_by_chat_id(chat_id)
        if not community:
            raise RuntimeError(f'Community with chat_id={chat_id} not found')
        user = self.users.get_by_telegram_user_id(telegram_user_id)
        invite_record = self.invite_links.get_by_url(invite_link) if invite_link else None
        campaign = self.campaigns.get(invite_record.get('campaign_id')) if invite_record and invite_record.get('campaign_id') else None
        recipient = self.campaigns.find_latest_recipient(
            community_id=community['id'],
            telegram_user_id=telegram_user_id,
            campaign_id=(campaign or {}).get('id'),
        )
        join_request = self.join_requests.create(
            community_id=community['id'],
            campaign_id=(campaign or {}).get('id'),
            user_id=(user or recipient or {}).get('user_id') or (user or {}).get('id'),
            telegram_user_id=telegram_user_id,
            invite_link_id=(invite_record or {}).get('id'),
            status='pending',
            decision_type='manual',
            decision_reason='awaiting_processing',
            requested_at=requested_at,
            request_key=f'{chat_id}:{telegram_user_id}:{requested_at or ""}',
        )
        if recipient:
            self.campaigns.update_recipient_status(
                recipient['id'],
                join_status='join_requested',
                invite_link_id=(invite_record or {}).get('id'),
            )
        self.audit.log(
            actor_id=None,
            action='join_request_received',
            entity_type='join_request',
            entity_id=join_request['id'],
            payload={
                'community_id': community['id'],
                'campaign_id': (campaign or {}).get('id'),
                'telegram_user_id': telegram_user_id,
                'invite_link': invite_link,
                'user_chat_id': user_chat_id,
                'username': username,
                'first_name': first_name,
            },
        )
        decision, reason = self._evaluate_request(community, campaign, user, recipient)
        if decision == 'approve':
            return self.approve_join_request(join_request['id'], reason=reason)
        if decision == 'decline':
            return self.decline_join_request(join_request['id'], reason=reason)
        return self.join_requests.update_status(
            join_request['id'],
            'pending',
            decision_type='manual',
            decision_reason=reason,
            moderator_id=None,
        )

    def approve_join_request(self, join_request_id, moderator_id=None, reason='approved'):
        join_request = self.join_requests.get(join_request_id)
        if not join_request:
            raise RuntimeError(f'Join request {join_request_id} not found')
        community = self.communities.get(join_request['community_id'])
        self.bot_api.approve_join_request(community['chat_id'], join_request['telegram_user_id'])
        updated = self.join_requests.update_status(
            join_request_id,
            'approved',
            decision_type='auto' if moderator_id is None else 'manual',
            decision_reason=reason,
            moderator_id=moderator_id,
        )
        recipient = self.campaigns.find_latest_recipient(
            community_id=join_request['community_id'],
            telegram_user_id=join_request['telegram_user_id'],
            campaign_id=join_request.get('campaign_id'),
        )
        if recipient:
            self.campaigns.update_recipient_status(recipient['id'], join_status='approved')
        self.audit.log(
            actor_id=moderator_id,
            action='join_request_approved',
            entity_type='join_request',
            entity_id=join_request_id,
            payload={'reason': reason},
        )
        return updated

    def decline_join_request(self, join_request_id, moderator_id=None, reason='declined'):
        join_request = self.join_requests.get(join_request_id)
        if not join_request:
            raise RuntimeError(f'Join request {join_request_id} not found')
        community = self.communities.get(join_request['community_id'])
        self.bot_api.decline_join_request(community['chat_id'], join_request['telegram_user_id'])
        updated = self.join_requests.update_status(
            join_request_id,
            'declined',
            decision_type='auto' if moderator_id is None else 'manual',
            decision_reason=reason,
            moderator_id=moderator_id,
        )
        recipient = self.campaigns.find_latest_recipient(
            community_id=join_request['community_id'],
            telegram_user_id=join_request['telegram_user_id'],
            campaign_id=join_request.get('campaign_id'),
        )
        if recipient:
            self.campaigns.update_recipient_status(recipient['id'], join_status='declined')
        self.audit.log(
            actor_id=moderator_id,
            action='join_request_declined',
            entity_type='join_request',
            entity_id=join_request_id,
            payload={'reason': reason},
        )
        return updated

    def process_pending_join_requests(self, limit=50):
        results = []
        for join_request in self.join_requests.list(status='pending', limit=limit):
            results.append(self.process_existing_join_request(join_request['id']))
        return results

    def process_existing_join_request(self, join_request_id):
        join_request = self.join_requests.get(join_request_id)
        if not join_request:
            raise RuntimeError(f'Join request {join_request_id} not found')
        community = self.communities.get(join_request['community_id'])
        campaign = self.campaigns.get(join_request['campaign_id']) if join_request.get('campaign_id') else None
        user = self.users.get(join_request['user_id']) if join_request.get('user_id') else self.users.get_by_telegram_user_id(join_request['telegram_user_id'])
        recipient = self.campaigns.find_latest_recipient(
            community_id=join_request['community_id'],
            telegram_user_id=join_request['telegram_user_id'],
            campaign_id=join_request.get('campaign_id'),
        )
        decision, reason = self._evaluate_request(community, campaign, user, recipient)
        if decision == 'approve':
            return self.approve_join_request(join_request['id'], reason=reason)
        if decision == 'decline':
            return self.decline_join_request(join_request['id'], reason=reason)
        return self.join_requests.update_status(
            join_request['id'],
            'pending',
            decision_type='manual',
            decision_reason=reason,
            moderator_id=None,
        )

    def mark_user_joined(self, chat_id, telegram_user_id):
        community = self.communities.get_by_chat_id(chat_id)
        if not community:
            return None
        recipient = self.campaigns.find_latest_recipient(community['id'], telegram_user_id)
        if not recipient:
            return None
        updated = self.campaigns.update_recipient_status(recipient['id'], join_status='joined')
        self.audit.log(
            actor_id=None,
            action='chat_member_joined',
            entity_type='campaign_recipient',
            entity_id=recipient['id'],
            payload={'community_id': community['id'], 'telegram_user_id': telegram_user_id},
        )
        return updated

    def process_unsubscribe(self, telegram_user_id):
        user = self.users.get_by_telegram_user_id(telegram_user_id)
        if not user:
            return None
        updated = self.users.unsubscribe(user['id'])
        self.audit.log(
            actor_id=telegram_user_id,
            action='user_unsubscribed',
            entity_type='user',
            entity_id=user['id'],
            payload={'telegram_user_id': telegram_user_id},
        )
        return updated
