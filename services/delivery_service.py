from datetime import datetime, timezone

from repositories.campaigns import CampaignRepository
from services.audit_service import AuditService
from services.template_service import TemplateService
from telegram.bot_api import TelegramBotAPI, TelegramBotAPIError


def _is_true(value):
    if isinstance(value, str):
        return value.strip().lower() in {'1', 'true', 'yes', 'on'}
    return bool(value)


class DeliveryService:
    TERMINAL_JOIN_STATUSES = {'joined', 'approved', 'declined'}

    def __init__(self, bot_api=None, campaigns=None, template_service=None, audit=None):
        self.bot_api = bot_api or TelegramBotAPI()
        self.campaigns = campaigns or CampaignRepository()
        self.template_service = template_service or TemplateService()
        self.audit = audit or AuditService()

    def _suppression_reason(self, recipient, campaign):
        if not recipient:
            return 'recipient_not_found'
        if _is_true(recipient.get('is_blacklisted')):
            return 'blacklisted'
        if recipient.get('unsubscribed_at'):
            return 'unsubscribed'
        if str(recipient.get('join_status') or '').strip() in self.TERMINAL_JOIN_STATUSES:
            return f'join_status:{recipient.get("join_status")}'
        max_attempts = int((campaign or {}).get('max_attempts') or 1)
        if int(recipient.get('attempts') or 0) >= max_attempts:
            return 'max_attempts_reached'
        telegram_user_id = recipient.get('telegram_user_id')
        if telegram_user_id in (None, ''):
            return 'missing_telegram_user_id'
        return ''

    def send_campaign_message(self, campaign, community, invite_link, recipient):
        reason = self._suppression_reason(recipient, campaign)
        if reason:
            updated = self.campaigns.update_recipient_status(
                recipient['id'],
                delivery_status='suppressed',
                last_error=reason,
            )
            self.audit.log(
                actor_id=(campaign or {}).get('created_by'),
                action='delivery_suppressed',
                entity_type='campaign_recipient',
                entity_id=recipient['id'],
                payload={'reason': reason},
            )
            return {'status': 'suppressed', 'recipient': updated, 'reason': reason}

        context = self.template_service.build_context(recipient, community, invite_link, campaign)
        text = self.template_service.render((campaign or {}).get('message_template'), context)
        if not text:
            text = (
                f'Здравствуйте, {context["first_name"] or context["username"] or "друг"}.\n'
                f'Присоединяйтесь к {context["community_title"]}: {context["invite_link"]}'
            )
        self.campaigns.update_recipient_status(
            recipient['id'],
            delivery_status='queued',
            invite_link_id=invite_link.get('id'),
        )
        try:
            response = self.bot_api.send_message(recipient['telegram_user_id'], text)
            updated = self.campaigns.update_recipient_status(
                recipient['id'],
                delivery_status='sent',
                last_error='',
                last_sent_at=datetime.now(timezone.utc).isoformat(),
                invite_link_id=invite_link.get('id'),
                attempts_increment=1,
            )
            self.audit.log(
                actor_id=(campaign or {}).get('created_by'),
                action='delivery_sent',
                entity_type='campaign_recipient',
                entity_id=recipient['id'],
                payload={'message_id': response.get('message_id'), 'invite_link_id': invite_link.get('id')},
            )
            return {'status': 'sent', 'recipient': updated, 'response': response}
        except TelegramBotAPIError as exc:
            updated = self.campaigns.update_recipient_status(
                recipient['id'],
                delivery_status='failed',
                last_error=str(exc),
                invite_link_id=invite_link.get('id'),
                attempts_increment=1,
            )
            self.audit.log(
                actor_id=(campaign or {}).get('created_by'),
                action='delivery_failed',
                entity_type='campaign_recipient',
                entity_id=recipient['id'],
                payload={'error': str(exc), 'retryable': exc.is_retryable},
            )
            return {'status': 'failed', 'recipient': updated, 'error': str(exc), 'retryable': exc.is_retryable}
