from telethon.errors import (
    AuthKeyUnregisteredError,
    ChannelPrivateError,
    ChannelsTooMuchError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    PhoneNumberBannedError,
    SessionPasswordNeededError,
    SessionRevokedError,
    UserAlreadyParticipantError,
    UserBannedInChannelError,
    UserDeactivatedBanError,
    UserDeactivatedError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

from services.target_normalizer import INVITE_TYPE, ParsedTarget, parse_target

try:
    from telethon.errors import InviteRequestSentError
except ImportError:  # pragma: no cover
    InviteRequestSentError = None


ACCOUNT_BANNED_ERRORS = (
    PhoneNumberBannedError,
    UserBannedInChannelError,
    UserDeactivatedError,
    UserDeactivatedBanError,
)
SESSION_INVALID_ERRORS = (
    AuthKeyUnregisteredError,
    SessionPasswordNeededError,
    SessionRevokedError,
)
ACCOUNT_LIMITED_ERRORS = (ChannelsTooMuchError,)


class JoinService:
    def parse_target(self, raw_target):
        return parse_target(raw_target)

    async def join_target(self, client, raw_target):
        parsed = raw_target if isinstance(raw_target, ParsedTarget) else self.parse_target(raw_target)
        if parsed.target_type == INVITE_TYPE:
            return await self.join_private_invite(client, parsed)
        return await self.join_public_target(client, parsed)

    async def join_private_invite(self, client, invite_target):
        parsed = invite_target if isinstance(invite_target, ParsedTarget) else self.parse_target(invite_target)
        try:
            result = await client(ImportChatInviteRequest(parsed.normalized_value))
            return self._result(parsed, status=self._result_status_from_success(result), result=result)
        except Exception as exc:
            return self._handle_exception(parsed, exc)

    async def join_public_target(self, client, public_target):
        parsed = public_target if isinstance(public_target, ParsedTarget) else self.parse_target(public_target)
        try:
            entity = await client.get_entity(parsed.normalized_value)
            result = await client(JoinChannelRequest(entity))
            return self._result(parsed, status='joined', result=result)
        except Exception as exc:
            return self._handle_exception(parsed, exc)

    def _result_status_from_success(self, result):
        class_name = getattr(result, '__class__', type(result)).__name__
        if 'InviteRequestSent' in class_name:
            return 'join_request_sent'
        return 'joined'

    def _handle_exception(self, parsed, exc):
        if isinstance(exc, UserAlreadyParticipantError):
            return self._result(parsed, status='already_in')
        if InviteRequestSentError and isinstance(exc, InviteRequestSentError):
            return self._result(parsed, status='join_request_sent')
        if isinstance(exc, InviteHashInvalidError):
            return self._result(parsed, status='invalid_invite', exc=exc)
        if isinstance(exc, InviteHashExpiredError):
            return self._result(parsed, status='expired_invite', exc=exc)
        if isinstance(exc, FloodWaitError):
            return self._result(parsed, status='flood_wait', exc=exc)
        if isinstance(exc, ACCOUNT_LIMITED_ERRORS):
            return self._result(parsed, status='account_limited', exc=exc)
        if isinstance(exc, ACCOUNT_BANNED_ERRORS):
            return self._result(parsed, status='account_banned', exc=exc)
        if isinstance(exc, SESSION_INVALID_ERRORS):
            return self._result(parsed, status='session_invalid', exc=exc)
        if isinstance(exc, ChannelPrivateError):
            return self._result(parsed, status='private_target_unresolved', exc=exc)
        if isinstance(exc, ValueError):
            return self._result(parsed, status='private_target_unresolved', exc=exc)
        return self._result(parsed, status='unknown_error', exc=exc)

    def _result(self, parsed, status, exc=None, result=None):
        error_code = status if status not in {'joined', 'already_in', 'join_request_sent'} else ''
        error_text = self._error_text(status, exc)
        return {
            'status': status,
            'error_code': error_code,
            'error_text': error_text,
            'target_raw': parsed.raw_target,
            'target_type': parsed.target_type,
            'target_normalized': parsed.normalized_value,
            'target_display': parsed.display_value,
            'join_method': parsed.join_method,
            'exception_name': exc.__class__.__name__ if exc else '',
            'result_class': getattr(result, '__class__', type(result)).__name__ if result is not None else '',
        }

    def _error_text(self, status, exc):
        if status == 'invalid_invite':
            return 'HASH не найден или ссылка недействительна'
        if status == 'expired_invite':
            return 'Ссылка истекла или более недоступна'
        if status == 'private_target_unresolved':
            return f'Не удалось определить приватную цель: {exc}'
        if status == 'flood_wait':
            seconds = int(getattr(exc, 'seconds', 0) or 0)
            return f'Нужно подождать {seconds} сек'
        if status == 'account_limited':
            return str(exc or 'Аккаунт ограничен')
        if status == 'account_banned':
            return str(exc or 'Аккаунт заблокирован')
        if status == 'session_invalid':
            return str(exc or 'Сессия недействительна')
        if status == 'unknown_error':
            return str(exc or 'Неизвестная ошибка')
        return ''
