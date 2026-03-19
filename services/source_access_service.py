from telethon.errors import (
    ChannelPrivateError,
    FloodWaitError,
    InviteHashExpiredError,
    InviteHashInvalidError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest

from services.target_normalizer import is_invite_target_type, parse_target


class SourceAccessService:
    async def ensure_source_access(self, client, parsed_target):
        parsed = parsed_target if hasattr(parsed_target, 'target_type') else parse_target(parsed_target)
        if is_invite_target_type(parsed.target_type):
            return await self.join_private_invite(client, parsed)
        return await self.resolve_public_target(client, parsed)

    async def join_private_invite(self, client, parsed_target):
        parsed = parsed_target if hasattr(parsed_target, 'target_type') else parse_target(parsed_target)
        preview = None
        try:
            preview = await client(CheckChatInviteRequest(parsed.normalized_value))
        except (InviteHashInvalidError, InviteHashExpiredError, FloodWaitError) as exc:
            return self._error(parsed, exc)
        except Exception:
            preview = None

        try:
            result = await client(ImportChatInviteRequest(parsed.normalized_value))
            entity = self._extract_entity(result) or self._extract_preview_chat(preview)
            return self._success(parsed, entity, preview=preview, join_status='joined')
        except UserAlreadyParticipantError:
            entity = self._extract_preview_chat(preview)
            return self._success(parsed, entity, preview=preview, join_status='already_in')
        except Exception as exc:
            return self._error(parsed, exc, preview=preview)

    async def resolve_public_target(self, client, parsed_target):
        parsed = parsed_target if hasattr(parsed_target, 'target_type') else parse_target(parsed_target)
        try:
            entity = await client.get_entity(parsed.normalized_value)
            try:
                await client(JoinChannelRequest(entity))
                join_status = 'joined'
            except UserAlreadyParticipantError:
                join_status = 'already_in'
            except Exception:
                join_status = 'resolved'
            return self._success(parsed, entity, join_status=join_status)
        except Exception as exc:
            return self._error(parsed, exc)

    def _extract_entity(self, result):
        chats = getattr(result, 'chats', None) or []
        if chats:
            return chats[0]
        chat = getattr(result, 'chat', None)
        if chat is not None:
            return chat
        return None

    def _extract_preview_chat(self, preview):
        return getattr(preview, 'chat', None)

    def _entity_title(self, entity, preview=None):
        title = getattr(entity, 'title', None) or getattr(preview, 'title', None)
        if title:
            return str(title)
        if getattr(entity, 'username', None):
            return '@' + str(entity.username).lstrip('@')
        return ''

    def _success(self, parsed, entity, preview=None, join_status='resolved'):
        return {
            'ok': entity is not None,
            'parsed_target': parsed,
            'entity': entity,
            'title': self._entity_title(entity, preview=preview) or parsed.display_value,
            'join_status': join_status,
            'error_code': '' if entity is not None else 'entity_missing',
            'error_text': '' if entity is not None else 'Не удалось получить объект источника после доступа',
        }

    def _error(self, parsed, exc, preview=None):
        if isinstance(exc, InviteHashInvalidError):
            code = 'invalid_private_access'
            text = 'Invite hash недействителен'
        elif isinstance(exc, InviteHashExpiredError):
            code = 'expired_private_access'
            text = 'Invite hash истек или ссылка больше недоступна'
        elif isinstance(exc, FloodWaitError):
            code = 'flood_wait'
            text = f'Нужно подождать {int(getattr(exc, "seconds", 0) or 0)} сек'
        elif isinstance(exc, ChannelPrivateError):
            code = 'private_target_unresolved'
            text = 'Цель приватна и недоступна для текущей сессии'
        elif isinstance(exc, ValueError):
            code = 'resolve_failed'
            text = str(exc)
        else:
            code = exc.__class__.__name__.replace('Error', '').lower() or 'unknown_error'
            text = str(exc)
        return {
            'ok': False,
            'parsed_target': parsed,
            'entity': self._extract_preview_chat(preview),
            'title': self._entity_title(self._extract_preview_chat(preview), preview=preview) or parsed.display_value,
            'join_status': 'failed',
            'error_code': code,
            'error_text': text,
        }
