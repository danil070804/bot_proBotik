from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import asyncio

from config import API_ID, API_HASH
from functions import build_telegram_client, get_proxy


STATUS_WORKING = 'working'
STATUS_LIMITED = 'limited'
STATUS_FLOODED = 'flooded'
STATUS_DEAD = 'dead'
STATUS_INVALID = 'invalid'


@dataclass
class HealthCheckResult:
    status: str
    reason_code: str
    reason_text: str
    me_id: int | None = None
    me_username: str = ''
    is_deleted: bool = False
    dialogs_ok: bool = False
    entity_ok: bool = False
    last_checked_at: str = ''

    def to_record(self):
        data = asdict(self)
        if not data.get('last_checked_at'):
            data['last_checked_at'] = datetime.now(timezone.utc).isoformat()
        return data


class AccountHealthService:
    def check_account_health(self, session, deep_check=False):
        if not API_ID or not API_HASH:
            return self._result(
                status=STATUS_LIMITED,
                reason_code='api_credentials_missing',
                reason_text='Не настроены API ID / API HASH',
            )
        return asyncio.run(self._check_async(session, deep_check=deep_check))

    async def _check_async(self, session, deep_check=False):
        client = None
        me = None
        dialogs_ok = False
        entity_ok = False
        try:
            client = build_telegram_client(session, API_ID, API_HASH, proxy=get_proxy())
            await client.connect()
            if not await client.is_user_authorized():
                return self._result(
                    status=STATUS_INVALID,
                    reason_code='unauthorized',
                    reason_text='Сессия не авторизована',
                )

            try:
                me = await client.get_me()
            except Exception as exc:
                return self._result_from_exception(exc, fallback_code='get_me_failed')

            if not me:
                return self._result(
                    status=STATUS_DEAD,
                    reason_code='deleted_account',
                    reason_text='Аккаунт недоступен для рабочих операций',
                )

            is_deleted = self._is_deleted_user(me)
            me_id = getattr(me, 'id', None)
            me_username = getattr(me, 'username', None) or ''
            if is_deleted:
                return self._result(
                    status=STATUS_DEAD,
                    reason_code='deleted_account',
                    reason_text='Аккаунт удалён или деактивирован',
                    me_id=me_id,
                    me_username=me_username,
                    is_deleted=True,
                )

            dialogs_error = None
            entity_error = None

            try:
                await client.get_dialogs(limit=1)
                dialogs_ok = True
            except Exception as exc:
                dialogs_error = exc

            try:
                await client.get_entity('telegram')
                entity_ok = True
            except Exception as exc:
                entity_error = exc

            if deep_check and dialogs_ok and entity_ok:
                try:
                    await client.get_messages('SpamBot', limit=1)
                except Exception as exc:
                    classified = self._result_from_exception(
                        exc,
                        fallback_code='deep_check_failed',
                        me_id=me_id,
                        me_username=me_username,
                        dialogs_ok=dialogs_ok,
                        entity_ok=entity_ok,
                    )
                    if classified.status in {STATUS_FLOODED, STATUS_LIMITED}:
                        return classified

            if dialogs_ok and entity_ok:
                return self._result(
                    status=STATUS_WORKING,
                    reason_code='ok',
                    reason_text='Аккаунт пригоден для рабочих операций',
                    me_id=me_id,
                    me_username=me_username,
                    dialogs_ok=True,
                    entity_ok=True,
                )

            if dialogs_error:
                classified = self._result_from_exception(
                    dialogs_error,
                    fallback_code='dialogs_failed',
                    me_id=me_id,
                    me_username=me_username,
                    dialogs_ok=False,
                    entity_ok=entity_ok,
                )
                if classified.status in {STATUS_INVALID, STATUS_DEAD, STATUS_FLOODED}:
                    return classified

            if entity_error:
                classified = self._result_from_exception(
                    entity_error,
                    fallback_code='entity_resolve_failed',
                    me_id=me_id,
                    me_username=me_username,
                    dialogs_ok=dialogs_ok,
                    entity_ok=False,
                )
                if classified.status in {STATUS_INVALID, STATUS_DEAD, STATUS_FLOODED}:
                    return classified

            if dialogs_error and entity_error:
                return self._result(
                    status=STATUS_DEAD,
                    reason_code='functional_checks_failed',
                    reason_text='Функциональная проверка не пройдена',
                    me_id=me_id,
                    me_username=me_username,
                    dialogs_ok=False,
                    entity_ok=False,
                )

            if dialogs_error:
                return self._result(
                    status=STATUS_LIMITED,
                    reason_code='dialogs_failed',
                    reason_text='Не удалось получить список диалогов',
                    me_id=me_id,
                    me_username=me_username,
                    dialogs_ok=False,
                    entity_ok=entity_ok,
                )

            return self._result(
                status=STATUS_LIMITED,
                reason_code='entity_resolve_failed',
                reason_text='Не удалось выполнить safe entity resolve',
                me_id=me_id,
                me_username=me_username,
                dialogs_ok=dialogs_ok,
                entity_ok=False,
            )
        except Exception as exc:
            return self._result_from_exception(exc, fallback_code='unknown_error')
        finally:
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    def _result(self, status, reason_code, reason_text, **extra):
        return HealthCheckResult(
            status=status,
            reason_code=reason_code,
            reason_text=reason_text,
            last_checked_at=datetime.now(timezone.utc).isoformat(),
            **extra,
        )

    def _result_from_exception(self, exc, fallback_code='rpc_error', **extra):
        name = exc.__class__.__name__
        text = str(exc or '').strip() or name
        name_lower = name.lower()
        text_lower = text.lower()

        if name in {'SessionPasswordNeededError', 'AuthKeyUnregisteredError', 'SessionRevokedError'}:
            return self._result(STATUS_INVALID, 'session_invalid', 'Сессия недействительна или требует повторного входа', **extra)
        if name in {'UserDeactivatedError', 'UserDeactivatedBanError', 'PhoneNumberBannedError'}:
            return self._result(STATUS_DEAD, 'deleted_account', 'Аккаунт деактивирован или заблокирован', **extra)
        if name in {'FloodWaitError', 'FloodWait'}:
            return self._result(STATUS_FLOODED, 'flood_wait', f'Временное ограничение Telegram: {text}', **extra)
        if name in {'PeerFloodError'}:
            return self._result(STATUS_FLOODED, 'flood_wait', 'Аккаунт временно ограничен flood-лимитами', **extra)
        if name in {'UserRestrictedError', 'UserBannedInChannelError', 'ChatWriteForbiddenError'}:
            return self._result(STATUS_LIMITED, 'rpc_error', 'Аккаунт ограничен для части операций', **extra)
        if name == 'ValueError':
            if 'entity' in text_lower:
                return self._result(STATUS_LIMITED, 'entity_resolve_failed', 'Не удалось разрешить Telegram-сущность', **extra)
            return self._result(STATUS_LIMITED, fallback_code, text, **extra)
        if 'deleted' in text_lower or 'deactivated' in text_lower:
            return self._result(STATUS_DEAD, 'deleted_account', 'Аккаунт удалён или деактивирован', **extra)
        if 'authorized' in text_lower or 'auth' in name_lower:
            return self._result(STATUS_INVALID, 'unauthorized', 'Сессия не авторизована', **extra)
        return self._result(STATUS_LIMITED, fallback_code if fallback_code != 'unknown_error' else 'unknown_error', text, **extra)

    def _is_deleted_user(self, user):
        if not user:
            return True
        if getattr(user, 'deleted', False):
            return True
        if user.__class__.__name__ == 'UserEmpty':
            return True
        first_name = str(getattr(user, 'first_name', '') or '').strip().lower()
        return first_name in {'deleted account', 'удалённый аккаунт'}
