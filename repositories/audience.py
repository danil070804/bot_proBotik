from db import (
    blacklist_audience_user,
    delete_audience_users,
    delete_all_audience_users,
    get_audience_source,
    get_audience_source_by_key,
    get_audience_summary,
    get_audience_user,
    get_audience_user_by_tg_id,
    get_or_create_audience_source,
    list_audience_sources,
    list_audience_users,
    mark_audience_unsubscribed,
    upsert_audience_user,
    upsert_user,
    get_user_by_telegram_user_id,
    blacklist_user,
    unsubscribe_user,
)


class AudienceRepository:
    def get(self, user_id):
        return get_audience_user(user_id)

    def get_by_telegram_user_id(self, telegram_user_id):
        return get_audience_user_by_tg_id(telegram_user_id)

    def list(self, filters=None, limit=None):
        return list_audience_users(filters=filters, limit=limit)

    def search(self, query, limit=20):
        return list_audience_users(filters={'search': query}, limit=limit)

    def summary(self):
        return get_audience_summary()

    def get_source(self, source_id):
        return get_audience_source(source_id)

    def get_source_by_key(self, source_type, source_value):
        return get_audience_source_by_key(source_type, source_value)

    def get_or_create_source(self, source_type, source_value, title=None, meta_json=None):
        return get_or_create_audience_source(
            source_type=source_type,
            source_value=source_value,
            title=title,
            meta_json=meta_json,
        )

    def list_sources(self, limit=None):
        return list_audience_sources(limit=limit)

    def clear_all(self):
        return delete_all_audience_users()

    def clear(self, filters=None):
        return delete_audience_users(filters=filters)

    def clear_by_source_value(self, source_value):
        return delete_audience_users(filters={'source_value': source_value})

    def upsert(self, sync_user=True, **payload):
        audience_user = upsert_audience_user(**payload)
        if sync_user:
            self._sync_user_copy(audience_user, payload)
        return audience_user

    def blacklist(self, user_id, is_blacklisted=True, sync_user=True):
        audience_user = blacklist_audience_user(user_id, is_blacklisted=is_blacklisted)
        if sync_user and audience_user:
            user_copy = get_user_by_telegram_user_id(audience_user.get('telegram_user_id'))
            if user_copy:
                blacklist_user(user_copy['id'], is_blacklisted=is_blacklisted)
        return audience_user

    def unsubscribe(self, user_id, unsubscribed_at=None, sync_user=True):
        audience_user = mark_audience_unsubscribed(user_id, unsubscribed_at=unsubscribed_at)
        if sync_user and audience_user:
            user_copy = get_user_by_telegram_user_id(audience_user.get('telegram_user_id'))
            if user_copy:
                unsubscribe_user(user_copy['id'], unsubscribed_at=unsubscribed_at)
        return audience_user

    def _sync_user_copy(self, audience_user, payload):
        if not audience_user:
            return
        if not audience_user.get('telegram_user_id'):
            return
        source = (
            audience_user.get('source_value')
            or payload.get('source_value')
            or payload.get('discovered_via')
            or ''
        )
        upsert_user(
            telegram_user_id=audience_user.get('telegram_user_id'),
            username=audience_user.get('username') or payload.get('username') or '',
            first_name=audience_user.get('first_name') or payload.get('first_name') or '',
            source=source,
            consent_status=audience_user.get('consent_status') or payload.get('consent_status') or 'unknown',
            tags=audience_user.get('tags_json') or payload.get('tags_json') or [],
            is_blacklisted=audience_user.get('is_blacklisted') or bool(payload.get('is_blacklisted')),
            unsubscribed_at=audience_user.get('unsubscribed_at') or payload.get('unsubscribed_at'),
        )
