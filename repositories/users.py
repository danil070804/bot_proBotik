from db import blacklist_user, get_user, get_user_by_telegram_user_id, unsubscribe_user, upsert_user


class UserRepository:
    def get(self, user_id):
        return get_user(user_id)

    def get_by_telegram_user_id(self, telegram_user_id):
        return get_user_by_telegram_user_id(telegram_user_id)

    def upsert(self, **payload):
        return upsert_user(**payload)

    def blacklist(self, user_id, is_blacklisted=True):
        return blacklist_user(user_id, is_blacklisted=is_blacklisted)

    def unsubscribe(self, user_id, unsubscribed_at=None):
        return unsubscribe_user(user_id, unsubscribed_at=unsubscribed_at)
