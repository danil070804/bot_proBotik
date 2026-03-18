from db import create_community, get_community, get_community_by_chat_id, list_communities, update_community


class CommunityRepository:
    def create(self, **payload):
        return create_community(**payload)

    def get(self, community_id):
        return get_community(community_id)

    def get_by_chat_id(self, chat_id):
        return get_community_by_chat_id(chat_id)

    def list(self, is_active=None):
        return list_communities(is_active=is_active)

    def update(self, community_id, **fields):
        return update_community(community_id, **fields)
