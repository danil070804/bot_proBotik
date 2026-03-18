from db import create_join_request, get_join_request, get_join_request_by_request_key, list_join_requests, update_join_request_status


class JoinRequestRepository:
    def create(self, **payload):
        return create_join_request(**payload)

    def get(self, join_request_id):
        return get_join_request(join_request_id)

    def get_by_request_key(self, request_key):
        return get_join_request_by_request_key(request_key)

    def list(self, status=None, limit=None):
        return list_join_requests(status=status, limit=limit)

    def update_status(self, join_request_id, status, **fields):
        return update_join_request_status(join_request_id, status, **fields)
