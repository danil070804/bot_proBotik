from db import create_segment, get_segment, get_segment_users, list_segments


class SegmentRepository:
    def get(self, segment_id):
        return get_segment(segment_id)

    def create(self, name, filter_json, created_by=None):
        return create_segment(name=name, filter_json=filter_json, created_by=created_by)

    def list(self, limit=None):
        return list_segments(limit=limit)

    def get_users(self, segment_id, limit=None):
        return get_segment_users(segment_id, limit=limit)
