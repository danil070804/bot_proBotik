from db import create_audit_log


class AuditService:
    def log(self, actor_id, action, entity_type, entity_id, payload=None):
        return create_audit_log(actor_id, action, entity_type, entity_id, payload_json=payload)
