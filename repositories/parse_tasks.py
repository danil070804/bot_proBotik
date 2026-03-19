from db import (
    create_parse_task,
    finish_parse_task,
    get_parse_task,
    get_parse_tasks_summary,
    list_parse_tasks,
    update_parse_task_details,
    update_parse_task_progress,
    update_parse_task_status,
)


class ParseTaskRepository:
    def get(self, task_id):
        return get_parse_task(task_id)

    def list(self, status=None, limit=None):
        return list_parse_tasks(status=status, limit=limit)

    def create(self, **payload):
        return create_parse_task(**payload)

    def update_status(self, task_id, status):
        return update_parse_task_status(task_id, status)

    def update_progress(self, task_id, found=None, saved=None, skipped=None, errors=None):
        return update_parse_task_progress(task_id, found=found, saved=saved, skipped=skipped, errors=errors)

    def update_details(self, task_id, source_report_json=None, last_error=None, meta_json=None):
        return update_parse_task_details(
            task_id,
            source_report_json=source_report_json,
            last_error=last_error,
            meta_json=meta_json,
        )

    def finish(self, task_id, status='finished'):
        return finish_parse_task(task_id, status=status)

    def summary(self):
        return get_parse_tasks_summary()
