from services.join_request_service import JoinRequestService


STOP_WORDS = {'stop', '/stop', 'отписка', 'unsubscribe', 'стоп'}


def _join_service(service=None):
    return service or JoinRequestService()


def handle_chat_join_request(chat_join_request, service=None):
    service = _join_service(service)
    invite_link = getattr(getattr(chat_join_request, 'invite_link', None), 'invite_link', '')
    from_user = getattr(chat_join_request, 'from_user', None)
    chat = getattr(chat_join_request, 'chat', None)
    return service.process_join_request(
        chat_id=getattr(chat, 'id', None),
        telegram_user_id=getattr(from_user, 'id', None),
        username=getattr(from_user, 'username', ''),
        first_name=getattr(from_user, 'first_name', ''),
        invite_link=invite_link,
        requested_at=getattr(chat_join_request, 'date', None),
        user_chat_id=getattr(chat_join_request, 'user_chat_id', None),
    )


def handle_chat_member_update(chat_member_update, service=None):
    service = _join_service(service)
    new_member = getattr(chat_member_update, 'new_chat_member', None)
    status = str(getattr(new_member, 'status', '') or '').strip().lower()
    if status not in {'member', 'administrator', 'creator'}:
        return None
    chat = getattr(chat_member_update, 'chat', None)
    user = getattr(new_member, 'user', None) or getattr(chat_member_update, 'from_user', None)
    return service.mark_user_joined(
        chat_id=getattr(chat, 'id', None),
        telegram_user_id=getattr(user, 'id', None),
    )


def handle_message(message, service=None):
    service = _join_service(service)
    text = str(getattr(message, 'text', '') or '').strip().lower()
    if text not in STOP_WORDS:
        return None
    user = getattr(message, 'from_user', None)
    return service.process_unsubscribe(getattr(user, 'id', None))
