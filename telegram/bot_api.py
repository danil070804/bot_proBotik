import requests

import config


class TelegramBotAPIError(RuntimeError):
    def __init__(self, method, description, error_code=None, parameters=None):
        self.method = method
        self.description = str(description or '').strip() or 'Telegram Bot API error'
        self.error_code = error_code
        self.parameters = parameters or {}
        super().__init__(self.__str__())

    @property
    def retry_after(self):
        value = self.parameters.get('retry_after')
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @property
    def is_retryable(self):
        return self.error_code in {429, 500, 502, 503, 504}

    def __str__(self):
        if self.error_code:
            return f'{self.method}: [{self.error_code}] {self.description}'
        return f'{self.method}: {self.description}'


class TelegramBotAPI:
    def __init__(self, token=None, base_url=None, timeout=None):
        self.token = str(token or config.bot_api_token or '').strip()
        self.base_url = str(base_url or config.bot_api_base_url or 'https://api.telegram.org').rstrip('/')
        self.timeout = int(timeout or config.bot_api_timeout or 30)
        if not self.token:
            raise RuntimeError('BOT_API_TOKEN or BOT_INVITE_TOKEN is not configured')
        self._session = requests.Session()

    def _request(self, method, payload=None):
        url = f'{self.base_url}/bot{self.token}/{method}'
        try:
            response = self._session.post(url, json=payload or {}, timeout=self.timeout)
        except requests.RequestException as exc:
            raise TelegramBotAPIError(method, exc, error_code=getattr(getattr(exc, 'response', None), 'status_code', None)) from exc
        try:
            data = response.json()
        except ValueError as exc:
            raise TelegramBotAPIError(method, 'Invalid JSON response from Telegram') from exc
        if not response.ok or not data.get('ok'):
            raise TelegramBotAPIError(
                method,
                data.get('description') or response.text,
                error_code=data.get('error_code') or response.status_code,
                parameters=data.get('parameters'),
            )
        return data.get('result')

    def create_chat_invite_link(self, chat_id, creates_join_request=False, expire_date=None, member_limit=None, name=None):
        payload = {
            'chat_id': chat_id,
            'creates_join_request': bool(creates_join_request),
        }
        if expire_date is not None:
            payload['expire_date'] = expire_date
        if member_limit is not None:
            payload['member_limit'] = member_limit
        if name:
            payload['name'] = str(name)
        return self._request('createChatInviteLink', payload)

    def revoke_chat_invite_link(self, chat_id, invite_link):
        return self._request('revokeChatInviteLink', {'chat_id': chat_id, 'invite_link': invite_link})

    def send_message(self, user_id, text, reply_markup=None, disable_web_page_preview=True):
        payload = {
            'chat_id': user_id,
            'text': str(text or ''),
            'disable_web_page_preview': bool(disable_web_page_preview),
        }
        if reply_markup is not None:
            payload['reply_markup'] = reply_markup
        return self._request('sendMessage', payload)

    def approve_join_request(self, chat_id, user_id):
        return self._request('approveChatJoinRequest', {'chat_id': chat_id, 'user_id': user_id})

    def decline_join_request(self, chat_id, user_id):
        return self._request('declineChatJoinRequest', {'chat_id': chat_id, 'user_id': user_id})
