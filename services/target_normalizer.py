from dataclasses import asdict, dataclass
from urllib.parse import urlparse


INVITE_TYPE = 'invite_hash'
PUBLIC_TYPE = 'public_username'


@dataclass(frozen=True)
class ParsedTarget:
    raw_target: str
    target_type: str
    normalized_value: str
    display_value: str
    join_method: str

    def to_dict(self):
        return asdict(self)


def _strip_url_prefix(value):
    text = str(value or '').strip()
    if text.startswith('http://') or text.startswith('https://'):
        return text
    if text.startswith('t.me/'):
        return f'https://{text}'
    if text.startswith('telegram.me/'):
        return f'https://{text}'
    return text


def extract_invite_hash(raw_target):
    text = _strip_url_prefix(raw_target)
    if not text:
        return ''
    if text.startswith('+'):
        return text[1:].strip().strip('/')
    lower = text.lower()
    if lower.startswith('tg://join?invite='):
        return text.split('invite=', 1)[1].strip().strip('/')
    if lower.startswith('http://') or lower.startswith('https://'):
        parsed = urlparse(text)
        path = str(parsed.path or '').strip('/')
        if path.startswith('+'):
            return path[1:].strip().strip('/')
        if path.lower().startswith('joinchat/'):
            return path.split('/', 1)[1].strip().strip('/')
        return ''
    return ''


def _looks_like_invite_target(raw_target):
    text = _strip_url_prefix(raw_target)
    lower = text.lower()
    if text.startswith('+'):
        return True
    if lower.startswith('tg://join?invite='):
        return True
    if lower.startswith('http://') or lower.startswith('https://'):
        parsed = urlparse(text)
        path = str(parsed.path or '').strip('/')
        return path.startswith('+') or path.lower().startswith('joinchat/')
    return False


def detect_target_type(raw_target):
    if extract_invite_hash(raw_target):
        return INVITE_TYPE
    return PUBLIC_TYPE


def _normalize_public_target(raw_target):
    text = _strip_url_prefix(raw_target)
    if not text:
        raise ValueError('Target is empty')
    if text.startswith('@'):
        text = text[1:].strip()
    if text.startswith('http://') or text.startswith('https://'):
        parsed = urlparse(text)
        path = str(parsed.path or '').strip('/')
        if not path:
            raise ValueError('Target username is empty')
        text = path.split('/', 1)[0].strip()
    text = text.strip().lstrip('@')
    if not text:
        raise ValueError('Target username is empty')
    return text


def parse_target(raw_target):
    raw_value = str(raw_target or '').strip()
    if not raw_value:
        raise ValueError('Target is empty')
    invite_hash = extract_invite_hash(raw_value)
    if invite_hash:
        return ParsedTarget(
            raw_target=raw_value,
            target_type=INVITE_TYPE,
            normalized_value=invite_hash,
            display_value=raw_value,
            join_method='ImportChatInviteRequest',
        )
    if _looks_like_invite_target(raw_value):
        raise ValueError('Invite hash is empty')
    username = _normalize_public_target(raw_value)
    return ParsedTarget(
        raw_target=raw_value,
        target_type=PUBLIC_TYPE,
        normalized_value=username,
        display_value='@' + username,
        join_method='JoinChannelRequest',
    )
