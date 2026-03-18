ALLOWED_TEMPLATE_VARIABLES = (
    'first_name',
    'username',
    'community_title',
    'invite_link',
    'campaign_name',
)


class TemplateService:
    def build_context(self, user, community, invite_link, campaign):
        return {
            'first_name': str((user or {}).get('first_name') or '').strip(),
            'username': str((user or {}).get('username') or '').strip(),
            'community_title': str((community or {}).get('title') or '').strip(),
            'invite_link': str((invite_link or {}).get('telegram_invite_link') or '').strip(),
            'campaign_name': str((campaign or {}).get('name') or '').strip(),
        }

    def render(self, template, context):
        result = str(template or '').strip()
        for key in ALLOWED_TEMPLATE_VARIABLES:
            result = result.replace('{' + key + '}', str(context.get(key, '') or ''))
        return result
