import argparse

import config
from telegram.bot_api import TelegramBotAPI


def main():
    parser = argparse.ArgumentParser(description='Set Telegram webhook for Railway web service')
    parser.add_argument('--url', default='', help='Absolute webhook URL, e.g. https://app.up.railway.app/webhook')
    parser.add_argument('--drop-pending-updates', action='store_true')
    args = parser.parse_args()

    webhook_url = str(args.url or '').strip()
    if not webhook_url:
        base_url = str(config.webhook_base_url or '').strip()
        path = str(config.webhook_path or '/webhook').strip()
        if not path.startswith('/'):
            path = '/' + path
        if not base_url:
            raise RuntimeError('Pass --url or set WEBHOOK_BASE_URL')
        webhook_url = f'{base_url}{path}'

    api = TelegramBotAPI()
    api.set_webhook(
        url=webhook_url,
        secret_token=config.webhook_secret_token,
        drop_pending_updates=args.drop_pending_updates,
        allowed_updates=['message', 'callback_query', 'chat_join_request', 'chat_member', 'my_chat_member'],
    )
    print(f'Webhook set: {webhook_url}')


if __name__ == '__main__':
    main()
