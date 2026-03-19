from fastapi import FastAPI, Header, HTTPException, Request
from telebot import types

import config
from main import bot
from telegram.bot_api import TelegramBotAPI


WEBHOOK_PATH = config.webhook_path if str(config.webhook_path).startswith('/') else '/' + str(config.webhook_path)
WEBHOOK_ALLOWED_UPDATES = ['message', 'callback_query', 'chat_join_request', 'chat_member', 'my_chat_member']

app = FastAPI(title='Telegram Webhook', version='1.0.0')


def _webhook_url():
    if not config.webhook_base_url:
        raise RuntimeError('WEBHOOK_BASE_URL is not configured')
    return f'{config.webhook_base_url}{WEBHOOK_PATH}'


@app.get('/health')
async def health():
    return {
        'ok': True,
        'transport': 'webhook',
        'webhook_path': WEBHOOK_PATH,
        'webhook_url': config.webhook_base_url + WEBHOOK_PATH if config.webhook_base_url else '',
    }


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request, x_telegram_bot_api_secret_token: str | None = Header(default=None)):
    expected_secret = str(config.webhook_secret_token or '').strip()
    if expected_secret and x_telegram_bot_api_secret_token != expected_secret:
        raise HTTPException(status_code=401, detail='Invalid Telegram webhook secret')
    payload = await request.json()
    try:
        update = types.Update.de_json(payload)
        bot.process_new_updates([update])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Webhook processing failed: {exc}') from exc
    return {'ok': True}


@app.post('/webhook/set')
async def set_webhook(drop_pending_updates: bool = False):
    api = TelegramBotAPI()
    result = api.set_webhook(
        url=_webhook_url(),
        secret_token=config.webhook_secret_token,
        drop_pending_updates=drop_pending_updates,
        allowed_updates=WEBHOOK_ALLOWED_UPDATES,
    )
    return {'ok': True, 'result': result, 'url': _webhook_url()}


@app.get('/webhook/set')
async def set_webhook_get(drop_pending_updates: bool = False):
    return await set_webhook(drop_pending_updates=drop_pending_updates)


@app.get('/webhook/info')
async def webhook_info():
    api = TelegramBotAPI()
    result = api.get_webhook_info()
    return {'ok': True, 'configured_url': _webhook_url(), 'result': result}


@app.post('/webhook/delete')
async def delete_webhook(drop_pending_updates: bool = False):
    api = TelegramBotAPI()
    result = api.delete_webhook(drop_pending_updates=drop_pending_updates)
    return {'ok': True, 'result': result}


@app.get('/webhook/delete')
async def delete_webhook_get(drop_pending_updates: bool = False):
    return await delete_webhook(drop_pending_updates=drop_pending_updates)
