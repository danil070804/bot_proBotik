from subprocess import check_output
from time import sleep
import os
from datetime import datetime


def polling_enabled():
    transport = str(os.getenv('BOT_TRANSPORT', '')).strip().lower()
    webhook_base_url = str(os.getenv('WEBHOOK_BASE_URL', '')).strip()
    if transport:
        return transport == 'polling'
    return not webhook_base_url


def process_exists(process_name):
    call = 'TASKLIST', '/FI', 'imagename eq %s' % process_name
    if check_output(call).splitlines()[3:]:
        return True


while True:
    try:
        if not polling_enabled():
            print('Polling watchdog disabled: webhook transport is active.')
            break

        if not process_exists('main.py'):
            os.system('python main.py')
            print('Бот перезапущен. Время: {}'.format(datetime.now()))

        sleep(2)
    except KeyboardInterrupt:
        break
