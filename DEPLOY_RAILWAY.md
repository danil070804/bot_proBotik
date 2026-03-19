# Railway Deploy

Проект должен быть развёрнут как два отдельных сервиса из одного и того же репозитория:

1. `web`
2. `worker`

Оба сервиса используют одну и ту же базу `Postgres`.

## Почему два сервиса

`web` принимает Telegram webhook и держит HTTP процесс.

`worker` крутит фоновые циклы:

- обработка кампаний
- обработка pending join requests

Не запускай на Railway:

- `python main.py`
- `python run_main.py`

Эти entrypoint'ы нужны только для legacy polling/локального режима и не должны быть основным transport в production.

## Важно про Config as Code

Один Railway config файл описывает один deployment.

Поэтому для этого проекта добавлены два отдельных файла:

- `/bot_proBotik/railway.web.toml`
- `/bot_proBotik/railway.worker.toml`

## Сервис `web`

### Settings

- `Root Directory`: `bot_proBotik`
- `Config as Code file`: `/bot_proBotik/railway.web.toml`

### Переменные

Обязательно:

```text
BOT_TRANSPORT=webhook
DATABASE_URL=<Railway Postgres DATABASE_URL>
BOT_TOKEN=<твой токен> 
```

Допустимо вместо `BOT_TOKEN` использовать `BOT_INVITE_TOKEN`.

Для webhook:

```text
WEBHOOK_BASE_URL=https://<public-domain>.up.railway.app
WEBHOOK_SECRET_TOKEN=<опционально>
```

### Networking

Для `web` обязательно нужен публичный домен:

1. `Settings -> Networking`
2. `Generate Domain`

Полученный домен нужно вставить в `WEBHOOK_BASE_URL`.

## Сервис `worker`

### Settings

- `Root Directory`: `bot_proBotik`
- `Config as Code file`: `/bot_proBotik/railway.worker.toml`

### Переменные

```text
BOT_TRANSPORT=webhook
DATABASE_URL=<Railway Postgres DATABASE_URL>
BOT_TOKEN=<тот же токен>
```

`WEBHOOK_BASE_URL` для `worker` не обязателен, но можно оставить тем же значением для единообразия.

Публичный домен `worker` не нужен.

## Что должно получиться в итоге

### `web`

- стартует через `uvicorn webhook:app --host 0.0.0.0 --port $PORT`
- отвечает на `/health`
- принимает Telegram updates

### `worker`

- стартует через `python worker_main.py`
- не открывает HTTP порт
- не запускает polling

## После настройки

1. Удали старые ручные `Start Command`, если они конфликтуют.
2. Сделай redeploy обоих сервисов.
3. После первого успешного деплоя выставь webhook:

```bash
python set_webhook.py
```

или вызови служебный endpoint, если используешь его.

## Быстрая проверка

### `web`

- логи не содержат `409 conflict`
- сервис не перезапускается сразу после старта
- `/health` отвечает `200`

### `worker`

- логи содержат `Worker started`
- нет `uvicorn`
- нет `getUpdates`

## Типовые ошибки

### 1. `409 Conflict`

Причина:

- где-то всё ещё запущен polling

Проверь:

- нет ли сервиса с `python main.py`
- нет ли сервиса с `python run_main.py`

### 2. `Polling skipped: BOT_TRANSPORT=webhook`

Это не ошибка само по себе.

Это означает, что сервис стартовал через `main.py`, но transport сейчас `webhook`, и polling правильно отключён.

Если этот лог идёт в цикле, значит сервис запущен не тем entrypoint'ом.

### 3. Контейнер уходит в restart loop

Обычно причина одна из двух:

- на `web` стоит не `uvicorn webhook:app --host 0.0.0.0 --port $PORT`
- на `worker` стоит не `python worker_main.py`
