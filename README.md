# Franken Stack — доставка платёжных вебхуков

Учебный стенд с одним типом события `payment.succeeded`. В первой версии FastAPI сохраняет платёж и событие в PostgreSQL. Партнёрский HTTP-эндпоинт можно проверить вручную, отправив ему событие из базы.

## План на четыре недели

| Неделя | Что строим | Что проверяем |
| --- | --- | --- |
| 1 **Готово** | FastAPI, PostgreSQL, SQLAlchemy, Alembic, таблица outbox и фальшивый партнёр | Повторы `POST /payments`, гонка при смене статуса, откат при ошибке записи события |
| 2 | Воркер читает outbox и доставляет вебхук по HTTP. Партнёр запоминает обработанные `event.id` и умеет возвращать ошибки | Повторы после `500` и таймаута, перезапуск воркера после того, как партнёр получил запрос, повтор с тем же `event.id` |
| 3 | RabbitMQ передаёт воркеру задания на доставку. Подтверждения публикации и обработки, очередь для исчерпанных попыток | Повторная выдача задания после падения воркера до `ack` и сбой после публикации, но до отметки в outbox |
| 4 | Kafka хранит поток событий платежей. Consumer groups `analytics` и `audit` читают один топик независимо | Остановка и возобновление одной группы, чтение сохранённой истории с начала новой группой |

## Запуск первой версии

При первом запуске скопируй шаблон настроек.

```bash
cp .env.template .env
docker compose up --build --wait
```

Swagger UI доступен для [платёжного API](http://localhost:8000/docs) и [партнёра](http://localhost:8001/docs). Если выбранный Docker-контекст недоступен, а системный Docker Engine запущен, используй `docker --context default compose` во всех командах.

Сервис `migrate` выполняет `alembic upgrade head` после готовности PostgreSQL. Compose запускает API после успешного завершения миграций.

Pydantic Settings читает `.env` из корня проекта. Переменные процесса имеют приоритет над файлом. Compose передаёт API и Alembic настройки `POSTGRES_DB`, `POSTGRES_USER` и `POSTGRES_PASSWORD`, а для подключения внутри сети задаёт адрес `db:5432`. При прямом запуске Python адрес можно задать через `POSTGRES_HOST` и `POSTGRES_PORT`. Полный `DATABASE_URL`, если задан, имеет приоритет над отдельными параметрами подключения. [Настройки Pydantic](https://docs.pydantic.dev/latest/concepts/pydantic_settings/#dotenv-env-support) · [Переменные Docker Compose](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/)

Для подключения из локального pgAdmin в разделе **Connection** укажи `127.0.0.1` и порт `5432`. В **Maintenance database** и **Username** укажи значения `POSTGRES_DB` и `POSTGRES_USER` из `.env` (по умолчанию `payments`). Пароль берётся из `POSTGRES_PASSWORD`. [Настройки подключения pgAdmin](https://www.pgadmin.org/docs/pgadmin4/latest/server_dialog.html)

## Структура кода

| Путь | Ответственность |
| --- | --- |
| `app/routers/` | HTTP-контракт, валидация входа и преобразование ошибок сервиса в HTTP-ответы |
| `app/services/payments.py` | Правила идемпотентности, переход статуса и границы транзакции |
| `app/repositories/` | Запросы SQLAlchemy к платежам и outbox |
| `app/core/` | Настройки, подключение к PostgreSQL и сессия на запрос |
| `app/models.py` | ORM-модели |
| `app/main.py` | Точка входа платёжного API |
| `migrations/` | Версионируемые изменения схемы Alembic |

## Упражнения

### 1. Создать платёж и повторить запрос

```bash
KEY="demo-$(date +%s)-$$"
curl -i http://localhost:8000/payments \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $KEY" \
  -d '{"amount_minor": 15000, "currency": "RUB"}'
```

Первый запрос вернёт `201` и статус `pending`. Повтори только команду `curl` с тем же `KEY`. Ответ будет `200` с тем же `id` и текущим статусом. При изменении суммы или валюты с тем же ключом получишь `409`. `amount_minor` хранится целым числом в минимальных единицах валюты, например в копейках для `RUB`.

### 2. Подтвердить платёж и посмотреть событие

Подставь `id` из ответа. Вызови `curl` дважды, чтобы проверить повтор перехода. Функция `lab_psql` берёт имя пользователя и базы из настроек контейнера.

```bash
PAYMENT_ID='<id из ответа>'
lab_psql() {
  docker compose exec -T db sh -c 'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"' psql "$@"
}

curl -i -X POST "http://localhost:8000/payments/$PAYMENT_ID/succeed"

lab_psql \
  -c "SELECT id, event_type, payload FROM outbox_events WHERE payment_id = '$PAYMENT_ID';"
```

Оба вызова вернут `200` и статус `succeeded`. В `outbox_events` будет одна строка. При двух параллельных запросах условие `status = 'pending'` позволяет обновить строку только одному из них. `PaymentService` выполняет `UPDATE` и `INSERT` в одной транзакции `Session.begin()`. Если `INSERT` завершится ошибкой, платёж останется в `pending`. [Транзакции SQLAlchemy](https://docs.sqlalchemy.org/en/20/orm/session_transaction.html) · [Изоляция в PostgreSQL](https://www.postgresql.org/docs/18/transaction-iso.html)

### 3. Вручную отправить событие партнёру

```bash
lab_psql -XAt \
  -c "SELECT jsonb_build_object('id', id, 'type', event_type, 'occurred_at', occurred_at, 'data', payload) FROM outbox_events WHERE payment_id = '$PAYMENT_ID';" \
  | curl -i http://localhost:8001/webhooks/payments \
      -H 'Content-Type: application/json' --data-binary @-

docker compose logs --tail=10 partner
```

Партнёр ответит `204` и запишет идентификаторы события и платежа в лог. На второй неделе эту отправку возьмёт на себя воркер.

### 4. Проверить откат при сбое записи события

Создай ещё один платёж с новым ключом и подставь его `id` вместо значения `PAYMENT_ID`. Временное ограничение отклонит запись события только для этого платежа.

```bash
lab_psql \
  -c "ALTER TABLE outbox_events ADD CONSTRAINT lab_reject_event CHECK (payment_id <> '$PAYMENT_ID');"

curl -i -X POST "http://localhost:8000/payments/$PAYMENT_ID/succeed"

lab_psql \
  -c "SELECT status FROM payments WHERE id = '$PAYMENT_ID';" \
  -c "SELECT count(*) FROM outbox_events WHERE payment_id = '$PAYMENT_ID';" \
  -c 'ALTER TABLE outbox_events DROP CONSTRAINT lab_reject_event;'

curl -i -X POST "http://localhost:8000/payments/$PAYMENT_ID/succeed"
```

Первый вызов вернёт `500`. Запросы к базе покажут `pending` и `0` событий. После снятия ограничения второй вызов вернёт `200` и `succeeded`.

## Остановить стенд

```bash
docker compose down
```

Данные сохраняются в Docker volume.

## Миграции и Ruff

Проверить текущую ревизию и совпадение схемы с ORM-моделями можно на работающем стенде.

```bash
docker compose run --rm --no-deps migrate alembic current
docker compose run --rm --no-deps migrate alembic check
```

Если том был создан предыдущей версией через `sql/001_init.sql` и схема с тех пор совпадает с исходной, один раз отмечаем базовую ревизию. `stamp` записывает номер ревизии, сохраняя существующие таблицы и строки. Для пустого тома достаточно обычного запуска Compose. [Команды Alembic](https://alembic.sqlalchemy.org/en/latest/api/commands.html#alembic.command.stamp)

```bash
docker compose build api
docker compose up -d --wait db
docker compose run --rm --no-deps migrate alembic stamp 0001_create_payments
docker compose run --rm --no-deps migrate alembic check
docker compose up --wait
```

Зависимости описаны в `pyproject.toml` и зафиксированы в `uv.lock`. Docker устанавливает зависимости через `uv sync --locked --no-dev`. Для локальной разработки, включая Ruff, выполни из корня проекта следующие команды. [Управление зависимостями в uv](https://docs.astral.sh/uv/concepts/projects/dependencies/)

```bash
uv sync --locked
uv run --locked ruff check .
uv run --locked ruff format --check .
```
