# Franken Stack — доставка платёжных вебхуков

Учебный стенд с одним типом события `payment.succeeded`. В первой версии FastAPI сохраняет платёж и событие в PostgreSQL. Партнёрский HTTP-эндпоинт можно проверить вручную, отправив ему событие из базы.

## План на четыре недели

| Неделя | Что строим | Что проверяем |
| --- | --- | --- |
| 1 **Готово** | FastAPI, PostgreSQL, SQLAlchemy, Alembic, таблица outbox и фальшивый партнёр | Повторы `POST /payments`, гонка при смене статуса, откат при ошибке записи события |
| 2 **Готово** | Воркер доставляет вебхук по HTTP. Партнёр сохраняет обработанные `event.id` | Повтор с тем же `event.id` даёт одну запись у партнёра |
| 3 **Готово** | Публикатор outbox, RabbitMQ, `ack` после HTTP-ответа, очередь повторов и очередь ошибок | Сбой брокера, повтор после `503`, уход в очередь ошибок после трёх попыток |
| 4 **Готово** | Kafka хранит поток событий платежей. Consumer groups `analytics` и `audit` читают один топик независимо | Остановка и возобновление одной группы, чтение сохранённой истории с начала новой группой |

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
| `app/workers/` | Публикация outbox в RabbitMQ и доставка вебхуков партнёру |
| `app/core/` | Настройки, подключение к PostgreSQL и сессия на запрос |
| `app/models.py` | ORM-модели |
| `app/main.py` | Точка входа платёжного API |
| `migrations/` | Версионируемые изменения схемы Alembic |

## Упражнения

В [лаборатории платежей](http://localhost:8000/lab) можно пройти основной сценарий в браузере. В журнале видны HTTP-статус, время ответа, заголовок идемпотентности и тела запросов и ответов.

1. Отправь `POST /payments`, повтори с тем же ключом для `200`, затем измени сумму с тем же ключом для `409`.
2. Создай платёж с новым ключом и нажми **Два запроса сразу**. В журнале будут два ответа `200`, а в outbox — одно событие.
3. После подтверждения через секунду нажми **Обновить**. Лаборатория покажет публикацию в RabbitMQ и приём события партнёром. Кнопки **Ручной тест 503** и **Отправить вручную** позволяют отдельно проверить HTTP-ответы партнёра.

Ниже те же сценарии с командами и эксперимент с откатом транзакции.

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

Партнёр ответит `204`. Первый приём сохраняется в `webhook_receipts`, повтор с тем же `event.id` фиксируется в логе как дубль. Автоматическая доставка выполняется через RabbitMQ.

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

## RabbitMQ в Docker Desktop

RabbitMQ запускается сервисом `rabbitmq` в текущем Compose-проекте. Браузерный [Management UI](http://localhost:15672/) доступен на порту `15672`, AMQP-клиенты подключаются к `127.0.0.1:5672`. Логин и пароль по умолчанию — `lab` и `lab`. Их можно задать через `RABBITMQ_DEFAULT_USER` и `RABBITMQ_DEFAULT_PASS` в `.env` при первом запуске на пустом томе. [Management UI](https://www.rabbitmq.com/docs/management)

### Событие платежа в RabbitMQ

1. В [лаборатории платежей](http://localhost:8000/lab) создай платёж с новым ключом и нажми **Подтвердить**. API сохраняет статус `succeeded` и событие `payment.succeeded` в одной транзакции PostgreSQL.
2. Сервис `outbox-publisher` забирает сохранённое событие и публикует его в [`webhook.delivery`](http://localhost:15672/#/queues/%2F/webhook.delivery). Если в лаборатории ещё показано **Ожидает публикации в RabbitMQ**, через несколько секунд нажми **Обновить**.
3. `delivery-worker` читает `webhook.delivery`, отправляет событие партнёру по HTTP и после `2xx` подтверждает брокеру доставку. На странице очереди смотри **Consumers** и график **Ack**. При быстрой обработке сообщение может исчезнуть из **Ready** между обновлениями страницы.

`published_at` означает, что RabbitMQ подтвердил публикацию. Отдельная запись в `webhook_receipts` означает, что партнёр принял событие. Если публикатор упадёт после publisher confirm или воркер упадёт после `2xx` и до `ack`, возможна повторная отправка. Партнёр записывает каждый `event.id` один раз. [Подтверждения и повторная выдача](https://www.rabbitmq.com/docs/confirms)

### Проверка сбоя партнёра

В Docker Desktop останови контейнер `partner`, подтверди новый платёж и открой [`webhook.delivery.retry`](http://localhost:15672/#/queues/%2F/webhook.delivery.retry). При временной ошибке воркер сначала публикует копию в очередь повторов с подтверждением брокера, затем отправляет `ack` исходному сообщению. Через пять секунд RabbitMQ возвращает копию в `webhook.delivery`. После трёх неудачных попыток событие появляется в [`webhook.delivery.failed`](http://localhost:15672/#/queues/%2F/webhook.delivery.failed). Постоянная ошибка, например `404`, отправляет его туда сразу. Запусти `partner` после опыта. [TTL](https://www.rabbitmq.com/docs/ttl) · [Dead lettering](https://www.rabbitmq.com/docs/dlx)

Чтобы задержать доставку ради наблюдения за **Ready**, останови `delivery-worker`, подтверди платёж и затем запусти воркер; **Ready** уменьшится после обработки, а в лаборатории появится отметка **Партнёр принял событие**.

### Ручной опыт в учебной очереди

Для первого опыта открой **Queues and Streams → webhook.demo**. Если запускаешь стенд на новом томе, добавь очередь `webhook.demo` через **Add a new queue**, выбери тип **Classic** и долговечность **Durable**.

1. В **Publish message** отправь `{"type":"payment.succeeded","id":"demo-1"}`. В **Ready** появится одно дополнительное сообщение.
2. В **Get messages** выбери **Nack message requeue true**. Посмотри тело сообщения и запроси его ещё раз — та же задача снова доступна для получения.
3. В этой учебной очереди выбери **Automatic ack**, чтобы прочитать и убрать сообщение из очереди. В обычном воркере будем подтверждать доставку явно после обработки.

Чтобы увидеть **Unacked** и возврат сообщения при закрытии канала, запусти сценарий. Он выводит ссылку на временную очередь и ждёт Enter между шагами. В Management UI счётчики обновляются с интервалом, поэтому дай им несколько секунд после каждого шага.

```bash
uv run --env-file .env scripts/rabbitmq_ack_demo.py
```

Цепочка состояний в опыте: `Ready 1 → Unacked 1 → Ready 1 → Ready 0 / Unacked 0`. При повторном получении появляется `redelivered=True`. Опыт использует тестовые сообщения. [Подтверждения и повторная выдача](https://www.rabbitmq.com/docs/confirms)

## Kafka в Docker Desktop

[Kafka UI](http://localhost:8082/) показывает кластер `franken-stack`, топики, сообщения и прогресс consumer groups. Локальный Kafka-клиент подключается к `127.0.0.1:9092`. В учебном брокере один узел. Топик `payment-events` хранится в Docker volume, имеет три партиции, фактор репликации `1` и retention семь дней. [Apache Kafka Quickstart](https://kafka.apache.org/quickstart/)

1. В [лаборатории](http://localhost:8000/lab) создай платёж и подтверди его. Блок **Outbox и вебхук** автоматически обновляет статус до 30 секунд. Когда обе группы обработают событие, появится `Kafka · прочитали analytics, audit`. Кнопка **Обновить** запускает проверку ещё раз.
2. В Kafka UI открой **Topics → payment-events → Messages**. Ключ записи — `payment_id`, поле `id` в JSON — `event.id`. Исходные сообщения доступны для чтения после обработки группами в пределах retention.
3. Останови `kafka-audit` в Docker Desktop и подтверди ещё несколько платежей. В **Consumers** сравни lag `audit` и `analytics`. Затем запусти `kafka-audit` и проверь, что группа догнала поток со своего сохранённого offset.

Для просмотра истории с начала создай новую группу. Она начнёт с самого раннего доступного offset. Останови её через Ctrl+C, когда увидишь нужные события в логе и Kafka UI.

```bash
docker compose run --rm --no-deps -e KAFKA_CONSUMER_GROUP=audit-replay-$(date +%s) kafka-audit
```

В Query Tool pgAdmin можно посмотреть сохранённые результаты обеих групп.

```sql
SELECT payload -> 'data' ->> 'currency' AS currency,
       count(*) AS payments,
       sum((payload -> 'data' ->> 'amount_minor')::numeric) AS amount_minor
FROM kafka_consumer_events
WHERE consumer_group = 'analytics'
GROUP BY 1;

SELECT event_id, partition, "offset", payload, processed_at
FROM kafka_consumer_events
WHERE consumer_group = 'audit'
ORDER BY processed_at DESC;
```

Публикаторы RabbitMQ и Kafka отмечают публикацию в отдельных колонках outbox. Каждая группа фиксирует offset после коммита PostgreSQL. При падении между этими двумя шагами группа может прочитать запись повторно. Уникальный ключ `(consumer_group, event_id)` сохраняет один результат на группу. Порядок Kafka гарантирует внутри партиции. [Работа с offset](https://docs.confluent.io/kafka-clients/python/current/index.html) · [Партиции и consumer groups](https://kafka.apache.org/41/getting-started/introduction/)

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
