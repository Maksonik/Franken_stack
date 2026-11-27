import logging
from functools import partial
from time import sleep

import httpx
import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError
from pika.spec import Basic
from pydantic import ValidationError

from app.core.rabbitmq import DELIVERY_QUEUE, connection_parameters
from app.core.settings import Settings
from app.schemas import PaymentSucceededEvent

RETRY_QUEUE = "webhook.delivery.retry"
FAILED_QUEUE = "webhook.delivery.failed"
MAX_ATTEMPTS = 3
RETRY_DELAY_MS = 5000
logger = logging.getLogger(__name__)


def declare_topology(channel: BlockingChannel) -> None:
    channel.queue_declare(queue=DELIVERY_QUEUE, durable=True)
    channel.queue_declare(queue=FAILED_QUEUE, durable=True)
    channel.queue_declare(
        queue=RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": RETRY_DELAY_MS,
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": DELIVERY_QUEUE,
        },
    )


def post_webhook(
    client: httpx.Client, url: str, body: bytes, event_id: str
) -> tuple[str | None, bool]:
    try:
        response = client.post(
            url,
            content=body,
            headers={"Content-Type": "application/json", "Idempotency-Key": event_id},
        )
    except httpx.RequestError as error:
        return type(error).__name__, True

    if response.is_success:
        return None, False
    retryable = response.status_code in (408, 429) or response.is_server_error
    return f"HTTP {response.status_code}", retryable


def handle_delivery(
    client: httpx.Client,
    url: str,
    channel: BlockingChannel,
    delivery: Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    attempts = (properties.headers or {}).get("delivery-attempts", 0)
    event_id = properties.message_id
    try:
        event = PaymentSucceededEvent.model_validate_json(body)
    except ValidationError:
        attempts, reason, retryable = 0, "Invalid webhook payload", False
    else:
        event_id = str(event.id)
        if type(attempts) is not int or not 0 <= attempts < MAX_ATTEMPTS:
            attempts, reason, retryable = 0, "Invalid attempt counter", False
        else:
            attempts += 1
            reason, retryable = post_webhook(client, url, body, event_id)

    if reason is not None:
        destination = RETRY_QUEUE if retryable and attempts < MAX_ATTEMPTS else FAILED_QUEUE
        # Publisher confirms are enabled. A failed transfer leaves the original delivery unacked.
        channel.basic_publish(
            exchange="",
            routing_key=destination,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=event_id,
                type=properties.type,
                headers={"delivery-attempts": attempts, "last-delivery-error": reason},
            ),
            mandatory=True,
        )
        logger.warning(
            "Moved event_id=%s attempt=%s to %s reason=%s", event_id, attempts, destination, reason
        )

    channel.basic_ack(delivery.delivery_tag)
    if reason is None:
        logger.info("Webhook accepted, ack sent event_id=%s attempt=%s", event_id, attempts)


def run() -> None:
    settings = Settings()
    parameters = connection_parameters(settings)
    partner_url = str(settings.partner_webhook_url)
    with httpx.Client(timeout=5, trust_env=False) as client:
        while True:
            try:
                with pika.BlockingConnection(parameters) as connection:
                    channel = connection.channel()
                    declare_topology(channel)
                    channel.confirm_delivery()
                    channel.basic_qos(prefetch_count=1)
                    channel.basic_consume(
                        queue=DELIVERY_QUEUE,
                        on_message_callback=partial(handle_delivery, client, partner_url),
                        auto_ack=False,
                    )
                    logger.info("Consuming webhooks from %s", DELIVERY_QUEUE)
                    channel.start_consuming()
            except AMQPError as error:
                logger.warning(
                    "Delivery interrupted (%s). Reconnecting in 2 s", type(error).__name__
                )
                sleep(2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    run()
