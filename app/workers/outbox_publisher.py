import logging
from functools import partial
from time import sleep

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError
from sqlalchemy.exc import SQLAlchemyError

from app.core.rabbitmq import DELIVERY_QUEUE, connection_parameters
from app.core.settings import Settings
from app.schemas import PaymentSucceededEvent
from app.services.outbox import publish_next_event

logger = logging.getLogger(__name__)


def publish_event(channel: BlockingChannel, event: PaymentSucceededEvent) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=DELIVERY_QUEUE,
        body=event.model_dump_json().encode(),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=str(event.id),
            type=event.type,
        ),
        mandatory=True,
    )


def run() -> None:
    parameters = connection_parameters(Settings())

    while True:
        try:
            with pika.BlockingConnection(parameters) as connection:
                channel = connection.channel()
                channel.queue_declare(queue=DELIVERY_QUEUE, durable=True)
                channel.confirm_delivery()
                publish = partial(publish_event, channel)
                logger.info("Publishing outbox events to %s", DELIVERY_QUEUE)

                while True:
                    if not publish_next_event(publish, "rabbitmq"):
                        connection.process_data_events(time_limit=1)
        except (AMQPError, SQLAlchemyError) as error:
            logger.warning("Publication interrupted (%s). Retrying in 2 s", type(error).__name__)
            sleep(2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    logging.getLogger("pika").setLevel(logging.CRITICAL)
    run()
