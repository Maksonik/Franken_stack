import logging
import signal
from contextlib import closing
from threading import Event

from confluent_kafka import Consumer, KafkaException, Message
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.db import get_engine
from app.core.kafka import PAYMENT_EVENTS_TOPIC
from app.core.settings import Settings
from app.repositories.kafka_events import KafkaEventRepository
from app.schemas import PaymentSucceededEvent

logger = logging.getLogger(__name__)


def record_event(group: str, message: Message) -> None:
    try:
        event = PaymentSucceededEvent.model_validate_json(message.value() or b"")
    except ValidationError:
        logger.warning(
            "Skipping invalid event group=%s partition=%s offset=%s",
            group,
            message.partition(),
            message.offset(),
        )
        return

    with Session(get_engine()) as session, session.begin():
        created = KafkaEventRepository(session).record(
            group, event, message.partition(), message.offset()
        )
    logger.info(
        "%s group=%s event_id=%s partition=%s offset=%s",
        "Recorded" if created else "Duplicate",
        group,
        event.id,
        message.partition(),
        message.offset(),
    )


def run() -> None:
    settings = Settings()
    group = settings.kafka_consumer_group
    stop = Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    }
    while not stop.is_set():
        try:
            with closing(Consumer(config)) as consumer:
                consumer.subscribe([PAYMENT_EVENTS_TOPIC])
                logger.info("Subscribed group=%s topic=%s", group, PAYMENT_EVENTS_TOPIC)
                while not stop.is_set():
                    message = consumer.poll(1)
                    if message is None:
                        continue
                    if message.error() is not None:
                        raise KafkaException(message.error())
                    record_event(group, message)
                    for offset in consumer.commit(message=message, asynchronous=False):
                        if offset.error is not None:
                            raise KafkaException(offset.error)
        except (KafkaException, SQLAlchemyError) as error:
            logger.warning("Consumer interrupted group=%s (%s)", group, type(error).__name__)
            stop.wait(2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    run()
