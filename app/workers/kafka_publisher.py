import logging
from functools import partial
from time import sleep

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from sqlalchemy.exc import SQLAlchemyError

from app.core.kafka import PAYMENT_EVENTS_TOPIC
from app.core.settings import Settings
from app.schemas import PaymentSucceededEvent
from app.services.outbox import publish_next_event

logger = logging.getLogger(__name__)


def publish_event(producer: Producer, event: PaymentSucceededEvent) -> None:
    errors: list[KafkaError] = []

    def on_delivery(error: KafkaError | None, _message: Message) -> None:
        if error is not None:
            errors.append(error)

    producer.produce(
        PAYMENT_EVENTS_TOPIC,
        key=str(event.data.id).encode(),
        value=event.model_dump_json().encode(),
        on_delivery=on_delivery,
    )
    if producer.flush(6):
        raise TimeoutError("Kafka publication timed out")
    if errors:
        raise KafkaException(errors[0])


def run() -> None:
    settings = Settings()
    config = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 5000,
    }
    while True:
        producer = Producer(config)
        publish = partial(publish_event, producer)
        try:
            while True:
                if not publish_next_event(publish, "kafka"):
                    sleep(1)
        except (KafkaException, SQLAlchemyError, TimeoutError) as error:
            logger.warning(
                "Kafka publication interrupted (%s). Retrying in 2 s", type(error).__name__
            )
            sleep(2)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    run()
