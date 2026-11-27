import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from sqlalchemy.orm import Session

from app.core.db import get_engine
from app.models import OutboxEventRow
from app.repositories.outbox import OutboxRepository
from app.schemas import PaymentSucceededData, PaymentSucceededEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OutboxSnapshot:
    event: PaymentSucceededEvent
    published_at: datetime | None
    received_at: datetime | None
    kafka_published_at: datetime | None
    kafka_groups: tuple[str, ...]


def event_payload(event: OutboxEventRow) -> PaymentSucceededEvent:
    return PaymentSucceededEvent(
        id=event.id,
        type=event.event_type,
        occurred_at=event.occurred_at,
        data=PaymentSucceededData.model_validate(event.payload),
    )


def publish_next_event(
    publish: Callable[[PaymentSucceededEvent], None], destination: Literal["rabbitmq", "kafka"]
) -> bool:
    publication_field = (
        OutboxEventRow.kafka_published_at if destination == "kafka" else OutboxEventRow.published_at
    )
    with Session(get_engine()) as session, session.begin():
        row = OutboxRepository(session).lock_next_unpublished(publication_field)
        if row is None:
            return False

        event = event_payload(row)
        publish(event)
        setattr(row, publication_field.key, datetime.now(UTC))

    logger.info(
        "Publication recorded broker=%s event_id=%s payment_id=%s",
        destination,
        event.id,
        event.data.id,
    )
    return True
