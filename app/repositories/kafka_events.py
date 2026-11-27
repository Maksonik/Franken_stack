from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import KafkaConsumerEventRow
from app.schemas import PaymentSucceededEvent


class KafkaEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def record(self, group: str, event: PaymentSucceededEvent, partition: int, offset: int) -> bool:
        statement = (
            insert(KafkaConsumerEventRow)
            .values(
                consumer_group=group,
                event_id=event.id,
                partition=partition,
                offset=offset,
                payload=event.model_dump(mode="json"),
            )
            .on_conflict_do_nothing(
                index_elements=[
                    KafkaConsumerEventRow.consumer_group,
                    KafkaConsumerEventRow.event_id,
                ]
            )
            .returning(KafkaConsumerEventRow.event_id)
        )
        return self.session.scalar(statement) is not None

    def groups_for_event(self, event_id: UUID) -> tuple[str, ...]:
        statement = (
            select(KafkaConsumerEventRow.consumer_group)
            .where(KafkaConsumerEventRow.event_id == event_id)
            .order_by(KafkaConsumerEventRow.consumer_group)
        )
        return tuple(self.session.scalars(statement))
