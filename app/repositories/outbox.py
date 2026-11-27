from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import InstrumentedAttribute, Session

from app.models import OutboxEventRow


class OutboxRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, payment_id: UUID, event_type: str, payload: dict) -> None:
        self.session.add(
            OutboxEventRow(
                id=uuid4(), payment_id=payment_id, event_type=event_type, payload=payload
            )
        )

    def get_for_payment(self, payment_id: UUID) -> OutboxEventRow | None:
        statement = select(OutboxEventRow).where(OutboxEventRow.payment_id == payment_id)
        return self.session.scalars(statement).one_or_none()

    def lock_next_unpublished(
        self, publication_field: InstrumentedAttribute[datetime | None]
    ) -> OutboxEventRow | None:
        statement = (
            select(OutboxEventRow)
            .where(publication_field.is_(None))
            .order_by(OutboxEventRow.occurred_at, OutboxEventRow.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        return self.session.scalars(statement).one_or_none()
