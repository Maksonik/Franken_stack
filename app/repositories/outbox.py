from uuid import UUID, uuid4

from sqlalchemy.orm import Session

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
