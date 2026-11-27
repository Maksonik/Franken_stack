from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import WebhookReceiptRow


class WebhookReceiptRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def record(self, event_id: UUID, payment_id: UUID) -> bool:
        statement = (
            insert(WebhookReceiptRow)
            .values(event_id=event_id, payment_id=payment_id)
            .on_conflict_do_nothing(index_elements=[WebhookReceiptRow.event_id])
            .returning(WebhookReceiptRow.event_id)
        )
        return self.session.scalar(statement) is not None

    def received_at(self, event_id: UUID) -> datetime | None:
        return self.session.scalar(
            select(WebhookReceiptRow.received_at).where(WebhookReceiptRow.event_id == event_id)
        )
