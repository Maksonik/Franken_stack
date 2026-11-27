from sqlalchemy.orm import Session

from app.repositories.webhook_receipts import WebhookReceiptRepository
from app.schemas import PaymentSucceededEvent


def accept_webhook(session: Session, event: PaymentSucceededEvent) -> bool:
    with session.begin():
        return WebhookReceiptRepository(session).record(event.id, event.data.id)
