from uuid import UUID

from sqlalchemy.orm import Session

from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import Payment, PaymentCreate, PaymentSucceededData


class IdempotencyConflictError(Exception):
    pass


class PaymentNotFoundError(Exception):
    pass


class PaymentService:
    def __init__(self, session: Session) -> None:
        self.session = session
        self.payments = PaymentRepository(session)
        self.outbox = OutboxRepository(session)

    def create(self, body: PaymentCreate, idempotency_key: str) -> tuple[Payment, bool]:
        with self.session.begin():
            payment = self.payments.try_create(idempotency_key, body.amount_minor, body.currency)
            if payment is not None:
                return Payment.model_validate(payment, from_attributes=True), True

            payment = self.payments.get_by_key(idempotency_key)
            if (payment.amount_minor, payment.currency) != (body.amount_minor, body.currency):
                raise IdempotencyConflictError
            return Payment.model_validate(payment, from_attributes=True), False

    def succeed(self, payment_id: UUID) -> Payment:
        with self.session.begin():
            payment = self.payments.try_mark_succeeded(payment_id)
            if payment is not None:
                data = PaymentSucceededData.model_validate(payment, from_attributes=True)
                self.outbox.add(payment_id, "payment.succeeded", data.model_dump(mode="json"))
                return data

            payment = self.payments.get(payment_id)
            if payment is None:
                raise PaymentNotFoundError
            return Payment.model_validate(payment, from_attributes=True)
