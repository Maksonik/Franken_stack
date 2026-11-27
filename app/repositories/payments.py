from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import PaymentRow


class PaymentRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def try_create(
        self, idempotency_key: str, amount_minor: int, currency: str
    ) -> PaymentRow | None:
        statement = (
            insert(PaymentRow)
            .values(
                id=uuid4(),
                idempotency_key=idempotency_key,
                amount_minor=amount_minor,
                currency=currency,
            )
            .on_conflict_do_nothing(index_elements=[PaymentRow.idempotency_key])
            .returning(PaymentRow)
        )
        return self.session.scalars(statement).one_or_none()

    def get_by_key(self, idempotency_key: str) -> PaymentRow:
        # A new statement under READ COMMITTED can see the committed winner.
        statement = select(PaymentRow).where(PaymentRow.idempotency_key == idempotency_key)
        return self.session.scalars(statement).one()

    def get(self, payment_id: UUID) -> PaymentRow | None:
        return self.session.get(PaymentRow, payment_id)

    def try_mark_succeeded(self, payment_id: UUID) -> PaymentRow | None:
        statement = (
            update(PaymentRow)
            .where(PaymentRow.id == payment_id, PaymentRow.status == "pending")
            .values(status="succeeded")
            .returning(PaymentRow)
        )
        return self.session.scalars(statement).one_or_none()
