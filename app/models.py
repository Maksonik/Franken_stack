from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class PaymentRow(Base):
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="payments_idempotency_key_key"),
        CheckConstraint("amount_minor > 0", name="payments_amount_minor_check"),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="payments_currency_check"),
        CheckConstraint("status IN ('pending', 'succeeded')", name="payments_status_check"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(Text)
    amount_minor: Mapped[int] = mapped_column(BigInteger)
    currency: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, server_default="pending")


class OutboxEventRow(Base):
    __tablename__ = "outbox_events"
    __table_args__ = (
        UniqueConstraint(
            "payment_id", "event_type", name="outbox_events_payment_id_event_type_key"
        ),
        CheckConstraint("event_type = 'payment.succeeded'", name="outbox_events_event_type_check"),
        Index(
            "outbox_events_unpublished_idx",
            "occurred_at",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index(
            "outbox_events_kafka_unpublished_idx",
            "occurred_at",
            "id",
            postgresql_where=text("kafka_published_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(
        ForeignKey("payments.id", name="outbox_events_payment_id_fkey")
    )
    event_type: Mapped[str] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    payload: Mapped[dict] = mapped_column(JSONB)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    kafka_published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WebhookReceiptRow(Base):
    __tablename__ = "webhook_receipts"

    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID]
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class KafkaConsumerEventRow(Base):
    __tablename__ = "kafka_consumer_events"
    __table_args__ = (Index("kafka_consumer_events_event_id_idx", "event_id"),)

    consumer_group: Mapped[str] = mapped_column(Text, primary_key=True)
    event_id: Mapped[UUID] = mapped_column(primary_key=True)
    partition: Mapped[int]
    offset: Mapped[int] = mapped_column(BigInteger)
    payload: Mapped[dict] = mapped_column(JSONB)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
