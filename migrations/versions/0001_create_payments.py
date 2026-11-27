"""Create payments and outbox events."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001_create_payments"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("amount_minor", sa.BigInteger(), nullable=False),
        sa.Column("currency", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default="pending", nullable=False),
        sa.UniqueConstraint("idempotency_key", name="payments_idempotency_key_key"),
        sa.CheckConstraint("amount_minor > 0", name="payments_amount_minor_check"),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name="payments_currency_check"),
        sa.CheckConstraint("status IN ('pending', 'succeeded')", name="payments_status_check"),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column(
            "payment_id",
            sa.Uuid(),
            sa.ForeignKey("payments.id", name="outbox_events_payment_id_fkey"),
            nullable=False,
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column(
            "occurred_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.UniqueConstraint(
            "payment_id", "event_type", name="outbox_events_payment_id_event_type_key"
        ),
        sa.CheckConstraint(
            "event_type = 'payment.succeeded'", name="outbox_events_event_type_check"
        ),
    )


def downgrade() -> None:
    op.drop_table("outbox_events")
    op.drop_table("payments")
