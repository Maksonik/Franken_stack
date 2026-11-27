"""Track Kafka publication and each group's processed events."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0004_kafka_events"
down_revision = "0003_webhook_receipts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("outbox_events", sa.Column("kafka_published_at", sa.DateTime(timezone=True)))
    op.create_index(
        "outbox_events_kafka_unpublished_idx",
        "outbox_events",
        ["occurred_at", "id"],
        postgresql_where=sa.text("kafka_published_at IS NULL"),
    )
    op.create_table(
        "kafka_consumer_events",
        sa.Column("consumer_group", sa.Text(), primary_key=True),
        sa.Column("event_id", sa.Uuid(), primary_key=True),
        sa.Column("partition", sa.Integer(), nullable=False),
        sa.Column("offset", sa.BigInteger(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("kafka_consumer_events_event_id_idx", "kafka_consumer_events", ["event_id"])


def downgrade() -> None:
    op.drop_table("kafka_consumer_events")
    op.drop_index("outbox_events_kafka_unpublished_idx", table_name="outbox_events")
    op.drop_column("outbox_events", "kafka_published_at")
