"""Track confirmed RabbitMQ publications."""

import sqlalchemy as sa
from alembic import op

revision = "0002_outbox_publication"
down_revision = "0001_create_payments"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("outbox_events", sa.Column("published_at", sa.DateTime(timezone=True)))
    op.create_index(
        "outbox_events_unpublished_idx",
        "outbox_events",
        ["occurred_at", "id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("outbox_events_unpublished_idx", table_name="outbox_events")
    op.drop_column("outbox_events", "published_at")
