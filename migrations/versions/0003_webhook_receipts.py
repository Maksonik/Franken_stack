"""Persist processed partner webhook IDs."""

import sqlalchemy as sa
from alembic import op

revision = "0003_webhook_receipts"
down_revision = "0002_outbox_publication"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "webhook_receipts",
        sa.Column("event_id", sa.Uuid(), primary_key=True),
        sa.Column("payment_id", sa.Uuid(), nullable=False),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )


def downgrade() -> None:
    op.drop_table("webhook_receipts")
