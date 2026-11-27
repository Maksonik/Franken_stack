from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class PaymentCreate(BaseModel):
    amount_minor: Annotated[int, Field(strict=True, gt=0, le=2**63 - 1)]
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]


class Payment(PaymentCreate):
    id: UUID
    status: Literal["pending", "succeeded"]


class PaymentSucceededData(Payment):
    status: Literal["succeeded"]


class PaymentSucceededEvent(BaseModel):
    id: UUID
    type: Literal["payment.succeeded"]
    occurred_at: datetime
    data: PaymentSucceededData
