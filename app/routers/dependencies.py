from typing import Annotated

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.db import get_session
from app.services.payments import PaymentService


def get_payment_service(session: Annotated[Session, Depends(get_session)]) -> PaymentService:
    return PaymentService(session)
