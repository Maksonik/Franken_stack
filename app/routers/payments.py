from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.orm import Session

from app.core.db import get_session
from app.schemas import Payment, PaymentCreate
from app.services.payments import IdempotencyConflictError, PaymentNotFoundError, PaymentService

router = APIRouter(prefix="/payments")


def get_payment_service(session: Annotated[Session, Depends(get_session)]) -> PaymentService:
    return PaymentService(session)


@router.post(
    "",
    response_model=Payment,
    status_code=status.HTTP_201_CREATED,
    responses={200: {"model": Payment}, 409: {"description": "Key already used"}},
)
def create_payment(
    body: PaymentCreate,
    response: Response,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=128, pattern=r"^\S+$")],
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> Payment:
    try:
        payment, created = service.create(body, idempotency_key)
    except IdempotencyConflictError as error:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Idempotency-Key already used with different data"
        ) from error

    if not created:
        response.status_code = status.HTTP_200_OK
    return payment


@router.post(
    "/{payment_id}/succeed",
    response_model=Payment,
    responses={404: {"description": "Payment not found"}},
)
def succeed_payment(
    payment_id: UUID,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> Payment:
    try:
        return service.succeed(payment_id)
    except PaymentNotFoundError as error:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found") from error
