import logging
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from app.core.db import get_session
from app.schemas import PaymentSucceededEvent
from app.services.partner import accept_webhook

router = APIRouter()
logger = logging.getLogger("uvicorn.error")


@router.post(
    "/webhooks/payments",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={503: {"description": "Simulated partner outage"}},
)
def receive_webhook(
    event: PaymentSucceededEvent,
    session: Annotated[Session, Depends(get_session)],
    mode: Literal["success", "unavailable"] = "success",
) -> Response:
    if mode == "unavailable":
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Simulated partner outage")

    if accept_webhook(session, event):
        logger.info("Received %s event_id=%s payment_id=%s", event.type, event.id, event.data.id)
    else:
        logger.info("Ignored duplicate event_id=%s payment_id=%s", event.id, event.data.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
