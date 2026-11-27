import logging

from fastapi import APIRouter, Response, status

from app.schemas import PaymentSucceededEvent

router = APIRouter()
logger = logging.getLogger("uvicorn.error")


@router.post("/webhooks/payments", status_code=status.HTTP_204_NO_CONTENT)
def receive_webhook(event: PaymentSucceededEvent) -> Response:
    logger.info("Received %s event_id=%s payment_id=%s", event.type, event.id, event.data.id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)
