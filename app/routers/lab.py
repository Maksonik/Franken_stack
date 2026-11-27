from pathlib import Path
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Response
from fastapi.responses import FileResponse, HTMLResponse

from app.routers.dependencies import get_payment_service
from app.schemas import PaymentSucceededEvent
from app.services.payments import PaymentService

LAB_FILES = Path(__file__).resolve().parents[1] / "lab"
router = APIRouter(prefix="/lab", include_in_schema=False)


@router.get("", response_class=HTMLResponse)
def lab_page() -> FileResponse:
    return FileResponse(LAB_FILES / "index.html", media_type="text/html")


@router.get("/payments/{payment_id}/outbox", response_model=PaymentSucceededEvent | None)
def payment_outbox(
    payment_id: UUID,
    response: Response,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentSucceededEvent | None:
    response.headers["Cache-Control"] = "no-store"
    result = service.get_outbox_event(payment_id)
    if result is None:
        return None

    if result.published_at is not None:
        response.headers["X-Outbox-Published-At"] = result.published_at.isoformat()
    if result.received_at is not None:
        response.headers["X-Webhook-Received-At"] = result.received_at.isoformat()
    if result.kafka_published_at is not None:
        response.headers["X-Kafka-Published-At"] = result.kafka_published_at.isoformat()
    if result.kafka_groups:
        response.headers["X-Kafka-Consumer-Groups"] = ", ".join(result.kafka_groups)
    return result.event
