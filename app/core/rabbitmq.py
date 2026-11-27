import pika

from app.core.settings import Settings

DELIVERY_QUEUE = "webhook.delivery"


def connection_parameters(settings: Settings) -> pika.ConnectionParameters:
    return pika.ConnectionParameters(
        host=settings.rabbitmq_host,
        credentials=pika.PlainCredentials(
            settings.rabbitmq_default_user,
            settings.rabbitmq_default_pass.get_secret_value(),
        ),
        heartbeat=15,
        socket_timeout=3,
        blocked_connection_timeout=5,
    )
