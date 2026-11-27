# /// script
# requires-python = ">=3.12"
# dependencies = ["pika==1.3.2"]
# ///

import json
import os
from uuid import uuid4

import pika


def main() -> None:
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_DEFAULT_USER") or "lab",
        os.getenv("RABBITMQ_DEFAULT_PASS") or "lab",
    )
    queue_name = f"webhook.demo.ack.{uuid4().hex[:8]}"
    event = {"id": str(uuid4()), "type": "payment.succeeded"}
    body = json.dumps(event).encode()

    with pika.BlockingConnection(
        pika.ConnectionParameters(host="127.0.0.1", credentials=credentials, heartbeat=600)
    ) as connection:
        publisher = connection.channel()
        publisher.queue_declare(queue=queue_name, exclusive=True)
        publisher.confirm_delivery()
        publisher.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json", message_id=event["id"]
            ),
            mandatory=True,
        )
        print(f"Очередь: http://localhost:15672/#/queues/%2F/{queue_name}", flush=True)
        input("1. Публикация подтверждена. Ready = 1. Enter, чтобы получить сообщение. ")

        consumer = connection.channel()
        delivery, _, received = consumer.basic_get(queue_name, auto_ack=False)
        if delivery is None or received != body:
            raise RuntimeError("Очередь не вернула опубликованное сообщение")
        input("2. Сообщение получено. Unacked = 1. Enter, чтобы закрыть канал без ack. ")
        consumer.close()

        input("3. Канал закрыт. Ready снова = 1. Enter, чтобы получить повторно. ")
        consumer = connection.channel()
        delivery, _, received = consumer.basic_get(queue_name, auto_ack=False)
        if delivery is None or received != body or not delivery.redelivered:
            raise RuntimeError("Ожидалось повторное получение того же сообщения")
        print("4. Получено то же сообщение с redelivered=True.", flush=True)
        consumer.basic_ack(delivery.delivery_tag)
        input("5. Отправлен ack. Ready = 0, Unacked = 0. Enter для завершения. ")


if __name__ == "__main__":
    main()
