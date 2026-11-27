from pathlib import Path

from pydantic import Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[2] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_ignore_empty=True,
    )

    postgres_host: str = "127.0.0.1"
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_db: str = "payments"
    postgres_user: str = "payments"
    postgres_password: SecretStr | None = None
    database_url: SecretStr | None = None

    rabbitmq_host: str = "127.0.0.1"
    rabbitmq_default_user: str = "lab"
    rabbitmq_default_pass: SecretStr = SecretStr("lab")
    partner_webhook_url: HttpUrl = HttpUrl("http://127.0.0.1:8001/webhooks/payments")

    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_consumer_group: str = Field(default="audit", pattern=r"^[A-Za-z0-9._-]+$", max_length=128)
