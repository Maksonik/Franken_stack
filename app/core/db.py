from collections.abc import Iterator
from functools import cache

from sqlalchemy import URL, Engine, create_engine, make_url
from sqlalchemy.orm import Session

from app.core.settings import Settings


def database_url() -> URL:
    settings = Settings()
    if settings.database_url is not None:
        url = make_url(settings.database_url.get_secret_value())
        if url.drivername in ("postgres", "postgresql"):
            return url.set(drivername="postgresql+psycopg")
        return url

    if settings.postgres_password is None:
        raise ValueError("Set POSTGRES_PASSWORD or DATABASE_URL in the environment or .env")
    return URL.create(
        "postgresql+psycopg",
        username=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
    )


@cache
def get_engine() -> Engine:
    return create_engine(database_url(), pool_pre_ping=True, isolation_level="READ COMMITTED")


def get_session() -> Iterator[Session]:
    with Session(get_engine()) as session:
        yield session
