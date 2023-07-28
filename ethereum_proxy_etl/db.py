from datetime import datetime

import snowflake.connector
from sqlalchemy import TIMESTAMP, URL
from sqlalchemy.ext.asyncio import AsyncAttrs, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from .env import (POSTGRES_DATABASE, POSTGRES_HOST, POSTGRES_PASSWORD,
                  POSTGRES_PORT, POSTGRES_USERNAME, SNOWFLAKE_ACCOUNT,
                  SNOWFLAKE_PASSWORD, SNOWFLAKE_USER)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class ProxyContracts(Base):
    __tablename__ = "proxy_contracts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    proxy_address: Mapped[str]
    proxy_type: Mapped[str]
    implementation_address: Mapped[str]
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP)


def async_engine():
    pg_url = URL(
        drivername='postgresql+asyncpg',
        username=POSTGRES_USERNAME,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DATABASE,
        query={}
    )
    return create_async_engine(pg_url, echo=False)


def snowflake_connection():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
    )
