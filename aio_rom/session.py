from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, AsyncIterator

from redis.asyncio import Redis

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline

    from aio_rom.types import Key

REDIS_CLIENT: ContextVar[Redis[str] | None] = ContextVar("redis_client", default=None)
CONNECTION: ContextVar[Redis[str] | None] = ContextVar("connection", default=None)
TRANSACTION: ContextVar[Pipeline[str] | None] = ContextVar("transaction", default=None)

config: dict[str, Any] = {}


def configure(address: str | None = None, *args: Any, **kwargs: Any) -> None:
    global config
    config["address"] = address
    config["args"] = args
    config["kwargs"] = kwargs


@asynccontextmanager
async def redis_client(
    address: str | None = None, **kwargs: Any
) -> AsyncIterator[Redis[str]]:
    client: Redis[str] | None = REDIS_CLIENT.get()
    if client:
        yield client
        return
    client = Redis.from_url(
        address if address else config.get("address", "redis://localhost"),
        encoding="utf-8",
        decode_responses=True,
        **kwargs if kwargs else config.get("kwargs", {}),
    )
    if not client:
        raise ValueError("Failed to initialize Redis client")
    t = REDIS_CLIENT.set(client)
    try:
        yield client
    finally:
        await client.close()
        REDIS_CLIENT.reset(t)


@asynccontextmanager
async def connection(
    address: str | None = None, **kwargs: Any
) -> AsyncIterator[Redis[str]]:
    connection = CONNECTION.get()
    if connection:
        yield connection
        return

    async with redis_client(address, **kwargs) as pool:
        redis = await pool.client()
        t = CONNECTION.set(redis)
        try:
            yield redis
        finally:
            CONNECTION.reset(t)
            await redis.close(close_connection_pool=True)


@asynccontextmanager
async def transaction(*watches: Key) -> AsyncIterator[Pipeline[str]]:
    transaction = TRANSACTION.get()
    if transaction:
        yield transaction
        return

    async with connection() as conn, conn.pipeline(transaction=True) as tr:
        if watches:
            await tr.watch(*watches)
        tr.multi()
        t = TRANSACTION.set(tr)
        try:
            yield tr
        finally:
            try:
                await tr.execute()
            finally:
                TRANSACTION.reset(t)
