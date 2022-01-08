from __future__ import annotations

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator

from aioredis import ConnectionPool, Redis
from aioredis.client import Pipeline

from aio_rom.types import Key

POOL: ContextVar[ConnectionPool | None] = ContextVar("pool", default=None)
CONNECTION: ContextVar[Redis | None] = ContextVar("connection", default=None)
TRANSACTION: ContextVar[Pipeline | None] = ContextVar("transaction", default=None)

config: dict[str, Any] = {}


def configure(address: str | None = None, *args: Any, **kwargs: Any) -> None:
    global config
    config["address"] = address
    config["args"] = args
    config["kwargs"] = kwargs


@asynccontextmanager
async def redis_pool(
    address: str | None = None, **kwargs: Any
) -> AsyncIterator[ConnectionPool]:
    pool = POOL.get()
    if pool:
        yield pool
        return
    pool = ConnectionPool.from_url(
        address if address else config.get("address", "redis://localhost"),
        encoding="utf-8",
        decode_responses=True,
        **kwargs if kwargs else config.get("kwargs", {}),
    )
    t = POOL.set(pool)
    try:
        yield pool
    finally:
        await pool.disconnect()
        POOL.reset(t)


@asynccontextmanager
async def connection(address: str | None = None, **kwargs: Any) -> AsyncIterator[Redis]:
    connection = CONNECTION.get()
    if connection:
        yield connection
        return

    async with redis_pool(address, **kwargs) as pool:
        async with Redis(single_connection_client=True, connection_pool=pool) as redis:
            t = CONNECTION.set(redis)
            try:
                yield redis
            finally:
                CONNECTION.reset(t)


@asynccontextmanager
async def transaction(*watches: Key) -> AsyncIterator[Pipeline]:
    transaction = TRANSACTION.get()
    if transaction:
        yield transaction
        return

    async with connection() as conn:
        async with conn.pipeline() as tr:
            keys = [str(key) if isinstance(key, int) else key for key in watches]
            if keys:
                await tr.watch(*keys)
            tr.multi()  # type: ignore[no-untyped-call]
            t = TRANSACTION.set(tr)
            try:
                yield tr
            finally:
                try:
                    await tr.execute()
                finally:
                    TRANSACTION.reset(t)
