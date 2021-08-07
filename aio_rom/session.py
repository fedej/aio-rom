from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Dict, Optional

from aioredis import Redis, ConnectionPool
from aioredis.client import Pipeline

from aio_rom.types import Key

POOL: ContextVar[Optional[ConnectionPool]] = ContextVar("pool", default=None)
CONNECTION: ContextVar[Optional[Redis]] = ContextVar("connection", default=None)
TRANSACTION: ContextVar[Optional[Pipeline]] = ContextVar("transaction", default=None)

config: Dict[str, Any] = {}


def configure(address: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
    global config
    config["address"] = address
    config["args"] = args
    config["kwargs"] = kwargs


@asynccontextmanager
async def redis_pool(
    address: Optional[str] = None, **kwargs: Any
) -> AsyncIterator[ConnectionPool]:
    pool = POOL.get()
    if pool:
        yield pool
        return
    pool = ConnectionPool.from_url(
        address if address else config.get("address", "redis://localhost"),
        encoding=kwargs.get("encoding", "utf-8"),
        **kwargs if kwargs else config.get("kwargs", {}),
    )
    t = POOL.set(pool)
    try:
        yield pool
    finally:
        await pool.disconnect()
        POOL.reset(t)


@asynccontextmanager
async def connection(
    address: Optional[str] = None, **kwargs: Any
) -> AsyncIterator[Redis]:
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
            await tr.watch(*watches)
            tr.multi()  # type: ignore[no-untyped-call]
            t = TRANSACTION.set(tr)
            try:
                yield tr
            finally:
                try:
                    await tr.execute()
                finally:
                    TRANSACTION.reset(t)
