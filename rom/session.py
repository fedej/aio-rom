from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Dict, Optional

from aioredis.commands import ContextRedis, Redis, create_redis_pool  # noqa: E501 type: ignore[import]
from aioredis.commands.transaction import MultiExec  # type: ignore[import]

REDIS: ContextVar[Optional[Redis]] = ContextVar("redis", default=None)
CONNECTION: ContextVar[Optional[ContextRedis]] = ContextVar("connection", default=None)
TRANSACTION: ContextVar[Optional[MultiExec]] = ContextVar("transaction", default=None)

config: Dict[str, Any] = {}


def configure(address: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
    global config
    config["address"] = address
    config["args"] = args
    config["kwargs"] = kwargs


@asynccontextmanager
async def redis_pool(
    address: Optional[str] = None, *args: Any, **kwargs: Any
) -> AsyncIterator[Redis]:
    redis = REDIS.get()
    if redis:
        yield redis
        return
    redis = await create_redis_pool(
        address if address else config.get("address", "redis://localhost"),
        encoding=kwargs.get("encoding", "utf-8"),
        *args if args else config.get("args", []),
        **kwargs if kwargs else config.get("kwargs", {}),
    )
    t = REDIS.set(redis)
    try:
        yield redis
    finally:
        redis.close()
        await redis.wait_closed()
        REDIS.reset(t)


@asynccontextmanager
async def connection(
    address: Optional[str] = None, *args: Any, **kwargs: Any
) -> AsyncIterator[ContextRedis]:
    connection = CONNECTION.get()
    if connection:
        yield connection
        return

    async with redis_pool(address, *args, **kwargs) as redis:
        with await redis as conn:
            t = CONNECTION.set(conn)
            try:
                yield conn
            finally:
                CONNECTION.reset(t)


@asynccontextmanager
async def transaction() -> AsyncIterator[MultiExec]:
    transaction = TRANSACTION.get()
    if transaction:
        yield transaction
        return

    async with connection() as conn:
        transaction = conn.multi_exec()
        t = TRANSACTION.set(transaction)
        try:
            yield transaction
        finally:
            try:
                await transaction.execute()
            finally:
                TRANSACTION.reset(t)
