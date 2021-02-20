from contextlib import asynccontextmanager
from contextvars import ContextVar

from aioredis.commands import Redis, create_redis_pool
from aioredis.commands.transaction import MultiExec

REDIS: ContextVar[Redis] = ContextVar("redis", default=None)
TRANSACTION: ContextVar[MultiExec] = ContextVar("transaction", default=None)


@asynccontextmanager
async def connection(address: str = None, *args, **kwargs):
    redis = REDIS.get()
    if redis:
        with await redis as conn:
            yield conn
            return

    redis = await create_redis_pool(address, encoding="utf-8", *args, **kwargs)
    t = REDIS.set(redis)
    try:
        with await redis as conn:
            yield conn
    finally:
        redis.close()
        await redis.wait_closed()
        REDIS.reset(t)


@asynccontextmanager
async def transaction():
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
            await transaction.execute()
            TRANSACTION.reset(t)
