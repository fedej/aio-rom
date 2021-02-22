from contextlib import asynccontextmanager
from contextvars import ContextVar

from aioredis.commands import ContextRedis, Redis, create_redis_pool
from aioredis.commands.transaction import MultiExec

REDIS: ContextVar[Redis] = ContextVar("redis", default=None)
CONNECTION: ContextVar[ContextRedis] = ContextVar("connection", default=None)
TRANSACTION: ContextVar[MultiExec] = ContextVar("transaction", default=None)

config = {}


def configure(address: str = None, *args, **kwargs):
    global config
    config["address"] = address
    config["args"] = args
    config["kwargs"] = kwargs


@asynccontextmanager
async def redis_pool(address: str = None, *args, **kwargs):
    redis = REDIS.get()
    if redis:
        yield redis
        return
    redis = await create_redis_pool(
        address if address else config.get("address", "redis://localhost"),
        encoding=kwargs.get("encoding", "utf-8"),
        *args if args else config.get("args", []),
        **kwargs if kwargs else config.get("kwargs", {})
    )
    t = REDIS.set(redis)
    try:
        yield redis
    finally:
        redis.close()
        await redis.wait_closed()
        REDIS.reset(t)


@asynccontextmanager
async def connection(address: str = None, *args, **kwargs):
    connection = CONNECTION.get()
    if connection:
        yield connection
        return

    async with redis_pool(address, *args, **kwargs) as redis:
        with await redis as conn:
            t = CONNECTION.set(conn)
            yield conn
            CONNECTION.reset(t)


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
