from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import os
import typing
from dataclasses import field

import pytest
from pytest_benchmark.fixture import BenchmarkFixture
from redis.asyncio.client import Redis
from typing_extensions import Annotated

from aio_rom import DataclassModel as Model
from aio_rom import Model as PlainModel
from aio_rom.collections import RedisList
from aio_rom.fields import Metadata
from aio_rom.session import CONNECTION

if not os.environ.get("CI") or not os.environ.get("BENCHMARK"):
    pytest.skip("Redis benchmark CI test only", allow_module_level=True)


class Bar(PlainModel):
    __slots__ = "field1", "field2", "field3"

    field1: int
    field2: str
    field3: RedisList[int]

    def __init__(self, id: str, field1: int, field2: str, field3: RedisList[int]):
        self.id = id
        self.field1 = field1
        self.field2 = field2
        self.field3 = field3


@dataclasses.dataclass
class Foo(Model):
    bars: Annotated[RedisList[Bar], Metadata(eager=True, cascade=True)] = field(
        default_factory=RedisList[Bar]
    )


ITEMS = 100


def run_benchmark(
    benchmark: BenchmarkFixture,
    coro: typing.Callable[..., typing.Awaitable[None]],
    event_loop: asyncio.AbstractEventLoop,
) -> None:
    def run() -> None:
        event_loop.run_until_complete(coro())

    with connection(event_loop):
        benchmark(run)


@pytest.fixture
async def item() -> typing.AsyncIterator[Bar]:
    item = Bar("1", 123, "value", RedisList[int]([1, 2, 3]))
    await item.save()
    yield item
    await Bar.delete_all()
    await Foo.delete_all()


@pytest.fixture
async def item_list() -> typing.AsyncIterator[None]:
    foo = Foo("1")
    for i in range(ITEMS):
        foo.bars.append(Bar(str(i), 123, "value", RedisList[int]([1, 2, 3])))
    await foo.save()
    yield
    await Bar.delete_all()
    await Foo.delete_all()
    await RedisList.delete_all()


@contextlib.contextmanager
def connection(
    event_loop: asyncio.AbstractEventLoop,
) -> typing.Iterator[None]:
    client: Redis[str] = event_loop.run_until_complete(
        Redis.from_url(
            "redis://localhost",
            encoding="utf-8",
            decode_responses=True,
        ).__aenter__()
    )
    t = CONNECTION.set(event_loop.run_until_complete(client.client().__aenter__()))
    yield
    CONNECTION.reset(t)
    event_loop.run_until_complete(client.close(close_connection_pool=True))


@pytest.mark.benchmark
def test_save(
    event_loop: asyncio.AbstractEventLoop, benchmark: BenchmarkFixture
) -> None:
    item = Bar("1", 123, "value", RedisList[int]([1, 2, 3]))

    async def do() -> None:
        for _ in range(ITEMS):
            await item.save()

    run_benchmark(benchmark, do, event_loop)


@pytest.mark.benchmark
def test_get(
    item: Bar, event_loop: asyncio.AbstractEventLoop, benchmark: BenchmarkFixture
) -> None:
    async def do() -> None:
        for _ in range(ITEMS):
            await Bar.get("1")

    run_benchmark(benchmark, do, event_loop)


@pytest.mark.benchmark
def test_get_eager_list(
    item_list: typing.Any,
    event_loop: asyncio.AbstractEventLoop,
    benchmark: BenchmarkFixture,
) -> None:
    async def do() -> None:
        foo = await Foo.get("1")
        assert len(foo.bars) == ITEMS

    run_benchmark(benchmark, do, event_loop)


@pytest.mark.benchmark
def test_get_all(
    item_list: typing.Any,
    benchmark: BenchmarkFixture,
    event_loop: asyncio.AbstractEventLoop,
) -> None:
    async def do() -> None:
        result = await Bar.all()
        assert len(result) == ITEMS

    run_benchmark(benchmark, do, event_loop)


@pytest.mark.benchmark
def test_scan_all(
    item_list: typing.Any,
    benchmark: BenchmarkFixture,
    event_loop: asyncio.AbstractEventLoop,
) -> None:
    async def do() -> None:
        result = [item async for item in Bar.scan()]
        assert len(result) == ITEMS

    run_benchmark(benchmark, do, event_loop)


@pytest.mark.benchmark
def test_cascade_save(
    benchmark: BenchmarkFixture, event_loop: asyncio.AbstractEventLoop
) -> None:
    async def do() -> None:
        foo = Foo("1")
        for i in range(ITEMS):
            foo.bars.append(Bar(str(i), 123, "value", RedisList[int]([1, 2, 3])))
        await foo.save()

    run_benchmark(benchmark, do, event_loop)
