from __future__ import annotations

import asyncio
import dataclasses
import os
import typing
from dataclasses import field

import pytest
from pytest_benchmark.fixture import BenchmarkFixture  # type: ignore[import]
from typing_extensions import Annotated

from aio_rom import DataclassModel as Model
from aio_rom.collections import RedisList
from aio_rom.fields import Metadata
from aio_rom.session import connection

if not os.environ.get("CI"):
    pytest.skip("Redis benchmark CI test only", allow_module_level=True)


@dataclasses.dataclass
class Bar(Model):
    field1: int
    field2: str
    field3: RedisList[int]


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


@pytest.mark.benchmark
def test_save(
    item: Bar, event_loop: asyncio.AbstractEventLoop, benchmark: BenchmarkFixture
) -> None:
    async def do() -> None:
        async with connection():
            for _ in range(ITEMS):
                await item.save()

    run_benchmark(benchmark, do, event_loop)


@pytest.mark.benchmark
def test_get(
    item: Bar, event_loop: asyncio.AbstractEventLoop, benchmark: BenchmarkFixture
) -> None:
    async def do() -> None:
        async with connection():
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
        async with connection():
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
