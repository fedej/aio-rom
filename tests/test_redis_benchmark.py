import asyncio
import os
from dataclasses import field
from typing import Any, Awaitable, Callable, List
from unittest import TestCase, skipUnless

import pytest
from pytest_benchmark.fixture import BenchmarkFixture  # type: ignore

from rom import Model
from rom.fields import Metadata
from rom.session import connection


class Bar(Model):
    field1: int
    field2: str
    field3: List[int]


class Foo(Model):
    bars: List[Bar] = field(
        metadata=Metadata(eager=True, cascade=True), default_factory=list
    )


@skipUnless(os.environ.get("CI"), "Redis benchmark CI test only")
class Benchmark(TestCase):
    items = 100

    @pytest.fixture(autouse=True)
    def setupBenchmark(self, benchmark: BenchmarkFixture) -> None:
        self.benchmark = benchmark

    def run_coro(self, coro_factory: Callable[[], Awaitable[Any]]) -> Any:
        return asyncio.run(coro_factory())

    async def delete_all(self) -> None:
        await Bar.delete_all()
        await Foo.delete_all()

    def setUp(self) -> None:
        self.bar = Bar(1, 123, "value", [1, 2, 3])
        asyncio.run(self.bar.save())

    def tearDown(self) -> None:
        asyncio.run(self.delete_all())

    def test_save(self) -> None:
        async def save() -> None:
            async with connection():
                for _ in range(self.items):
                    await self.bar.save()

        self.benchmark(self.run_coro, save)

    def test_get(self) -> None:
        async def get() -> None:
            async with connection():
                for _ in range(self.items):
                    await Bar.get(1)

        self.benchmark(self.run_coro, get)

    def test_get_eager_list(self) -> None:
        foo = Foo(1)
        for i in range(self.items):
            foo.bars.append(Bar(i, 123, "value", [1, 2, 3]))
        asyncio.run(foo.save())

        async def get() -> Foo:
            async with connection():
                return await Foo.get(1)

        result = self.benchmark(self.run_coro, get)
        assert self.items == len(result.bars)

    def test_get_all(self) -> None:
        for i in range(self.items):
            asyncio.run(Bar(i, 123, "value", [1, 2, 3]).save())

        result = self.benchmark(self.run_coro, Bar.all)
        assert self.items == len(result)

    def test_scan_all(self) -> None:
        for i in range(self.items):
            asyncio.run(Bar(i, 123, "value", [1, 2, 3]).save())

        async def scan() -> List[Bar]:
            result = []
            async for item in Bar.scan():
                result.append(item)
            return result

        result = self.benchmark(self.run_coro, scan)
        assert self.items == len(result)

    def test_cascade_save(self) -> None:
        foo = Foo(1)
        for i in range(self.items):
            foo.bars.append(Bar(i, 123, "value", [1, 2, 3]))
        self.benchmark(self.run_coro, foo.save)
