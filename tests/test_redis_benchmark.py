import asyncio
import os
from typing import List
from unittest import TestCase, skipUnless

import pytest
from rom import Model, field
from rom.session import connection


class Bar(Model):
    field1: int
    field2: str
    field3: List[int]


class Foo(Model):
    bars: List[Bar] = field(eager=True, cascade=True, default_factory=list)


@skipUnless(os.environ.get("CI"), "Redis benchmark CI test only")
class Benchmark(TestCase):
    items = 100

    @pytest.fixture(autouse=True)
    def setupBenchmark(self, benchmark):
        self.benchmark = benchmark

    def run_coro(self, coro_factory):
        return asyncio.run(coro_factory())

    async def delete_all(self):
        await Bar.delete_all()
        await Foo.delete_all()

    def setUp(self):
        self.bar = Bar(1, 123, "value", [1, 2, 3])
        asyncio.run(self.bar.save())

    def tearDown(self):
        asyncio.run(self.delete_all())

    def test_save(self):
        async def save():
            async with connection():
                for _ in range(self.items):
                    await self.bar.save()

        self.benchmark(self.run_coro, save)

    def test_get(self):
        async def get():
            async with connection():
                for _ in range(self.items):
                    await Bar.get(1)

        self.benchmark(self.run_coro, get)

    def test_get_eager_list(self):
        foo = Foo(1)
        for i in range(self.items):
            foo.bars.append(Bar(i, 123, "value", [1, 2, 3]))
        asyncio.run(foo.save())

        async def get():
            async with connection():
                return await Foo.get(1)

        result = self.benchmark(self.run_coro, get)
        assert self.items == len(result.bars)

    def test_get_all(self):
        for i in range(self.items):
            asyncio.run(Bar(i, 123, "value", [1, 2, 3]).save())

        result = self.benchmark(self.run_coro, Bar.all)
        assert self.items == len(result)

    def test_scan_all(self):
        for i in range(self.items):
            asyncio.run(Bar(i, 123, "value", [1, 2, 3]).save())

        async def scan():
            result = []
            async for item in Bar.scan():
                result.append(item)
            return result

        result = self.benchmark(self.run_coro, scan)
        assert self.items == len(result)


    def test_cascade_save(self):
        foo = Foo(1)
        for i in range(self.items):
            foo.bars.append(Bar(i, 123, "value", [1, 2, 3]))
        self.benchmark(self.run_coro, foo.save)
