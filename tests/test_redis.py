import dataclasses
import os
import sys
from dataclasses import field
from typing import List, Optional, Set
from unittest import skipUnless

from typing_extensions import Annotated

from aio_rom import DataclassModel as Model
from aio_rom.attributes import RedisModelSet

if sys.version_info >= (3, 8):
    from unittest.async_case import IsolatedAsyncioTestCase as TestCase

    ASYNCTEST = False
else:
    from asynctest import TestCase

    ASYNCTEST = True

from aio_rom.fields import Metadata
from aio_rom.session import connection


@dataclasses.dataclass(unsafe_hash=True)
class Bar(Model):
    field1: Annotated[int, Metadata(eager=True)]
    field2: str
    field3: Annotated[List[int], Metadata(eager=True)] = field(
        default_factory=list, hash=False
    )
    field4: int = 3


@dataclasses.dataclass(unsafe_hash=True)
class Foo(Model):
    eager_bars: Annotated[List[Bar], Metadata(eager=True)] = field(
        default_factory=list, hash=False
    )
    lazy_bars: Annotated[Set[Bar], Metadata(cascade=True)] = field(
        default_factory=set, compare=False
    )
    f1: Optional[str] = None


@dataclasses.dataclass
class FooBar(Model):
    foos: Annotated[Set[Foo], Metadata(eager=True, cascade=True)]


@skipUnless(os.environ.get("CI"), "Redis CI test only")
class RedisIntegrationTestCase(TestCase):
    async def asyncSetUp(self) -> None:
        self.bar = Bar("1", 123, "value", [1, 2, 3])

    async def asyncTearDown(self) -> None:
        await Foo.delete_all()
        await Bar.delete_all()
        await FooBar.delete_all()

    if ASYNCTEST:
        tearDown = asyncTearDown  # type: ignore[assignment]
        setUp = asyncSetUp  # type: ignore[assignment]

    async def test_save(self) -> None:
        await self.bar.save()

        async with connection() as redis:
            field1 = await redis.hget("bar:1", "field1")
            field2 = await redis.hget("bar:1", "field2")
            field3 = await redis.hget("bar:1", "field3")
            field3_value = await redis.lrange("bar:1:field3", 0, -1)

        assert "123" == field1
        assert "value" == field2
        assert "bar:1:field3" == field3
        assert ["1", "2", "3"] == field3_value

    async def test_get(self) -> None:
        await self.bar.save()
        bar = await Bar.get("1")
        assert bar == self.bar

    async def test_get_with_references(self) -> None:
        await self.bar.save()
        foo = Foo("123", [self.bar], {self.bar})
        await foo.save()
        gotten_foo = await Foo.get("123")
        assert foo == gotten_foo
        assert isinstance(gotten_foo.lazy_bars, RedisModelSet)
        assert 1 == await gotten_foo.lazy_bars.total_count()
        await gotten_foo.lazy_bars.load()
        for bar in gotten_foo.lazy_bars:
            assert bar in foo.lazy_bars
        assert len(foo.lazy_bars) == len(gotten_foo.lazy_bars)

    async def _test_collection_references(self, test_cascade: bool = False) -> None:
        await self.bar.save()
        foo = Foo("123", [self.bar], {self.bar})
        if not test_cascade:
            await foo.save()
        foobar = FooBar("321", {foo})
        await foobar.save()

        gotten_foobar = await FooBar.get("321")
        assert foobar == gotten_foobar
        assert {foo} == gotten_foobar.foos
        for gotten_foo in gotten_foobar.foos:
            assert 1 == len(gotten_foo.eager_bars)
            assert isinstance(gotten_foo.lazy_bars, RedisModelSet)
            await gotten_foo.lazy_bars.load()
            for bar in gotten_foo.lazy_bars:
                assert bar in foo.lazy_bars

    async def test_collections(self) -> None:
        await self._test_collection_references()

    async def test_collection_cascades_references(self) -> None:
        await self._test_collection_references(test_cascade=True)

    async def test_update_collection_references(self) -> None:
        await self.bar.save()
        foo = Foo("123", [self.bar], {self.bar})
        foobar = FooBar("321", {foo})
        await foobar.save()
        refreshed = await foobar.refresh()
        foo2 = Foo("222", [], set())
        refreshed.foos.add(foo2)
        await refreshed.save()

        gotten_foobar = await FooBar.get("321")
        assert refreshed == gotten_foobar
        assert {foo, foo2} == gotten_foobar.foos

    async def test_update(self) -> None:
        await self.bar.save()
        await self.bar.update(field2="updated")
        async with connection() as redis:
            field2 = await redis.hget("bar:1", "field2")
        assert "updated" == field2
        bar = await Bar.get("1")
        assert "updated" == bar.field2

    async def test_update_reference(self) -> None:
        await self.bar.save()
        foo = Foo("123", [self.bar], {self.bar})
        await foo.save()

        bar2 = Bar("2", 123, "otherbar", [1, 2, 3, 4])
        await bar2.save()

        await foo.update(lazy_bars={bar2})
        async with connection() as redis:
            lazy_bars = await redis.smembers("foo:123:lazy_bars")
            assert {"2"} == lazy_bars

        await foo.update(eager_bars=[bar2])
        async with connection() as redis:
            eager_bars = await redis.lrange("foo:123:eager_bars", 0, -1)
            assert ["2"] == eager_bars

        gotten_foo = await Foo.get("123")
        assert foo == gotten_foo

    async def test_save_again_overrides_previous(self) -> None:
        await self.bar.save()
        bar = await Bar.get("1")
        bar.field2 = "updated"
        await bar.save()
        async with connection() as redis:
            field2 = await redis.hget("bar:1", "field2")
        assert "updated" == field2

    async def test_delete(self) -> None:
        await self.bar.save()
        async with connection() as redis:
            assert await redis.exists("bar:1")
            await self.bar.delete()
            assert not await redis.exists("bar:1")

    async def test_delete_all(self) -> None:
        await self.bar.save()
        async with connection() as redis:
            await Bar.delete_all()
            assert not await redis.keys("bar*")

    async def test_lazy_collection_cascade(self) -> None:
        foo = Foo("123", [self.bar], {self.bar})
        await foo.save()
        foo = await Foo.get("123")
        other_bar = Bar("2", 124, "value2", [])
        assert isinstance(foo.lazy_bars, RedisModelSet)
        await foo.lazy_bars.load()
        foo.lazy_bars.add(other_bar)
        await foo.save()
        gotten_foo = await Foo.get("123")
        assert foo == gotten_foo
        assert isinstance(gotten_foo.lazy_bars, RedisModelSet)
        await gotten_foo.lazy_bars.load()
        assert 2 == len(foo.lazy_bars) == len(gotten_foo.lazy_bars)
