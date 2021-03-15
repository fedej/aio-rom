import os
from typing import List, Optional, Set

from asynctest import TestCase, skipUnless
from rom import Model, field
from rom.session import redis_pool


class Bar(Model):
    field1: int
    field2: str
    field3: List[int] = field(eager=True, hash=False)


class Foo(Model):
    eager_bars: List[Bar] = field(eager=True, hash=False)
    f1: Optional[str] = None
    lazy_bars: Set[Bar] = field(compare=False, cascade=True)


class FooBar(Model):
    foos: Set[Foo] = field(eager=True, cascade=True)


@skipUnless(os.environ.get("CI"), "Redis CI test only")
class RedisIntegrationTestCase(TestCase):
    async def setUp(self):
        self.bar = Bar(1, 123, "value", [1, 2, 3])

    async def tearDown(self):
        await Foo.delete_all()
        await Bar.delete_all()
        await FooBar.delete_all()

    async def test_save(self):
        await self.bar.save()

        async with redis_pool() as redis:
            field1 = await redis.hget("bar:1", "field1")
            field2 = await redis.hget("bar:1", "field2")
            field3 = await redis.hget("bar:1", "field3")
            field3_value = await redis.lrange("bar:1:field3", 0, -1)

        assert "123" == field1
        assert "value" == field2
        assert "bar:1:field3" == field3
        assert ["1", "2", "3"] == field3_value

    async def test_get(self):
        await self.bar.save()
        bar = await Bar.get(1)
        assert self.bar == bar

    async def test_get_with_references(self):
        await self.bar.save()
        foo = Foo(123, [self.bar], None, set([self.bar]))
        await foo.save()
        gotten_foo = await Foo.get(123)
        assert foo == gotten_foo
        await gotten_foo.lazy_bars.load()
        for bar in gotten_foo.lazy_bars:
            assert bar in foo.lazy_bars
        assert len(foo.lazy_bars) == len(gotten_foo.lazy_bars)

    async def _test_collection_references(self, test_cascade=False):
        await self.bar.save()
        foo = Foo(123, [self.bar], None, set([self.bar]))
        if not test_cascade:
            await foo.save()
        foobar = FooBar(321, set([foo]))
        await foobar.save()

        gotten_foobar = await FooBar.get(321)
        assert foobar == gotten_foobar
        assert set([foo]) == gotten_foobar.foos
        for gotten_foo in gotten_foobar.foos:
            assert 1 == len(gotten_foo.eager_bars)
            await gotten_foo.lazy_bars.load()
            for bar in gotten_foo.lazy_bars:
                assert bar in foo.lazy_bars

    async def test_collections(self):
        await self._test_collection_references()

    async def test_collection_cascades_references(self):
        await self._test_collection_references(test_cascade=True)

    async def test_update_collection_references(self):
        await self.bar.save()
        foo = Foo(123, [self.bar], None, set([self.bar]))
        foobar = FooBar(321, set([foo]))
        await foobar.save()
        refreshed = await foobar.refresh()
        foo2 = Foo(222, [], None, set([]))
        refreshed.foos.add(foo2)
        await refreshed.save()

        gotten_foobar = await FooBar.get(321)
        assert refreshed == gotten_foobar
        assert set([foo, foo2]) == gotten_foobar.foos

    async def test_update(self):
        await self.bar.save()
        await self.bar.update(field2="updated")
        async with redis_pool() as redis:
            field2 = await redis.hget("bar:1", "field2")
        assert "updated" == field2
        bar = await Bar.get(1)
        assert "updated" == bar.field2

    async def test_update_reference(self):
        await self.bar.save()
        foo = Foo(123, [self.bar], None, set([self.bar]))
        await foo.save()

        bar2 = Bar(2, 123, "otherbar", [1, 2, 3, 4])
        await bar2.save()

        foo = await foo.update(lazy_bars=set([bar2]))
        async with redis_pool() as redis:
            lazy_bars = await redis.smembers("foo:123:lazy_bars")
            assert ["2"] == lazy_bars

        foo = await foo.update(eager_bars=[bar2])
        async with redis_pool() as redis:
            eager_bars = await redis.lrange("foo:123:eager_bars", 0, -1)
            assert ["2"] == eager_bars

        gotten_foo = await Foo.get(123)
        assert foo == gotten_foo

    async def test_save_again_overrides_previous(self):
        await self.bar.save()
        bar = await Bar.get(1)
        bar.field2 = "updated"
        await bar.save()
        async with redis_pool() as redis:
            field2 = await redis.hget("bar:1", "field2")
        assert "updated" == field2

    async def test_delete(self):
        await self.bar.save()
        async with redis_pool() as redis:
            assert await redis.exists("bar:1")
            await self.bar.delete()
            assert not await redis.exists("bar:1")

    async def test_delete_all(self):
        await self.bar.save()
        async with redis_pool() as redis:
            await Bar.delete_all()
            assert not await redis.keys("bar*")

    async def test_lazy_collection_cascade(self):
        foo = Foo(123, [self.bar], None, set([self.bar]))
        await foo.save()
        foo = await Foo.get(123)
        other_bar = Bar(2, 124, "value2", [])
        foo.lazy_bars.add(other_bar)
        await foo.save()
        gotten_foo = await Foo.get(123)
        assert foo == gotten_foo
        await gotten_foo.lazy_bars.load()
        await foo.lazy_bars.load()
        assert 2 == len(foo.lazy_bars) == len(gotten_foo.lazy_bars)
