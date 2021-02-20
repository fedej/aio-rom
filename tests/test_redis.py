import functools
import os
from typing import List, Optional, Set

from asynctest import TestCase, skipUnless
from rom import Model, field
from rom.session import REDIS
from rom.session import connection as redis_connection


def connection():
    def wrapper(func):
        @functools.wraps(func)
        async def wrapped(*args):
            async with redis_connection("redis://localhost"):
                return await func(*args)

        return wrapped

    return wrapper


class Bar(Model):
    field1: int
    field2: str
    field3: List[int] = field(eager=True, hash=False)


class Foo(Model):
    eager_bars: List[Bar] = field(eager=True)
    f1: Optional[str] = None
    lazy_bars: Set[Bar] = field(compare=False)


@skipUnless(os.environ.get("CI"), "Redis CI test only")
class RedisIntegrationTestCase(TestCase):
    async def setUp(self):
        self.bar = Bar(1, 123, "value", [1, 2, 3])

    @connection()
    async def tearDown(self):
        await Foo.delete_all()
        await Bar.delete_all()

    @connection()
    async def test_save(self):
        await self.bar.save()

        field1 = await REDIS.get().hget("bar:1", "field1")
        field2 = await REDIS.get().hget("bar:1", "field2")
        field3 = await REDIS.get().hget("bar:1", "field3")
        field3_value = await REDIS.get().lrange("bar:1:field3", 0, -1)

        assert "123" == field1
        assert "value" == field2
        assert "bar:1:field3" == field3
        assert ["1", "2", "3"] == field3_value

    @connection()
    async def test_get(self):
        await self.bar.save()
        bar = await Bar.get(1)
        assert self.bar == bar

    @connection()
    async def test_get_with_references(self):
        await self.bar.save()
        foo = Foo(123, [self.bar], None, set([self.bar]))
        await foo.save()
        gotten_foo = await Foo.get(123)
        assert foo == gotten_foo
        async for bar in gotten_foo.lazy_bars:
            bar in foo.lazy_bars
        assert len(foo.lazy_bars) == len(gotten_foo.lazy_bars)

    @connection()
    async def test_update(self):
        await self.bar.save()
        await self.bar.update(field2="updated")
        field2 = await REDIS.get().hget("bar:1", "field2")
        assert "updated" == field2
        bar = await Bar.get(1)
        assert "updated" == bar.field2

    @connection()
    async def test_update_reference(self):
        await self.bar.save()
        foo = Foo(123, [self.bar], None, set([self.bar]))
        await foo.save()

        bar2 = Bar(2, 123, "otherbar", [1, 2, 3, 4])
        await bar2.save()

        foo = await foo.update(lazy_bars=set([bar2]))
        lazy_bars = await REDIS.get().smembers("foo:123:lazy_bars")
        assert ["2"] == lazy_bars

        foo = await foo.update(eager_bars=[bar2])
        eager_bars = await REDIS.get().lrange("foo:123:eager_bars", 0, -1)
        assert ["2"] == eager_bars

        gotten_foo = await Foo.get(123)
        assert foo == gotten_foo

    @connection()
    async def test_save_again_overrides_previous(self):
        await self.bar.save()
        bar = await Bar.get(1)
        bar.field2 = "updated"
        await bar.save()
        field2 = await REDIS.get().hget("bar:1", "field2")
        assert "updated" == field2

    @connection()
    async def test_delete(self):
        await self.bar.save()
        assert await REDIS.get().exists("bar:1")
        await self.bar.delete()
        assert not await REDIS.get().exists("bar:1")
