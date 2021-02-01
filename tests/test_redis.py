import os

from typing import List, Optional
from asynctest import TestCase, skipUnless
from rom import Model, init, model
from rom.fields import LazyModelSet


class Bar(Model):
    field1: int
    field2: str
    field3: List[int]


class Foo(Model):
    f1: Optional[str]
    eager_bars: List[Bar]
    lazy_bars: LazyModelSet[Bar]


@skipUnless(os.environ.get("CI"), "Redis CI test only")
class RedisIntegrationTestCase(TestCase):

    async def setUp(self):
        redis_connection_url = "redis://localhost"
        await init(redis_connection_url)
        self.redis = model.redis
        self.bar = Bar(1, 123, "value", [1, 2, 3])

    async def tearDown(self):
        await Foo.delete_all()
        await Bar.delete_all()
        self.redis.close()
        await self.redis.wait_closed()
        model.redis = None

    async def test_save(self):
        await self.bar.save()

        field1 = await self.redis.hget("bar:1", "field1")
        field2 = await self.redis.hget("bar:1", "field2")
        field3 = await self.redis.hget("bar:1", "field3")
        field3_value = await self.redis.lrange("bar:1:field3", 0, -1)

        assert "123" == field1
        assert "value" == field2
        assert "bar:1:field3" == field3
        assert ["1", "2", "3"] == field3_value

    async def test_get(self):
        await self.bar.save()
        bar = await Bar.get(1)
        assert self.bar == bar

    async def test_update(self):
        await self.bar.save()
        bar = await Bar.get(1)
        bar.field2 = "updated"
        await bar.save()
        field2 = await self.redis.hget("bar:1", "field2")
        assert "updated" == field2

    async def test_delete(self):
        await self.bar.save()
        assert await self.redis.exists("bar:1")
        await self.bar.delete()
        assert not await self.redis.exists("bar:1")