import dataclasses
import os
from unittest import skipUnless

from aio_rom import DataclassModel as Model
from aio_rom.attributes import (
    ModelCollection,
    RedisList,
    RedisModelList,
    RedisModelSet,
    RedisSet,
)
from aio_rom.exception import ModelNotFoundException
from aio_rom.session import connection

from . import RedisTestCase


@dataclasses.dataclass(unsafe_hash=True)
class Foo(Model):
    field1: int


@skipUnless(os.environ.get("CI"), "Redis CI test only")
class RedisCollectionIntegrationTestCase(RedisTestCase):
    async def asyncSetUp(self) -> None:
        self.foo = Foo("1", 123)

    async def asyncTearDown(self) -> None:
        await Foo.delete_all()

    async def _test_save_model_collection(
        self, redis_collection: ModelCollection[Foo], cascade: bool = False
    ) -> None:
        await redis_collection.save()
        if cascade:
            for item in redis_collection:
                assert await Foo.get(item.id) == item
            fresh = await redis_collection.get(
                redis_collection.id, eager=True, item_class=Foo
            )
            assert redis_collection == fresh
        else:
            with self.assertRaises(ModelNotFoundException):
                await redis_collection.get(
                    redis_collection.id, eager=True, item_class=Foo
                )

    async def test_save_model_list(self) -> None:
        redis_list = RedisModelList("my_list", [self.foo], Foo)
        await self._test_save_model_collection(redis_list)

    async def test_save_model_list_cascade(self) -> None:
        redis_list = RedisModelList("my_list", [self.foo], Foo, cascade=True)
        await self._test_save_model_collection(redis_list, cascade=True)

    async def test_save_model_set(self) -> None:
        redis_set = RedisModelSet("my_set", {self.foo}, Foo)
        await self._test_save_model_collection(redis_set)

    async def test_save_model_set_cascade(self) -> None:
        redis_set = RedisModelSet("my_set", {self.foo}, Foo, cascade=True)
        await self._test_save_model_collection(redis_set, cascade=True)

    async def test_save_redis_list(self) -> None:
        redis_list = RedisList("int_list", None, int)
        redis_list.append(1)
        redis_list.append(2)
        redis_list.append(3)
        await redis_list.save()

        async with connection() as redis:
            assert ["1", "2", "3"] == await redis.lrange("int_list", 0, -1)

        assert (await RedisList.get("int_list", item_class=int, eager=True)).values == [
            1,
            2,
            3,
        ]

    async def test_save_redis_set(self) -> None:
        redis_set = RedisSet("some_set", None, str)
        redis_set.add("test")
        redis_set.add("ing")
        await redis_set.save()

        async with connection() as redis:
            assert {"test", "ing"} == await redis.smembers("some_set")

        assert (await RedisSet.get("some_set", item_class=str, eager=True)).values == {
            "test",
            "ing",
        }
