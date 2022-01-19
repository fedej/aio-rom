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
        async with connection() as conn:
            await conn.flushdb()

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

    async def test_iteration_set(self) -> None:
        redis_set = RedisSet("some_set", None, str)
        redis_set.add("test")
        redis_set.add("ing")
        await redis_set.save()
        some_set = await RedisSet.get("some_set", item_class=str)
        async for item in some_set:
            assert item in redis_set
        assert not some_set

    async def test_iteration_list(self) -> None:
        redis_list = RedisList("some_list", None, str)
        redis_list.append("test")
        redis_list.append("ing")
        await redis_list.save()
        some_list = await RedisList.get("some_list", item_class=str)
        async for item in some_list:
            assert item in redis_list
        assert not some_list

    async def test_model_iteration(self) -> None:
        model_list = RedisModelList("models", [self.foo], Foo, cascade=True)
        await model_list.save()
        models = await RedisModelList.get("models", item_class=Foo)
        async for item in models:
            assert item in model_list
        assert not models

    async def test_async_set(self) -> None:
        a_set = RedisSet("a_set", None, str)
        await a_set.async_add("123")
        await a_set.async_add("456")
        assert len(a_set) == 2
        await a_set.async_discard("456")
        assert len(a_set) == 1
        assert a_set == await RedisSet.get("a_set", item_class=str, eager=True)

    async def test_async_list(self) -> None:
        a_list = RedisList("a_list", None, str)
        await a_list.async_append("123")
        await a_list.async_append("456")
        assert len(a_list) == 2
        assert a_list == await RedisList.get("a_list", item_class=str, eager=True)

    async def test_async_model_set(self) -> None:
        a_set = RedisModelSet("a_set", None, Foo, cascade=True)
        await a_set.async_add(Foo("123", 123))
        await a_set.async_add(Foo("456", 456))
        assert len(a_set) == 2
        await a_set.async_discard(Foo("456", 456))
        assert len(a_set) == 1
        assert a_set == await RedisModelSet.get("a_set", item_class=Foo, eager=True)

    async def test_async_model_list(self) -> None:
        a_list = RedisModelList("a_list", None, Foo, cascade=True)
        await a_list.async_append(Foo("123", 123))
        await a_list.async_append(Foo("456", 456))
        assert len(a_list) == 2
        assert a_list == await RedisModelList.get("a_list", item_class=Foo, eager=True)

    async def test_delete(self) -> None:
        a_list = RedisList("a_list", None, str, cascade=True)
        await a_list.async_append("123")
        await a_list.async_append("456")
        assert len(a_list) == 2
        await a_list.delete()
        with self.assertRaises(ModelNotFoundException):
            await RedisList.get("a_list", item_class=str)

    async def test_delete_model(self) -> None:
        a_list = RedisModelList("a_list", None, Foo, cascade=True)
        await a_list.async_append(Foo("123", 123))
        await a_list.async_append(Foo("456", 456))
        assert len(a_list) == 2
        assert Foo.get("123")
        await a_list.delete()
        with self.assertRaises(ModelNotFoundException):
            await RedisModelList.get("a_list", item_class=Foo)
        with self.assertRaises(ModelNotFoundException):
            await Foo.get("123")
