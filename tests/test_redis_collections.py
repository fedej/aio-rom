import dataclasses
import os
from unittest import skipUnless

from aio_rom import DataclassModel as Model
from aio_rom.collections import RedisCollection, RedisList, RedisSet
from aio_rom.exception import ModelNotFoundException
from aio_rom.session import connection

from . import RedisTestCase


@dataclasses.dataclass(unsafe_hash=True)
class Foo(Model):
    field1: int


# @skipUnless(os.environ.get("CI"), "Redis CI test only")
class RedisCollectionIntegrationTestCase(RedisTestCase):
    async def asyncSetUp(self) -> None:
        self.foo = Foo("1", 123)

    async def asyncTearDown(self) -> None:
        async with connection() as conn:
            await conn.flushdb()

    async def _test_save_model_collection(
        self, redis_collection: RedisCollection[Foo], cascade: bool = False
    ) -> None:
        assert redis_collection.id
        await redis_collection.save(cascade=cascade)
        if cascade:
            for item in redis_collection:
                assert await Foo.get(item.id) == item
            fresh = await redis_collection.get(redis_collection.id)
            assert redis_collection == fresh
        else:
            with self.assertRaises(ModelNotFoundException):
                await redis_collection.get(redis_collection.id)

    async def test_save_model_list(self) -> None:
        redis_list = RedisList[Foo]([self.foo], id="test")
        await self._test_save_model_collection(redis_list)

    async def test_save_model_list_cascade(self) -> None:
        redis_list = RedisList[Foo]([self.foo], id="test")
        await self._test_save_model_collection(redis_list, cascade=True)

    async def test_save_model_set(self) -> None:
        redis_set = RedisSet[Foo]({self.foo}, id="test")
        await self._test_save_model_collection(redis_set)

    async def test_save_model_set_cascade(self) -> None:
        redis_set = RedisSet[Foo]({self.foo}, id="test")
        await self._test_save_model_collection(redis_set, cascade=True)

    async def test_save_redis_list(self) -> None:
        redis_list = RedisList[int](id="int_list")
        redis_list.append(1)
        redis_list.append(2)
        redis_list.append(3)
        await redis_list.save()

        async with connection() as redis:
            assert ["1", "2", "3"] == await redis.lrange("redislist:int_list", 0, -1)

        assert (await RedisList[int].get("int_list")).values == [
            1,
            2,
            3,
        ]

    async def test_save_redis_set(self) -> None:
        redis_set = RedisSet[str](id="some_set")
        redis_set.add("test")
        redis_set.add("ing")
        await redis_set.save()

        async with connection() as redis:
            assert {"test", "ing"} == await redis.smembers("redisset:some_set")

        assert (await RedisSet[str].get("some_set")).values == {
            "test",
            "ing",
        }

    async def test_iteration_set(self) -> None:
        redis_set = RedisSet[str](id="some_set")
        redis_set.add("test")
        redis_set.add("ing")
        await redis_set.save()
        some_set = await RedisSet[str].get("some_set")
        async for item in some_set:
            assert item in redis_set

    async def test_iteration_list(self) -> None:
        redis_list = RedisList[str](id="some_list")
        redis_list.append("test")
        redis_list.append("ing")
        await redis_list.save()
        some_list = await RedisList[str].get("some_list")
        async for item in some_list:
            assert item in redis_list

    async def test_model_iteration(self) -> None:
        model_list = RedisList[Foo]([self.foo], id="models")
        await model_list.save(cascade=True)
        models = await RedisList[Foo].get("models")
        async for item in models:
            assert item in model_list
        assert not models

    async def test_async_set(self) -> None:
        a_set = RedisSet[str](id="a_set")
        await a_set.async_add("123")
        await a_set.async_add("456")
        assert len(a_set) == 2
        await a_set.async_discard("456")
        assert len(a_set) == 1
        new_set = await RedisSet[str].get("a_set")
        assert a_set == new_set

    async def test_async_list(self) -> None:
        a_list = RedisList[str](id="a_list")
        await a_list.async_append("123")
        await a_list.async_append("456")
        assert len(a_list) == 2
        assert a_list == await RedisList[str].get("a_list")

    async def test_async_model_set(self) -> None:
        a_set = RedisSet[Foo](id="a_set")
        await a_set.async_add(Foo("123", 123), cascade=True)
        await a_set.async_add(Foo("456", 456), cascade=True)
        assert len(a_set) == 2
        await a_set.async_discard(Foo("456", 456), cascade=True)
        assert len(a_set) == 1
        assert a_set == await RedisSet[Foo].get("a_set")

    async def test_async_model_list(self) -> None:
        a_list = RedisList[Foo](id="a_list")
        await a_list.async_append(Foo("123", 123), cascade=True)
        await a_list.async_append(Foo("456", 456), cascade=True)
        assert len(a_list) == 2
        assert a_list == await RedisList[Foo].get("a_list")

    async def test_delete(self) -> None:
        a_list = RedisList[str](id="a_list")
        await a_list.async_append("123", cascade=True)
        await a_list.async_append("456", cascade=True)
        assert len(a_list) == 2
        await a_list.delete()
        with self.assertRaises(ModelNotFoundException):
            await RedisList.get("a_list")

    async def test_delete_model(self) -> None:
        a_list = RedisList[Foo](id="a_list")
        await a_list.async_append(Foo("123", 123))
        await a_list.async_append(Foo("456", 456))
        assert len(a_list) == 2
        assert await Foo.get("123")
        await a_list.delete()
        with self.assertRaises(ModelNotFoundException):
            await RedisList[Foo].get("a_list")
        with self.assertRaises(ModelNotFoundException):
            await Foo.get("123")
