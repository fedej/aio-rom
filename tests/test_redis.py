import dataclasses
import os
import typing
from dataclasses import field
from typing import Optional

import pytest
from typing_extensions import Annotated

from aio_rom import DataclassModel as Model
from aio_rom.collections import RedisList, RedisSet
from aio_rom.fields import Metadata
from aio_rom.session import connection

if not os.environ.get("CI"):
    pytest.skip("Redis CI test only", allow_module_level=True)


@dataclasses.dataclass(unsafe_hash=True)
class Bar(Model):
    field1: Annotated[int, Metadata(eager=True)]
    field2: str
    field3: Annotated[RedisList[int], Metadata(eager=True)] = field(
        default_factory=RedisList[int], hash=False
    )
    field4: int = 3
    field5: bool = True
    field6: typing.Optional[bool] = None


@dataclasses.dataclass(unsafe_hash=True)
class Foo(Model):
    eager_bars: Annotated[RedisList[Bar], Metadata(eager=True)] = field(
        default_factory=RedisList[Bar], hash=False
    )
    lazy_bars: Annotated[RedisSet[Bar], Metadata(cascade=True)] = field(
        default_factory=RedisSet[Bar], compare=False
    )
    f1: Optional[str] = None


@dataclasses.dataclass
class FooBar(Model):
    foos: Annotated[RedisSet[Foo], Metadata(eager=True, cascade=True)] = field(
        default_factory=RedisSet[Foo]
    )


@pytest.fixture
def bar() -> Bar:
    return Bar("1", 123, "value", RedisList[int]([1, 2, 3]))


@pytest.fixture(autouse=True)
async def cleanup() -> typing.AsyncIterator[None]:
    yield
    await Foo.delete_all()
    await Bar.delete_all()
    await FooBar.delete_all()
    await RedisList.delete_all()
    await RedisSet.delete_all()


async def test_save(bar: Bar) -> None:
    await bar.save()

    async with connection() as redis:
        field1 = await redis.hget("bar:1", "field1")
        field2 = await redis.hget("bar:1", "field2")
        field3 = await redis.hget("bar:1", "field3")
        field4 = await redis.hget("bar:1", "field4")
        field5 = await redis.hget("bar:1", "field5")
        field6 = await redis.hget("bar:1", "field6")
        field3_value = await redis.lrange("redislist:bar:1:field3", 0, -1)

    assert "123" == field1
    assert "value" == field2
    assert "bar:1:field3" == field3
    assert ["1", "2", "3"] == field3_value
    assert "3" == field4
    assert "1" == field5
    assert field6 is None


async def test_save_with_empty_reference(bar: Bar) -> None:
    bar.field3 = RedisList[int]()
    await bar.save()

    async with connection() as redis:
        field1 = await redis.hget("bar:1", "field1")
        field2 = await redis.hget("bar:1", "field2")
        field3 = await redis.hget("bar:1", "field3")
        field3_value = await redis.lrange("redislist:bar:1:field3", 0, -1)

    assert "123" == field1
    assert "value" == field2
    assert "bar:1:field3" == field3
    assert [] == field3_value


async def test_get(bar: Bar) -> None:
    await bar.save()
    bar = await Bar.get("1")
    assert bar == bar


async def test_get_with_references(bar: Bar) -> None:
    await bar.save()
    foo = Foo("123", RedisList[Bar]([bar]), RedisSet[Bar]({bar}))
    await foo.save()
    gotten_foo = await Foo.get("123")
    assert foo == gotten_foo
    assert isinstance(gotten_foo.lazy_bars, RedisSet)
    await gotten_foo.lazy_bars.refresh()
    assert 1 == await gotten_foo.lazy_bars.total_count()
    for bar in gotten_foo.lazy_bars:
        assert bar in foo.lazy_bars
    assert len(foo.lazy_bars) == len(gotten_foo.lazy_bars)


async def _test_collection_references(bar: Bar, test_cascade: bool = False) -> None:
    await bar.save()
    foo = Foo("123")
    foo.eager_bars.append(bar)
    foo.lazy_bars.add(bar)
    if not test_cascade:
        await foo.save()
    foobar = FooBar("321", RedisSet[Foo]({foo}))
    await foobar.save()

    gotten_foobar = await FooBar.get("321")
    assert foobar == gotten_foobar
    assert {foo} == gotten_foobar.foos
    for gotten_foo in gotten_foobar.foos:
        assert 1 == len(gotten_foo.eager_bars)
        assert isinstance(gotten_foo.lazy_bars, RedisSet)
        await gotten_foo.lazy_bars.refresh()
        for bar in gotten_foo.lazy_bars:
            assert bar in foo.lazy_bars


async def test_collections(bar: Bar) -> None:
    await _test_collection_references(bar)


async def test_collection_cascades_references(bar: Bar) -> None:
    await _test_collection_references(bar, test_cascade=True)


async def test_update_collection_references(bar: Bar) -> None:
    await bar.save()
    foo = Foo("123", RedisList[Bar]([bar]), RedisSet[Bar]({bar}))
    foobar = FooBar("321", RedisSet[Foo]({foo}))
    await foobar.save()
    await foobar.refresh()
    foo2 = Foo("222", RedisList[Bar]([]), RedisSet[Bar](set()))
    foobar.foos.add(foo2)
    await foobar.save()

    gotten_foobar = await FooBar.get("321")
    assert foobar == gotten_foobar
    assert {foo, foo2} == gotten_foobar.foos


async def test_update(bar: Bar) -> None:
    await bar.save()
    await bar.update(field2="updated")
    async with connection() as redis:
        field2 = await redis.hget("bar:1", "field2")
    assert "updated" == field2
    bar = await Bar.get("1")
    assert "updated" == bar.field2


async def test_update_reference(bar: Bar) -> None:
    await bar.save()
    foo = Foo("123", RedisList[Bar]([bar]), RedisSet[Bar]({bar}))
    await foo.save()

    bar2 = Bar("2", 123, "otherbar", RedisList[int]([1, 2, 3, 4]))
    await bar2.save()

    await foo.update(lazy_bars=RedisSet[Bar]({bar2}))
    async with connection() as redis:
        lazy_bars = await redis.smembers("redisset:foo:123:lazy_bars")
        assert {"2"} == lazy_bars

    await foo.update(eager_bars=RedisList[Bar]([bar2]))
    async with connection() as redis:
        eager_bars = await redis.lrange("redislist:foo:123:eager_bars", 0, -1)
        assert ["2"] == eager_bars

    gotten_foo = await Foo.get("123")
    assert foo == gotten_foo


async def test_save_again_overrides_previous(bar: Bar) -> None:
    await bar.save()
    bar = await Bar.get("1")
    bar.field2 = "updated"
    await bar.save()
    async with connection() as redis:
        field2 = await redis.hget("bar:1", "field2")
    assert "updated" == field2


async def test_delete(bar: Bar) -> None:
    await bar.save()
    async with connection() as redis:
        assert await redis.exists("bar:1")
        await bar.delete()
        assert not await redis.exists("bar:1")


async def test_delete_all(bar: Bar) -> None:
    await bar.save()
    async with connection() as redis:
        await Bar.delete_all()
        assert not await redis.keys("bar*")


async def test_lazy_collection_cascade(bar: Bar) -> None:
    foo = Foo("123", RedisList[Bar]([bar]), RedisSet[Bar]({bar}))
    await foo.save()
    foo = await Foo.get("123")
    other_bar = Bar("2", 124, "value2", RedisList[int]())
    assert isinstance(foo.lazy_bars, RedisSet)
    await foo.lazy_bars.refresh()
    foo.lazy_bars.add(other_bar)
    await foo.save()
    gotten_foo = await Foo.get("123")
    assert foo == gotten_foo
    assert isinstance(gotten_foo.lazy_bars, RedisSet)
    await gotten_foo.lazy_bars.refresh()
    assert 2 == len(foo.lazy_bars) == len(gotten_foo.lazy_bars)
