import dataclasses
import typing
from dataclasses import field
from typing import Optional

import pytest
from redis.asyncio.client import Pipeline, Redis

from aio_rom import DataclassModel as Model
from aio_rom.collections import RedisSet
from aio_rom.exception import ModelNotFoundException

from . import CoroutineMock, MagicMock, patch


@dataclasses.dataclass
class ForTesting(Model):
    f1: int
    f2: Optional[str] = None
    f3: int = 3
    f4: RedisSet[int] = field(default_factory=RedisSet[int])


@pytest.fixture
def mock_redis_transaction() -> typing.Iterator[MagicMock]:
    with patch("aio_rom.model.transaction") as tr:
        transaction = MagicMock(autospec=Pipeline)
        transaction.execute = CoroutineMock()
        transaction.delete = CoroutineMock()
        transaction.srem = CoroutineMock()
        transaction.sadd = CoroutineMock()
        transaction.hset = CoroutineMock()
        transaction.hdel = CoroutineMock()
        tr.return_value.__aenter__.return_value = transaction
        yield transaction


@pytest.fixture
def mock_redis_client(
    mock_redis_transaction: typing.Any,
) -> typing.Iterator[MagicMock]:
    with patch("aio_rom.model.connection") as connection_mock, patch(
        "aio_rom.types.connection"
    ) as abc_connection_mock:
        redis_client = MagicMock(autospec=Redis)
        redis_client.pipeline.return_value = mock_redis_transaction
        connection_mock.return_value.__aenter__.return_value = redis_client
        abc_connection_mock.return_value.__aenter__.return_value = redis_client
        yield redis_client


async def test_save(mock_redis_transaction: MagicMock) -> None:
    await ForTesting("123", 123).save()
    mock_redis_transaction.hset.assert_called_with(
        "fortesting:123",
        mapping={"id": "123", "f1": 123, "f3": 3, "f4": "fortesting:123:f4"},
    )
    mock_redis_transaction.sadd.assert_called_with("fortesting", "123")


async def test_update(mock_redis_transaction: MagicMock) -> None:
    await ForTesting("123", 123).update(f1=987)
    mock_redis_transaction.hset.assert_called_once_with(
        "fortesting:123", mapping={"f1": 987}
    )
    mock_redis_transaction.sadd.assert_not_called()


async def test_get(mock_redis_client: MagicMock) -> None:
    mock_redis_client.hgetall.side_effect = CoroutineMock(
        return_value={"id": "123", "f1": "123"}
    )
    value = await ForTesting.get("123")
    assert "123" == value.id
    assert 123 == value.f1
    assert ForTesting("123", 123) == value


async def test_failed_get(mock_redis_client: MagicMock) -> None:
    mock_redis_client.hgetall.side_effect = CoroutineMock(return_value=None)
    with pytest.raises(ModelNotFoundException):
        await ForTesting.get("123")


async def test_scan(mock_redis_client: MagicMock) -> None:
    mock_redis_client.hgetall.side_effect = CoroutineMock(
        side_effect=[{"id": "123", "f1": "123"}, {"id": "124", "f1": "123"}]
    )
    keys = MagicMock()
    keys.__aiter__.return_value = ["123", "124"]
    mock_redis_client.sscan_iter.return_value = keys
    items = 0
    async for obj in ForTesting.scan():
        assert 123 == obj.f1
        assert isinstance(obj.id, str)
        items += 1
    assert 2 == items


async def test_all(mock_redis_client: MagicMock) -> None:
    mock_redis_client.smembers = CoroutineMock(return_value=["123", "124"])
    mock_redis_client.hgetall.side_effect = CoroutineMock(
        side_effect=[{"id": "123", "f1": "123"}, {"id": "124", "f1": "123"}]
    )
    items = 0
    for obj in await ForTesting.all():
        assert 123 == obj.f1
        assert isinstance(obj.id, str)
        items += 1
    assert 2 == items


async def test_count(mock_redis_client: MagicMock) -> None:
    mock_redis_client.scard.side_effect = CoroutineMock(return_value=10)
    assert 10 == await ForTesting.total_count()


async def test_delete(
    mock_redis_client: MagicMock, mock_redis_transaction: MagicMock
) -> None:
    mock_redis_client.keys.side_effect = CoroutineMock(
        return_value=["fortesting:123:reference"]
    )
    await ForTesting("123", 987).delete()
    mock_redis_transaction.delete.assert_called_with(
        "fortesting:123:reference", "fortesting:123"
    )
    mock_redis_transaction.srem.assert_called_with("fortesting", "fortesting:123")


async def test_delete_all(mock_redis_client: MagicMock) -> None:
    mock_redis_client.keys.side_effect = CoroutineMock(return_value=["fortesting:1"])
    delete = CoroutineMock()
    mock_redis_client.delete.side_effect = delete
    await ForTesting.delete_all()
    delete.assert_called_with("fortesting", "fortesting:1")


async def test_exists(mock_redis_client: MagicMock) -> None:
    mock_redis_client.exists.side_effect = CoroutineMock(
        side_effect=[True, False, True, False]
    )
    assert await ForTesting.persisted(1)
    assert not await ForTesting.persisted(1)
    assert await ForTesting("1", 123).exists()
    assert not await ForTesting("1", 123).exists()


async def test_refresh(mock_redis_client: MagicMock) -> None:
    mock_redis_client.hgetall.side_effect = CoroutineMock(
        return_value={"id": "123", "f1": "124"}
    )
    value = ForTesting("123", 123)
    await value.refresh()
    assert "123" == value.id
    assert 124 == value.f1
    assert ForTesting("123", 124) == value
