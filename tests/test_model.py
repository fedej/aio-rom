from aioredis import Redis
from asynctest import CoroutineMock, MagicMock, TestCase
from asynctest.mock import patch
from rom import Model
from rom.exception import ModelNotFoundException


class ForTesting(Model):
    f1: int


class ModelTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        ModelTestCase.redis_await = Redis.__await__
        # Redis & ContextRedis are awaitable, asynctest will try to yield from them
        delattr(Redis, "__await__")

    @classmethod
    def tearDownClass(cls):
        setattr(Redis, "__await__", ModelTestCase.redis_await)

    async def setUp(self):
        self.mock_redis_transaction = MagicMock(spec=Redis)
        self.mock_redis_transaction.execute.side_effect = CoroutineMock(spec=Redis)
        self.mock_redis_client = MagicMock(spec=Redis)
        self.mock_redis_client.multi_exec.return_value = self.mock_redis_transaction
        self.mock_redis_client.smembers.side_effect = CoroutineMock()
        self.transaction_patcher = patch("rom.model.transaction")
        self.transaction_mock = self.transaction_patcher.start()
        self.transaction_mock.return_value.__aenter__.return_value = (
            self.mock_redis_transaction
        )
        self.connection_patcher = patch("rom.model.connection")
        self.connection_mock = self.connection_patcher.start()
        self.connection_mock.return_value.__aenter__.return_value = (
            self.mock_redis_client
        )

    async def tearDown(self):
        patch.stopall()

    async def test_save(self):
        await ForTesting(123, 123).save()
        self.mock_redis_transaction.hmset_dict.assert_called_with(
            "fortesting:123", {"id": "123", "f1": "123"}
        )
        self.mock_redis_transaction.sadd.assert_called_with("fortesting", 123)

    async def test_update(self):
        await ForTesting(123, 123).update(f1=987)
        self.mock_redis_transaction.hset.assert_called_once_with(
            "fortesting:123", "f1", "987"
        )
        self.mock_redis_transaction.sadd.assert_not_called()

    async def test_get(self):
        self.mock_redis_client.hgetall.side_effect = CoroutineMock(
            return_value={"id": "123", "f1": "123"}
        )
        value = await ForTesting.get(123)
        assert 123 == value.id
        assert 123 == value.f1
        assert ForTesting(123, 123) == value

    async def test_failed_get(self):
        self.mock_redis_client.hgetall.side_effect = CoroutineMock(return_value=None)
        await self.assertAsyncRaises(ModelNotFoundException, ForTesting.get(123))

    async def test_scan(self):
        self.mock_redis_client.hgetall.side_effect = CoroutineMock(
            side_effect=[{"id": "123", "f1": "123"}, {"id": "124", "f1": "123"}]
        )
        keys = MagicMock()
        keys.__aiter__.return_value = ["123", "124"]
        self.mock_redis_client.isscan.return_value = keys
        items = 0
        async for obj in ForTesting.scan():
            assert 123 == obj.f1
            assert isinstance(obj.id, int)
            items += 1
        assert 2 == items

    async def test_all(self):
        self.mock_redis_client.smembers.side_effect.return_value = ["123", "124"]
        self.mock_redis_client.hgetall.side_effect = CoroutineMock(
            side_effect=[{"id": "123", "f1": "123"}, {"id": "124", "f1": "123"}]
        )
        items = 0
        for obj in await ForTesting.all():
            assert 123 == obj.f1
            assert isinstance(obj.id, int)
            items += 1
        assert 2 == items

    async def test_count(self):
        self.mock_redis_client.scard.side_effect = CoroutineMock(return_value=10)
        assert 10 == await ForTesting.count()

    async def test_delete(self):
        self.mock_redis_client.keys.side_effect = CoroutineMock(
            return_value=["fortesting:123:reference"]
        )
        await ForTesting(123, 987).delete()
        self.mock_redis_transaction.delete.assert_called_with(
            "fortesting:123:reference", "fortesting:123"
        )
        self.mock_redis_transaction.srem.assert_called_with(
            "fortesting", "fortesting:123"
        )

    async def test_delete_all(self):
        self.mock_redis_client.keys.side_effect = CoroutineMock(
            return_value=["fortesting:1"]
        )
        delete = CoroutineMock()
        self.mock_redis_client.delete.side_effect = delete
        await ForTesting.delete_all()
        delete.assert_called_with("fortesting", "fortesting:1")

    async def test_flush(self):
        self.mock_redis_client.flushdb.side_effect = CoroutineMock()
        await ForTesting.flush()
        self.mock_redis_client.flushdb.assert_called_once()

    async def test_exists(self):
        self.mock_redis_client.exists.side_effect = CoroutineMock(
            side_effect=[True, False, True, False]
        )
        assert await ForTesting.persisted(1)
        assert not await ForTesting.persisted(1)
        assert await ForTesting(1, "123").exists()
        assert not await ForTesting(1, "123").exists()

    async def test_refresh(self):
        self.mock_redis_client.hgetall.side_effect = CoroutineMock(
            return_value={"id": "123", "f1": "124"}
        )
        value = await ForTesting(123, 123).refresh()
        assert 123 == value.id
        assert 124 == value.f1
        assert ForTesting(123, 124) == value
