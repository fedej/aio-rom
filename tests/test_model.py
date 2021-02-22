from asynctest import CoroutineMock, MagicMock, TestCase
from aioredis import Redis
from asynctest.mock import patch
from rom import Model

class ForTesting(Model):
    f1: int

class ModelTestCase(TestCase):
    def setUpClass():
        ModelTestCase.redis_await = Redis.__await__
        # Redis & ContextRedis are awaitable, asynctest will try to yield from them
        delattr(Redis, "__await__")

    def tearDownClass():
        setattr(Redis, "__await__", ModelTestCase.redis_await)

    async def setUp(self):
        self.mock_redis_transaction = MagicMock(spec=Redis)
        self.mock_redis_transaction.execute.side_effect = CoroutineMock(spec=Redis)
        self.mock_redis_client = MagicMock(spec=Redis)
        self.mock_redis_client.multi_exec.return_value = self.mock_redis_transaction
        self.mock_redis_client.smembers.side_effect = CoroutineMock()
        self.transaction_patcher = patch("rom.model.transaction")
        self.transaction_mock = self.transaction_patcher.start()
        self.transaction_mock.return_value.__aenter__.return_value = self.mock_redis_transaction
        self.connection_patcher = patch("rom.model.connection")
        self.connection_mock = self.connection_patcher.start()
        self.connection_mock.return_value.__aenter__.return_value = self.mock_redis_client

    async def tearDown(self):
        patch.stopall()

    async def test_save(self):
        await ForTesting(123, 123).save()
        self.mock_redis_transaction.hmset_dict.assert_called_with("fortesting:123", {"id": "123", "f1": "123"})
        self.mock_redis_transaction.sadd.assert_called_with("fortesting", 123)
        
    async def test_get(self):
        self.mock_redis_client.hgetall.side_effect = CoroutineMock(
            return_value={
                "id": "123",
                "f1": "123"
            }
        )
        value = await ForTesting.get(123)
        assert 123 == value.id
        assert 123 == value.f1
        assert ForTesting(123, 123) == value
