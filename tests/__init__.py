import sys

if sys.version_info >= (3, 8):
    from unittest.async_case import IsolatedAsyncioTestCase as TestCase
    from unittest.mock import AsyncMock as CoroutineMock
    from unittest.mock import MagicMock, patch

    ASYNCTEST = False
else:
    from asynctest import TestCase
    from asynctest.mock import CoroutineMock, MagicMock, patch

    ASYNCTEST = True


class RedisTestCase(TestCase):
    async def asyncSetUp(self) -> None:
        pass

    async def asyncTearDown(self) -> None:
        pass

    if ASYNCTEST:

        async def tearDown(self) -> None:  # type: ignore
            await self.asyncTearDown()

        async def setUp(self) -> None:  # type: ignore
            await self.asyncSetUp()


__all__ = ["RedisTestCase", "CoroutineMock", "MagicMock", "patch"]
