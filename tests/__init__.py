import sys

if sys.version_info >= (3, 8):
    from unittest.mock import AsyncMock as CoroutineMock
    from unittest.mock import MagicMock, patch

else:
    from asynctest.mock import CoroutineMock, MagicMock, patch


__all__ = ["CoroutineMock", "MagicMock", "patch"]
