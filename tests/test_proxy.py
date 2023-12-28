from __future__ import annotations

from unittest.mock import patch

import pytest

from aio_rom import Model
from aio_rom.proxy import ProxyModel
from aio_rom.types import Key


class UnderTest(Model):
    attribute: int = 20

    def __init__(self, id: Key):
        self.id = id

    @classmethod
    async def get(cls: type[UnderTest], id: Key) -> UnderTest:
        return cls(id)


def test_proxy_is_instance_of_model() -> None:
    model = ProxyModel(UnderTest, "123")
    assert isinstance(model, UnderTest)
    assert isinstance(model, ProxyModel)
    assert model.__class__ is UnderTest


def test_lazy_proxy_access_fails() -> None:
    model = ProxyModel(UnderTest, "123")
    with pytest.raises(ValueError) as exc:
        model.attribute
    assert "wrapper has not been" in str(exc.value)


async def test_proxy_fetch() -> None:
    model = ProxyModel(UnderTest, "123")
    await model.refresh()
    assert model.attribute == 20
    assert model.db_id() == "undertest:123"


async def test_lazy_save_is_ignored() -> None:
    model = ProxyModel(UnderTest, "123")
    with patch.object(UnderTest, "save") as mock_save:
        await model.save()
    mock_save.assert_not_awaited()


async def test_wrapped_object_is_saved() -> None:
    model = ProxyModel(UnderTest, "123")
    await model.refresh()
    with patch.object(UnderTest, "save") as mock_save:
        await model.save(optimistic=True, cascade=True)
    mock_save.assert_awaited_once_with(optimistic=True, cascade=True)
