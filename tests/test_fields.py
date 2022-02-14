from __future__ import annotations

import datetime
import json
from decimal import Decimal
from typing import Any

import orjson
import ujson

from aio_rom import fields
from tests import RedisTestCase


class FieldsTestCase(RedisTestCase):
    def test_ujson_serializer(self) -> None:
        fields.serializer = ujson.dumps
        self.assertEqual("123", fields.serialize(123))
        self.assertEqual("true", fields.serialize(True))
        self.assertEqual("123.99", fields.serialize(123.99))
        self.assertEqual(b"123", fields.serialize(b"123"))
        self.assertEqual("123.0", fields.serialize(Decimal(123)))
        with self.assertRaises(TypeError):
            fields.serialize(datetime.datetime.now())

    async def test_ujson_deserializer(self) -> None:
        fields.deserializer = ujson.loads
        self.assertEqual(123, await fields.deserialize(int, "123"))
        self.assertEqual(True, await fields.deserialize(bool, "true"))
        self.assertEqual(123.99, await fields.deserialize(float, "123.99"))
        self.assertEqual(b"123", await fields.deserialize(bytes, b"123"))
        with self.assertRaises(TypeError):
            await fields.deserialize(Decimal, "123.0")

        @fields.deserialize.register(Decimal)
        async def _(_: type[Decimal], value: Any) -> Decimal:
            return Decimal(fields.deserializer(value))

        self.assertEqual(Decimal(123), await fields.deserialize(Decimal, "123.0"))

    def test_orjson_serializer(self) -> None:
        fields.serializer = orjson.dumps
        self.assertEqual(b"123", fields.serialize(123))
        self.assertEqual(b"true", fields.serialize(True))
        self.assertEqual(b"123.99", fields.serialize(123.99))
        self.assertEqual(b"123", fields.serialize(b"123"))
        with self.assertRaises(TypeError):
            fields.serialize(Decimal(123))
        self.assertEqual(
            b'"1970-01-01T00:00:00"',
            fields.serialize(datetime.datetime.utcfromtimestamp(0)),
        )

    async def test_orjson_deserializer(self) -> None:
        fields.deserializer = orjson.loads
        self.assertEqual(123, await fields.deserialize(int, "123"))
        self.assertEqual(True, await fields.deserialize(bool, "true"))
        self.assertEqual(123.99, await fields.deserialize(float, "123.99"))
        self.assertEqual(b"123", await fields.deserialize(bytes, b"123"))
        with self.assertRaises(TypeError):
            await fields.deserialize(Decimal, "123.0")

        @fields.deserialize.register(Decimal)
        async def _(_: type[Decimal], value: Any) -> Decimal:
            return Decimal(fields.deserializer(value))

        @fields.deserialize.register(datetime.datetime)
        async def _(_: type[datetime.datetime], value: Any) -> datetime.datetime:
            return datetime.datetime.fromisoformat(fields.deserializer(value))

        self.assertEqual(Decimal(123), await fields.deserialize(Decimal, "123.0"))
        self.assertEqual(
            datetime.datetime.utcfromtimestamp(0),
            await fields.deserialize(datetime.datetime, b'"1970-01-01T00:00:00"'),
        )

    def test_default_serializer(self) -> None:
        fields.serializer = json.dumps
        self.assertEqual("123", fields.serialize(123))
        self.assertEqual("true", fields.serialize(True))
        self.assertEqual("123.99", fields.serialize(123.99))
        self.assertEqual(b"123", fields.serialize(b"123"))
        with self.assertRaises(TypeError):
            fields.serialize(Decimal(123))
