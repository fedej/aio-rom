Python Redis Object Mapper
======================

asyncio based Redis object mapper

## Table of content

- [Installation](#installation)
- [Usage](#usage)
- [Features](#usage)
- [TODO](#todo)
- [Limitations](#limitations)

## Installation

TODO

## Usage

```python
import asyncio

from dataclasses import field
from typing import Set, Dict

from aio_rom import Model
from aio_rom.fields import Metadata
from aio_rom.session import redis_pool


class Foo(Model):
    bar: int
    foobar: Set[int] = field(default_factory=set)
    my_boolean: bool = False
    transient_field: Dict = field(metadata=Metadata(transient=True))


class OtherFoo(Model):
    foo: Foo

async def main():
    async with redis_pool("redis://localhost"):
        foo = Foo(123, {1,2,3}, True)
        await foo.save()
        ...
        foo2 = await Foo.get(321)
        other_foo = OtherFoo(303, foo2)
        await other_foo.save()

asyncio.run(main())
```
## Features
TODO

## TODO
1. Docs
1. Tests

## Limitations
1. `configure` must be called before other calls to Redis can succeed, no defaults to localhost atm.
1. You cannot use `from __future__ import annotations` in the same file you define your models. See https://bugs.python.org/issue39442
1. TODO Supported datatypes
1. Probably more ...
