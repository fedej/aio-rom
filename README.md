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
from typing import Set

import rom

# Library will wrap the class into a dataclass and detect fields from that
class Foo(rom.Model):
    bar: int
    foobar: Set[int] = rom.field(default_factory=set)
    my_boolean: bool = False
    transient_field: Dict = rom.field(transient=True)

class OtherFoo(rom.Model):
    foo: Foo

async def main():
    await rom.init("redis://localhost")
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
1. Pypi
1. Update specific model fields

## Limitations
1. `init` must be called before other calls to Redis can succeed, no defaults to localhost atm.
1. You cannot use `from __future__ import annotations` in the same file you define your models. See https://bugs.python.org/issue39442
1. TODO Supported datatypes
1. Probably more ...
