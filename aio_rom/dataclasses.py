import dataclasses

from aio_rom.model import Model
from aio_rom.types import Key


@dataclasses.dataclass
class DataclassModel(Model):
    # TODO: add 1st class support for dataclasses.field's metadata
    id: Key = dataclasses.field(init=True, repr=False, compare=False)
