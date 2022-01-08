import dataclasses

from .model import Model
from .types import Key


@dataclasses.dataclass
class DataclassModel(Model):
    id: Key = dataclasses.field(init=True, repr=False, compare=False)
    # TODO: add 1st class support for dataclasses.field's metadata
