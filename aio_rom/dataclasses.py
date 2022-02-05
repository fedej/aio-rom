import dataclasses

from .model import Model
from .types import Key


@dataclasses.dataclass
class DataclassModel(Model):
    # TODO: add 1st class support for dataclasses.field's metadata
    id: Key = dataclasses.field(init=True, repr=False, compare=False)
