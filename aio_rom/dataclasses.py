import dataclasses

from .model import Model
from .types import IModel, Key


@dataclasses.dataclass
class DataclassModel(Model):
    # TODO: add 1st class support for dataclasses.field's metadata
    id: Key = dataclasses.field(init=True, repr=False, compare=False)

    def __post_init__(self) -> None:
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if isinstance(value, IModel) and not getattr(value, "id", None):
                value.id = f"{self.db_id()}:{field.name}"  # type: ignore[attr-defined]
