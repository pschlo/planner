from __future__ import annotations
from dataclasses import dataclass, fields as get_dataclass_fields
from typing import get_type_hints
import inspect

from ..asset import Asset
from ..recipe import Recipe


type Contract[T: Asset] = tuple[type[T], str | None]


@dataclass(frozen=True)
class Dependency:
    name: str
    contract: Contract


def _parse_dependencies(recipe: type[Recipe]) -> list[Dependency]:
    fields = [f for f in get_dataclass_fields(recipe) if '_injected' in f.metadata]
    try:
        type_hints = get_type_hints(recipe)
    except Exception as e:
        raise RuntimeError(f"Failed to get type hints for recipe '{recipe}'") from e

    deps: list[Dependency] = []
    for field in fields:
        typ = type_hints[field.name]
        if not (inspect.isclass(typ) and issubclass(typ, Asset)):
            raise ValueError(f"Invalid dependency '{typ}' in recipe '{recipe}': Must be a subclass of Asset")
        deps.append(Dependency(
            name=field.name,
            contract=(typ, field.metadata['key'])
        ))

    return deps
