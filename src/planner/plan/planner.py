from __future__ import annotations
from dataclasses import dataclass
import logging
from collections.abc import Collection, Sequence, Set
from typing import Any, cast, Union
import inspect
from pathlib import Path
import itertools as it

from ..asset import Asset, Recipe, is_asset_class, RecipeBundle
from .common import Contract
from .plan import Plan
from .algorithm import _PlanningAlgorithm

log = logging.getLogger(__name__)


type ContractDef = Union[
    type[Asset],
    Set[type[Asset]],

    tuple[type[Asset], str | None],
    tuple[Set[type[Asset]], str | None],

    tuple[type[Asset], Set[str | None]],
    tuple[Set[type[Asset]], Set[str | None]]
]


def resolve_contract_def(cdef: ContractDef) -> set[Contract]:
    if is_asset_class(cdef):
        # Single asset
        return {(cdef, None)}

    # Check if set of asset classes
    if isinstance(cdef, Set) and cdef and all(is_asset_class(c) for c in cdef):
        return {(asset, None) for asset in cdef}

    # Cdef is 2-tuple
    if not isinstance(cdef, tuple) or len(cdef) != 2:
        raise ValueError("Invalid input")

    # Collect assets
    if is_asset_class(asset := cdef[0]):
        assets = {asset}
    else:
        if isinstance(cdef[0], Set) and cdef[0] and all(is_asset_class(c) for c in cdef[0]):
            assets = set(cdef[0])
        else:
            raise ValueError("Invalid input")

    # Collect keys
    if isinstance(key := cdef[1], str | None):
        keys = {key}
    else:
        if isinstance(cdef[1], Set) and cdef[1] and all(isinstance(c, str | None) for c in cdef[1]):
            keys = set(cdef[1])
        else:
            raise ValueError("Invalid input")

    return set(it.product(assets, keys))


class Planner:
    """Collects recipe registrations and produces a concrete build `Plan`.

    A Planner maps *contracts* (Asset type + optional key) to one or more
    `Recipe` classes and associates each recipe with one or more *context paths*
    (ordered sequences of contracts that describe where in the DAG this recipe
    should be preferred). During `plan()`, the best-fit recipes are selected
    based on those contexts and assembled into a DAG.
    """

    # Each recipe may only have one set of context paths.
    # Context set must not be empty, since the empty context set matches nothing.
    recipe_to_context: dict[type[Recipe], set[tuple[Contract, ...]]]
    # Each contract may have one or more recipes
    contract_to_recipes: dict[Contract, set[type[Recipe]]]

    def __init__(self) -> None:
        self.contract_to_recipes = {}
        self.recipe_to_context = {}

    # def set_base_recipe(self, recipe: type[Recipe])

    def add(self, recipe: type[Recipe] | RecipeBundle, key: str | None = None, context: ContractDef | Sequence[ContractDef] = ()):
        """Register a recipe under the asset it produces and the given `key`, with optional context."""
        if isinstance(recipe, RecipeBundle):
            for r in recipe.recipes:
                self._add(r[0], key=key or r[1], context=context)
        else:
            self._add(recipe, key=key, context=context)
        return self

    def _add(self, recipe: type[Recipe], key: str | None, context: ContractDef | Sequence[ContractDef]):
        context_paths: set[tuple[Contract, ...]]
        try:
            # Single contract def
            contracts = resolve_contract_def(context)  # type: ignore
            context_paths = {(contract, ) for contract in contracts}
        except Exception:
            # Context is sequence of contractdef
            context = cast(Sequence[ContractDef], context)
            parts = [resolve_contract_def(elem) for elem in context]
            context_paths = set(it.product(*parts))
        assert context_paths

        # Register under contract
        contract = (recipe._makes, key)
        if contract not in self.contract_to_recipes:
            self.contract_to_recipes[contract] = set()
        self.contract_to_recipes[contract].add(recipe)

        # Register context paths
        if recipe not in self.recipe_to_context:
            self.recipe_to_context[recipe] = set()
        self.recipe_to_context[recipe] |= context_paths

        return self


    def plan[T: Asset](self, asset: type[T], key: str | None = None, root: Path | str | None = None, project: str | None = None) -> Plan[T]:
        target_contract = (asset, key)

        # Determine target recipe. Must match the empty context path.
        _target_recipes = {r for r in self.contract_to_recipes.get(target_contract, []) if () in self.recipe_to_context[r]}
        if len(_target_recipes) > 1:
            raise ValueError(f"Too many recipes for target asset {target_contract}")
        elif not _target_recipes:
            raise ValueError(f"Missing recipe for target asset {target_contract}")
        target_recipe = next(iter(_target_recipes))

        log.info("Creating plan")
        algo = _PlanningAlgorithm(target_recipe=target_recipe, contract_to_recipes=self.contract_to_recipes, recipe_to_context=self.recipe_to_context)
        G = algo.run()
        return Plan(G, root=root, project=project)
