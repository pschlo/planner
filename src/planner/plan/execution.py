from __future__ import annotations
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from collections.abc import Generator, Sequence
import networkx as nx
import logging
from typing import Self, TYPE_CHECKING, ContextManager, Callable
from pathlib import Path
from contextlib import ExitStack

from ..asset import Asset, Recipe
from .common import _parse_dependencies, Contract
if TYPE_CHECKING:
    from .algorithm import GraphNode

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AssetRecord[T: Asset]:
    asset: T
    stack: ExitStack

    def _cleanup(self, exc_type=None, exc=None, tb=None):
        self.stack.__exit__(exc_type, exc, tb)


class PlanExecution:
    """Imperative executor for a compiled plan.

    Builds nodes in topological order, resolves each recipe's working directory,
    instantiates recipe objects with injected dependencies, and captures the
    produced assets. On exit/cleanup, resources are released in reverse order.
    """
    def __init__(
        self,
        graph: nx.MultiDiGraph[GraphNode],
        seq: Sequence[GraphNode],
        root: Path | None = None,
        project: str | None = None,
        defer_cleanup: bool = False  # Unload and cleanup assets as soon as they are not needed anymore
    ) -> None:
        self.graph = graph
        self.seq = seq
        self.node_to_asset: dict[GraphNode, AssetRecord] = {}
        self.root = root
        self.project = project
        self.defer_cleanup = defer_cleanup
        self._cleanup_errors: list[Exception] = []
    
    @property
    def target(self):
        return self.seq[-1]

    def __enter__(self: Self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self._cleanup(exc_type, exc, tb)
        except Exception as cleanup_exc:
            if exc_type is None:
                # No original error → propagate cleanup failure
                raise
            log.exception("Cleanup also failed; ignoring in favor of original exception")
        return False  # never suppress the original exception


    def run(self):
        """Build every node in order; return the target node's `AssetRecord`."""
        if self.root is not None:
            self.root.mkdir(exist_ok=True)

        log.info("Starting plan execution")

        # remaining downstream contract-uses
        remaining_uses = {n: self.graph.out_degree(n) for n in self.seq}

        for node in self.seq:
            rec = self._build_node(node)
            self.node_to_asset[node] = rec

            # after building this node, its inputs might be freeable
            for u, _, _ in self.graph.in_edges(node, keys=True):
                remaining_uses[u] -= 1
                assert remaining_uses[u] >= 0  # cannot be negative

                if not self.defer_cleanup and remaining_uses[u] == 0 and u is not self.target:
                    # Unregister loaded asset
                    rec = self.node_to_asset.pop(u, None)
                    if rec is None:
                        continue

                    # Clean up asset
                    log.debug(f"Cleaning up {u}")
                    try:
                        rec._cleanup(None, None, None)
                    except Exception as e:
                        log.exception(f"Cleanup failed for {u}")
                        self._cleanup_errors.append(e)

        return self.node_to_asset[self.target]


    def cleanup(self):
        self._cleanup(None, None, None)

    def _cleanup(self, exc_type=None, exc=None, tb=None):
        log.info("Cleaning up assets")

        for node in self.seq[::-1]:
            # Unregister loaded asset
            rec = self.node_to_asset.pop(node, None)
            if rec is None:
                continue

            # Clean up asset
            log.debug(f"Cleaning up {node}")
            try:
                rec._cleanup(exc_type, exc, tb)
            except Exception as e:
                log.exception(f"Cleanup failed for {node}")
                self._cleanup_errors.append(e)

        assert not self.node_to_asset

        if self._cleanup_errors:
            raise ExceptionGroup("Cleanup failed", self._cleanup_errors)


    def _build_node(self, node: GraphNode) -> AssetRecord:
        _Recipe = node.recipe
        path, temp_dir = self._resolve_build_path(_Recipe)

        stack = ExitStack()
        try:
            # if temp_dir exists, make it part of the stack for guaranteed cleanup on failure
            if temp_dir is not None:
                stack.callback(temp_dir.cleanup)

            # create dependencies
            input_assets: dict[Contract, AssetRecord[Asset]] = {
                c: self.node_to_asset[u]
                for u, _, c in self.graph.in_edges(node, keys=True)
            }
            recipe_kwargs: dict[str, Asset] = {
                dep.name: input_assets[dep.contract].asset._for_recipe(_Recipe)
                for dep in _parse_dependencies(_Recipe)
            }

            recipe_instance = _Recipe(workdir=path, **recipe_kwargs)
            _Asset = _Recipe._makes

            try:
                _res = recipe_instance.make()
            except Exception as e:
                raise RuntimeError(f"Failed to make asset '{_Asset}' with recipe '{_Recipe}'") from e

            if isinstance(_res, Asset):
                # transfer ownership of tmpdir cleanup to the AssetRecord via stack
                return AssetRecord(asset=_res, stack=stack)

            is_context_manager = callable(getattr(_res, "__enter__", None)) and callable(getattr(_res, "__exit__", None))
            if is_context_manager:
                try:
                    asset = stack.enter_context(_res)
                except Exception as e:
                    raise RuntimeError(f"Failed to make asset '{_Asset}' with recipe '{_Recipe}'") from e

                # Transfer ownership: AssetRecord will close the stack during cleanup
                return AssetRecord(asset=asset, stack=stack)

            raise TypeError(f"Recipe '{_Recipe}' produced an asset of invalid type {type(_res)}")

        except Exception:
            stack.close()
            raise

    def _resolve_build_path(self, recipe: type[Recipe]):
        root_path = self.root

        if recipe._dir is None:
            tmpdir = TemporaryDirectory()
            path = Path(tmpdir.name).resolve()
            return path, tmpdir

        if root_path is None:
            raise ValueError("Recipe requires persistent workdir, but Plan has no root path set")

        if recipe._shared:
            path = (root_path / 'shared' / recipe._dir).resolve()
        else:
            if self.project is None:
                raise ValueError(f"Recipe workdir is project-specific, but Plan has no project set")
            path = (root_path / 'projects' / self.project / recipe._dir).resolve()

        if not path.is_relative_to(root_path):
            raise ValueError(f"Recipe workdir path '{path}' escapes root")
        # Allow creation of missing relative path components
        path.mkdir(exist_ok=True, parents=True)

        return path, None
