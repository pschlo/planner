from __future__ import annotations
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from collections.abc import Generator, Sequence
import networkx as nx
import logging
from typing import Self, TYPE_CHECKING, ContextManager, Callable
from contextlib import nullcontext, AbstractContextManager
from pathlib import Path

from ..asset import Asset, Recipe
from .common import _parse_dependencies, Contract
if TYPE_CHECKING:
    from .algorithm import GraphNode

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AssetRecord[T: Asset]:
    asset: T
    contextmanager: ContextManager | None
    tmpdir: TemporaryDirectory | None

    def _cleanup(self):
        err = None
        try:
            self._cleanup_generator()
        except Exception as e:
            err = e
            raise
        finally:
            if self.tmpdir is not None:
                try:
                    self.tmpdir.cleanup()
                except Exception:
                    # log it, but don't mask the original error
                    log.error("Temporary workdir cleanup failed")
                    if err is None:
                        raise

    def _cleanup_generator(self):
        if self.contextmanager is not None:
            self.contextmanager.__exit__(None, None, None)


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
        eager_cleanup: bool = True  # Unload and cleanup assets as soon as they are not needed anymore
    ) -> None:
        self.graph = graph
        self.seq = seq
        self.node_to_asset: dict[GraphNode, AssetRecord] = {}
        self.root = root
        self.project = project
        self.eager_cleanup = eager_cleanup
    
    @property
    def target(self):
        return self.seq[-1]

    def __enter__(self: Self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.cleanup()
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

        # how many consumers each node has
        remaining_uses = {n: self.graph.out_degree(n) for n in self.seq}

        for node in self.seq:
            rec = self._build_node(node)
            self.node_to_asset[node] = rec

            # after building this node, its inputs might be freeable
            for u, _, _ in self.graph.in_edges(node, keys=True):
                remaining_uses[u] -= 1
                assert remaining_uses[u] >= 0  # cannot be negative

                if self.eager_cleanup and remaining_uses[u] == 0 and u is not self.target:
                    # Unregister loaded asset
                    rec = self.node_to_asset.pop(u, None)
                    if rec is None:
                        continue

                    # Clean up asset
                    log.debug(f"Cleaning up {u}")
                    try:
                        rec._cleanup()
                    except Exception as e:
                        log.exception(f"Cleanup failed for {u}")

        return self.node_to_asset[self.target]


    def cleanup(self):
        log.info("Cleaning up assets")
        errors: list[Exception] = []

        for node in self.seq[::-1]:
            # Unregister loaded asset
            rec = self.node_to_asset.pop(node, None)
            if rec is None:
                continue

            # Clean up asset
            log.debug(f"Cleaning up {node}")
            try:
                rec._cleanup()
            except Exception as e:
                log.error(f"Cleanup failed for {node}")
                errors.append(e)

        assert not self.node_to_asset

        if errors:
            raise ExceptionGroup("Cleanup failed", errors)


    def _build_node(self, node: GraphNode) -> AssetRecord:
        _Recipe = node.recipe
        path, temp_dir = self._resolve_build_path(_Recipe)

        # create dependencies
        input_assets: dict[Contract, AssetRecord] = {
            c: self.node_to_asset[u]
            for u, _, c in self.graph.in_edges(node, keys=True)
        }
        recipe_kwargs: dict[str, Asset] = {
            dep.name: input_assets[dep.contract].asset
            for dep in _parse_dependencies(_Recipe)
        }

        recipe_instance = _Recipe(workdir=path, **recipe_kwargs)

        try:
            _res = recipe_instance.make()
        except Exception as e:
            _Asset = _Recipe._makes
            raise RuntimeError(f"Failed to make asset '{_Asset}' with recipe '{_Recipe}'") from e

        if isinstance(_res, AbstractContextManager):
            asset = _res.__enter__()
            return AssetRecord(asset=asset, contextmanager=_res, tmpdir=temp_dir)
        elif isinstance(_res, Asset):
            return AssetRecord(asset=_res, contextmanager=None, tmpdir=temp_dir)
        else:
            raise RuntimeError(f"Recipe '{_Recipe}' produced an asset of invalid type {type(_res)}")
        

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
