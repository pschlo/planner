from __future__ import annotations
from dataclasses import dataclass
from collections.abc import Sequence
import networkx as nx
import logging
from typing import Self, TYPE_CHECKING
from contextlib import ExitStack

from ..asset import Asset
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
        defer_cleanup: bool = False  # Unload and cleanup assets as soon as they are not needed anymore
    ) -> None:
        self.graph = graph
        self.seq = seq
        self.node_to_asset: dict[GraphNode, AssetRecord] = {}
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
        stack = ExitStack()

        try:
            # create dependencies
            input_assets: dict[Contract, AssetRecord[Asset]] = {
                c: self.node_to_asset[u]
                for u, _, c in self.graph.in_edges(node, keys=True)
            }
            recipe_kwargs: dict[str, Asset] = {
                dep.name: input_assets[dep.contract].asset
                for dep in _parse_dependencies(_Recipe)
            }

            recipe_instance = _Recipe(**recipe_kwargs)
            _Asset = _Recipe._makes

            try:
                _res = recipe_instance.make()
            except Exception as e:
                raise RuntimeError(f"Failed to make asset '{_Asset}' with recipe '{_Recipe}'") from e

            if isinstance(_res, Asset):
                # transfer ownership of cleanup to the AssetRecord via stack
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
