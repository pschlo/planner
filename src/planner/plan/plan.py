from __future__ import annotations
from dataclasses import dataclass
from contextlib import contextmanager
import networkx as nx
from pathlib import Path
import logging
import random
from typing import TYPE_CHECKING
from collections.abc import Generator
from matplotlib import pyplot as plt

from ..asset import Asset
from .execution import PlanExecution
if TYPE_CHECKING:
    from .algorithm import GraphNode

log = logging.getLogger(__name__)


@dataclass
class DrawCounter:
    value: int = 0

    def inc(self):
        self.value += 1

    def __str__(self) -> str:
        return str(self.value)

DRAW_COUNTER = DrawCounter()


class Plan[T: Asset]:
    """An executable, acyclic build plan for a target Asset.

    Wraps the planned graph and provides:
      - `run()` as a context manager that builds nodes in topological order,
        returns the final Asset, and guarantees cleanup.
      - `draw()` to persist a labeled visualization of the DAG (for debugging).
    """
    def __init__(self, graph: nx.MultiDiGraph[GraphNode], root: Path | str | None = None, project: str | None = None) -> None:
        if root is not None:
            root = Path(root)
            if not root.is_absolute():
                raise ValueError(f"Root path must be absolute")
            root = root.resolve()
        self.root = root

        self.project = project

        assert nx.is_directed_acyclic_graph(graph)
        _target_nodes = {node for node, deg in graph.out_degree if deg == 0}
        assert len(_target_nodes) == 1
        self.graph = graph

    @contextmanager
    def run(self) -> Generator[T]:
        """Execute the plan, yielding the final built Asset.

        All upstream assets (including generator-based recipes) are cleaned up
        when the context exits, in reverse topological order."""
        order = list(nx.topological_sort(self.graph))
        with PlanExecution(graph=self.graph, seq=order, root=self.root, project=self.project) as e:
            record = e.run()
            yield record.asset

    
    def draw(self, folder: Path | str = "."):
        """Save a PNG of the DAG using a layered (multipartite) layout."""
        folder = Path(folder)

        log.info("Drawing plan")
        # return
        G = self.graph
        # print(f"DRAWING {str(DRAW_COUNTER)} ({len(G)} nodes)")
        for layer, nodes in enumerate(nx.topological_generations(G)):
            # `multipartite_layout` expects the layer as a node attribute, so add the
            # numeric layer value as a node attribute
            for node in nodes:
                self.graph.nodes[node]["layer"] = layer

        # Compute the multipartite_layout using the "layer" node attribute
        pos = nx.multipartite_layout(G, subset_key="layer")
        for node, p in pos.items():
            pos[node][1] += (random.random() - 0.5) / 10

        fig, ax = plt.subplots()
        fig.set_size_inches(19.20, 10.80)
        nx.draw_networkx(G, pos=pos, ax=ax, with_labels=False, arrowsize=5, width=0.5, node_size=1000)
        text = nx.draw_networkx_labels(G, pos=pos, ax=ax, labels={node: str(node.recipe) for node in G}, font_size=10)
        for _, t in text.items():
            t.set_rotation('vertical')

        _labels: dict[tuple[GraphNode, GraphNode], str] = {}
        for u, v, c in G.edges(keys=True):
            if (u, v) in _labels:
                _labels[(u, v)] += f",{c[1]}"
            else:
                _labels[(u, v)] = f"{str(c[0]).removesuffix("Asset")}-{c[1]}"
        text = nx.draw_networkx_edge_labels(G, pos=pos, ax=ax, rotate=True, edge_labels=_labels, font_size=4)
        # for _, t in text.items():
        #     t.set_rotation('vertical')
        ax.set_title("DAG layout in topological order")
        fig.tight_layout()


        folder.mkdir(exist_ok=True)
        plt.savefig(folder / f"graph-{DRAW_COUNTER}.png", bbox_inches="tight", dpi=300)
        plt.close()
        DRAW_COUNTER.inc()
