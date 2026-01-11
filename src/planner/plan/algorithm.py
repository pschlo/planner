from __future__ import annotations
from dataclasses import dataclass
import networkx as nx
from collections.abc import Sequence
from collections import deque
from typing import cast

from ..recipe import Recipe
from .fitness_check import strict_order_match_score
from .common import Contract, _parse_dependencies


type MultiPathNode = tuple[GraphNode, GraphNode, Contract]

@dataclass(frozen=True, eq=False)
class GraphNode:
    recipe: type[Recipe]
    context: set[tuple[Contract, ...]]


@dataclass
class RecipePick:
    recipe: type[Recipe]
    fitness: float


@dataclass
class ExistingNodePick:
    node: GraphNode
    fitness: float


class _PlanningAlgorithm:
    """Greedy context-aware DAG constructor used by `Planner.plan()`.

    Starting from the target node, it walks dependencies, picks best-fit
    recipes based on a context-match fitness score, reuses compatible nodes
    when possible, and performs *splits* to isolate subgraphs when a better
    context-specific recipe appears along only some paths.
    """
    G: nx.MultiDiGraph[GraphNode]
    target_node: GraphNode
    queue: deque[tuple[MultiPathNode, ...]]
    recipe_to_context: dict[type[Recipe], set[tuple[Contract, ...]]]
    contract_to_recipes: dict[Contract, set[type[Recipe]]]

    def __init__(self, target_recipe: type[Recipe], contract_to_recipes: dict[Contract, set[type[Recipe]]], recipe_to_context: dict[type[Recipe], set[tuple[Contract, ...]]]) -> None:
        self.target_node = GraphNode(target_recipe, context={()})
        self.G = nx.MultiDiGraph()
        self.G.add_node(self.target_node)
        self.queue = deque([()])
        self.contract_to_recipes = contract_to_recipes
        self.recipe_to_context = recipe_to_context

    def run(self):
        while self.queue:
            parent_path = self.queue.popleft()
            parent_node = parent_path[0][0] if parent_path else self.target_node

            # If the path does not exist anymore, skip
            if not all(self.G.has_edge(*e) for e in parent_path):
                continue

            # print()
            # print(f"PARENT NODE IS: {parent_node}")
            # print(f"LOCATION PATH IS:")
            # for x in parent_path:
                # print(f"    {x}")
            for dep in _parse_dependencies(parent_node.recipe):
                self.satisfy_dependency(parent_node, dep.contract, parent_path)

        # Finally, prune the graph to keep only nodes that can reach the target.
        # This should not happen, but just to be sure.
        _keep = nx.ancestors(self.G, self.target_node) | {self.target_node}
        _remove = set(self.G) - _keep
        # print(f"PRUNING {len(_remove)} nodes")
        self.G.remove_nodes_from(_remove)

        return self.G


    def satisfy_dependency(self, parent_node: GraphNode, contract: Contract, parent_path: tuple[MultiPathNode, ...]):
        """Satisfy the given contract on the given parent, depending on the parent_path."""
        # print(f"CONTRACT: {dep.contract}")

        # Pick best-fit recipe to satisfy the dependency.
        picked_recipe = self.pick_recipe(
            contract=contract,
            path=parent_path
        )
        if picked_recipe is None:
            # Format error message
            node_path = [u for (u, _, _) in parent_path] + [self.target_node]
            contracts_path = [contract] + [c[2] for c in parent_path]
            parts = ["MISSING_RECIPE"]
            parts += [f"--{c}-> {n.recipe}" for n, c in zip(node_path, contracts_path)]
            path_str = " ".join(parts)
            raise ValueError(f"Missing recipe for dependency {contract}. Needed because: {path_str}")

        # Pick an existing node that we can reuse because it is just as good or even better than the picked recipe.
        reuse_node = self.pick_existing_node(
            picked_recipe=picked_recipe,
            path=parent_path,
        )

        # CASE 1: Contract is already fulfilled. Then check if a best-fit one was used. If not, split and use our pick or pick existing node.
        # CASE 2: Contract is not yet fulfilled. Either reuse existing node or create new node.

        # Get the node that is currently used to satisfy the dependency, if it exists.
        curr_child_node = next(iter(
            n for n, _, c in self.G.in_edges(parent_node, keys=True) if c == contract
        ), None)

        if curr_child_node is None:
            # print("Contract not yet fulfilled")
            if reuse_node is not None:
                # print("An existing node is a good fit and can be reused")
                child_node = reuse_node.node
                self.add_edge(child_node, parent_node, contract, parent_path=parent_path)
                # print(f"Added edge from {child_node} to {parent_node} with contract {dep.contract}")
            else:
                # print("Could not find an existing node to reuse, creating new node")
                child_node = GraphNode(
                    recipe=picked_recipe.recipe,
                    context=self.recipe_to_context[picked_recipe.recipe]
                )
                self.add_edge(child_node, parent_node, contract, parent_path=parent_path)

        else:
            # print("Contract is already satisfied; checking if best-fit")
            _curr_fitness = self.compute_fitness(
                context=curr_child_node.context,
                path=parent_path,
            )
            # print("curr_child_node context:", curr_child_node.context)
            if reuse_node is not None and reuse_node.fitness > _curr_fitness:
                # print("A different existing node is a better fit than the currently used recipe node")
                # print("reuse_node fitness:", reuse_node.fitness)
                # print("curr fitness:", _curr_fitness)
                isolating_edges = self.compute_isolating_edge(
                    context=reuse_node.node.context,
                    parent_node=parent_node,
                    curr_child_node=curr_child_node
                )
                new_parent = self.perform_split(
                    parent_node=parent_node,
                    isolating_edges=isolating_edges,
                    context=reuse_node.node.context,
                    curr_child_edge=(curr_child_node, parent_node, contract),
                )
                child_node = reuse_node.node
                self.add_edge(child_node, new_parent, contract, parent_path=parent_path)

            elif picked_recipe.fitness > _curr_fitness:
                # print("Creating a new recipe node is a better fit than the currently used recipe node")
                isolating_edges = self.compute_isolating_edge(
                    context=self.recipe_to_context[picked_recipe.recipe],
                    parent_node=parent_node,
                    curr_child_node=curr_child_node
                )
                new_parent = self.perform_split(
                        parent_node=parent_node,
                        isolating_edges=isolating_edges,
                        context=self.recipe_to_context[picked_recipe.recipe],
                        curr_child_edge=(curr_child_node, parent_node, contract),
                )
                child_node = GraphNode(
                    recipe=picked_recipe.recipe,
                    context=self.recipe_to_context[picked_recipe.recipe]
                )
                self.add_edge(child_node, new_parent, contract, parent_path=parent_path)
            else:
                # print("Currently used recipe node is already best fit")
                child_node = curr_child_node
                self.use_edge(child_node, parent_node, contract, parent_path=parent_path)


    def pick_recipe(self, contract: Contract, path: Sequence[MultiPathNode]) -> RecipePick | None:
        """For the location given by `path`, pick a suitable recipe to satisfy `contract` that is as fitting as possible."""

        # Compute fitness for each recipe
        max_fitness: float = 0
        best_recipes: set[type[Recipe]] = set()

        if contract not in self.contract_to_recipes:
            return None

        for r in self.contract_to_recipes[contract]:
            _context = self.recipe_to_context[r]
            _fitness = self.compute_fitness(_context, path)
            if _fitness == 0:
                # print("SKIPPING because fitness is 0")
                continue
            if _fitness == max_fitness:
                # print("ADDING")
                best_recipes.add(r)
            elif _fitness > max_fitness:
                # print("ADDING")
                max_fitness = _fitness
                best_recipes.clear()
                best_recipes.add(r)

        if len(best_recipes) > 1:
            raise RuntimeError("Found multiple best-fit recipe records")
        if not best_recipes:
            return None
        best_recipe = next(iter(best_recipes))
        
        return RecipePick(
            recipe=best_recipe,
            fitness=max_fitness,
        )


    def compute_fitness(self, context: set[tuple[Contract, ...]], path: Sequence[MultiPathNode]) -> float:
        """Score how well a recipe's context set matches the current path."""

        contracts_path = [c[2] for c in path] + [(self.target_node.recipe._makes, None)]
        fitness = max(
            strict_order_match_score(context_path[::-1], contracts_path[::-1], epsilon=1e-9, early_tie_breaker=0.1)
            for context_path in context
        )

        return fitness


    def compute_isolating_edge(self, context: set[tuple[Contract, ...]], parent_node: GraphNode, curr_child_node: GraphNode) -> set[MultiPathNode]:
        """Find a minimal set of edges that separates better-matching paths.

        Given the candidate context and the currently used child node, identify
        edges whose redirection isolates all paths for which the candidate has
        strictly higher fitness than the current node. Used by `perform_split`."""
        # print("ISOLATING EDGE")
        # print("context_path (a):")
        # for x in context:
            # print("  " + str(x))
        # print("location_path (b):")
        # for x in path:
        #     print("  " + str(x))

        # Determine isolating edge

        # Find all paths that the context path matches
        # Paths that match the existing child node better are not considered as matching
        # Thus: Find all paths that the context path matches better than the current node


        # Isolating edge of matching path P = Edge that is traversed by path P (and possibly other matching paths), but NOT by any nonmatching paths.
        # I.e., an edge that separates P from all nonmatching paths.
        matching_paths: set[tuple[MultiPathNode, ...]] = set()

        # Stores edges that are isolating edges for one or more paths
        isolating_edges: dict[MultiPathNode, set[tuple[MultiPathNode, ...]]] = {}

        nonmatching: set[MultiPathNode] = set()
        for _path in nx.all_simple_edge_paths(self.G, parent_node, self.target_node):
            _path = cast(list[MultiPathNode], _path)
            _path = tuple(_path)
            _curr_fitness = self.compute_fitness(curr_child_node.context, _path)
            _fitness = self.compute_fitness(context, _path)
            # print(f"    Path has fitness {_fitness}")
            if _fitness > 0 and _fitness > _curr_fitness:
                # This path is matched by context.
                # Each edge is potentially an isolating edge for the path.
                # print("Matching path")
                matching_paths.add(_path)
                # for x in _path:
                    # print("    " + str(x))
                for edge in _path:
                    if edge not in nonmatching:
                        # Edge may be isolating edge
                        if edge not in isolating_edges:
                            isolating_edges[edge] = set()
                        isolating_edges[edge].add(_path)
            else:
                # Each edge in the path cannot be an isolating edge anymore
                for edge in _path:
                    nonmatching.add(edge)
                    if edge in isolating_edges:
                        del isolating_edges[edge]

        # dict now contains only edges are matched only by matching paths
        # check if all matching paths have an isolating edge
        # I.e. there may be a matching path whose edges all got removed again and of which there are no edges in the dict now

        # print()
        # print(f"Number of possible paths:", len(list(nx.all_simple_edge_paths(graph, parent_node, target_node))))
        # print()
        # print("MATCHING PER EDGE")
        # print(isolating_edges)
        # print()
        # print("NONMATCHING")
        # print(nonmatching)


        # GOAL: for each matching path, must find an isolating edge (edge that matches the path but no nonmatches)
        # May also be that one isolating edge covers multiple paths

        # iteratively pick the isolating edge that covers the most matching paths
        # in the end, there are no isolating edges left. Did we cover all matching paths?


        picked_edges: set[MultiPathNode] = set()
        while isolating_edges:
            best_edge = max(isolating_edges, key=lambda e: len(isolating_edges[e]))
            assert isolating_edges[best_edge]  # cannot be empty
            picked_edges.add(best_edge)
            for path in isolating_edges[best_edge].copy():
                # Remove every path covered by this isolating edge
                matching_paths.remove(path)
                for e in path:
                    if e in isolating_edges:
                        isolating_edges[e].remove(path)
                        if not isolating_edges[e]:
                            # remove empty sets
                            del isolating_edges[e]
            assert best_edge not in isolating_edges

        # Every matching path should have been covered by an isolating edge
        if matching_paths:
            raise ValueError(f"No isolating edge for path {next(iter(matching_paths))}")


        # print("PICKED EDGES")
        # print(picked_edges)
        # print()
        return picked_edges




    def add_edge(self, child: GraphNode, parent: GraphNode, contract: Contract, parent_path: tuple[MultiPathNode, ...]):
        G = self.G

        satisfied_contracts = {c for _, _, c in G.in_edges(parent, keys=True)}
        if contract in satisfied_contracts:
            raise ValueError(f"Contract {contract} is already satisfied for node {parent}")
        G.add_edge(child, parent, key=contract)
        self.use_edge(child, parent, contract, parent_path=parent_path)

    def use_edge(self, child: GraphNode, parent: GraphNode, contract: Contract, *, parent_path: tuple[MultiPathNode, ...]):
        if (child, parent, contract) in parent_path:
            raise ValueError("Cycle detected")
        self.queue.append(
            ((child, parent, contract), *parent_path)
        )


    def perform_split(self, parent_node: GraphNode, isolating_edges: set[MultiPathNode], context: set[tuple[Contract, ...]], curr_child_edge: MultiPathNode) -> GraphNode:
        """Duplicate a subgraph to route better-fitting contexts.

        Copies the subgraph reachable from `parent_node` along the matched paths,
        tags the copies with `context`, rewires `isolating_edges` to the copies,
        and returns the *new* parent copy (from which we keep building)."""
        G = self.G
        
        # print()
        # print("PERFORMING SPLIT")
        # print("parent_node:", parent_node)
        # print("isolating_edges:", isolating_edges)
        # print("context:", context)
        # print()

        # Define subgraph to be duplicated
        _ancestors: set[GraphNode] = set()
        for e in isolating_edges:
            _ancestors |= nx.ancestors(G, e[1])
        H = cast("nx.MultiDiGraph[GraphNode]", G.subgraph(
            (nx.descendants(G, parent_node) & _ancestors) | {parent_node}
        ))

        # Duplicate the subgraph nodes
        _node_copies: dict[GraphNode, GraphNode] = {
            _n: GraphNode(_n.recipe, context=context) for _n in H.nodes
        }
        G.add_nodes_from(_node_copies.values())

        # Replicate any edges within the subgraph
        G.add_edges_from(
            (_node_copies[u], _node_copies[v], c) for u, v, c in set(H.edges(keys=True))
        )

        # Replicate edges going from outside into subgraph, EXCEPT for the edge currently providing the contract
        G.add_edges_from(
            (u, _node_copies[v], c)
            for u, v, c in set(nx.edge_boundary(G, set(G) - set(H), set(H), keys=True))  # type: ignore
            if (u, v, c) != curr_child_edge
        )
        
        # Remove old isolating edge and re-insert to point to new subgraph
        for edge in isolating_edges:
            G.remove_edge(*edge)
            G.add_edge(_node_copies[edge[0]], edge[1], edge[2])

        return _node_copies[parent_node]
    


    def pick_existing_node(self, picked_recipe: RecipePick, path: Sequence[MultiPathNode]) -> ExistingNodePick | None:
        """Prefer reuse: find an existing node of same Recipe with ≥ fitness."""

        picked_node: GraphNode | None = None
        picked_fitness: float = 0

        for node in self.G:
            if node.recipe == picked_recipe.recipe:
                _fitness = self.compute_fitness(node.context, path)
                if _fitness >= picked_recipe.fitness and (picked_node is None or _fitness > picked_fitness):
                    picked_node = node
                    picked_fitness = _fitness

        if picked_node is None:
            return None
        return ExistingNodePick(
            node=picked_node,
            fitness=picked_fitness,
        )
