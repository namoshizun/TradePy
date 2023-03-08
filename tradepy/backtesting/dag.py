import inspect
import networkx as nx
from typing import TYPE_CHECKING, Generator

if TYPE_CHECKING:
    from tradepy.backtesting.strategy import StrategyBase


IndicatorsResolution = dict[str, list[str]]  # mapping of indicator => its predecessors

Edge = tuple[str, str]


class Node:

    def __init__(self, name: str) -> None:
        self.name = name

    def resolve(self, strategy: "StrategyBase") -> Generator[tuple["Node", Edge | None], None, None]:
        if not hasattr(strategy, self.name):
            # No compute method for this indicator is defined.
            # Assume this indicator is computed outside of the DAG, hence unable to infer its predecessors
            yield self, None
            return

        # Infer predecessors recursively
        method = getattr(strategy, self.name)
        for predecessor in inspect.getfullargspec(method).args[1:]:
            yield self, (predecessor, str(self))
            yield from Node(predecessor).resolve(strategy)

    def __str__(self) -> str:
        return self.name


class IndicatorsResolver:

    def __init__(self, strategy: "StrategyBase") -> None:
        self.indicators_graph = self.compute_graph(strategy)

    def compute_graph(self, strategy: "StrategyBase"):
        all_indicators = strategy.buy_indicators + strategy.close_indicators
        G = nx.DiGraph()

        nodes, edges = set(), set()

        for ind in all_indicators:
            for node, edge in Node(ind).resolve(strategy):
                nodes.add(str(node))
                if edge:
                    edges.add(edge)

        G.add_nodes_from(map(str, nodes))
        G.add_edges_from(edges)
        return G

    def get_compute_order(self) -> dict[str, list[str]]:
        return {
            ind: list(self.indicators_graph.predecessors(ind))
            for ind in nx.topological_sort(self.indicators_graph)
        }
