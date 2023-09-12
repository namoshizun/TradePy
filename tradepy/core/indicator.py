import networkx as nx
from dataclasses import dataclass, field
from typing import Union


class IndicatorSet:
    def __init__(self, *indicators: "Indicator"):
        self.indicators = set(indicators)

    def add(self, indicator: "Indicator"):
        self.indicators.add(indicator)

    def get(self, item: Union[str, "Indicator"]) -> Union["Indicator", None]:
        for ind in self.indicators:
            if isinstance(item, str) and ind.name == item:
                return ind
            elif ind == item:
                return ind

    def build_graph(self):
        G = nx.DiGraph()

        nodes, edges = set(), set()

        for ind in self.indicators:
            name = ind.name
            nodes.add(name)

            # Predecessors => Indicator
            for pred in ind.predecessors:
                nodes.add(pred)
                edges.add((pred, name))

            # Indicator => Successors
            for out in ind.outputs:
                if out != name:
                    nodes.add(out)
                    edges.add((name, out))

        G.add_nodes_from(nodes)
        G.add_edges_from(edges)
        return G

    def sort_by_execute_order(
        self, target_list: list[str] | None = None
    ) -> list["Indicator"]:
        G = self.build_graph()
        res = []

        for node in nx.topological_sort(G):
            if ind := self.get(node):
                if not target_list:
                    res.append(ind)
                elif node in target_list:
                    # Right in the target list, yay
                    res.append(ind)
                else:
                    outputs = G.successors(node)
                    if any(o in target_list for o in outputs):
                        # An intermediate node whose output is useful, cool~
                        res.append(ind)

        return res

    def __contains__(self, item: Union["Indicator", str]) -> bool:
        if isinstance(item, str):
            return any(ind.name == item for ind in self.indicators)
        return item in self.indicators

    def __or__(self, other: Union["Indicator", "IndicatorSet"]) -> "IndicatorSet":
        if isinstance(other, Indicator):
            other = IndicatorSet(other)
        return IndicatorSet(*(self.indicators | other.indicators))

    def __str__(self) -> str:
        return f"IndicatorSet({', '.join(map(str, self.indicators))})"

    def __repr__(self) -> str:
        return str(self)

    def __iter__(self):
        return iter(self.indicators)


@dataclass
class Indicator:
    name: str

    notna: bool = False
    outputs: list[str] = field(default_factory=list)
    predecessors: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.outputs:
            self.outputs = [self.name]

    @property
    def is_multi_output(self) -> bool:
        return len(self.outputs) > 1

    def __hash__(self) -> int:
        return hash(self.name)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return str(self)

    def __or__(self, other: Union["Indicator", "IndicatorSet"]) -> "IndicatorSet":
        return IndicatorSet(self) | other
