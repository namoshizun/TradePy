import networkx as nx
import matplotlib.pyplot as plt
from networkx.drawing.nx_agraph import graphviz_layout


from tradepy.core.indicator import Indicator


class IndicatorsResolver:

    def __init__(self, indicators: list[Indicator]) -> None:
        self.indicators = {
            ind.name: ind
            for ind in indicators
        }

    def build_graph(self):
        G = nx.DiGraph()

        nodes, edges = set(), set()

        for name, ind in self.indicators.items():
            nodes.add(name)

            # Predecessors => Indicator
            for pred in ind.predecessors:
                nodes.add(pred)
                edges.add((pred, name))

            # Indicator => Sucessors
            for out in ind.outputs:
                if out != name:
                    nodes.add(out)
                    edges.add((name, out))

        G.add_nodes_from(nodes)
        G.add_edges_from(edges)
        return G

    def sort_by_execute_order(self, target_list: list[str] | None = None) -> list[Indicator]:
        G = self.build_graph()
        res = []

        for node in nx.topological_sort(G):
            if ind := self.indicators.get(node):
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

    def export_depndency_graph(self, path: str, **draw_kws):
        G = self.build_graph()
        args = dict(
            pos=graphviz_layout(G),
            with_labels=True,
            font_size=5,
            node_size=225,
        )

        args.update(draw_kws)
        nx.draw(G, **args)
        plt.savefig(path, dpi=300)
