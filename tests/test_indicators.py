import pytest
import networkx as nx
from tradepy.core.indicator import (
    Indicator,
    IndicatorSet,
)


@pytest.fixture
def sample_ma60_indicators():
    """
    close -> ma60 -> ma60 price percentile
                  -> ma60 slope
    """
    close_price = Indicator(name="close")
    ma60 = Indicator(name="ma60", predecessors=["close"])
    ma60_percentile = Indicator(name="ma60_percentile", predecessors=["ma60"])
    ma60_slope = Indicator(name="ma60_slope", predecessors=["ma60"])
    return [close_price, ma60, ma60_percentile, ma60_slope]


def test_basic_indicator():
    # Test param validation
    ind = Indicator(name="TestIndicator", outputs=["Output1", "Output2"])

    assert ind.name == "TestIndicator"
    assert ind.is_multi_output
    assert ind.outputs == ["Output1", "Output2"]
    assert ind.predecessors == []

    # Outputs is itself if not set explicitly
    ind = Indicator(name="TestIndicator")
    assert ind.outputs == ["TestIndicator"]


def test_indicator_set(sample_ma60_indicators: list[Indicator]):
    ind_set = IndicatorSet(*sample_ma60_indicators)

    # Test adding new indicators to the set
    ma20 = Indicator(name="ma20")
    ind_set.add(ma20)
    assert ma20 in ind_set

    # Test get method
    retrieved_ind = ind_set.get("ma20")
    assert retrieved_ind is not None
    assert retrieved_ind.name == "ma20"

    # Test computation graph typology
    graph = ind_set.build_graph()
    all_indicators = set(ind.name for ind in sample_ma60_indicators) | {"ma20"}
    all_dependencies = {
        (pred_ind, ind.name)
        for ind in sample_ma60_indicators
        for pred_ind in ind.predecessors
    }

    assert isinstance(graph, nx.DiGraph)
    assert set(graph.nodes) == all_indicators
    assert set(graph.edges) == all_dependencies

    # Test execution order is properly resolved
    sorted_indicators = [ind.name for ind in ind_set.sort_by_execute_order()]
    for ind in sample_ma60_indicators:
        for pred in ind.predecessors:
            pred_compute_order = sorted_indicators.index(pred)
            ind_compute_order = sorted_indicators.index(ind.name)
            assert pred_compute_order < ind_compute_order
