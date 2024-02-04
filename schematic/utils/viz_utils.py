"""viz utils"""

from typing import Optional
import graphviz


def visualize(edges, size: Optional[float] = None) -> graphviz.Digraph:
    """_summary_

    Args:
        edges (_type_): _description_
        size (Optional[float], optional): _description_. Defaults to None.

    Returns:
        graphviz.Digraph: _description_
    """
    if size:
        digraph = graphviz.Digraph(graph_attr=[("size", size)])
    else:
        digraph = graphviz.Digraph()

    for _item in edges:
        digraph.edge(_item[0], _item[1])
    return digraph
