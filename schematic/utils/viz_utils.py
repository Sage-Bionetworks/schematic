"""viz utils"""

from typing import Optional, Iterable, Sequence
import graphviz  # type: ignore


def visualize(
    edges: Iterable[Sequence[str]], size: Optional[float] = None
) -> graphviz.Digraph:
    """Creates a digraph with an edge for every edge in the edges input

    Args:
        edges (Iterable[Sequence[str]]): Any iterable type that contains
          indexable types with at least two strings
        size (Optional[float], optional): Defaults to None.

    Returns:
        graphviz.Digraph:
    """
    if size:
        digraph = graphviz.Digraph(graph_attr=[("size", size)])
    else:
        digraph = graphviz.Digraph()

    for item in edges:
        digraph.edge(item[0], item[1])
    return digraph
