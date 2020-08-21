import graphviz

def visualize(edges, size=None):
    if size:
        d = graphviz.Digraph(graph_attr=[('size', size)])
    else:
        d = graphviz.Digraph()
        
    for _item in edges:
        d.edge(_item[0], _item[1])
    return d