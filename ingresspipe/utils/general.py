# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

import graphviz

def find_duplicates(_list):
    """Find duplicate items in a list
    """
    return set([x for x in _list if _list.count(x) > 1])


def dict2list(dictionary):
    if type(dictionary) == list:
        return dictionary
    elif type(dictionary) == dict:
        return [dictionary]


def str2list(_str):
    if type(_str) == str:
        return [_str]
    elif type(_str) == list:
        return _str


def unlist(_list):
    if len(_list) == 1:
        return _list[0]
    else:
        return _list


def visualize(edges, size=None):
    if size:
        d = graphviz.Digraph(graph_attr=[('size', size)])
    else:
        d = graphviz.Digraph()
        
    for _item in edges:
        d.edge(_item[0], _item[1])
    return d