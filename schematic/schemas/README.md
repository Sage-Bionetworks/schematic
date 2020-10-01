## Usage of methods in `schematic.schemas.explorer` module

Path to the data model/schema that you want to load using the `SchemaExplorer` object:

```python
PATH_TO_JSONLD = os.path.join(DATA_PATH, config_data["model"]["input"]["location"])
```

Create an object of the SchemaExplorer() class:

```python
schema_explorer = SchemaExplorer()
```

Check if object has been instantiated or not:

```python
if isinstance(schema_explorer, SchemaExplorer):
    print("'schema_explorer' - an object of the SchemaExplorer class has been created successfully.")
else:
    print("object of class SchemaExplorer could not be created.")    
```

By default schema exploerer loads the biothings schema. To explicitly load a different data model/json-ld schema, 
use `load_schema()`:

```python
schema_explorer.load_schema(PATH_TO_JSONLD)
print("schema at {} has been loaded.".format(PATH_TO_JSONLD))
```

Get the networkx graph generated from the json-ld:

```python
nx_graph = schema_explorer.get_nx_schema()
```

Check if `nx_graph` has been instantiated correctly:

```python
if isinstance(nx_graph, nx.MultiDiGraph):
    print("'nx_graph' - object of class MultiDiGraph has been retreived successfully.")
else:
    print("object of class SchemaExplorer could not be retreived.")
```

Check if a particular class is in the current `HTAN JSON-LD` schema (or any schema that has been loaded):

```python
TEST_CLASS = 'Sequencing'
is_or_not = schema_explorer.is_class_in_schema(TEST_CLASS)

if is_or_not == True:
    print("The class {} is present in the schema.".format(TEST_CLASS))
else:
    print("The class {} is not present in the schema.".format(TEST_CLASS))
```

Generate graph visualization of the entire HTAN JSON-LD schema using `graphviz` package:

```python
gv_digraph = schema_explorer.full_schema_graph()
```

Since the graph is very big, we will generate an svg viz. of it. Please allow some time for the visualization to be rendered:

```python
gv_digraph.format = 'svg'
gv_digraph.render(os.path.join(DATA_PATH, '', 'viz/HTAN-GV'), view=True)
print("The svg visualization of the entire schema has been rendered.")
```

_Note: The above visualization is too big to be rendered here, but see the sub-schema below for a small visualization._

Generate graph visualization of a sub-schema:

```python
seq_subgraph = schema_explorer.sub_schema_graph(TEST_CLASS, "up")

seq_subgraph.format = 'svg'
seq_subgraph.render('SUB-GV', view=True)
print("The svg visualization of the sub-schema with {} as the source node has been rendered.".format(TEST_CLASS))
```

_Fig.: Output from the execution of above block of code_

![alt text](https://github.com/sujaypatil96/HTAN-data-pipeline/blob/develop/data/gviz/SUB-GV.png)

Returns list of successors of a node:

```python    
seq_children = schema_explorer.find_children_classes(TEST_CLASS)
print("These are the children of {} class: {}".format(TEST_CLASS, seq_children))
```

Returns list of parents of a node:

```python
seq_parents = schema_explorer.find_parent_classes(TEST_CLASS)
print("These are the parents of {} class: {}".format(TEST_CLASS, seq_parents))
```

Find the properties that are associated with a class:

```python
PROP_CLASS = 'BiologicalEntity'
class_props = schema_explorer.find_class_specific_properties(PROP_CLASS)
print("The properties associated with class {} are: {}".format(PROP_CLASS, class_props))
```

Find the schema classes that inherit from a given class:

```python
inh_classes = schema_explorer.find_child_classes("Assay")
print("classes that inherit from class 'Assay' are: {}".format(inh_classes))
```

Get all details about a specific class in the schema:

```python
class_details = schema_explorer.explore_class(TEST_CLASS)
print("information/details about class {} : {} ".format(TEST_CLASS, class_details))
```

Get all details about a specific property in the schema:

```python
TEST_PROP = 'increasesActivityOf'
prop_details = schema_explorer.explore_property(TEST_PROP)
print("information/details about property {} : {}".format(TEST_PROP, prop_details))
```

Get name/label of the property associated with a given class' display name:

```python
prop_label = schema_explorer.get_property_label_from_display_name("Basic Statistics")
print("label of the property associated with 'Basic Statistics': {}".format(prop_label))
```

Get name/label of the class associated with a given class' display name:

```python
class_label = schema_explorer.get_property_label_from_display_name("Basic Statistics")
print("label of the class associated with 'Basic Statistics': {}".format(class_label))
```

Generate template of class in schema:

```python
class_temp = schema_explorer.generate_class_template()
print("generic template of a class in the schema/data model: {}".format(class_temp))
```

Modified `TEST_CLASS ("Sequencing")` based on the above generated template:

```python
class_mod = {
                "@id": "bts:Sequencing",
                "@type": "rdfs:Class",
                "rdfs:comment": "Modified Test: Module for next generation sequencing assays",
                "rdfs:label": "Sequencing",
                "rdfs:subClassOf": [
                    {
                        "@id": "bts:Assay"
                    }
                ],
                "schema:isPartOf": {
                    "@id": "http://schema.biothings.io"
                },
                "sms:displayName": "Sequencing",
                "sms:required": "sms:false"
            }
```

Make edits to `TEST_CLASS` based on the above template and pass it to `edit_class()`: 

```python
schema_explorer.edit_class(class_info=class_mod)
```

Verify that the comment associated with `TEST_CLASS` has indeed been changed:

```python
class_details = schema_explorer.explore_class(TEST_CLASS)
print("Modified {} details : {}".format(TEST_CLASS, class_details))
```

## Usage of methods in `schematic.schemas.generator` module

Create an object of the `SchemaGenerator` class:

```python
schema_generator = SchemaGenerator(PATH_TO_JSONLD)
```

Check if object has been properly instantiated or not:

```python
if isinstance(schema_generator, SchemaGenerator):
    print("'schema_generator' - an object of the SchemaGenerator class has been created successfully.")
else:
    print("object of class SchemaGenerator could not be created.")
```

Get the list of out-edges from a specific node, based on a particular type of relationship:

```python
TEST_NODE = "Sequencing"
TEST_REL = "parentOf"

out_edges = schema_generator.get_edges_by_relationship(TEST_NODE, TEST_REL)

if out_edges:
    print("The out-edges from class {}, based on {} relationship are: {}".format(TEST_NODE, TEST_REL, out_edges))
else:
    print("The class does not have any out-edges.")
```

Get the list of nodes that are adjacent to a given node, based on a particular type of relationship:

```python
adj_nodes = schema_generator.get_adjacent_nodes_by_relationship(TEST_NODE, TEST_REL)

if adj_nodes:
    print("The node(s) adjacent to {}, based on {} relationship are: {}".format(TEST_NODE, TEST_REL, adj_nodes))
else:
    print("The class does not have any adjacent nodes.")
```

Get the list of descendants (all nodes that are reachable from a given node) of a node:

```python
desc_nodes = schema_generator.get_descendants_by_edge_type(TEST_NODE, TEST_REL)

if desc_nodes:
    print("The descendant(s) from {} are: {}".format(TEST_NODE, desc_nodes))
else:
    print("The class does not have descendants.")
```

Get the list of components that are associated with a given component:

```python
TEST_COMP = "Patient"
req_comps = schema_generator.get_component_requirements(TEST_COMP)

if req_comps:
    print("The component(s) that are associated with a given component: {}".format(req_comps))
else:
    print("There are no components associated with {}".format(TEST_COMP))
```

Get the list of immediate dependencies of a particular node:

```python
node_deps = schema_generator.get_node_dependencies(TEST_COMP)

if node_deps:
    print("The immediate dependencies of {} are: {}".format(TEST_COMP, node_deps))
else:
    print("The node has no immediate dependencies.")
```

Get the `label` associated with a node, based on the node's display name:

```python
try:
    node_label = schema_generator.get_node_label(TEST_NODE)

    print("The label name for the node {} is: {}".format(TEST_NODE, node_label))
except KeyError:
    print("Please try a valid node name.")
```

Get the `definition/comment` associated with a given node:

```python
try:
    node_def = schema_generator.get_node_definition(TEST_NODE)

    print("The node definition for node {} is: {}".format(TEST_NODE, node_def))
except KeyError:
    print("Please try a valid node name.")
```

Gather all the dependencies and value-constraints associated with a particular node

```python
json_schema = schema_generator.get_json_schema_requirements(TEST_COMP, "Patient-Schema")

print("The JSON schema based on {} as source node is:".format(TEST_COMP))
print(json_schema)
```