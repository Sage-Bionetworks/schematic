## Usage of methods in `schemas.explorer` module

Path to the data model/schema that you want to load using the `SchemaExplorer` object:

```python
PATH_TO_JSONLD = "./data/schema_org_schemas/HTAN.jsonld"
```

Create an object of the SchemaExplorer() class:
```python
schema_explorer = SchemaExplorer()
```

Check if object has been instantiated or not:

```python
if isinstance(schema_explorer, SchemaExplorer):
    logger.info("'schema_explorer' - an object of the SchemaExplorer class has been created successfully.")
else:
    logger.error("object of class SchemaExplorer could not be created.")    
```

By default schema exploerer loads the biothings schema. To explicitly load a different data model/json-ld schema, 
use `load_schema()`:

```python
schema_explorer.load_schema(PATH_TO_JSONLD)
logger.info("schema at {} has been loaded.".format(PATH_TO_JSONLD))
```

Get the networkx graph generated from the json-ld:

```python
nx_graph = schema_explorer.get_nx_schema()
```

Check if `nx_graph` has been instantiated correctly:

```python
if isinstance(nx_graph, nx.MultiDiGraph):
    logger.info("'nx_graph' - object of class MultiDiGraph has been retreived successfully.")
else:
    logger.error("object of class SchemaExplorer could not be retreived.")
```

Check if a particular class is in the current `HTAN JSON-LD` schema (or any schema that has been loaded):

```python
TEST_CLASS = 'Sequencing'
is_or_not = schema_explorer.is_class_in_schema(TEST_CLASS)

if is_or_not == True:
    logger.info("The class {} is present in the schema.".format(TEST_CLASS))
else:
    logger.error("The class {} is not present in the schema.".format(TEST_CLASS))
```

Generate graph visualization of the entire HTAN JSON-LD schema using `graphviz` package:

```python
gv_digraph = schema_explorer.full_schema_graph()
```

Since the graph is very big, we will generate an svg viz. of it. Please allow some time for the visualization to be rendered:

```python
gv_digraph.format = 'svg'
gv_digraph.render('./data/viz/HTAN-GV', view=True)
logger.info("The svg visualization of the entire schema has been rendered.")
```

_Note: The above visualization is too big to be rendered here, but see the sub-schema below for a small visualization._

Generate graph visualization of a sub-schema:

```python
seq_subgraph = schema_explorer.sub_schema_graph(TEST_CLASS, "up")

seq_subgraph.format = 'svg'
seq_subgraph.render('SUB-GV', view=True)
logger.info("The svg visualization of the sub-schema with {} as the source node has been rendered.".format(TEST_CLASS))
```

_Fig.: Output from the execution of above block of code_

![alt text](https://github.com/sujaypatil96/HTAN-data-pipeline/blob/organized-into-packages/data/gviz/SUB-GV.png)

Returns list of successors of a node:

```python    
seq_children = schema_explorer.find_children_classes(TEST_CLASS)
logger.info("These are the children of {} class: {}".format(TEST_CLASS, seq_children))
```

Returns list of parents of a node:

```python
seq_parents = schema_explorer.find_parent_classes(TEST_CLASS)
logger.info("These are the parents of {} class: {}".format(TEST_CLASS, seq_parents))
```

Find the properties that are associated with a class:

```python
PROP_CLASS = 'BiologicalEntity'
class_props = schema_explorer.find_class_specific_properties(PROP_CLASS)
logger.info("The properties associated with class {} are: {}".format(PROP_CLASS, class_props))
```

Find the schema classes that inherit from a given class:

```python
inh_classes = schema_explorer.find_child_classes("Assay")
logger.info("classes that inherit from class 'Assay' are: {}".format(inh_classes))
```

Get all details about a specific class in the schema:

```python
class_details = schema_explorer.explore_class(TEST_CLASS)
logger.info("information/details about class {} : {} ".format(TEST_CLASS, class_details))
```

Get all details about a specific property in the schema:

```python
TEST_PROP = 'increasesActivityOf'
prop_details = schema_explorer.explore_property(TEST_PROP)
logger.info("information/details about property {} : {}".format(TEST_PROP, prop_details))
```

Get name/label of the property associated with a given class' display name:

```python
prop_label = schema_explorer.get_property_label_from_display_name("Basic Statistics")
logger.info("label of the property associated with 'Basic Statistics': {}".format(prop_label))
```

Get name/label of the class associated with a given class' display name:

```python
class_label = schema_explorer.get_property_label_from_display_name("Basic Statistics")
logger.info("label of the class associated with 'Basic Statistics': {}".format(class_label))
```

Generate template of class in schema:

```python
class_temp = schema_explorer.generate_class_template()
logger.info("generic template of a class in the schema/data model: {}".format(class_temp))
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
logger.info("Modified {} details : {}".format(TEST_CLASS, class_details))
```