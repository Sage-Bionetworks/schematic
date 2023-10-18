import copy
import json
import logging
import networkx as nx
import numpy as np
import os
import pandas as pd
import pytest

from schematic.utils.df_utils import load_df
from schematic.utils.io_utils import load_json

from schematic.schemas.data_model_graph import DataModelGraph
from schematic.schemas.data_model_nodes import DataModelNodes
from schematic.schemas.data_model_edges import DataModelEdges
from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.data_model_relationships import DataModelRelationships
from schematic.schemas.data_model_jsonld import DataModelJsonLD, convert_graph_to_jsonld, BaseTemplate, PropertyTemplate, ClassTemplate
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_parser import DataModelParser, DataModelCSVParser, DataModelJSONLDParser

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DATA_MODEL_DICT = {
   'example.model.csv': "CSV",
   'example.model.jsonld': "JSONLD"
 }

def generate_graph_data_model(helpers, data_model_name):
    """
    Simple helper function to generate a networkx graph data model from a CSV or JSONLD data model
    """
    
    # Instantiate Parser
    data_model_parser = helpers.get_data_model_parser(data_model_name=data_model_name)

    #Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    graph_data_model = data_model_grapher.generate_data_model_graph()

    return graph_data_model

@pytest.fixture(name='dmjsonldp')
def fixture_dm_jsonld_parser():
    yield DataModelJSONLDParser()

@pytest.fixture
def DME(helpers, data_model_name='example.model.csv'):
    path_to_data_model = helpers.get_data_path("example.model.jsonld")

    graph_data_model = generate_graph_data_model(helpers, data_model_name=path_to_data_model)
    DME = DataModelGraphExplorer(graph_data_model)
    yield DME

@pytest.fixture(name="dmr")
def fixture_dmr():
    """Yields a data model relationships object for testing"""
    yield DataModelRelationships()

class TestDataModelParser:
    def test_get_base_schema_path(self, helpers):
        return

    def test_get_model_type(self):
        return

    def test_parse_model(self):
        return

class TestDataModelCsvParser:
    def test_check_schema_definition(self):
        return
    def test_gather_csv_attributes_relationships(self):
        return
    def test_parse_csv_model(self ):
        return

class TestDataModelJsonLdParser:
    @pytest.mark.parametrize("data_model", ['example.model.jsonld'], ids=["jsonld"])
    def test_gather_jsonld_attributes_relationships(self, helpers, data_model, dmjsonldp):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly.
        """
        path_to_data_model = helpers.get_data_path(path=data_model)
        model_jsonld = load_json(path_to_data_model)

        # Get output of the function:
        attr_rel_dict = dmjsonldp.gather_jsonld_attributes_relationships(model_jsonld=model_jsonld['@graph'])

        # Test the attr_rel_dict is formatted as expected:
        # Get a key in the model
        attribute_key = list(attr_rel_dict.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dict) == dict
        assert attribute_key in attr_rel_dict.keys()
        assert 'Relationships' in attr_rel_dict[attribute_key]
        assert 'Attribute' in attr_rel_dict[attribute_key]['Relationships']

    @pytest.mark.parametrize("data_model", ['example.model.jsonld'], ids=["jsonld"])
    def test_parse_jsonld_model(self, helpers, data_model, dmjsonldp):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly.
        """
        path_to_data_model = helpers.get_data_path(path=data_model)
        model_jsonld = load_json(path_to_data_model)

        # Get output of the function:
        model_dict = dmjsonldp.parse_jsonld_model(path_to_data_model=path_to_data_model)

        # Test the model_dict is formatted as expected:
        # Get a key in the model
        attribute_key = list(model_dict.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(model_dict) == dict
        assert attribute_key in model_dict.keys()
        assert 'Relationships' in model_dict[attribute_key]
        assert 'Attribute' in model_dict[attribute_key]['Relationships']

class TestDataModelRelationships:
    """Tests for DataModelRelationships class"""
    def test_define_data_model_relationships(self, dmr: DataModelRelationships):
        """Tests relationships_dictionary created has correct keys"""
        required_keys = [
            'jsonld_key',
            'csv_header',
            'type',
            'edge_rel',
            'required_header'
        ]
        required_edge_keys = ['edge_key', 'edge_dir']
        required_node_keys = ['node_label', 'node_attr_dict']

        relationships = dmr.relationships_dictionary

        for relationship in relationships.values():
            for key in required_keys:
                assert key in relationship.keys()
            if relationship['edge_rel']:
                for key in required_edge_keys:
                    assert key in relationship.keys()
            else:
                for key in required_node_keys:
                    assert key in relationship.keys()

    def test_define_required_csv_headers(self, dmr: DataModelRelationships):
        """Tests method returns correct values"""
        assert dmr.define_required_csv_headers() == [
            'Attribute',
            'Description',
            'Valid Values',
            'DependsOn',
            'DependsOn Component',
            'Required', 'Parent',
            'Validation Rules',
            'Properties',
            'Source'
        ]

    @pytest.mark.parametrize("edge", [True, False], ids=["True", "False"])
    def test_retreive_rel_headers_dict(self, dmr: DataModelRelationships, edge:bool):
        """Tests method returns correct values"""
        if edge:
            assert dmr.retreive_rel_headers_dict(edge=edge) == {
                'rangeIncludes': 'Valid Values',
                'requiresDependency': 'DependsOn',
                'requiresComponent': 'DependsOn Component',
                'subClassOf': 'Parent',
                'domainIncludes': 'Properties'
            }
        else:
            assert dmr.retreive_rel_headers_dict(edge=edge) == {
                'displayName': 'Attribute',
                'label': None,
                'comment': 'Description',
                'required': 'Required',
                'validationRules': 'Validation Rules',
                'isPartOf': None,
                'id': 'Source'
            }


class TestDataModelGraph:
    @pytest.mark.parametrize("data_model", ['example.model.csv', 'example.model.jsonld'], ids=["csv", "jsonld"])
    def test_generate_data_model_graph(self, helpers, data_model):
        '''Check that data model graph is constructed properly, requires calling various classes.
        TODO: In another test, check conditional dependencies.
        '''
        graph = generate_graph_data_model(helpers=helpers, data_model_name=data_model)
        
        #Check that some edges are present as expected:
        assert ('FamilyHistory', 'Breast') in graph.edges('FamilyHistory')
        assert ('BulkRNA-seqAssay', 'Biospecimen') in graph.edges('BulkRNA-seqAssay')
        assert ['Ab', 'Cd', 'Ef', 'Gh'] == [k for k,v in graph['CheckList'].items() for vk, vv in v.items() if vk == 'rangeValue']

        # Check that all relationships recorded between 'CheckList' and 'Ab' are present
        assert 'rangeValue' and 'parentOf' in graph['CheckList']['Ab']
        assert 'requiresDependency' not in graph['CheckList']['Ab']
        
        # Check nodes:
        assert 'Patient' in graph.nodes
        assert 'GRCh38' in graph.nodes


        # Check weights
        assert graph['Sex']['Female']['rangeValue']['weight'] == 0
        assert graph['MockComponent']['CheckRegexFormat']['requiresDependency']['weight'] == 4

        # Check Edge directions
        assert 4 == (len(graph.out_edges('TissueStatus')))
        assert 2 == (len(graph.in_edges('TissueStatus')))

class TestDataModelGraphExplorer:
    def test_find_properties(self):
        return

    def test_find_classes(self):
        return

    def test_find_node_range(self):
        return

    def test_get_adjacent_nodes_by_relationship(self):
        return

    def test_get_component_requirements(self):
        return

    def test_get_component_requirements_graph(self):
        return

    def get_descendants_by_edge_type(self):
        return

    def test_get_digraph_by_edge_type(self):
        return

    def test_get_edges_by_relationship(self):
        return

    def test_get_ordered_entry(self):
        return

    def test_get_nodes_ancestors(self):
        return

    def test_get_node_comment(self):
        return

    def test_get_node_dependencies(self):
        return

    def test_get_nodes_descendants(self):
        return

    def test_get_nodes_display_names(self):
        return

    def test_get_node_label(self):
        return

    def test_get_node_range(self):
        return

    def test_get_node_required(self):
        return

    def test_get_node_validation_rules(self):
        return

    def test_get_subgraph_by_edge_type(self):
        return

    def test_find_adjacent_child_classes(self):
        return

    def test_find_parent_classes(self):
        return

    def test_full_schema_graph(self):
        return

    @pytest.mark.parametrize("class_name, expected_in_schema", [("Patient",True), ("ptaient",False), ("Biospecimen",True), ("InvalidComponent",False)])
    def test_is_class_in_schema(self, DME, class_name, expected_in_schema):
        """
        Test to cover checking if a given class is in a schema.
        `is_class_in_schema` should return `True` if the class is in the schema
        and `False` if it is not.
        """

        # Check if class is in schema
        class_in_schema = DME.is_class_in_schema(class_name)

        # Assert value is as expected
        assert class_in_schema == expected_in_schema

    def test_sub_schema_graph(self):
        return

class TestDataModelNodes:
    def test_gather_nodes(self):
        return
    def test_gather_all_nodes(self):
        return
    def test_get_rel_node_dict_info(self):
        return
    def test_get_data_model_properties(self):
        return
    def test_get_entry_type(self):
        return
    def test_run_rel_functions(self):
        return
    def test_generate_node_dict(self):
        return
    def test_generate_node(self):
        return

class TestDataModelEdges:
    def test_generate_edge(self,helpers):
        return


class TestDataModelJsonSchema:
    @pytest.mark.parametrize("data_model", ['example.model.csv', 'example.model.jsonld'], ids=["csv", "jsonld"])
    @pytest.mark.parametrize("node_range", [[], ['healthy'], ['healthy', 'cancer']], ids=['empty_range', "single_range", "multi_range"])
    @pytest.mark.parametrize("node_name", ['', 'Diagnosis'], ids=['empty_node_name', "Diagnosis_node_name"])
    @pytest.mark.parametrize("blank", [True, False], ids=["True_blank", "False_blank"])
    def test_get_array_schema(self, helpers, data_model, node_range, node_name, blank):
        dmjs = helpers.get_data_model_json_schema(data_model_name=data_model)
        array_schema = dmjs.get_array_schema(node_range=node_range, node_name=node_name, blank=blank)

        # check node_name is recoreded as the key to the array schema
        assert node_name in array_schema

        # Check maxItems is the lenghth of node_range
        assert len(node_range) == array_schema[node_name]['maxItems']

        # Check that blank value is added at the end of node_range, if true
        if blank:
            assert array_schema[node_name]['items']['enum'][-1]== ''
            assert len(array_schema[node_name]['items']['enum'])==len(node_range)+1
        else:
            assert array_schema[node_name]['items']['enum']== node_range
            assert len(array_schema[node_name]['items']['enum'])==len(node_range)

    @pytest.mark.parametrize("data_model", ['example.model.csv', 'example.model.jsonld'], ids=["csv", "jsonld"])
    @pytest.mark.parametrize("node_name", ['', 'Diagnosis'], ids=['empty_node_name', "Diagnosis_node_name"])
    def test_get_non_blank_schema(self, helpers, data_model, node_name):
        dmjs = helpers.get_data_model_json_schema(data_model_name=data_model)
        non_blank_schema = dmjs.get_non_blank_schema(node_name=node_name)
        # check node_name is recoreded as the key to the array schema
        assert node_name in non_blank_schema
        assert non_blank_schema[node_name] == {"not": {"type": "null"}, "minLength": 1}
    
    @pytest.mark.parametrize("data_model", ['example.model.csv', 'example.model.jsonld'], ids=["csv", "jsonld"])
    @pytest.mark.parametrize("node_range", [[], ['healthy'], ['healthy', 'cancer']], ids=['empty_range', "single_range", "multi_range"])
    @pytest.mark.parametrize("node_name", ['', 'Diagnosis'], ids=['empty_node_name', "Diagnosis_node_name"])
    @pytest.mark.parametrize("blank", [True, False], ids=["True_blank", "False_blank"])
    def test_get_range_schema(self, helpers, data_model, node_range, node_name, blank):
        dmjs = helpers.get_data_model_json_schema(data_model_name=data_model)
        range_schema = dmjs.get_range_schema(node_range=node_range, node_name=node_name, blank=blank)

        # check node_name is recoreded as the key to the array schema
        assert node_name in range_schema

        # Check that blank value is added at the end of node_range, if true
        if blank:
            assert range_schema[node_name]['enum'][-1]== ''
            assert len(range_schema[node_name]['enum'])==len(node_range)+1
        else:
            assert range_schema[node_name]['enum']== node_range
            assert len(range_schema[node_name]['enum'])==len(node_range)

    @pytest.mark.parametrize("data_model", ['example.model.csv', 'example.model.jsonld'], ids=["csv", "jsonld"])
    @pytest.mark.parametrize("source_node", ['', 'Patient'], ids=['empty_node_name', "patient_source"])
    @pytest.mark.parametrize("schema_name", ['', 'Test_Schema_Name'], ids=['empty_schema_name', "schema_name"])
    def test_get_json_validation_schema(self, helpers, data_model, source_node, schema_name):
        dmjs = helpers.get_data_model_json_schema(data_model_name=data_model)

        try:
            # Get validation schema
            json_validation_schema = dmjs.get_json_validation_schema(source_node=source_node, schema_name=schema_name)

            # Check Keys in Schema
            expected_jvs_keys = ['$schema', '$id', 'title', 'type', 'properties', 'required', 'allOf']
            actual_jvs_keys = list( json_validation_schema.keys())
            assert expected_jvs_keys == actual_jvs_keys

            # Check title
            assert schema_name == json_validation_schema['title']

            # Check contents of validation schema
            assert 'Diagnosis' in json_validation_schema['properties']
            assert 'Cancer' in json_validation_schema['properties']['Diagnosis']['enum']
        except:
            # Should only fail if no source node is provided.
            assert source_node == ''

class TestDataModelJsonLd:
    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_init(self, helpers, data_model):
        # Test that __init__ is being set up properly        
        # Get Graph
        graph_data_model = generate_graph_data_model(helpers, data_model_name=data_model)
        
        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(Graph=graph_data_model)

        # Test that __init__ is being set up properly 
        assert type(data_model_jsonld.graph) == nx.MultiDiGraph
        assert type(data_model_jsonld.rel_dict) == dict
        assert 'required' in data_model_jsonld.rel_dict
        assert type(data_model_jsonld.DME) == DataModelGraphExplorer
        assert data_model_jsonld.output_path == ''

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_base_jsonld_template(self, helpers, data_model):
        # Gather the templates
        base_template = BaseTemplate()
        base_jsonld_template = json.loads(base_template.to_json())

        # Test base template is constructed as expected
        assert '@context' in base_jsonld_template
        assert '@graph' in base_jsonld_template
        assert '@id' in base_jsonld_template

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    @pytest.mark.parametrize("template_type", ['property', 'class'], ids=['property', 'class'])
    @pytest.mark.parametrize("node", ['', 'Patient'], ids=['no node', 'Patient'])
    def test_fill_entry_template(self, helpers, data_model, template_type, node):
        # Get Graph
        graph_data_model = generate_graph_data_model(helpers, data_model_name=data_model)

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(Graph=graph_data_model)

        # Get empty template
        if template_type == 'property':
            property_template = PropertyTemplate()
            template = json.loads(property_template.to_json())
        elif template_type == 'class':
            class_template = ClassTemplate()
            template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = copy.deepcopy(template)

        try:
            # Fill out template for given node.
            object_template = data_model_jsonld.fill_entry_template(template=template_copy, node=node)
            # Ensure template keys are present (not all original keys will be present due to cleaning empty values):
        except:
            # Should only fail if no node is given
            assert node == ''

        if 'object_template' in locals():
            # Check that object template keys match the expected keys
            actual_keys = list(object_template.keys())
            if template_type == 'property':
                expected_keys = ['@id', '@type', 'rdfs:comment', 'rdfs:label', 'schema:isPartOf', 'sms:displayName', 'sms:required', 'sms:validationRules']
            elif template_type == 'class':
                expected_keys = ['@id', '@type', 'rdfs:comment', 'rdfs:label', 'rdfs:subClassOf', 'schema:isPartOf', 'sms:displayName', 'sms:required', 'sms:requiresDependency', 'sms:validationRules']
            assert (set(actual_keys) - set(expected_keys)) == (set(expected_keys) - set(actual_keys))

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    @pytest.mark.parametrize("template_type", ['property', 'class'], ids=['property', 'class'])
    def test_add_contexts_to_entries(self, helpers, data_model, template_type):
        # Will likely need to change when contexts added to model.
        # Get Graph
        graph_data_model = generate_graph_data_model(helpers, data_model_name=data_model)

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(Graph=graph_data_model)

        # Get empty template
        if template_type == 'property':
            property_template = PropertyTemplate()
            template = json.loads(property_template.to_json())
        elif template_type == 'class':
            class_template = ClassTemplate()
            template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = copy.deepcopy(template)

        # Fill out template for given node.
        object_template = data_model_jsonld.fill_entry_template(template=template_copy, node='Patient')

        if 'sms:required' in object_template:
            assert 'sms' in object_template['sms:required']
        if '@id' in object_template:
            assert 'bts' in object_template['@id']

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_clean_template(self, helpers, data_model):
        # TODO: This will need to change with contexts bc they are hard coded here.
        # Get Graph
        graph_data_model = generate_graph_data_model(helpers, data_model_name=data_model)

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(Graph=graph_data_model)

        # Get empty template
        class_template = ClassTemplate()
        template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = copy.deepcopy(template)

        assert 'sms:requiresDependency' in template_copy

        # Fill out some mock entries in the template:
        template_copy['@id'] == 'bts:CheckURL'
        template_copy['rdfs:label'] == 'CheckURL'
        data_model_relationships=data_model_jsonld.dmr.relationships_dictionary

        # Clean template
        data_model_jsonld.clean_template(template=template_copy, data_model_relationships=data_model_relationships)
        
        # Look for expected changes after cleaning
        # Check that expected JSONLD default is added
        assert template_copy['sms:required'] == 'sms:false'
        assert template_copy['sms:validationRules'] == []
        
        # Check that non-required JSONLD keys are removed.
        assert 'sms:requiresDependency' not in template_copy

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    @pytest.mark.parametrize("valid_values", [[], ['Other', 'Female', 'Male'], ['A', 'Bad', 'Entry']], ids=['Empty List', 'Disordered List', 'Incorrect List'])
    def test_reorder_template_entries(self, helpers, data_model, valid_values):
        # Note the way test_reorder_template_entries works, is that as long as an entry has recordings in the template
        # even if they are incorrect, they will be corrected within this function.
        # Get Graph
        graph_data_model = generate_graph_data_model(helpers, data_model_name=data_model)

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(Graph=graph_data_model)

        # Get empty template
        class_template = ClassTemplate()
        template = json.loads(class_template.to_json())

        # Make a copy of the template, since template is mutable
        template_copy = copy.deepcopy(template)

        # Fill out template with 'Sex' attribute from example model
        template_copy['@id'] = 'Sex'
        template_copy['rdfs:label'] = 'Sex'
        template_copy['sms:required'] = 'sms:false'
        template_copy['schema:rangeIncludes'] = valid_values

        # Now reorder:
        data_model_jsonld.reorder_template_entries(template=template_copy)
        if valid_values:
            assert template_copy['schema:rangeIncludes'] == [{'@id': 'bts:Female'}, {'@id': 'bts:Male'}, {'@id': 'bts:Other'}]
        else:
            assert template_copy['schema:rangeIncludes'] == []

    def test_property_template(self, helpers):
        # Get Property Template
        empty_template = PropertyTemplate()
        property_template = json.loads(empty_template.to_json())

        expected_property_template =  {
                                        "@id": "",
                                        "@type": "rdf:Property",
                                        "rdfs:comment": "",
                                        "rdfs:label": "",
                                        "schema:domainIncludes": [],
                                        "schema:rangeIncludes": [],
                                        "schema:isPartOf": {},
                                        "sms:displayName": "",
                                        "sms:required": "sms:false",
                                        "sms:validationRules": [],
                                        }
        assert property_template == expected_property_template

    def test_class_template(self, helpers):
        # Get Class Template
        empty_template = ClassTemplate()
        class_template = json.loads(empty_template.to_json())
        
        expected_class_template = {
                                "@id": "",
                                "@type": "rdfs:Class",
                                "rdfs:comment": "",
                                "rdfs:label": "",
                                "rdfs:subClassOf": [],
                                "schema:isPartOf": {},
                                "schema:rangeIncludes": [],
                                "sms:displayName": "",
                                "sms:required": "sms:false",
                                "sms:requiresDependency": [],
                                "sms:requiresComponent": [],
                                "sms:validationRules": [],
                            }
        assert class_template == expected_class_template

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_generate_jsonld_object(self, helpers, data_model):
        # Check that JSONLD object is being made, and has some populated entries.

        # Get Graph
        graph_data_model = generate_graph_data_model(helpers, data_model_name=data_model)

        # Instantiate DataModelJsonLD
        data_model_jsonld = DataModelJsonLD(Graph=graph_data_model)
        jsonld_dm = data_model_jsonld.generate_jsonld_object()

        assert list(jsonld_dm.keys()) == ['@context', '@graph', '@id']
        assert len(jsonld_dm['@graph']) > 1

    @pytest.mark.parametrize("data_model", list(DATA_MODEL_DICT.keys()), ids=list(DATA_MODEL_DICT.values()))
    def test_convert_graph_to_jsonld(self, helpers, data_model):
        # Get Graph
        graph_data_model = generate_graph_data_model(helpers, data_model_name=data_model)

        # Generate JSONLD
        jsonld_dm = convert_graph_to_jsonld(Graph=graph_data_model)
        assert list(jsonld_dm.keys()) == ['@context', '@graph', '@id']
        assert len(jsonld_dm['@graph']) > 1

class TestSchemas:
    def test_convert_csv_to_graph(self, helpers):
        return
    def test_convert_jsonld_to_graph(self, helpers):
        return

