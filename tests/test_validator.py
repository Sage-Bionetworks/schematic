from io import StringIO
import json
import networkx as nx
import os
import pandas as pd
import pytest
import logging


from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph
from schematic.schemas.data_model_validator import DataModelValidator
from schematic.schemas.data_model_jsonld import DataModelJsonLD, convert_graph_to_jsonld


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def graph_data_model_func(helpers, data_model_name):
    path_to_data_model = helpers.get_data_path(data_model_name)

    # Instantiate Parser
    data_model_parser = DataModelParser(path_to_data_model=path_to_data_model)

    # Parse Model
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    # Instantiate DataModelGraph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    return graph_data_model


class TestDataModelValidator:
    def test_check_blacklisted_characters(self, helpers):
        # Get graph data model
        graph_data_model = graph_data_model_func(
            helpers, data_model_name="validator_test.model.csv"
        )

        # Instantiate Data Model Validator
        DMV = DataModelValidator(graph_data_model)

        # Run validation
        validator_errors = DMV.check_blacklisted_characters()

        # Expected Error
        expected_error = [
            "Node: Patient) contains a blacklisted character(s): ), they will be striped if used in Synapse annotations.",
            "Node: Patient ID. contains a blacklisted character(s): ., they will be striped if used in Synapse annotations.",
            "Node: Sex- contains a blacklisted character(s): -, they will be striped if used in Synapse annotations.",
            "Node: Year of Birth( contains a blacklisted character(s): (, they will be striped if used in Synapse annotations.",
            "Node: Bulk RNA-seq Assay contains a blacklisted character(s): -, they will be striped if used in Synapse annotations.",
        ]

        assert expected_error == validator_errors

    def test_check_reserved_names(self, helpers):
        # Get graph data model
        graph_data_model = graph_data_model_func(
            helpers, data_model_name="validator_test.model.csv"
        )

        # Instantiate Data Model Validator
        DMV = DataModelValidator(graph_data_model)

        # Run validation
        validator_errors = DMV.check_reserved_names()

        # Expected Error
        expected_error = [
            "Your data model entry name: EntityId overlaps with the reserved name: entityId. Please change this name in your data model."
        ]
        assert expected_error == validator_errors

    def test_check_graph_has_required_node_fields(self, helpers):
        # Get graph data model
        graph_data_model = graph_data_model_func(
            helpers, data_model_name="validator_test.model.csv"
        )

        # Remove a field from an entry graph
        del graph_data_model.nodes["Cancer"]["label"]

        # Instantiate Data Model Validator
        DMV = DataModelValidator(graph_data_model)

        # Run validation
        validator_errors = DMV.check_graph_has_required_node_fields()

        # Expected Error
        expected_error = [
            "For entry: Cancer, the required field label is missing in the data model graph, please double check your model and generate the graph again."
        ]
        assert expected_error == validator_errors

    def test_dag(self, helpers):
        # TODO: The schema validator currently doesn't catch the Diagnosis-Diagnosis self loop.
        # It is an expected error but it will need to be decided if the validator should prevent or allow such self loops

        # Get graph data model
        graph_data_model = graph_data_model_func(
            helpers, data_model_name="validator_dag_test.model.csv"
        )

        # Instantiate Data Model Validator
        DMV = DataModelValidator(graph_data_model)

        # Run validation
        validator_errors = DMV.check_is_dag()

        # nodes could be in different order so need to account for that
        expected_errors = [
            "Schematic requires models be a directed acyclic graph (DAG). Please inspect your model."
        ]

        assert validator_errors[0] in expected_errors
