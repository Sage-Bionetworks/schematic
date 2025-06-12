import json
import logging

from copy import deepcopy

import pytest

from schematic.schemas.data_model_jsonld import (
    DataModelJsonLD,
    PropertyTemplate,
)
from tests.test_schemas import generate_graph_data_model

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

DATA_MODEL_DICT = {"example.model.csv": "CSV", "example.model.jsonld": "JSONLD"}
COL_TYPE_MODEL_DICT = {"example.model.column_type_component.csv": "CSV_Column_Type"}
ORG_AND_COL_TYPE_MODEL_DICT = DATA_MODEL_DICT | COL_TYPE_MODEL_DICT


class TestDataModelJsonLD:
    @pytest.mark.parametrize(
        "data_model",
        list(COL_TYPE_MODEL_DICT.keys()),
        ids=list(COL_TYPE_MODEL_DICT.values()),
    )
    @pytest.mark.parametrize(
        "node, expected_col_type, expected_keys",
        [
            (
                "Stringtype",
                "string",
                [
                    "@id",
                    "@type",
                    "rdfs:comment",
                    "rdfs:label",
                    "schema:isPartOf",
                    "sms:displayName",
                    "sms:required",
                    "sms:validationRules",
                    "sms:columnType",
                ],
            ),
            (
                "Missingtype",
                "",
                [
                    "@id",
                    "@type",
                    "rdfs:comment",
                    "rdfs:label",
                    "schema:isPartOf",
                    "sms:displayName",
                    "sms:required",
                    "sms:validationRules",
                ],
            ),
        ],
        ids=["type specified", "no type specified"],
    )
    def test_fill_entry_col_type_template(
        self, helpers, data_model, node, expected_col_type, expected_keys
    ):
        # Given a graph data model
        graph_data_model = generate_graph_data_model(
            helpers,
            data_model_name=data_model,
            data_model_labels="class_label",
        )

        # AND a graph data model to jsonld converter
        data_model_jsonld = DataModelJsonLD(graph=graph_data_model)

        # AND an empty property template
        property_template = PropertyTemplate()
        template = json.loads(property_template.to_json())
        # Make a copy of the template, since template is mutable
        template_copy = deepcopy(template)

        # WHEN the template is filled out for a given node
        object_template = data_model_jsonld.fill_entry_template(
            template=template_copy, node=node
        )

        # AND the keys from the template are checked
        actual_keys = list(object_template.keys())

        # THEN there should be no dicts that contain extra keys
        actual_set = set(actual_keys)
        expected_set = set(expected_keys)
        symmetric_difference = actual_set.symmetric_difference(expected_set)
        assert (
            not symmetric_difference
        ), f"Expected equal dictionaries but got anomalous key {symmetric_difference}"

        # AND the columnType should be set as expected
        if expected_col_type:
            assert (
                object_template["sms:columnType"] == expected_col_type
            ), f"Expected column type to be {expected_col_type} got {object_template['sms:columnType']}"
