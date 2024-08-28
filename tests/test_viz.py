import json
import logging
import os
from io import StringIO

import pandas as pd
import pytest

from schematic.visualization.attributes_explorer import AttributesExplorer
from schematic.visualization.tangled_tree import TangledTree

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(
    params=[
        ("example.model.jsonld", "example.model.pickle"),
        ("example.model.jsonld", ""),
        pytest.param(("", ""), marks=pytest.mark.xfail),
        pytest.param(("", "example.model.pickle"), marks=pytest.mark.xfail),
    ]
)
def attributes_explorer(request, helpers):
    # Get JSONLD file path
    param1, param2 = request.param
    path_to_jsonld = helpers.get_data_path(param1)
    path_to_graph = helpers.get_data_path(param2)

    # Initialize TangledTree
    if param2 != "":
        attributes_explorer = AttributesExplorer(
            path_to_jsonld,
            data_model_graph_pickle=path_to_graph,
            data_model_labels="class_label",
        )
    else:
        attributes_explorer = AttributesExplorer(
            path_to_jsonld,
            data_model_labels="class_label",
        )
    yield attributes_explorer


@pytest.fixture(
    params=[
        ("example.model.jsonld", "example.model.pickle"),
        ("example.model.jsonld", ""),
        pytest.param(("", ""), marks=pytest.mark.xfail),
        pytest.param(("", "example.model.pickle"), marks=pytest.mark.xfail),
    ]
)
def tangled_tree(helpers, request):
    figure_type = "component"

    # Get JSONLD file path
    param1, param2 = request.param
    path_to_jsonld = helpers.get_data_path(param1)
    path_to_graph = helpers.get_data_path(param2)

    # Initialize TangledTree
    if param2 == "":
        tangled_tree = TangledTree(
            path_to_jsonld, figure_type, data_model_labels="class_label"
        )
    else:
        tangled_tree = TangledTree(
            path_to_jsonld,
            figure_type,
            data_model_labels="class_label",
            data_model_graph_pickle=path_to_graph,
        )
    yield tangled_tree


class TestVisualization:
    def test_ae(self, helpers, attributes_explorer):
        attributes_str = attributes_explorer.parse_attributes(save_file=False)

        df = pd.read_csv(StringIO(attributes_str)).drop(columns=["Unnamed: 0"])

        # For the attributes df define expected columns
        expect_col_names = [
            "Attribute",
            "Label",
            "Description",
            "Required",
            "Cond_Req",
            "Valid Values",
            "Conditional Requirements",
            "Component",
        ]
        expected_components = ["Biospecimen", "Patient", "BulkRNA-seqAssay"]

        # Get actual values
        actual_column_names = df.columns.tolist()
        actual_components = df.loc[df["Attribute"] == "Component"]["Component"].tolist()

        assert actual_column_names == expect_col_names
        assert actual_components == expected_components

    @pytest.mark.parametrize("component", ["Patient", "BulkRNA-seqAssay"])
    def test_ce(self, component, attributes_explorer):
        """
        Test the output of parse_component_attributes
        """
        # get attributes string
        component_attributes_str = attributes_explorer._parse_component_attributes(
            component=component, save_file=False, include_index=False
        )
        # convert to dataframe
        component_attributes = pd.read_csv(StringIO(component_attributes_str))

        # For the attributes df define expected columns
        expect_col_names = [
            "Attribute",
            "Label",
            "Description",
            "Required",
            "Cond_Req",
            "Valid Values",
            "Conditional Requirements",
            "Component",
        ]

        actual_column_names = component_attributes.columns.tolist()

        # assert all columns are present
        assert actual_column_names == expect_col_names
        # assert all attributes belong to the same component
        assert (component_attributes.Component == component).all()

    def test_text(self, helpers, tangled_tree):
        text_format = "plain"

        # Get text for tangled tree.
        text_str = tangled_tree.get_text_for_tangled_tree(text_format, save_file=False)

        df = pd.read_csv(StringIO(text_str)).drop(columns=["Unnamed: 0"])

        # Define expected text associated with 'Patient' and 'Imaging' tree
        expected_patient_text = ["Biospecimen", "BulkRNA-seqAssay"]

        expected_Biospecimen_text = ["BulkRNA-seqAssay"]

        # Get actual text
        actual_patient_text = df.loc[df["Component"] == "Patient"]["name"].tolist()

        actual_Biospecimen_text = df.loc[df["Component"] == "Biospecimen"][
            "name"
        ].tolist()

        # Check some random pieces of text we would assume to be in the plain text.
        assert ((df["Component"] == "Patient") & (df["name"] == "Biospecimen")).any()

        # Check the extracted text matches expected text.
        assert actual_patient_text == expected_patient_text
        assert actual_Biospecimen_text == expected_Biospecimen_text

    @pytest.mark.parametrize(
        "conditional_requirements, expected",
        [
            # Test case 1: Multiple file formats
            (
                [
                    "['File Format is \"BAM\"', 'File Format is \"CRAM\"', 'File Format is \"CSV/TSV\"']"
                ],
                {"BAM": "FileFormat", "CRAM": "FileFormat", "CSV/TSV": "FileFormat"},
            ),
            # Test case 2: Single file format
            (["['File Format is \"CRAM\"']"], {"CRAM": "FileFormat"}),
            # Test case 3: with "OR" keyword
            (
                ['[\'File Format is "BAM" OR "CRAM" OR "CSV/TSV"\']'],
                {"BAM": "File Format", "CRAM": "File Format", "CSV/TSV": "File Format"},
            ),
        ],
    )
    def test_get_ca_alias(
        self, helpers, tangled_tree, conditional_requirements, expected
    ):
        ca_alias = tangled_tree._get_ca_alias(conditional_requirements)
        assert ca_alias == expected

    def test_layers(self, helpers, tangled_tree):
        layers_str = tangled_tree.get_tangled_tree_layers(save_file=False)[0]

        # Define what we expect the layers list to be.
        expected_layers_list = [
            [
                {
                    "id": "Patient",
                    "parents": [],
                    "direct_children": ["Biospecimen"],
                    "children": [
                        "BulkRNA-seqAssay",
                        "Biospecimen",
                    ],
                }
            ],
            [
                {
                    "id": "Biospecimen",
                    "parents": ["Patient"],
                    "direct_children": ["BulkRNA-seqAssay"],
                    "children": ["BulkRNA-seqAssay"],
                }
            ],
            [
                {
                    "id": "BulkRNA-seqAssay",
                    "parents": ["Biospecimen"],
                    "direct_children": [],
                    "children": [],
                }
            ],
        ]

        # Get actual layers.
        actual_layers_list = json.loads(layers_str)

        # compare
        for index, item in enumerate(actual_layers_list):
            assert item[0]["id"] == expected_layers_list[index][0]["id"]
            assert item[0]["parents"] == expected_layers_list[index][0]["parents"]
            assert (
                item[0]["direct_children"]
                == expected_layers_list[index][0]["direct_children"]
            )

            # ensure that order of children doesn't matterß
            assert set(item[0]["children"]) == set(
                expected_layers_list[index][0]["children"]
            )
