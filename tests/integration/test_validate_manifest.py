from typing import Any

import pytest

from schematic.models.validate_manifest import ValidateManifest
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.utils.df_utils import load_df


class TestValidateManifest:
    """Tests for ValidateManifest class"""

    @pytest.mark.parametrize(
        ("manifest", "model", "root_node"),
        [
            (
                "mock_manifests/Valid_Test_Manifest_with_nones.csv",
                "example_test_nones.model.csv",
                "MockComponent",
            ),
            (
                "mock_manifests/Invalid_Test_Manifest_with_nones.csv",
                "example_test_nones.model.csv",
                "MockComponent",
            ),
        ],
    )
    def test_validate_manifest_values(
        self, helpers: Any, manifest: str, model: str, root_node: str
    ):
        """Tests for ValidateManifest.validate_manifest_values

        Args:
            helpers (Any): An object with helper functions
            manifest (str): A path to the manifest to be tested
            model (str): A path to the model to be tested
            root_node (str): The name of the component to be tested
        """
        # Get manifest and data model path
        manifest_path: str = helpers.get_data_path(manifest)
        model_path: str = helpers.get_data_path(model)

        # Gather parameters needed to run validate_manifest_rules
        errors = []
        load_args = {
            "dtype": "string",
        }

        dmge = helpers.get_data_model_graph_explorer(path=model)

        data_model_js = DataModelJSONSchema(jsonld_path=model_path, graph=dmge.graph)
        json_schema = data_model_js.get_json_validation_schema(
            root_node, root_node + "_validation"
        )

        manifest_df = load_df(
            manifest_path,
            preserve_raw_input=False,
            allow_na_values=True,
            **load_args,
        )

        vm = ValidateManifest(errors, manifest_df, manifest_path, dmge, json_schema)
        errors, warnings = vm.validate_manifest_values(manifest_df, json_schema, dmge)
        assert not warnings
        assert errors

        # Both manifests will have errors of type "<xxx> is not of type 'array'
        # Only the invalid manifest will have errors of type "<xxx is not one of
        #  ['Breast', 'None', 'Prostate', 'Colorectal', 'Skin', 'Lung']" in the
        #  "Cancer Type" column.
        error_attributes = [error[1] for error in errors]
        if manifest == "mock_manifests/Valid_Test_Manifest_with_nones.csv":
            assert "Cancer Type" not in error_attributes
        else:
            assert "Cancer Type" in error_attributes
