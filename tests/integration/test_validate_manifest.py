
import pytest

from schematic.models.validate_manifest import ValidateManifest
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.utils.df_utils import load_df


class TestValidateManifest:

    @pytest.mark.parametrize(
        ("manifest", "model", "root_node"),
        [
            (
                "mock_manifests/Valid_Test_Manifest_with_nones.csv",
                "example_test_nones.model.csv",
                "MockComponent",
            ),
            #(
            #    "mock_manifests/Valid_Test_Manifest_with_nones.csv",
            #    "example_test_nones.model2.csv",
            #    "MockComponent",
            #),
        ],
    )
    def test_validate_manifest_values(
        self, helpers, manifest, model, root_node
    ):
        # Get manifest and data model path
        manifest_path = helpers.get_data_path(manifest)
        model_path = helpers.get_data_path(model)

        # Gather parmeters needed to run validate_manifest_rules
        errors = []
        load_args = {
            "dtype": "string",
        }

        dmge = helpers.get_data_model_graph_explorer(path=model)

        self.data_model_js = DataModelJSONSchema(
            jsonld_path=model_path, graph=dmge.graph
        )
        json_schema = self.data_model_js.get_json_validation_schema(
            root_node, root_node + "_validation"
        )

        manifest = load_df(
            manifest_path,
            preserve_raw_input=False,
            allow_na_values=True,
            **load_args,
        )

        vm = ValidateManifest(errors, manifest, manifest_path, dmge, json_schema)
        errors, warnings = vm.validate_manifest_values(manifest, json_schema, dmge)
        import logging
        assert not warnings
        assert errors
        for error in errors:
            logging.warning(error)
        assert False
