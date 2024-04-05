import os
import logging
import re
import networkx as nx
import jsonschema
import pytest
from pathlib import Path
import itertools

from schematic.models.validate_attribute import ValidateAttribute, GenerateError
from schematic.models.validate_manifest import ValidateManifest
from schematic.models.metadata import MetadataModel
from schematic.store.synapse import SynapseStorage

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_json_schema import DataModelJSONSchema

from schematic.utils.validate_rules_utils import validation_rule_info

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(name="dmge")
def DMGE(helpers):
    dmge = helpers.get_data_model_graph_explorer(path="example.model.jsonld")
    yield dmge


@pytest.fixture
def metadataModel(helpers):
    metadataModel = MetadataModel(
        inputMModelLocation=helpers.get_data_path("example.model.jsonld"),
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )

    yield metadataModel


def get_rule_combinations():
    rule_info = validation_rule_info()
    for base_rule, indiv_info in rule_info.items():
        complementary_rules = indiv_info["complementary_rules"]
        if complementary_rules:
            for second_rule in complementary_rules:
                yield base_rule, second_rule
        else:
            continue


class TestManifestValidation:
    # check if suite has been created. If so, delete it
    if os.path.exists("great_expectations/expectations/Manifest_test_suite.json"):
        os.remove("great_expectations/expectations/Manifest_test_suite.json")

    @pytest.mark.parametrize(
        "project_scope",
        ["syn54126707", "syn55250368"],
        ids=["project_scope_with_manifests", "project_scope_without_manifests"],
    )
    def test_valid_manifest(self, helpers, metadataModel, project_scope):
        manifestPath = helpers.get_data_path("mock_manifests/Valid_Test_Manifest.csv")
        rootNode = "MockComponent"

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode,
            project_scope=[project_scope],
        )

        assert errors == []
        # When submitting the first manifest for cross manifest validation, check that proper warning (to alert
        # users that no validation will be run), is raised. The manifest is still valid to submit.
        if project_scope == "syn55250368":
            rule_sets = [
                ('Check Match at Least', 'matchAtLeastOne'),
                ('Check Match at Least values', 'matchAtLeastOne MockComponent.checkMatchatLeastvalues value'),
                ('Check Match Exactly', 'matchExactlyOne MockComponent.checkMatchExactly set'),
                ('Check Match Exactly values', 'matchExactlyOne MockComponent.checkMatchExactlyvalues value'),
                ('Check Match None', 'matchNone MockComponent.checkMatchNone set error'),
                ('Check Match None values', 'matchNone MockComponent.checkMatchNonevalues value error'),
            ]
            for error_col, val_rule in rule_sets:
                assert GenerateError.generate_no_cross_warning(
                        attribute_name=error_col,
                        val_rule=val_rule)[1] in warnings
        else:
            assert warnings == []

    def test_invalid_manifest(self, helpers, dmge, metadataModel):
        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = "MockComponent"

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode,
            project_scope=["syn54126707"],
        )

        # Check errors
        assert GenerateError.generate_type_error(
                val_rule="num",
                row_num="3",
                attribute_name="Check Num",
                invalid_entry="c",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_type_error(
                val_rule="int",
                row_num="3",
                attribute_name="Check Int",
                invalid_entry="5.63",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_type_error(
                val_rule="str",
                row_num="3",
                attribute_name="Check String",
                invalid_entry="94",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list strict",
                list_string="invalid list values",
                row_num="3",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="invalid list values",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list strict",
                list_string="ab cd ef",
                row_num="3",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="ab cd ef",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Format",
                module_to_call="match",
                invalid_entry="m",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Single",
                module_to_call="search",
                invalid_entry="q",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="^\d+$",
                row_num="2",
                attribute_name="Check Regex Integer",
                module_to_call="search",
                invalid_entry="5.4",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_url_error(
                val_rule="url",
                url="http://googlef.com/",
                url_error="invalid_url",
                row_num="3",
                attribute_name="Check URL",
                argument=None,
                invalid_entry="http://googlef.com/",
                dmge=dmge,
            )[0] in errors

        date_err = GenerateError.generate_content_error(
            val_rule="date",
            attribute_name="Check Date",
            dmge=dmge,
            row_num=["2", "3", "4"],
            invalid_entry=["84-43-094", "32-984", "notADate"],
        )[0]
        error_in_list = [date_err[2] in error for error in errors]
        assert any(error_in_list)

        assert GenerateError.generate_content_error(
                val_rule="unique error",
                attribute_name="Check Unique",
                dmge=dmge,
                row_num=["2", "3", "4"],
                invalid_entry=["str1"],
            )[0] in errors

        assert GenerateError.generate_content_error(
                val_rule="inRange 50 100 error",
                attribute_name="Check Range",
                dmge=dmge,
                row_num=["3"],
                invalid_entry=["30"],
            )[0] in errors

        assert (
            GenerateError.generate_cross_warning(
                val_rule="matchNone error",
                row_num=["3"],
                attribute_name="Check Match None",
                manifest_id=["syn54126950"],
                invalid_entry=["123"],
                dmge=dmge,
            )[0]
             in errors
        )

        assert (
            GenerateError.generate_cross_warning(
                val_rule="matchNone value error",
                row_num=["4"],
                attribute_name="Check Match None values",
                invalid_entry=["123"],
                dmge=dmge,
            )[0]
             in errors
        )


        # check warnings
        assert GenerateError.generate_content_error(
                val_rule="recommended",
                attribute_name="Check Recommended",
                dmge=dmge,
            )[1] in warnings

        assert GenerateError.generate_content_error(
                val_rule="protectAges",
                attribute_name="Check Ages",
                dmge=dmge,
                row_num=["2", "3"],
                invalid_entry=["6549", "32851"],
            )[1] in warnings

        assert GenerateError.generate_cross_warning(
                val_rule="matchAtLeastOne",
                row_num=["3"],
                attribute_name="Check Match at Least",
                invalid_entry=["7163"],
                manifest_id=["syn54126997", "syn54127001"],
                dmge=dmge,
            )[1] in warnings

        assert GenerateError.generate_cross_warning(
                val_rule="matchAtLeastOne MockComponent.checkMatchatLeastvalues value",
                row_num=["3"],
                attribute_name="Check Match at Least values",
                invalid_entry=["51100"],
                dmge=dmge,
            )[1] in warnings

        assert \
            GenerateError.generate_cross_warning(
                val_rule="matchExactlyOne",
                attribute_name="Check Match Exactly",
                matching_manifests=["syn54126950", "syn54127008"],
                dmge=dmge,
            )[1] in warnings \
            or GenerateError.generate_cross_warning(
                val_rule="matchExactlyOne",
                attribute_name="Check Match Exactly",
                matching_manifests=["syn54127702", "syn54127008"],
                dmge=dmge,
            )[1] in warnings

        cross_warning = GenerateError.generate_cross_warning(
            val_rule="matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value",
            row_num=["2", "3", "4"],
            attribute_name="Check Match Exactly values",
            invalid_entry=["71738", "98085", "210065"],
            dmge=dmge,
        )[1]

        warning_in_list = [cross_warning[1] in warning for warning in warnings]
        assert any(warning_in_list)


    def test_in_house_validation(self, helpers, dmge, metadataModel):
        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = "MockComponent"

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode,
            restrict_rules=True,
            project_scope=["syn54126707"],
        )

        # Check errors
        assert GenerateError.generate_type_error(
                val_rule="num",
                row_num="3",
                attribute_name="Check Num",
                invalid_entry="c",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_type_error(
                val_rule="int",
                row_num="3",
                attribute_name="Check Int",
                invalid_entry="5.63",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_type_error(
                val_rule="str",
                row_num="3",
                attribute_name="Check String",
                invalid_entry="94",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_type_error(
                val_rule="int",
                row_num="3",
                attribute_name="Check NA",
                invalid_entry="9.5",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list strict",
                list_string="invalid list values",
                row_num="3",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="invalid list values",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list strict",
                list_string="ab cd ef",
                row_num="3",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="ab cd ef",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Single",
                module_to_call="search",
                invalid_entry="q",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Format",
                module_to_call="match",
                invalid_entry="m",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_url_error(
                val_rule="url",
                url="http://googlef.com/",
                url_error="invalid_url",
                row_num="3",
                attribute_name="Check URL",
                argument=None,
                invalid_entry="http://googlef.com/",
                dmge=dmge,
            )[0] in errors

        assert (
            GenerateError.generate_cross_warning(
                val_rule="matchNone error",
                row_num=["3"],
                attribute_name="Check Match None",
                manifest_id=["syn54126950"],
                invalid_entry=["123"],
                dmge=dmge,
            )[0]
             in errors
        )

        assert (
            GenerateError.generate_cross_warning(
                val_rule="matchNone value error",
                row_num=["4"],
                attribute_name="Check Match None values",
                invalid_entry=["123"],
                dmge=dmge,
            )[0]
             in errors
        )

        # Check Warnings
        assert GenerateError.generate_cross_warning(
                val_rule="matchAtLeastOne",
                row_num=["3"],
                attribute_name="Check Match at Least",
                invalid_entry=["7163"],
                manifest_id=["syn54126997", "syn54127001"],
                dmge=dmge,
            )[1] in warnings

        assert GenerateError.generate_cross_warning(
                val_rule="matchAtLeastOne MockComponent.checkMatchatLeastvalues value",
                row_num=["3"],
                attribute_name="Check Match at Least values",
                invalid_entry=["51100"],
                dmge=dmge,
            )[1] in warnings

        assert \
            GenerateError.generate_cross_warning(
                val_rule="matchExactlyOne",
                attribute_name="Check Match Exactly",
                matching_manifests=["syn54126950", "syn54127008"],
                dmge=dmge,
            )[1] in warnings \
            or GenerateError.generate_cross_warning(
                val_rule="matchExactlyOne",
                attribute_name="Check Match Exactly",
                matching_manifests=["syn54127702", "syn54127008"],
                dmge=dmge,
            )[1] in warnings

        assert GenerateError.generate_cross_warning(
                val_rule="matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value",
                row_num=["2", "3", "4"],
                attribute_name="Check Match Exactly values",
                invalid_entry=["71738", "98085", "210065"],
                dmge=dmge,
            )[1] in warnings


    @pytest.mark.parametrize(
        "manifest_path",
        [
            "mock_manifests/example.biospecimen_component_rule.manifest.csv",
            "mock_manifests/example.patient_component_rule.manifest.csv",
        ],
        ids=["biospecimen_manifest", "patient_manifest"],
    )
    def test_component_validations(self, helpers, manifest_path, dmge):
        full_manifest_path = helpers.get_data_path(manifest_path)
        manifest = helpers.get_data_frame(full_manifest_path)

        root_node = manifest["Component"][0]

        data_model_js = DataModelJSONSchema(
            jsonld_path=helpers.get_data_path("example.model.csv"),
            graph=dmge.graph,
        )

        json_schema = data_model_js.get_json_validation_schema(
            source_node=root_node, schema_name=root_node + "_validation"
        )

        validateManifest = ValidateManifest(
            errors=[],
            manifest=manifest,
            manifestPath=full_manifest_path,
            dmge=dmge,
            jsonSchema=json_schema,
        )

        _, vmr_errors, vmr_warnings = validateManifest.validate_manifest_rules(
            manifest=manifest,
            dmge=dmge,
            restrict_rules=False,
            project_scope=None,
        )

        if root_node == "Biospecimen":
            assert (
                vmr_errors
                and vmr_errors[0][0] == ["2", "3"]
                and vmr_errors[0][-1] == ["123"]
            )
            assert vmr_warnings == []
        elif root_node == "Patient":
            assert vmr_errors == []
            assert (
                vmr_warnings
                and vmr_warnings[0][0] == ["2", "3"]
                and vmr_warnings[0][-1] == ["123"]
            )


    @pytest.mark.rule_combos(
        reason="This introduces a great number of tests covering every possible rule combination that are only necessary on occasion."
    )
    @pytest.mark.parametrize("base_rule, second_rule", get_rule_combinations())
    def test_rule_combinations(
        self, helpers, dmge, base_rule, second_rule, metadataModel
    ):
        """
        TODO: Describe what this test is doing.
        Updating the data model graph to allow testing of allowable rule combinations.
        Works one rule combo at a time using (get_rule_combinations.)
        """
        rule_regex = re.compile(base_rule + ".*")
        rootNode = "MockComponent"

        manifestPath = helpers.get_data_path("mock_manifests/Rule_Combo_Manifest.csv")
        manifest = helpers.get_data_frame(manifestPath)

        # Get a view of the node data
        all_node_data = dmge.graph.nodes.data()

        # Update select validation rules in the data model graph for columns in the manifest
        for attribute in manifest.columns:
            # Get the node label
            node_label = dmge.get_node_label(attribute)

            # Get a view of the recorded info for current node
            node_info = all_node_data[node_label]
            if node_info["validationRules"]:
                if node_info["displayName"] == "Check NA":
                    # Edit the node info -in place-
                    node_info["validationRules"].remove("int")
                    break

                if base_rule in node_info["validationRules"] or re.match(
                    rule_regex, node_info["validationRules"][0]
                ):
                    if second_rule.startswith(
                        "matchAtLeastOne"
                    ) or second_rule.startswith("matchExactlyOne"):
                        rule_args = f" MockComponent.{node_label} Patient.PatientID"
                    elif second_rule.startswith("inRange"):
                        rule_args = " 1 1000 warning"
                    elif second_rule.startswith("regex"):
                        rule_args = " search [a-f]"
                    else:
                        rule_args = ""
                    # Edit the node info -in place-
                    node_info["validationRules"].append(second_rule + rule_args)
                    break

        # Update the manifest to only contain the Component and attribute column where the rule was changed.
        manifest = manifest[["Component", attribute]]

        data_model_js = DataModelJSONSchema(
            jsonld_path=helpers.get_data_path("example.model.jsonld"), graph=dmge.graph
        )
        json_schema = data_model_js.get_json_validation_schema(
            source_node=rootNode, schema_name=rootNode + "_validation"
        )

        validateManifest = ValidateManifest(
            errors=[],
            manifest=manifest,
            manifestPath=manifestPath,
            dmge=dmge,
            jsonSchema=json_schema,
        )

        # perform validation with no exceptions raised
        _, errors, warnings = validateManifest.validate_manifest_rules(
            manifest=manifest,
            dmge=dmge,
            restrict_rules=False,
            project_scope=None,
        )
