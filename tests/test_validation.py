import logging
import os
import re
from typing import Generator
from unittest.mock import patch

import pytest
from pandas import Series, DataFrame, concat

from schematic.models.metadata import MetadataModel
from schematic.models.validate_attribute import (
    GenerateError,
    ValidateAttribute,
    SetValidationOutput,
    ValueValidationOutput,
)
from schematic.models.validate_manifest import ValidateManifest
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.utils.validate_rules_utils import validation_rule_info
import schematic.models.validate_attribute

# pylint: disable=protected-access

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(name="dmge")
def DMGE(helpers) -> Generator[DataModelGraphExplorer, None, None]:
    dmge = helpers.get_data_model_graph_explorer(path="example.model.jsonld")
    yield dmge


def get_metadataModel(helpers, model_name: str):
    metadataModel = MetadataModel(
        inputMModelLocation=helpers.get_data_path(model_name),
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )
    return metadataModel


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
        ("model_name", "manifest_name", "root_node"),
        [
            (
                "example.model.csv",
                "mock_manifests/Valid_Test_Manifest.csv",
                "MockComponent",
            ),
            (
                "example.model.csv",
                "mock_manifests/Patient_test_no_entry_for_cond_required_column.manifest.csv",
                "Patient",
            ),
            (
                "example_test_nones.model.csv",
                "mock_manifests/Valid_Test_Manifest_with_nones.csv",
                "MockComponent",
            ),
        ],
        ids=[
            "example_model",
            "example_with_no_entry_for_cond_required_columns",
            "example_with_nones",
        ],
    )
    @pytest.mark.parametrize(
        "project_scope",
        ["syn54126707", "syn55250368", "syn55271234"],
        ids=[
            "project_scope_with_manifests",
            "project_scope_without_manifests",
            "project_scope_with_empty_manifest",
        ],
    )
    def test_valid_manifest(
        self,
        helpers,
        model_name: str,
        manifest_name: str,
        root_node: str,
        project_scope: str,
        dmge: DataModelGraph,
    ):
        """Run the valid manifest in various situations, some of which will generate errors or warnings,
            if there are "issues" with target manifests on manifests. Since there are so many parameters, limit
            the combinations that are being run to the ones that are relevant.
        Args:
            project_scope: scope to limit cross manifest validation
                project_scope_with_manifests, is a scope that contains all the necessary manifests with the proper
                entries, for the Valid_Test_Manifest.csv to pass validation
                project_scope_without_manifests, is a scope that does not contain any manifests. For each cross manifest
                    validation run, there will be a warning raised that validation will pass and its assumed the
                    manifest is the first one being submitted.
                project_scope_with_empty_manifest, is a scope that contains a single empty MockComponent manifest,
                    depending on messaging level, a warning or error will be raised to alert users that the
                    target manifest is empty.
            model_name, str: csv model used for validation, located in tests/data
            manifest_name, str: valid manifest being validated
            root_node, str:
                Component name for the manifest being validated

        """
        manifest_path = helpers.get_data_path(manifest_name)

        warning_rule_sets_1 = [
            ("Check Match at Least", "matchAtLeastOne Patient.PatientID set"),
            (
                "Check Match at Least values",
                "matchAtLeastOne MockComponent.checkMatchatLeastvalues value",
            ),
            (
                "Check Match Exactly",
                "matchExactlyOne MockComponent.checkMatchExactly set",
            ),
            (
                "Check Match Exactly values",
                "matchExactlyOne MockComponent.checkMatchExactlyvalues value",
            ),
        ]
        warning_rule_sets_2 = warning_rule_sets_1[1:]
        error_rule_sets = [
            ("Check Match None", "matchNone MockComponent.checkMatchNone set error"),
            (
                "Check Match None values",
                "matchNone MockComponent.checkMatchNonevalues value error",
            ),
        ]

        # For the standard project scope, models and manifest should pass without warnings or errors
        if project_scope == "syn54126707":
            metadataModel = get_metadataModel(helpers, model_name)
            errors, warnings = metadataModel.validateModelManifest(
                manifestPath=manifest_path,
                rootNode=root_node,
                project_scope=[project_scope],
            )
            assert (errors, warnings) == ([], [])

        # When submitting the first manifest for cross manifest validation (MockComponent), check that proper warning
        # (to alert users that no validation will be run), is raised. The manifest is still valid to submit.
        if (
            project_scope == "syn55250368"
            and root_node == "MockComponent"
            and model_name in ["example.model.csv", "example_test_nones.model.csv"]
        ):
            metadataModel = get_metadataModel(helpers, model_name)
            errors, warnings = metadataModel.validateModelManifest(
                manifestPath=manifest_path,
                rootNode=root_node,
                project_scope=[project_scope],
            )

            for attribute_name, val_rule in warning_rule_sets_1:
                assert (
                    GenerateError.generate_no_cross_warning(
                        dmge=dmge, attribute_name=attribute_name, val_rule=val_rule
                    )[0]
                    in warnings
                )
            assert errors == []

        # When submitting a manifest to a project that contains a manifest without data, ensure that the proper
        # warnings/errors are raised.
        elif (
            project_scope == "syn55271234"
            and root_node == "MockComponent"
            and model_name == "example.model.csv"
        ):
            metadataModel = get_metadataModel(helpers, model_name)
            errors, warnings = metadataModel.validateModelManifest(
                manifestPath=manifest_path,
                rootNode=root_node,
                project_scope=[project_scope],
            )
            for attribute_name, val_rule in warning_rule_sets_2:
                assert (
                    GenerateError.generate_no_value_in_manifest_error(
                        dmge=dmge, attribute_name=attribute_name, val_rule=val_rule
                    )[1][0]
                    in warnings
                )

            for attribute_name, val_rule in error_rule_sets:
                assert (
                    GenerateError.generate_no_value_in_manifest_error(
                        dmge=dmge, attribute_name=attribute_name, val_rule=val_rule
                    )[0][0]
                    in errors
                )

    def test_invalid_manifest(self, helpers, dmge):
        metadataModel = get_metadataModel(helpers, model_name="example.model.jsonld")

        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = "MockComponent"

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode,
            project_scope=["syn54126707"],
        )

        # Check errors
        assert (
            GenerateError.generate_type_error(
                val_rule="num",
                row_num="3",
                attribute_name="Check Num",
                invalid_entry="c",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_type_error(
                val_rule="int",
                row_num="3",
                attribute_name="Check Int",
                invalid_entry="5.63",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_type_error(
                val_rule="str",
                row_num="3",
                attribute_name="Check String",
                invalid_entry="94",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_list_error(
                val_rule="list",
                list_string="9",
                row_num="3",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="9",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_list_error(
                val_rule="list",
                list_string="ab",
                row_num="4",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="ab",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_list_error(
                val_rule="list",
                list_string="a c f",
                row_num="3",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="a c f",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_list_error(
                val_rule="list",
                list_string="a",
                row_num="4",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="a",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_list_error(
                val_rule="list",
                list_string="a",
                row_num="4",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="a",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Format",
                module_to_call="match",
                invalid_entry="m",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Single",
                module_to_call="search",
                invalid_entry="q",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="^\d+$",
                row_num="2",
                attribute_name="Check Regex Integer",
                module_to_call="search",
                invalid_entry="5.4",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_url_error(
                val_rule="url",
                url="http://googlef.com/",
                url_error="invalid_url",
                row_num="3",
                attribute_name="Check URL",
                argument=None,
                invalid_entry="http://googlef.com/",
                dmge=dmge,
            )[0]
            in errors
        )

        date_err = GenerateError.generate_content_error(
            val_rule="date",
            attribute_name="Check Date",
            dmge=dmge,
            row_num=["2", "3", "4"],
            invalid_entry=["84-43-094", "32-984", "notADate"],
        )[0]
        error_in_list = [date_err[2] in error for error in errors]
        assert any(error_in_list)

        assert (
            GenerateError.generate_content_error(
                val_rule="unique error",
                attribute_name="Check Unique",
                dmge=dmge,
                row_num=["2", "3", "4"],
                invalid_entry=["str1"],
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_content_error(
                val_rule="inRange 50 100 error",
                attribute_name="Check Range",
                dmge=dmge,
                row_num=["3"],
                invalid_entry=["30"],
            )[0]
            in errors
        )

        assert (
            "Rule: matchNone set; "
            "Attribute: Check Match None; Manifest matched one or more "
            "manifests: [syn54127008]"
        ) in errors

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
        assert (
            GenerateError.generate_content_error(
                val_rule="recommended",
                attribute_name="Check Recommended",
                dmge=dmge,
            )[1]
            in warnings
        )

        assert (
            GenerateError.generate_content_error(
                val_rule="protectAges",
                attribute_name="Check Ages",
                dmge=dmge,
                row_num=["2", "3"],
                invalid_entry=["6549", "32851"],
            )[1]
            in warnings
        )

        assert (
            "Rule: matchAtLeastOne set; "
            "Attribute: Check Match at Least; Manifest did not match any target "
            "manifests: [syn54126997, syn54127001]"
        ) in warnings

        assert (
            GenerateError.generate_cross_warning(
                val_rule="matchAtLeastOne MockComponent.checkMatchatLeastvalues value",
                row_num=["3"],
                attribute_name="Check Match at Least values",
                invalid_entry=["51100"],
                dmge=dmge,
            )[1]
            in warnings
        )

        assert (
            "Rule: matchExactlyOne set; "
            "Attribute: Check Match Exactly; Manifest did not match any target "
            "manifests: [syn54126950, syn54127008]"
        ) in warnings

        cross_warning = GenerateError.generate_cross_warning(
            val_rule="matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value",
            row_num=["2", "3", "4"],
            attribute_name="Check Match Exactly values",
            invalid_entry=["71738", "98085", "210065"],
            dmge=dmge,
        )[1]

        warning_in_list = [cross_warning[1] in warning for warning in warnings]
        assert any(warning_in_list)

    def test_in_house_validation(self, helpers, dmge):
        metadataModel = get_metadataModel(helpers, model_name="example.model.jsonld")
        manifestPath = helpers.get_data_path("mock_manifests/Invalid_Test_Manifest.csv")
        rootNode = "MockComponent"

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode,
            restrict_rules=True,
            project_scope=["syn54126707"],
        )

        # Check errors
        assert (
            GenerateError.generate_type_error(
                val_rule="num",
                row_num="3",
                attribute_name="Check Num",
                invalid_entry="c",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_type_error(
                val_rule="int",
                row_num="3",
                attribute_name="Check Int",
                invalid_entry="5.63",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_type_error(
                val_rule="str",
                row_num="3",
                attribute_name="Check String",
                invalid_entry="94",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_type_error(
                val_rule="int",
                row_num="3",
                attribute_name="Check NA",
                invalid_entry="9.5",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_list_error(
                val_rule="list",
                list_string="9",
                row_num="3",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="9",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_list_error(
                val_rule="list",
                list_string="ab",
                row_num="4",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="ab",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Single",
                module_to_call="search",
                invalid_entry="q",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_regex_error(
                val_rule="regex",
                reg_expression="[a-f]",
                row_num="3",
                attribute_name="Check Regex Format",
                module_to_call="match",
                invalid_entry="m",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_url_error(
                val_rule="url",
                url="http://googlef.com/",
                url_error="invalid_url",
                row_num="3",
                attribute_name="Check URL",
                argument=None,
                invalid_entry="http://googlef.com/",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            "Rule: matchNone set; "
            "Attribute: Check Match None; Manifest matched one or more "
            "manifests: [syn54127008]"
        ) in errors

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
        assert (
            "Rule: matchAtLeastOne set; "
            "Attribute: Check Match at Least; Manifest did not match any target "
            "manifests: [syn54126997, syn54127001]"
        ) in warnings

        assert (
            GenerateError.generate_cross_warning(
                val_rule="matchAtLeastOne MockComponent.checkMatchatLeastvalues value",
                row_num=["3"],
                attribute_name="Check Match at Least values",
                invalid_entry=["51100"],
                dmge=dmge,
            )[1]
            in warnings
        )

        assert (
            "Rule: matchExactlyOne set; "
            "Attribute: Check Match Exactly; Manifest did not match any target "
            "manifests: [syn54126950, syn54127008]"
        ) in warnings

        assert (
            GenerateError.generate_cross_warning(
                val_rule="matchExactlyOne MockComponent.checkMatchExactlyvalues MockComponent.checkMatchExactlyvalues value",
                row_num=["2", "3", "4"],
                attribute_name="Check Match Exactly values",
                invalid_entry=["71738", "98085", "210065"],
                dmge=dmge,
            )[1]
            in warnings
        )

    def test_filename_manifest(self, helpers, dmge):
        metadataModel = get_metadataModel(helpers, model_name="example.model.jsonld")

        manifestPath = helpers.get_data_path(
            "mock_manifests/InvalidFilenameManifest.csv"
        )
        rootNode = "MockFilename"

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifestPath,
            rootNode=rootNode,
            project_scope=["syn23643250"],
        )

        # Check errors
        assert (
            GenerateError.generate_filename_error(
                val_rule="filenameExists syn61682648",
                attribute_name="Filename",
                row_num="3",
                invalid_entry="schematic - main/MockFilenameComponent/txt4.txt",
                error_type="mismatched entityId",
                dmge=dmge,
            )[0]
            in errors
        )

        assert (
            GenerateError.generate_filename_error(
                val_rule="filenameExists syn61682648",
                attribute_name="Filename",
                row_num="4",
                invalid_entry="schematic - main/MockFilenameComponent/txt5.txt",
                error_type="path does not exist",
                dmge=dmge,
            )[0]
            in errors
        )

        assert len(errors) == 2
        assert len(warnings) == 0

    def test_missing_column(self, helpers, dmge: DataModelGraph):
        """Test that a manifest missing a column returns the proper error."""
        model_name = "example.model.csv"
        manifest_name = "mock_manifests/Invalid_Biospecimen_Missing_Column_Manifest.csv"
        root_node = "Biospecimen"
        manifest_path = helpers.get_data_path(manifest_name)

        metadataModel = get_metadataModel(helpers, model_name)
        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifest_path,
            rootNode=root_node,
        )

        assert (
            GenerateError.generate_schema_error(
                row_num="2",
                attribute_name="Wrong schema",
                error_message="'Tissue Status' is a required property",
                invalid_entry="Wrong schema",
                dmge=dmge,
            )[0]
            in errors
        )

    @pytest.mark.parametrize(
        "model_name",
        [
            "example.model.csv",
            "example_required_vr_test.model.csv",
        ],
        ids=["example_model", "example_with_requirements_from_vr"],
    )
    @pytest.mark.parametrize(
        [
            "manifest_name",
            "root_node",
        ],
        [
            (
                "mock_manifests/Biospecimen_required_vr_test_fail.manifest.csv",
                "Biospecimen",
            ),
            (
                "mock_manifests/Biospecimen_required_vr_test_pass.manifest.csv",
                "Biospecimen",
            ),
            ("mock_manifests/Patient_required_vr_test_pass.manifest.csv", "Patient"),
            (
                "mock_manifests/Patient_test_no_entry_for_cond_required_column.manifest.csv",
                "Patient",
            ),
            (
                "mock_manifests/BulkRNAseq_component_based_required_rule_test.manifest.csv",
                "BulkRNA-seqAssay",
            ),
        ],
        ids=[
            "biospeciment_required_vr_empty",
            "biospecimen_required_filled",
            "patient_not_required_empty",
            "patient_conditionally_required_not_filled",
            "bulk_rna_seq_component_based_rule_test",
        ],
    )
    def test_required_validation_rule(
        self,
        helpers,
        model_name: str,
        manifest_name: str,
        root_node: str,
        dmge: DataModelGraphExplorer,
    ) -> None:
        """
        Args:
            model_name, str: model to run test validation against
                Model Difference:
                    - example.model.csv:
                        PatientID attribute:
                            Required=True
                        FileFormat attribute:
                            Required = True, Genome Build/ Genome Fasta are conditionally required

                    - example_required_vr_test.model.csv,
                        PatientID attribute
                            Required=False,
                            validation rule: #Patient unique warning^^#Biospecimen unique required error
                                meaning PatientID is required for the Biospecimen manifest (but not Patient)
                        FileFormat attribute:
                            Required = False
                            validation rule: ^^#BulkRNA-seqAssay list required
                                meaning for BulkRNA=seqAssay (only) FileFormat is conditionally required.
                                Genome Build/ Genome Fasta are conditionally required
            manifest_name, str: manfiest to run validation with
                What Each Manifest is Testing:
                    -Biospecimen_required_vr_test_fail: PatentID is required for Biospecimen in each model (through different routes) and not provided
                    -Biospecimen_required_vr_test_pass: PatentID is required for Biospecimen in each model (through different routes) and provided
                    -Patient_required_vr_test_pass: PatentID not provided, will fail for example model and pass for vr test model (where it is not required)
                    -Patient_test_no_entry_for_cond_required_column: Tests conditionally required value not required if preceeding condition not met (helps test an edge case)
                    -BulkRNAseq_component_based_required_rule_test: FileFormat, this manifest checks conditional requirements, provided for some rows and not for others

            root_node, str: component for the given manifest
            dmge, DataModelGraphExplorer Object
        """

        manifest_path = helpers.get_data_path(manifest_name)
        metadataModel = get_metadataModel(helpers, model_name)

        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifest_path,
            rootNode=root_node,
        )

        error_and_warning_free_manifests = [
            "Biospecimen_required_vr_test_pass",
            "Patient_test_no_entry_for_cond_required_column",
            "",
        ]

        # For each model, these manifest should pass, bc either the value is being passed as requierd, or its not currently required
        for manifest in error_and_warning_free_manifests:
            if manifest_name in manifest:
                assert errors == []
                assert warnings == []

        messages = {
            "patient_id_empty_warning": {
                "row_num": "2",
                "attribute_name": "Patient ID",
                "error_message": "'' should be non-empty",
                "invalid_entry": "",
            },
            "bulk_rnaseq_cbr_error_1": {
                "row_num": "3",
                "attribute_name": "Genome FASTA",
                "error_message": "'' should be non-empty",
                "invalid_entry": "",
            },
            "bulk_rnaseq_cbr_error_2": {
                "row_num": "4",
                "attribute_name": "File Format",
                "error_message": "'' is not one of ['CSV/TSV', 'CRAM', 'FASTQ', 'BAM']",
                "invalid_entry": "",
            },
        }

        # This manifest should fail in the example_model bc the manifest Required=False, and in the example_with_requirements_from_vr
        # bc the requirments are set to false in the validation rule
        if ("Biospecimen_required_vr_test_fail" in manifest_name) or (
            "Patient_required_vr_test_pass" in manifest_name
            and model_name == "example.model.csv"
        ):
            message_key = "patient_id_empty_warning"
            assert (
                GenerateError.generate_schema_error(
                    row_num=messages[message_key]["row_num"],
                    attribute_name=messages[message_key]["attribute_name"],
                    error_message=messages[message_key]["error_message"],
                    invalid_entry=messages[message_key]["invalid_entry"],
                    dmge=dmge,
                )[0]
                in errors
            )
            assert warnings == []

        if (
            "Patient_required_vr_test_pass" in manifest_name
            and model_name == "example_required_vr_test.model.csv"
        ):
            assert errors == []
            assert warnings == []

        if "BulkRNAseq_component_based_required_rule_test" in manifest_name:
            message_key = "bulk_rnaseq_cbr_error_1"
            assert (
                GenerateError.generate_schema_error(
                    row_num=messages[message_key]["row_num"],
                    attribute_name=messages[message_key]["attribute_name"],
                    error_message=messages[message_key]["error_message"],
                    invalid_entry=messages[message_key]["invalid_entry"],
                    dmge=dmge,
                )[0]
                in errors
            )

            message_key = "bulk_rnaseq_cbr_error_2"
            expected_error = GenerateError.generate_schema_error(
                row_num=messages[message_key]["row_num"],
                attribute_name=messages[message_key]["attribute_name"],
                error_message=messages[message_key]["error_message"],
                invalid_entry=messages[message_key]["invalid_entry"],
                dmge=dmge,
            )[0]

            # since the valid value order isnt set in error reporting, check a portion of the expected output
            # Check the error row is expected
            assert expected_error[1] in errors[1]
            # Check that one of the values for the expected valid values is present
            # Extract a valid value
            valid_value = (
                expected_error[2].split(",")[-1].split("]")[0].strip(" ").strip("'")
            )
            assert valid_value in errors[1][2]
            assert warnings == []

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
        self,
        helpers,
        dmge,
        base_rule,
        second_rule,
    ):
        """
        TODO: Describe what this test is doing.
        Updating the data model graph to allow testing of allowable rule combinations.
        Works one rule combo at a time using (get_rule_combinations.)
        """
        rule_regex = re.compile(base_rule + ".*")
        rootNode = "MockComponent"

        metadataModel = get_metadataModel(helpers, model_name="example.model.jsonld")

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


class TestValidateAttributeObject:
    def test_login(self, dmge: DataModelGraphExplorer) -> None:
        """
        Tests that sequential logins update the view query as necessary
        """
        validate_attribute = ValidateAttribute(dmge)
        validate_attribute._login()

        assert (
            validate_attribute.synStore.fileview_query == "SELECT * FROM syn23643253 ;"
        )

        validate_attribute._login(
            project_scope=["syn23643250"],
            columns=["name", "id", "path"],
            where_clauses=["parentId='syn61682648'", "type='file'"],
        )
        assert (
            validate_attribute.synStore.fileview_query
            == "SELECT name,id,path FROM syn23643253 WHERE parentId='syn61682648' AND type='file' AND projectId IN ('syn23643250', '') ;"
        )

    def test__get_target_manifest_dataframes(self, va_obj: ValidateAttribute) -> None:
        """Testing for ValidateAttribute._get_target_manifest_dataframes"""
        manifests = va_obj._get_target_manifest_dataframes(
            "patient", project_scope=["syn54126707"]
        )
        assert list(manifests.keys()) == ["syn54126997", "syn54127001"]


@pytest.fixture(name="cross_val_df1")
def fixture_cross_val_df1() -> Generator[DataFrame, None, None]:
    """Yields a dataframe"""
    df = DataFrame(
        {
            "PatientID": ["A", "B", "C"],
            "component": ["comp1", "comp1", "comp1"],
            "id": ["id1", "id2", "id3"],
            "entityid": ["x", "x", "x"],
        }
    )
    yield df


@pytest.fixture(name="cross_val_df2")
def fixture_cross_val_df2(cross_val_df1: DataFrame) -> Generator[DataFrame, None, None]:
    """Yields dataframe df1 with an extra row"""
    df = concat(
        [
            cross_val_df1,
            DataFrame(
                {
                    "PatientID": ["D"],
                    "component": ["comp1"],
                    "id": ["id4"],
                    "entityid": ["x"],
                }
            ),
        ]
    )
    yield df


@pytest.fixture(name="cross_val_df3")
def fixture_cross_val_df3() -> Generator[DataFrame, None, None]:
    """Yields empty dataframe"""
    df = DataFrame(
        {
            "PatientID": [],
            "component": [],
            "id": [],
            "entityid": [],
        }
    )
    yield df


@pytest.fixture(name="cross_val_col_names")
def fixture_cross_val_col_names() -> Generator[dict[str, str], None, None]:
    """
    Yields:
        Generator[dict[str, str], None, None]: A dicitonary of column names
          keys are the label, and values are the display name
    """
    column_names = {
        "patientid": "PatientID",
        "component": "component",
        "id": "id",
        "entityid": "entityid",
    }
    yield column_names


@pytest.fixture(name="va_obj")
def fixture_va_obj(
    dmge: DataModelGraphExplorer,
) -> Generator[ValidateAttribute, None, None]:
    """Yield a ValidateAttribute object"""
    yield ValidateAttribute(dmge)


class TestUnitValidateAttributeObject:
    """Testing for ValidateAttribute class with all Synapse calls mocked"""

    def test_cross_validation_match_atleast_one_set_rules_passing(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchAtLeastOne set rules"""
        val_rule = "matchAtLeastOne Patient.PatientID set error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C"])) == (
                [],
                [],
            )
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C", "C"])) == (
                [],
                [],
            )
            assert va_obj.cross_validation(
                val_rule, Series(["A", "B", "C", "A", "B", "C"])
            ) == (
                [],
                [],
            )

    def test_cross_validation_match_atleast_one_set_rules_errors(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchAtLeastOne set rules"""
        val_rule = "matchAtLeastOne Patient.PatientID set error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            errors, warnings = va_obj.cross_validation(
                val_rule, Series(["A", "B"], index=[0, 1], name="PatientID")
            )
            assert len(warnings) == 0
            assert len(errors) == 1

            errors, warnings = va_obj.cross_validation(
                val_rule,
                Series(["A", "B", "C", "D"], index=[0, 1, 2, 3], name="PatientID"),
            )
            assert len(warnings) == 0
            assert len(errors) == 1

            errors, warnings = va_obj.cross_validation(
                val_rule, Series([], index=[], name="PatientID")
            )
            assert len(warnings) == 0
            assert len(errors) == 1

            errors, warnings = va_obj.cross_validation(
                val_rule, Series([""], index=[0], name="PatientID")
            )
            assert len(warnings) == 0
            assert len(errors) == 1

    def test_cross_validation_match_atleast_one_set_rules_warnings(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchAtLeastOne set rules"""
        val_rule = "matchAtLeastOne Patient.PatientID set warning"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            errors, warnings = va_obj.cross_validation(
                val_rule, Series(["A", "B"], index=[0, 1], name="PatientID")
            )
            assert len(warnings) == 1
            assert len(errors) == 0

            errors, warnings = va_obj.cross_validation(
                val_rule,
                Series(["A", "B", "C", "D"], index=[0, 1, 2, 3], name="PatientID"),
            )
            assert len(warnings) == 1
            assert len(errors) == 0

    def test_cross_validation_match_exactly_one_set_rules_passing(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchExactlyOne set rules"""
        val_rule = "matchExactlyOne Patient.PatientID set error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C"])) == (
                [],
                [],
            )
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C", "C"])) == (
                [],
                [],
            )

    def test_cross_validation_match_exactly_one_set_rules_errors(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchExactlyOne set rules"""
        val_rule = "matchExactlyOne Patient.PatientID set error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            errors, _ = va_obj.cross_validation(
                val_rule, Series(["A", "B"], index=[0, 1], name="PatientID")
            )
            assert len(errors) == 1

            errors, _ = va_obj.cross_validation(
                val_rule,
                Series(["A", "B", "C", "D"], index=[0, 1, 2, 3], name="PatientID"),
            )
            assert len(errors) == 1

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
        ):
            errors, _ = va_obj.cross_validation(
                val_rule, Series(["A", "B", "C"], index=[0, 1, 2], name="PatientID")
            )
            assert len(errors) == 1

    def test_cross_validation_match_none_set_rules_passing(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchNone set rules"""
        val_rule = "matchNone Patient.PatientID set error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            assert va_obj.cross_validation(val_rule, Series(["A", "B"])) == (
                [],
                [],
            )
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C", "D"])) == (
                [],
                [],
            )

    def test_cross_validation_match_none_set_rules_errors(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchNone set rules"""
        val_rule = "matchNone Patient.PatientID set error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            errors, _ = va_obj.cross_validation(
                val_rule,
                Series(["A", "B", "C"], index=[0, 1, 2], name="PatientID"),
            )
            assert len(errors) == 1

            errors, _ = va_obj.cross_validation(
                val_rule,
                Series(["A", "B", "C", "C"], index=[0, 1, 2, 3], name="PatientID"),
            )
            assert len(errors) == 1

    def test_cross_validation_value_match_atleast_one_rules_passing(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchAtLeastOne value rules"""
        val_rule = "matchAtLeastOne Patient.PatientID value error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            assert va_obj.cross_validation(val_rule, Series([])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A"])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A", "A"])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A", "B"])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C"])) == (
                [],
                [],
            )
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C", "C"])) == (
                [],
                [],
            )

    def test_cross_validation_value_match_atleast_one_rules_errors(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchAtLeastOne value rules"""
        val_rule = "matchAtLeastOne Patient.PatientID value error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            errors, _ = va_obj.cross_validation(
                val_rule, Series(["D"], index=[0], name="PatientID")
            )
            assert len(errors) == 1

    def test_cross_validation_match_exactly_ne_value_rules_passing(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchExactlyOne value rules"""
        val_rule = "matchExactlyOne Patient.PatientID value error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            assert va_obj.cross_validation(val_rule, Series([])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A"])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A", "A"])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A", "B"])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C"])) == (
                [],
                [],
            )
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C", "C"])) == (
                [],
                [],
            )

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
        ):
            assert va_obj.cross_validation(val_rule, Series([])) == ([], [])

    def test_cross_validation_match_exactly_ne_value_rules_errors(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchExactlyOne value rules"""
        val_rule = "matchExactlyOne Patient.PatientID value error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            errors, _ = va_obj.cross_validation(
                val_rule, Series(["D"], index=[0], name="PatientID")
            )
            assert len(errors) == 1

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
        ):
            errors, _ = va_obj.cross_validation(
                val_rule, Series(["A"], index=[0], name="PatientID")
            )
            assert len(errors) == 1

            errors, _ = va_obj.cross_validation(
                val_rule, Series(["D"], index=[0], name="PatientID")
            )
            assert len(errors) == 1

    def test_cross_validation_match_none_value_rules_passing(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchNone value rules"""
        val_rule = "matchNone Patient.PatientID value error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            assert va_obj.cross_validation(val_rule, Series([])) == ([], [])
            assert va_obj.cross_validation(val_rule, Series(["D"])) == ([], [])

    def test_cross_validation_match_none_value_rules_errors(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ):
        """Tests for cross manifest validation for matchNone value rules"""
        val_rule = "matchNone Patient.PatientID value error"

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            errors, _ = va_obj.cross_validation(
                val_rule, Series(["A"], index=[0], name="PatientID")
            )
            assert len(errors) == 1

    def test__run_validation_across_target_manifests_failures(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
        cross_val_df3: DataFrame,
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_target_manifests with failures"""
        # This shows that when no target manifests are found to check against, False is returned
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={},
        ):
            _, result = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="value",
                access_token="xxx",
                val_rule="rule comp.att",
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert result is False

        # This shows that when the only target manifest is empty, only a message is returned
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df3},
        ):
            _, result = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="value",
                access_token="xxx",
                val_rule="rule Patient.PatientID",
                manifest_col=Series([]),
                target_column=Series([]),
            )

            assert result == "values not recorded in targets stored"

        # This shows that when the only target manifest is empty, only a message is returned
        # even if the tested column has values
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df3},
        ):
            _, result = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="value",
                access_token="xxx",
                val_rule="rule Patient.PatientID",
                manifest_col=Series(["A", "B", "C"]),
                target_column=Series([]),
            )

            assert result == "values not recorded in targets stored"

        # This shows that when any target manifest is empty, only a message is returned
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df3},
        ):
            _, result = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="value",
                access_token="xxx",
                val_rule="rule Patient.PatientID",
                manifest_col=Series([]),
                target_column=Series([]),
            )

            assert result == "values not recorded in targets stored"

    def test__run_validation_across_target_manifests_value_rules(
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_target_manifests with value rule"""

        # This tests when an empty column is validated there are no missing values to be returned
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="value",
                access_token="xxx",
                val_rule="rule Patient.PatientID value",
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, ValueValidationOutput)
            assert validation_output.missing_values.empty
            assert validation_output.duplicated_values.empty
            assert validation_output.repeat_values.empty

    def test__run_validation_across_target_manifests_set_rules(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_target_manifests with set rule"""

        # This tests when an empty column is validated, all values in the target
        # manifest are returned as missing in the column
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="set",
                access_token="xxx",
                val_rule="rule Patient.PatientID set",
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, SetValidationOutput)
            assert validation_output.target_manifests == ["syn1"]
            assert validation_output.matching_manifests == []
            mmv = validation_output.missing_manifest_values
            assert list(mmv.keys()) == ["syn1"]
            assert mmv["syn1"].to_list() == ["A", "B", "C"]
            assert validation_output.missing_target_values == {}

        # This tests when series is validated and has all values in the target
        # manifest, no missing values are returned
        # In addition this shows that both manifests are listed in the target
        # nad matching manifests
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="set",
                access_token="xxx",
                val_rule="rule Patient.PatientID set",
                manifest_col=Series(["A", "B", "C"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, SetValidationOutput)
            assert validation_output.target_manifests == ["syn1", "syn2"]
            assert validation_output.matching_manifests == ["syn1", "syn2"]
            assert validation_output.missing_manifest_values == {}
            assert validation_output.missing_target_values == {}

    def test__run_validation_across_targets_value(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_targets_value"""

        validation_output = va_obj._run_validation_across_targets_value(
            manifest_col=Series(["A", "B", "C"]),
            concatenated_target_column=Series(["A", "B", "C"]),
        )
        assert validation_output.missing_values.empty
        assert validation_output.duplicated_values.empty
        assert validation_output.repeat_values.to_list() == ["A", "B", "C"]

        validation_output = va_obj._run_validation_across_targets_value(
            manifest_col=Series(["C"]),
            concatenated_target_column=Series(["A", "B", "B", "C", "C"]),
        )
        assert validation_output.missing_values.empty
        assert validation_output.duplicated_values.to_list() == ["C"]
        assert validation_output.repeat_values.to_list() == ["C"]

        validation_output = va_obj._run_validation_across_targets_value(
            manifest_col=Series(["A", "B", "C"]),
            concatenated_target_column=Series(["A"]),
        )
        assert validation_output.missing_values.to_list() == ["B", "C"]
        assert validation_output.duplicated_values.empty
        assert validation_output.repeat_values.to_list() == ["A"]

    def test__gather_value_warnings_errors_passing(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_value_warnings_errors"""
        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID value error",
            source_attribute="PatientID",
            validation_output=ValueValidationOutput(
                duplicated_values=Series(["A", "B", "C"])
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID value error",
            source_attribute="PatientID",
            validation_output=ValueValidationOutput(
                duplicated_values=Series(["A", "B", "C"]),
                repeat_values=Series(["A", "B", "C"]),
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule="matchAtLeastOne comp.att value error",
            source_attribute="att",
            validation_output=ValueValidationOutput(),
        )
        assert len(errors) == 0
        assert len(warnings) == 0

    def test__gather_value_warnings_errors_with_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_value_warnings_errors"""

        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID value error",
            source_attribute="PatientID",
            validation_output=(
                ValueValidationOutput(missing_values=Series(["A", "B", "C"]))
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert len(errors[0]) == 4
        assert errors[0][1] == "PatientID"
        assert errors[0][2] == (
            "Value(s) ['A', 'B', 'C'] from row(s) ['2', '3', '4'] of the attribute "
            "PatientID in the source manifest are missing."
        )

    def test__run_validation_across_targets_set(
        self,
        va_obj: ValidateAttribute,
        cross_val_col_names: dict[str, str],
        cross_val_df1: DataFrame,
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_targets_set for matchAtLeastOne"""

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn1",
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
            validation_output=SetValidationOutput(),
        )
        assert output.target_manifests == ["syn1"]
        assert output.matching_manifests == ["syn1"]
        assert output.missing_manifest_values == {}
        assert output.missing_target_values == {}
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn2",
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
            validation_output=output,
        )
        assert output.target_manifests == ["syn1", "syn2"]
        assert output.matching_manifests == ["syn1", "syn2"]
        assert not output.missing_manifest_values
        assert not output.missing_target_values
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C", "D"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn1",
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
            validation_output=SetValidationOutput(),
        )
        assert output.target_manifests == ["syn1"]
        assert not output.matching_manifests
        assert not output.missing_manifest_values
        assert output.missing_target_values["syn1"].to_list() == ["D"]
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C", "D"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn2",
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
            validation_output=output,
        )
        assert output.target_manifests == ["syn1", "syn2"]
        assert not output.matching_manifests
        assert not output.missing_manifest_values
        assert output.missing_target_values["syn1"].to_list() == ["D"]
        assert output.missing_target_values["syn2"].to_list() == ["D"]
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn1",
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
            validation_output=SetValidationOutput(),
        )
        assert output.target_manifests == ["syn1"]
        assert not output.matching_manifests
        assert output.missing_manifest_values["syn1"].to_list() == ["C"]
        assert not output.missing_target_values
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn2",
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
            validation_output=output,
        )
        assert output.target_manifests == ["syn1", "syn2"]
        assert not output.matching_manifests
        assert output.missing_manifest_values["syn1"].to_list() == ["C"]
        assert output.missing_manifest_values["syn2"].to_list() == ["C"]
        assert not output.missing_target_values
        assert bool_list1 == [True]
        assert bool_list2 == [False]

    def test__gather_set_warnings_errors_match_atleast_one_passing(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchAtLeastOne"""
        val_rule = "matchAtLeastOne Patient.PatientID set error"

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(
                target_manifests=["syn1"], matching_manifests=["syn1"]
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(
                target_manifests=["syn1", "syn2"], matching_manifests=["syn1", "syn2"]
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

    def test__gather_set_warnings_errors_match_atleast_one_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchAtLeastOne"""
        val_rule = "matchAtLeastOne Patient.PatientID set error"

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(target_manifests=["syn1"]),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0] == (
            "Rule: matchAtLeastOne set; Attribute: PatientID; Manifest did not match any target "
            "manifests: [syn1]"
        )

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(target_manifests=["syn1", "syn2"]),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0] == (
            "Rule: matchAtLeastOne set; Attribute: PatientID; Manifest did not match any target "
            "manifests: [syn1, syn2]"
        )

    def test__gather_set_warnings_errors_match_exactly_one_passing(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchExactlyOne"""
        val_rule = "matchExactlyOne Patient.PatientID set error"

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(
                matching_manifests=["syn1"], target_manifests=["syn1"]
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

    def test__gather_set_warnings_errors_match_exactly_one_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchExactlyOne"""
        val_rule = "matchExactlyOne Patient.PatientID set error"

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(target_manifests=["syn1"]),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0] == (
            "Rule: matchExactlyOne set; Attribute: PatientID; Manifest did not match any target "
            "manifests: [syn1]"
        )

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(target_manifests=["syn1", "syn2"]),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0] == (
            "Rule: matchExactlyOne set; Attribute: PatientID; Manifest did not match any target "
            "manifests: [syn1, syn2]"
        )

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(
                matching_manifests=["syn1", "syn2"], target_manifests=["syn1", "syn2"]
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0] == (
            "Rule: matchExactlyOne set; Attribute: PatientID; Manifest matched multiple "
            "manifests: [syn1, syn2]"
        )

    def test__gather_set_warnings_errors_match_none_passing(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchNone"""
        val_rule = "matchNone Patient.PatientID set error"

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(target_manifests=["syn1"]),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

    def test__gather_set_warnings_errors_match_none_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchNone"""
        val_rule = "matchNone Patient.PatientID set error"

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=val_rule,
            source_attribute="PatientID",
            validation_output=SetValidationOutput(
                matching_manifests=["syn1"], target_manifests=["syn1"]
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0] == (
            "Rule: matchNone set; Attribute: PatientID; Manifest matched one or more "
            "manifests: [syn1]"
        )

    def test__get_column_names(self, va_obj: ValidateAttribute) -> None:
        """Tests for ValidateAttribute._get_column_names"""
        assert not va_obj._get_column_names(DataFrame())
        assert va_obj._get_column_names(DataFrame({"col1": []})) == {"col1": "col1"}
        assert va_obj._get_column_names(DataFrame({"col1": [], "col2": []})) == {
            "col1": "col1",
            "col2": "col2",
        }
        assert va_obj._get_column_names(DataFrame({"COL 1": []})) == {"col1": "COL 1"}
        assert va_obj._get_column_names(DataFrame({"ColId": []})) == {"colid": "ColId"}
        assert va_obj._get_column_names(DataFrame({"ColID": []})) == {"colid": "ColID"}
