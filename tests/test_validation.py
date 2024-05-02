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

def get_metadataModel(helpers, model_name:str):
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
            ("example.model.csv","mock_manifests/Valid_Test_Manifest.csv", "MockComponent"),
            ("example.model.csv", "mock_manifests/Patient_test_no_entry_for_cond_required_column.manifest.csv", "Patient"),
            ("example_test_nones.model.csv","mock_manifests/Valid_Test_Manifest_with_nones.csv", "MockComponent"),
        ],
        ids=["example_model", "example_with_no_entry_for_cond_required_columns", "example_with_nones"],
    )
    @pytest.mark.parametrize(
        "project_scope",
        ["syn54126707", "syn55250368", "syn55271234"],
        ids=["project_scope_with_manifests", "project_scope_without_manifests", "project_scope_with_empty_manifest"],
    )
    def test_valid_manifest(self, helpers, model_name:str, manifest_name:str,
            root_node:str, project_scope:str, dmge:DataModelGraph):
        """ Run the valid manifest in various situations, some of which will generate errors or warnings,
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
                ('Check Match at Least', 'matchAtLeastOne Patient.PatientID set'),
                ('Check Match at Least values', 'matchAtLeastOne MockComponent.checkMatchatLeastvalues value'),
                ('Check Match Exactly', 'matchExactlyOne MockComponent.checkMatchExactly set'),
                ('Check Match Exactly values', 'matchExactlyOne MockComponent.checkMatchExactlyvalues value'),
            ]
        warning_rule_sets_2 = warning_rule_sets_1[1:]
        error_rule_sets = [
                ('Check Match None', 'matchNone MockComponent.checkMatchNone set error'),
                ('Check Match None values', 'matchNone MockComponent.checkMatchNonevalues value error'),
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
        if (project_scope == "syn55250368" and root_node=="MockComponent" and
            model_name in ["example.model.csv", "example_test_nones.model.csv"]):
            metadataModel = get_metadataModel(helpers, model_name)
            errors, warnings = metadataModel.validateModelManifest(
                manifestPath=manifest_path,
                rootNode=root_node,
                project_scope=[project_scope],
            )
            
            for attribute_name, val_rule in warning_rule_sets_1:
                assert GenerateError.generate_no_cross_warning(
                    dmge=dmge,
                    attribute_name=attribute_name,
                    val_rule=val_rule)[0] in warnings
            assert errors == []
        
        # When submitting a manifest to a project that contains a manifest without data, ensure that the proper
        # warnings/errors are raised.
        elif project_scope == "syn55271234" and root_node=="MockComponent" and model_name == "example.model.csv":
            metadataModel = get_metadataModel(helpers, model_name)
            errors, warnings = metadataModel.validateModelManifest(
                manifestPath=manifest_path,
                rootNode=root_node,
                project_scope=[project_scope],
            )
            for attribute_name, val_rule in warning_rule_sets_2:
                assert GenerateError.generate_no_value_in_manifest_error(
                    dmge=dmge,
                    attribute_name=attribute_name,
                    val_rule=val_rule)[1][0] in warnings
            
            for attribute_name, val_rule in error_rule_sets:
                assert GenerateError.generate_no_value_in_manifest_error(
                    dmge=dmge,
                    attribute_name=attribute_name,
                    val_rule=val_rule)[0][0] in errors


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
                val_rule="list",
                list_string="9",
                row_num="3",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="9",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list",
                list_string="ab",
                row_num="4",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="ab",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list",
                list_string="a c f",
                row_num="3",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="a c f",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list",
                list_string="a",
                row_num="4",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="a",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list",
                list_string="a",
                row_num="4",
                attribute_name="Check Regex List",
                list_error="not_comma_delimited",
                invalid_entry="a",
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
                val_rule="list",
                list_string="9",
                row_num="3",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="9",
                dmge=dmge,
            )[0] in errors

        assert GenerateError.generate_list_error(
                val_rule="list",
                list_string="ab",
                row_num="4",
                attribute_name="Check List",
                list_error="not_comma_delimited",
                invalid_entry="ab",
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


    def test_missing_column(self, helpers,  dmge:DataModelGraph):
        """ Test that a manifest missing a column returns the proper error.
        """
        model_name="example.model.csv"
        manifest_name="mock_manifests/Invalid_Biospecimen_Missing_Column_Manifest.csv"
        root_node="Biospecimen"
        manifest_path = helpers.get_data_path(manifest_name)

        metadataModel = get_metadataModel(helpers, model_name)
        errors, warnings = metadataModel.validateModelManifest(
            manifestPath=manifest_path,
            rootNode=root_node,
        )

        assert GenerateError.generate_schema_error(
                row_num='2',
                attribute_name="Wrong schema",
                error_message="'Tissue Status' is a required property",
                invalid_entry="Wrong schema",
                dmge=dmge,
            )[0] in errors


    @pytest.mark.parametrize(
        "model_name",
        [
            "example.model.csv",
            "example_required_vr_test.model.csv",
        ],
        ids=["example_model", "example_with_requirements_from_vr"],
    )

    @pytest.mark.parametrize(
        ["manifest_name", "root_node",],
        [
            ("mock_manifests/Biospecimen_required_vr_test_fail.manifest.csv", "Biospecimen"),
            ("mock_manifests/Biospecimen_required_vr_test_pass.manifest.csv", "Biospecimen"),
            ("mock_manifests/Patient_required_vr_test_pass.manifest.csv", "Patient"),
            ("mock_manifests/Patient_test_no_entry_for_cond_required_column.manifest.csv", "Patient"),
            ("mock_manifests/BulkRNAseq_component_based_required_rule_test.manifest.csv", "BulkRNA-seqAssay"),
        ],
        ids=["biospeciment_required_vr_empty", "biospecimen_required_filled", "patient_not_required_empty", "patient_conditionally_required_not_filled", "bulk_rna_seq_component_based_rule_test"],
    )
    def test_required_validation_rule(self, helpers, model_name:str, manifest_name:str, root_node:str, dmge:DataModelGraphExplorer) -> None:
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

        error_and_warning_free_manifests = ["Biospecimen_required_vr_test_pass", "Patient_test_no_entry_for_cond_required_column", ""]

        # For each model, these manifest should pass, bc either the value is being passed as requierd, or its not currently required
        for manifest in error_and_warning_free_manifests:
            if manifest_name in manifest:
                assert errors == []
                assert warnings == []

        messages = {"patient_id_empty_warning": {
                        "row_num":"2",
                        "attribute_name":"Patient ID",
                        "error_message":"'' should be non-empty",
                        "invalid_entry":""},
                    "bulk_rnaseq_cbr_error_1":{
                        "row_num":"3",
                        "attribute_name":"Genome FASTA",
                        "error_message":"'' should be non-empty",
                        "invalid_entry":""},
                    "bulk_rnaseq_cbr_error_2":{
                        "row_num":"4",
                        "attribute_name":"File Format",
                        "error_message":"'' is not one of ['CSV/TSV', 'CRAM', 'FASTQ', 'BAM']",
                        "invalid_entry":""},
            }

        # This manifest should fail in the example_model bc the manifest Required=False, and in the example_with_requirements_from_vr
        # bc the requirments are set to false in the validation rule
        if (("Biospecimen_required_vr_test_fail" in manifest_name) or
             ("Patient_required_vr_test_pass" in manifest_name and model_name == "example.model.csv")
             ):
            message_key = "patient_id_empty_warning"
            assert GenerateError.generate_schema_error(
                        row_num=messages[message_key]["row_num"],
                        attribute_name=messages[message_key]["attribute_name"],
                        error_message=messages[message_key]["error_message"],
                        invalid_entry=messages[message_key]["invalid_entry"],
                        dmge=dmge,
                    )[0] in errors
            assert warnings == []

        if "Patient_required_vr_test_pass" in manifest_name and model_name == "example_required_vr_test.model.csv":
            assert errors == []
            assert warnings == []

        if "BulkRNAseq_component_based_required_rule_test" in manifest_name:
            message_key = "bulk_rnaseq_cbr_error_1"
            assert GenerateError.generate_schema_error(
                    row_num=messages[message_key]["row_num"],
                    attribute_name=messages[message_key]["attribute_name"],
                    error_message=messages[message_key]["error_message"],
                    invalid_entry=messages[message_key]["invalid_entry"],
                    dmge=dmge,
                )[0] in errors

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
            valid_value = expected_error[2].split(',')[-1].split(']')[0].strip(' ').strip("\'")
            assert  valid_value in errors[1][2]
            assert warnings==[]


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
        self, helpers, dmge, base_rule, second_rule,
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
