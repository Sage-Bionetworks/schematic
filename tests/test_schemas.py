import os
import logging

import pandas as pd
import pytest

from schematic.schemas import df_parser
from schematic.utils.df_utils import load_df

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture
def extended_schema_path(helpers, tmp_path):
    data_model_csv_path = helpers.get_data_path("example.model.csv")

    example_model_df = load_df(data_model_csv_path)

    # additional "Assay" attribute to be added to example schema
    assay_attr_row = {
        "Attribute": "Assay",
        "Description": (
            "A planned process with the objective to produce information "
            "about the material entity that is the evaluant, by physically "
            "examining it or its proxies.[OBI_0000070]"
        ),
        "Valid Values": "",
        "DependsOn": "",
        "Properties": "",
        "Required": False,
        "Parent": "",
        "DependsOn Component": "",
        "Source": "http://purl.obolibrary.org/obo/OBI_0000070",
        "Validation Rules": "",
    }

    example_model_df = example_model_df.append(assay_attr_row, ignore_index=True)

    # create empty temporary file to write extended schema to
    schemas_folder = tmp_path / "schemas"
    schemas_folder.mkdir()
    extended_schema_path = schemas_folder / "extended_example.model.csv"

    example_model_df.to_csv(extended_schema_path)

    yield extended_schema_path


class TestDfParser:
    def test_get_class(self, helpers):

        se_obj = helpers.get_schema_explorer("example.model.jsonld")

        actual = df_parser.get_class(
            se=se_obj,
            class_display_name="Test",
            description="This is a dummy test class",
            subclass_of=["Thing"],
            requires_dependencies=["Test_Dep_1", "Test_Dep_2"],
            requires_range=["Test_Start", "Test_End"],
            requires_components=["Test_Comp_1", "Test_Comp_2"],
            required=True,
            validation_rules=["Rule_1", "Rule_2"],
        )

        expected = {
            "@id": "bts:Test",
            "@type": "rdfs:Class",
            "rdfs:comment": "This is a dummy test class",
            "rdfs:label": "Test",
            "rdfs:subClassOf": [{"@id": "bts:Thing"}],
            "schema:isPartOf": {"@id": "http://schema.biothings.io"},
            "schema:rangeIncludes": [{"@id": "bts:TestStart"}, {"@id": "bts:TestEnd"}],
            "sms:displayName": "Test",
            "sms:required": "sms:true",
            "sms:requiresComponent": [
                {"@id": "bts:Test_Comp_1"},
                {"@id": "bts:Test_Comp_2"},
            ],
            "sms:requiresDependency": [
                {"@id": "bts:Test_Dep_1"},
                {"@id": "bts:Test_Dep_2"},
            ],
            "sms:validationRules": ["Rule_1", "Rule_2"],
        }

        assert expected == actual

    def test_get_property(self, helpers):

        se_obj = helpers.get_schema_explorer("example.model.jsonld")

        actual = df_parser.get_property(
            se=se_obj,
            property_display_name="Test",
            property_class_name="Prop_Class",
            description="This is a dummy test property",
            requires_range=["Test_Start", "Test_End"],
            requires_dependencies=["Test_Dep_1", "Test_Dep_2"],
            required=True,
            validation_rules=["Rule_1", "Rule_2"],
        )

        expected = {
            "@id": "bts:test",
            "@type": "rdf:Property",
            "rdfs:comment": "This is a dummy test property",
            "rdfs:label": "test",
            "schema:isPartOf": {"@id": "http://schema.biothings.io"},
            "schema:rangeIncludes": [{"@id": "bts:TestStart"}, {"@id": "bts:TestEnd"}],
            "sms:displayName": "Test",
            "sms:required": "sms:true",
            "schema:domainIncludes": {"@id": "bts:PropClass"},
            "sms:requiresDependency": [
                {"@id": "bts:Test_Dep_1"},
                {"@id": "bts:Test_Dep_2"},
            ],
            "sms:validationRules": ["Rule_1", "Rule_2"],
        }

        assert expected == actual

    def test_attribute_exists(self, helpers):

        se_obj = helpers.get_schema_explorer("example.model.jsonld")

        # test when attribute is present in data model
        attribute_present = df_parser.attribute_exists(se_obj, "Patient")

        # test when attribute is not present in data model
        attribute_absent = df_parser.attribute_exists(se_obj, "RandomAttribute")

        assert attribute_present
        assert not attribute_absent

    def test_check_schema_definition(self, helpers):

        data_model_csv_path = helpers.get_data_path("example.model.csv")

        example_model_df = load_df(data_model_csv_path)

        # when all required headers are provided in the CSV data model
        actual_df = df_parser.check_schema_definition(example_model_df)

        assert actual_df is None

        # when either "Requires" or "Requires Component" is present
        # in column headers, raise ValueError
        if "DependsOn Component" in example_model_df.columns:
            del example_model_df["DependsOn Component"]

            example_model_df["Requires Component"] = ""

            with pytest.raises(ValueError):
                df_parser.check_schema_definition(example_model_df)

    def test_create_nx_schema_objects(self, helpers, extended_schema_path):

        se_obj = helpers.get_schema_explorer("example.model.jsonld")

        # path to extended CSV data model which has one additional attribute
        # namely, "Assay"
        extended_csv_model_path = helpers.get_data_path(extended_schema_path)

        extended_model_df = load_df(extended_csv_model_path)

        extended_csv_model_se = df_parser.create_nx_schema_objects(
            extended_model_df, se_obj
        )

        # check if the "Assay" attribute has been added to the new SchemaExplorer
        # object with attributes from the extended schema
        result = df_parser.attribute_exists(extended_csv_model_se, "Assay")

        assert result

    def test_get_base_schema_path(self):

        base_schema_path = "/path/to/base_schema.jsonld"

        # path to base schema is returned when base_schema is passed
        result_path = df_parser._get_base_schema_path(base_schema=base_schema_path)

        assert result_path == "/path/to/base_schema.jsonld"

        # path to default BioThings data model is returned when no
        # base schema path is passed explicitly
        biothings_path = df_parser._get_base_schema_path()

        assert os.path.basename(biothings_path) == "biothings.model.jsonld"

    def test_convert_csv_to_data_model(self, helpers, extended_schema_path):

        csv_path = helpers.get_data_path("example.model.jsonld")

        extended_csv_model_path = helpers.get_data_path(extended_schema_path)

        # convert extended CSV data model to JSON-LD using provided
        # CSV data model as base schema
        extended_csv_model_se = df_parser._convert_csv_to_data_model(
            extended_csv_model_path, csv_path
        )

        # if new attribute can be found in extended_csv_model_se
        # we know the conversion was successful
        attribute_present = df_parser.attribute_exists(extended_csv_model_se, "Assay")

        assert attribute_present
