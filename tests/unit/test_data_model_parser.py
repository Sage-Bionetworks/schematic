"""Unit tests for DataModelParser"""
from typing import Any
import pytest
from pytest_mock import MockerFixture

from schematic.schemas.data_model_parser import (
    DataModelParser,
    DataModelJSONLDParser,
    DataModelCSVParser,
)
from schematic.utils.schema_utils import parsed_model_as_dataframe

# pylint: disable=protected-access


class TestDataModelJsonLdParser:
    """Unit tests for DataModelJsonLdParser"""

    def test_parse_jsonld_model_column_type(self) -> None:
        """
        Tests DataModelJsonLdParser.parse_jsonld_model for columnTypes
        """
        # GIVEN a parser
        parser = DataModelJSONLDParser()
        # GIVEN a dict of attributes and expected column types
        expected_column_types = {
            "Check Regex List": "string",
            "Check Regex List Like": "string",
            "Check Regex List Strict": "string",
            "Check Regex Single": "string",
            "Check Regex Format": "string",
            "Check Regex Integer": "string",
            "Check Num": "number",
            "Check Float": "number",
            "Check Int": "integer",
            "Check String": "string",
            "Check Boolean": "boolean",
            "Check Range": "number",
            "Check NA": "integer",
            "MockRDB_id": "integer",
        }
        # WHEN the data model with column types is parsed
        result = parser.parse_jsonld_model(
            path_to_data_model="tests/data/example.model_column_type.jsonld",
        )
        attributes_with_column_type = [
            k for k, v in result.items() if "ColumnType" in v["Relationships"]
        ]

        # THEN attributes with a ColumnType in the result should match the keys in the
        #  expected dict
        assert sorted(attributes_with_column_type) == sorted(
            expected_column_types.keys()
        )
        # THEN the ColumnTypes in the result should match the types in the
        #  expected dict
        for attribute in attributes_with_column_type:
            test_type = result[attribute]["Relationships"]["ColumnType"]
            expected_type = expected_column_types[attribute]
            assert test_type == expected_type

class TestDataModelCSVParser:
    """Unit tests for DataModelCSVParser"""

    def test_parse_csv_model_with_column_type(
        self,
    ) -> None:
        # GIVEN a parser
        parser = DataModelCSVParser()
        # AND a data model that includes the column type column
        path_to_data_model = "tests/data/example.model.column_type_component.csv"
        # AND a dictionary of expected column types
        expected_column_types = {
            "String type": "string",
            "String type caps": "string",
            "Int type": "integer",
            "Int type caps": "integer",
            "Num type": "number",
            "Num type caps": "number",
            "Nan type": None,
            "Missing type": None,
            "Boolean type": "boolean",
            "Boolean type caps": "boolean",
        }

        # WHEN the data model is parsed
        result = parser.parse_csv_model(path_to_data_model=path_to_data_model)

        for expected_attribute, expected_type in expected_column_types.items():
            # THEN each expected attribute should be in the attribute relationship dictionary
            assert (
                expected_attribute in result
            ), f"Expected attribute '{expected_attribute}' not found in attribute relationship dictionary."

            # AND the attributes with no types specified should have no column type key value pair
            if not expected_type:
                # If the expected type is None, we expect the column type to be missing
                assert (
                    "columnType" not in result[expected_attribute]
                ), f"Expected no column type for '{expected_attribute}', but got '{result[expected_attribute].get('columnType')}'"
                continue

            # AND the column type of each attribute should match the expected type if a column type is specified
            assert (
                result[expected_attribute]["columnType"] == expected_type
            ), f"Expected column type for '{expected_attribute}' to be '{expected_type}', but got '{result[expected_attribute]['columnType']}'"

    def test_parse_csv_model_without_column_type(
        self,
    ) -> None:
        # GIVEN a parser
        parser = DataModelCSVParser()
        # AND a data model
        path_to_data_model = "tests/data/example.model.csv"
        # WHEN the data model is parsed
        result = parser.parse_csv_model(path_to_data_model=path_to_data_model)

        # AND unpacked
        unpacked_model_df = parsed_model_as_dataframe(result)

        # THEN the model should not have a 'columnType' column
        assert (
            "columnType" not in unpacked_model_df.columns
        ), "Expected no 'columnType' column in the unpacked model DataFrame."

    def test_parse_csv_model_with_invalid_type(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Tests DataModelCSVParser.parse_csv_model with an invalid column type
        """
        # GIVEN a parser
        parser = DataModelCSVParser()
        # AND a data model with an invalid column type
        path_to_data_model = (
            "tests/data/example.model.column_type_component.invalid.csv"
        )

        allowed_values_spy = mocker.spy(DataModelCSVParser, "_check_allowed_values")

        # WHEN the data model is parsed, THEN it should raise a ValueError
        with pytest.raises(ValueError):
            parser.parse_csv_model(path_to_data_model=path_to_data_model)

        # AND the _check_allowed_values method should have raised the error
        assert (
            allowed_values_spy.spy_exception is not None
        ), "Expected _check_allowed_values to raise an exception"
        assert isinstance(
            allowed_values_spy.spy_exception, ValueError
        ), "Expected _check_allowed_values to raise a ValueError"
