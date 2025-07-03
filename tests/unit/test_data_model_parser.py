"""Unit tests for DataModelParser"""
import pytest
from pytest_mock import MockerFixture

from schematic.schemas import data_model_parser
from schematic.schemas.data_model_parser import (
    DataModelJSONLDParser,
    DataModelCSVParser,
    check_allowed_values,
)
from schematic.utils.schema_utils import parsed_model_as_dataframe

# pylint: disable=protected-access


class TestDataModelJSONLDParser:
    """Unit tests for DataModelJSONLDParser"""

    def test_parse_jsonld_model_with_column_type(
        self,
    ) -> None:
        # GIVEN a parser
        parser = DataModelJSONLDParser()
        # AND a data model that includes the column type column
        path_to_data_model = "tests/data/example.model.column_type_component.jsonld"
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
        result = parser.parse_jsonld_model(path_to_data_model=path_to_data_model)

        for expected_attribute, expected_type in expected_column_types.items():
            # THEN each expected attribute should be in the attribute relationship dictionary
            assert (
                expected_attribute in result
            ), f"Expected attribute '{expected_attribute}' not found in attribute relationship dictionary."

            # AND the attributes with no types specified should have no column type key value pair
            if not expected_type:
                # If the expected type is None, we expect the column type to be missing
                assert (
                    "ColumnType" not in result[expected_attribute]["Relationships"]
                ), f"Expected no column type for '{expected_attribute}', but got '{result[expected_attribute]['Relationships'].get('ColumnType')}'"
                continue

            # AND the column type of each attribute should match the expected type if a column type is specified
            assert (
                result[expected_attribute]["Relationships"]["ColumnType"]
                == expected_type
            ), f"Expected column type for '{expected_attribute}' to be '{expected_type}', but got '{result[expected_attribute]['Relationships']['ColumnType']}'"

    def test_parse_jsonld_model_without_column_type(
        self,
    ) -> None:
        # GIVEN a parser
        parser = DataModelJSONLDParser()
        # AND a data model
        path_to_data_model = "tests/data/example.model.jsonld"
        # WHEN the data model is parsed
        result = parser.parse_jsonld_model(path_to_data_model=path_to_data_model)

        # AND unpacked
        unpacked_model_df = parsed_model_as_dataframe(result)

        # THEN the model should not have a 'columnType' column
        assert (
            "columnType" not in unpacked_model_df.columns
        ), "Expected no 'columnType' column in the unpacked model DataFrame."

    def test_parse_jsonld_model_with_invalid_type(
        self,
        mocker: MockerFixture,
    ) -> None:
        """
        Tests DataModelJSONLDParser.parse_jsonld_model with an invalid column type
        """
        # GIVEN a parser
        parser = DataModelJSONLDParser()
        # AND a data model with an invalid column type
        path_to_data_model = (
            "tests/data/example.model.column_type_component.invalid.jsonld"
        )

        allowed_values_spy = mocker.spy(data_model_parser, "check_allowed_values")

        # WHEN the data model is parsed, THEN it should raise a ValueError
        with pytest.raises(ValueError):
            parser.parse_jsonld_model(path_to_data_model=path_to_data_model)

        # AND the check_allowed_values method should have raised the error
        assert (
            allowed_values_spy.spy_exception is not None
        ), "Expected check_allowed_values to raise an exception"
        assert isinstance(
            allowed_values_spy.spy_exception, ValueError
        ), "Expected check_allowed_values to raise a ValueError"


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
                    "ColumnType" not in result[expected_attribute]["Relationships"]
                ), f"Expected no column type for '{expected_attribute}', but got '{result[expected_attribute]['Relationships'].get('ColumnType')}'"
                continue

            # AND the column type of each attribute should match the expected type if a column type is specified
            assert (
                result[expected_attribute]["Relationships"]["ColumnType"]
                == expected_type
            ), f"Expected column type for '{expected_attribute}' to be '{expected_type}', but got '{result[expected_attribute]['Relationships']['ColumnType']}'"

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

        allowed_values_spy = mocker.spy(data_model_parser, "check_allowed_values")

        # WHEN the data model is parsed, THEN it should raise a ValueError
        with pytest.raises(ValueError):
            parser.parse_csv_model(path_to_data_model=path_to_data_model)

        # AND the check_allowed_values method should have raised the error
        assert (
            allowed_values_spy.spy_exception is not None
        ), "Expected check_allowed_values to raise an exception"
        assert isinstance(
            allowed_values_spy.spy_exception, ValueError
        ), "Expected check_allowed_values to raise a ValueError"
