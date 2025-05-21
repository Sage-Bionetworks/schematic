"""Unit tests for DataModelParser"""
from typing import Any
import pytest
from schematic.schemas.data_model_parser import DataModelJSONLDParser

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

    @pytest.mark.parametrize(
        "value, relationship",
        [
            ("string", "columnType"),
            ("boolean", "columnType"),
            ("integer", "columnType"),
        ],
    )
    def test__check_allowed_values(self, value: Any, relationship: str) -> None:
        """Tests for DataModelJSONLDParser.check_allowed_values"""
        parser = DataModelJSONLDParser()
        parser._check_allowed_values(
            entry_id="id", value=value, relationship=relationship
        )

    @pytest.mark.parametrize(
        "value, relationship, message",
        [
            (
                "not_allowed",
                "columnType",
                "For entry: 'id', 'not_allowed' not in allowed values",
            ),
            (1, "columnType", "For entry: 'id', '1' not in allowed values"),
            (None, "columnType", "For entry: 'id', 'None' not in allowed values"),
        ],
    )
    def test_check_allowed_values_exceptions(
        self, value: Any, relationship: str, message: str
    ) -> None:
        """Tests for DataModelJSONLDParser.check_allowed_values with exceptions"""
        parser = DataModelJSONLDParser()
        with pytest.raises(ValueError, match=message):
            parser._check_allowed_values(
                entry_id="id", value=value, relationship=relationship
            )
