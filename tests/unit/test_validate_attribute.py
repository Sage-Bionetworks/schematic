"""Unit testing for the ValidateAttribute class"""

from typing import Generator
from unittest.mock import Mock, patch

import numpy as np
import pytest
from jsonschema import ValidationError
from pandas import DataFrame, Series, concat

import schematic.models.validate_attribute
from schematic.models.validate_attribute import GenerateError, ValidateAttribute
from schematic.schemas.data_model_graph import DataModelGraphExplorer

# pylint: disable=protected-access
# pylint: disable=too-many-public-methods
# pylint: disable=too-many-arguments

MATCH_ATLEAST_ONE_SET_RULES = [
    "matchAtLeastOne Patient.PatientID set error",
    "matchAtLeastOne Patient.PatientID set warning",
]
MATCH_EXACTLY_ONE_SET_RULES = [
    "matchExactlyOne Patient.PatientID set error",
    "matchExactlyOne Patient.PatientID set warning",
]
MATCH_NONE_SET_RULES = [
    "matchNone Patient.PatientID set error",
    "matchNone Patient.PatientID set warning",
]
ALL_SET_RULES = (
    MATCH_ATLEAST_ONE_SET_RULES + MATCH_EXACTLY_ONE_SET_RULES + MATCH_NONE_SET_RULES
)
MATCH_ATLEAST_ONE_VALUE_RULES = [
    "matchAtLeastOne Patient.PatientID value error",
    "matchAtLeastOne Patient.PatientID value warning",
]
MATCH_EXACTLY_ONE_VALUE_RULES = [
    "matchExactlyOne Patient.PatientID value error",
    "matchExactlyOne Patient.PatientID value warning",
]
MATCH_NONE_VALUE_RULES = [
    "matchNone Patient.PatientID value error",
    "matchNone Patient.PatientID value warning",
]
ALL_VALUE_RULES = (
    MATCH_ATLEAST_ONE_VALUE_RULES
    + MATCH_EXACTLY_ONE_VALUE_RULES
    + MATCH_NONE_VALUE_RULES
)
EXACTLY_ATLEAST_PASSING_SERIES = [
    Series(["A", "B", "C"], index=[0, 1, 2], name="PatientID"),
    Series(["A", "B", "C", "C"], index=[0, 1, 2, 3], name="PatientID"),
    Series(["A", "B", "C", "A", "B", "C"], index=[0, 1, 2, 3, 4, 5], name="PatientID"),
    Series(["A", "B"], index=[0, 1], name="PatientID"),
    Series([], name="PatientID"),
]

TEST_DF1 = DataFrame(
    {
        "PatientID": ["A", "B", "C"],
        "component": ["comp1", "comp1", "comp1"],
        "id": ["id1", "id2", "id3"],
        "entityid": ["x", "x", "x"],
    }
)

TEST_DF2 = DataFrame(
    {
        "PatientID": ["D", "E", "F"],
        "component": ["comp1", "comp1", "comp1"],
        "id": ["id1", "id2", "id3"],
        "entityid": ["x", "x", "x"],
    }
)

TEST_DF3 = DataFrame(
    {
        "PatientID": ["A", "A", "A", "B", "C"],
        "component": ["comp1", "comp1", "comp1", "comp1", "comp1"],
        "id": ["id1", "id2", "id3", "id4", "id5"],
        "entityid": ["x", "x", "x", "x", "x"],
    }
)

TEST_DF_MISSING_VALS = DataFrame(
    {
        "PatientID": [np.isnan, ""],
        "component": ["comp1", "comp1"],
        "id": ["id1", "id2"],
        "entityid": ["x", "x"],
    }
)

TEST_DF_MISSING_PATIENT = DataFrame(
    {
        "component": ["comp1", "comp1"],
        "id": ["id1", "id2"],
        "entityid": ["x", "x"],
    }
)

TEST_DF_EMPTY_COLS = DataFrame(
    {
        "PatientID": [],
        "component": [],
        "id": [],
        "entityid": [],
    }
)

TEST_DF_FILEVIEW = DataFrame(
    {
        "id": ["syn1", "syn2", "syn3"],
        "path": ["test1.txt", "test2.txt", "test3.txt"],
    }
)

TEST_MANIFEST_GOOD = DataFrame(
    {
        "Component": ["Mockfilename", "Mockfilename", "Mockfilename"],
        "Filename": ["test1.txt", "test2.txt", "test3.txt"],
        "entityId": ["syn1", "syn2", "syn3"],
    }
)

TEST_MANIFEST_MISSING_ENTITY_ID = DataFrame(
    {
        "Component": ["Mockfilename", "Mockfilename", "Mockfilename"],
        "Filename": ["test1.txt", "test2.txt", "test3.txt"],
        "entityId": ["syn1", "syn2", ""],
    }
)

TEST_MANIFEST_FILENAME_NOT_IN_VIEW = DataFrame(
    {
        "Component": ["Mockfilename", "Mockfilename", "Mockfilename"],
        "Filename": ["test1.txt", "test2.txt", "test_bad.txt"],
        "entityId": ["syn1", "syn2", "syn3"],
    }
)

TEST_MANIFEST_ENTITY_ID_NOT_IN_VIEW = DataFrame(
    {
        "Component": ["Mockfilename", "Mockfilename", "Mockfilename"],
        "Filename": ["test1.txt", "test2.txt", "test3.txt"],
        "entityId": ["syn1", "syn2", "syn_bad"],
    }
)

TEST_MANIFEST_ENTITY_ID_MISMATCH = DataFrame(
    {
        "Component": ["Mockfilename", "Mockfilename", "Mockfilename"],
        "Filename": ["test1.txt", "test2.txt", "test3.txt"],
        "entityId": ["syn1", "syn2", "syn2"],
    }
)


@pytest.fixture(name="va_obj")
def fixture_va_obj(
    dmge: DataModelGraphExplorer,
) -> Generator[ValidateAttribute, None, None]:
    """Yield a ValidateAttribute object"""
    yield ValidateAttribute(dmge)


@pytest.fixture(name="test_df1")
def fixture_test_df1() -> Generator[DataFrame, None, None]:
    """Yields a dataframe"""
    yield DataFrame(
        {
            "PatientID": ["A", "B", "C"],
            "component": ["comp1", "comp1", "comp1"],
            "id": ["id1", "id2", "id3"],
            "entityid": ["x", "x", "x"],
        }
    )


@pytest.fixture(name="test_df_col_names")
def fixture_test_df_col_names() -> Generator[dict[str, str], None, None]:
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


class TestGenerateError:
    """Unit tests for the GenerateError class"""

    val_rule = "filenameExists syn123456"
    attribute_name = "Filename"
    row_num = "2"
    invalid_entry = "test_file.txt"

    @pytest.mark.parametrize(
        "error_type, expected_message",
        [
            (
                "mismatched entityId",
                "The entityId for file path 'test_file.txt' on row 2 does not match the entityId for the file in the file view.",
            ),
            (
                "path does not exist",
                "The file path 'test_file.txt' on row 2 does not exist in the file view.",
            ),
            (
                "entityId does not exist",
                "The entityId for file path 'test_file.txt' on row 2 does not exist in the file view.",
            ),
            (
                "missing entityId",
                "The entityId is missing for file path 'test_file.txt' on row 2.",
            ),
        ],
        ids=[
            "mismatched entityId",
            "path does not exist",
            "entityId does not exist",
            "missing entityId",
        ],
    )
    def test_generate_filename_error(
        self, dmge: DataModelGraphExplorer, error_type: str, expected_message: str
    ):
        with patch.object(
            GenerateError,
            "raise_and_store_message",
            return_value=(
                [
                    self.row_num,
                    self.attribute_name,
                    expected_message,
                    self.invalid_entry,
                ],
                [],
            ),
        ) as mock_raise_and_store:
            error_list, _ = GenerateError.generate_filename_error(
                val_rule=self.val_rule,
                attribute_name=self.attribute_name,
                row_num=self.row_num,
                invalid_entry=self.invalid_entry,
                error_type=error_type,
                dmge=dmge,
            )
        mock_raise_and_store.assert_called_once_with(
            dmge=dmge,
            val_rule=self.val_rule,
            error_row=self.row_num,
            error_col=self.attribute_name,
            error_message=expected_message,
            error_val=self.invalid_entry,
        )

        assert len(error_list) == 4
        assert error_list[2] == expected_message

    def test_generate_filename_error_unsupported_error_type(
        self, dmge: DataModelGraphExplorer
    ):
        with pytest.raises(
            KeyError, match="Unsupported error type provided: 'unsupported error type'"
        ) as exc_info:
            GenerateError.generate_filename_error(
                dmge=dmge,
                val_rule=self.val_rule,
                attribute_name=self.attribute_name,
                row_num=self.row_num,
                invalid_entry=self.invalid_entry,
                error_type="unsupported error type",
            )

    @pytest.mark.parametrize(
        "input_rule, input_num, input_name, input_entry, expected_error, expected_warning",
        [
            (
                "x",
                0,
                "Patient",
                "y",
                [],
                [
                    0,
                    "Patient",
                    "On row 0 the attribute Patient does not contain the proper value type x.",
                    "y",
                ],
            ),
            (
                "x warning",
                0,
                "Patient",
                "y",
                [],
                [
                    0,
                    "Patient",
                    "On row 0 the attribute Patient does not contain the proper value type x.",
                    "y",
                ],
            ),
            (
                "x error",
                0,
                "Patient",
                "y",
                [
                    0,
                    "Patient",
                    "On row 0 the attribute Patient does not contain the proper value type x.",
                    "y",
                ],
                [],
            ),
        ],
    )
    def test_generate_type_error(
        self,
        dmge: DataModelGraphExplorer,
        input_rule: str,
        input_num: int,
        input_name: str,
        input_entry: str,
        expected_error: list[str],
        expected_warning: list[str],
    ) -> None:
        """Testing for GenerateError.generate_type_error"""
        error, warning = GenerateError.generate_type_error(
            val_rule=input_rule,
            row_num=input_num,
            attribute_name=input_name,
            invalid_entry=input_entry,
            dmge=dmge,
        )
        import logging

        logging.warning(error)
        logging.warning(warning)
        assert error == expected_error
        assert warning == expected_warning

    @pytest.mark.parametrize(
        "input_rule, input_num, input_name, input_entry, exception",
        [
            # Empty rule or entry causes a key error
            ("", 0, "x", "x", KeyError),
            ("x", 0, "x", "", KeyError),
            # Empty attribute causes an index error
            ("x", 0, "", "x", IndexError),
        ],
    )
    def test_generate_type_error_exceptions(
        self,
        dmge: DataModelGraphExplorer,
        input_rule: str,
        input_num: int,
        input_name: str,
        input_entry: str,
        exception: Exception,
    ) -> None:
        """Testing for GenerateError.generate_type_error"""
        with pytest.raises(exception):
            GenerateError.generate_type_error(
                val_rule=input_rule,
                row_num=input_num,
                attribute_name=input_name,
                invalid_entry=input_entry,
                dmge=dmge,
            )


class TestValidateAttributeObject:
    """Testing for ValidateAttribute class with all Synapse calls mocked"""

    ##################
    # cross_validation
    ##################

    @pytest.mark.parametrize("series", EXACTLY_ATLEAST_PASSING_SERIES)
    @pytest.mark.parametrize("rule", MATCH_ATLEAST_ONE_SET_RULES)
    def test_cross_validation_match_atleast_one_set_passing_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        series: Series,
        rule: str,
    ):
        """
        Tests for ValidateAttribute.cross_validation using matchAtLeastOne rule
        These tests pass with no errors or warnings
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            assert va_obj.cross_validation(rule, series) == ([], [])

    @pytest.mark.parametrize("series", EXACTLY_ATLEAST_PASSING_SERIES)
    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_SET_RULES)
    def test_cross_validation_match_exactly_one_set_passing_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        series: Series,
        rule: str,
    ):
        """
        Tests for ValidateAttribute.cross_validation using matchExactlyOne rule
        These tests pass with no errors or warnings
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            assert va_obj.cross_validation(rule, series) == ([], [])

    @pytest.mark.parametrize(
        "series",
        [
            Series(["A", "B", "C", "D"], index=[0, 1, 2, 3], name="PatientID"),
            Series([np.nan], index=[0], name="PatientID"),
            Series([""], index=[0], name="PatientID"),
            Series([1], index=[0], name="PatientID"),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_ATLEAST_ONE_SET_RULES)
    def test_cross_validation_match_atleast_one_set_errors_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        series: Series,
        rule: str,
    ):
        """
        Tests for ValidateAttribute.cross_validation using matchAtLeastOne rule
        These tests fail with either one error or warning depending on the rule
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, series)
            if rule.endswith("error"):
                assert warnings == []
                assert len(errors) == 1
            else:
                assert len(warnings) == 1
                assert errors == []

    @pytest.mark.parametrize(
        "series",
        [
            Series(["A", "B", "C"], index=[0, 1, 2], name="PatientID"),
            Series(["A", "B", "C", "C"], index=[0, 1, 2, 3], name="PatientID"),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_SET_RULES)
    def test_cross_validation_match_exactly_one_set_errors_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        series: Series,
        rule: str,
    ):
        """
        Tests for ValidateAttribute.cross_validation using matchExactlyOne rule
        These tests fail with either one error or warning depending on the rule
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1, "syn2": test_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, series)
            if rule.endswith("error"):
                assert warnings == []
                assert len(errors) == 1
            else:
                assert len(warnings) == 1
                assert errors == []

    @pytest.mark.parametrize(
        "series",
        [
            Series(["D"], index=[0], name="PatientID"),
            Series(["D", "D"], index=[0, 1], name="PatientID"),
            Series([np.nan], index=[0], name="PatientID"),
            Series([""], index=[0], name="PatientID"),
            Series([1], index=[0], name="PatientID"),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_NONE_SET_RULES)
    def test_cross_validation_match_none_set_passing_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        series: Series,
        rule: str,
    ):
        """
        Tests for cross manifest validation for matchNone set rules
        These tests pass with no errors or warnings
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            assert va_obj.cross_validation(rule, series) == ([], [])

    @pytest.mark.parametrize(
        "series",
        [
            Series(["A", "B", "C"], index=[0, 1, 2], name="PatientID"),
            Series(["A", "B", "C", "D"], index=[0, 1, 2, 3], name="PatientID"),
            Series(["A", "B"], index=[0, 1], name="PatientID"),
            Series(["A"], index=[0], name="PatientID"),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_NONE_SET_RULES)
    def test_cross_validation_match_none_set_errors_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        series: Series,
        rule: str,
    ):
        """
        Tests for cross manifest validation for matchNone set rules
        These tests fail with either one error or warning depending on the rule
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, series)
            if rule.endswith("error"):
                assert warnings == []
                assert len(errors) == 1
            else:
                assert len(warnings) == 1
                assert errors == []

    @pytest.mark.parametrize("rule", MATCH_ATLEAST_ONE_VALUE_RULES)
    @pytest.mark.parametrize("target_manifest", [TEST_DF1, TEST_DF3])
    @pytest.mark.parametrize(
        "tested_column",
        [
            ([]),
            (["A"]),
            (["A", "A"]),
            (["A", "B"]),
            (["A", "B", "C"]),
            (["A", "B", "C", "C"]),
        ],
    )
    def test_cross_validation_match_atleast_one_value_passing_one_df(
        self,
        va_obj: ValidateAttribute,
        rule: str,
        tested_column: list,
        target_manifest: DataFrame,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns pass for matchAtLeastOne
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": target_manifest},
        ):
            assert va_obj.cross_validation(rule, Series(tested_column)) == ([], [])

    @pytest.mark.parametrize("rule", MATCH_ATLEAST_ONE_VALUE_RULES)
    @pytest.mark.parametrize(
        "tested_column",
        [
            Series(["D"], index=[0], name="PatientID"),
            Series(["D", "D"], index=[0, 1], name="PatientID"),
            Series(["D", "F"], index=[0, 1], name="PatientID"),
            Series([np.nan], index=[0], name="PatientID"),
            Series([1], index=[0], name="PatientID"),
        ],
    )
    def test_cross_validation_match_atleast_one_value_errors_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        rule: str,
        tested_column: Series,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns fail for matchAtLeastOne
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, tested_column)
            if rule.endswith("error"):
                assert len(errors) == 1
                assert warnings == []
            else:
                assert errors == []
                assert len(warnings) == 1

    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_VALUE_RULES)
    @pytest.mark.parametrize(
        "tested_column, target_manifest",
        [
            ([], TEST_DF1),
            ([], TEST_DF3),
            (["C"], TEST_DF1),
            (["C"], TEST_DF3),
            (["C", "C"], TEST_DF1),
            (["C", "C"], TEST_DF3),
            (["A"], TEST_DF1),
            (["A", "A"], TEST_DF1),
            (["A", "B"], TEST_DF1),
            (["A", "B", "C"], TEST_DF1),
            (["A", "B", "C", "C"], TEST_DF1),
        ],
    )
    def test_cross_validation_match_exactly_one_value_passing_one_df(
        self,
        va_obj: ValidateAttribute,
        rule: str,
        tested_column: list,
        target_manifest: DataFrame,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns pass for matchExactlyOne
        The first group are ones that pass for TEST_DF1 and TEST_DF3
        The second group are those that pass only for test
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": target_manifest},
        ):
            assert va_obj.cross_validation(rule, Series(tested_column)) == ([], [])

    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_VALUE_RULES)
    @pytest.mark.parametrize(
        "tested_column",
        [
            Series(["D"], index=[0], name="PatientID"),
            Series(["D", "D"], index=[0, 1], name="PatientID"),
            Series(["D", "F"], index=[0, 1], name="PatientID"),
            Series([1], index=[0], name="PatientID"),
        ],
    )
    def test_cross_validation_match_exactly_one_value_errors_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        rule: str,
        tested_column: Series,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns fail for matchExactlyOne
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, tested_column)
            if rule.endswith("error"):
                assert len(errors) == 1
                assert warnings == []
            else:
                assert errors == []
                assert len(warnings) == 1

    @pytest.mark.parametrize("rule", MATCH_NONE_VALUE_RULES)
    @pytest.mark.parametrize(
        "tested_column",
        [([]), (["D"]), (["D", "D"]), (["D", "F"]), ([1]), ([np.nan])],
    )
    def test_cross_validation_match_none_value_passing_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        rule: str,
        tested_column: list,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns pass for matchNone
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            assert va_obj.cross_validation(rule, Series(tested_column)) == ([], [])

    @pytest.mark.parametrize("rule", MATCH_NONE_VALUE_RULES)
    @pytest.mark.parametrize(
        "tested_column",
        [
            Series(["A"], index=[0], name="PatientID"),
            Series(["A", "B"], index=[0, 1], name="PatientID"),
            Series(["A", "A"], index=[0, 1], name="PatientID"),
        ],
    )
    def test_cross_validation_value_match_none_errors_one_df(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        rule: str,
        tested_column: Series,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns fail for matchNone
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, tested_column)
            if rule.endswith("error"):
                assert len(errors) == 1
                assert warnings == []
            else:
                assert errors == []
                assert len(warnings) == 1

    @pytest.mark.parametrize(
        "manifest, expected_errors, expected_warnings, generates_error",
        [
            (TEST_MANIFEST_GOOD, [], [], False),
            (
                TEST_MANIFEST_MISSING_ENTITY_ID,
                [
                    [
                        "4",
                        "Filename",
                        "The entityId for file path 'test3.txt' on row 4 does not exist in the file view.",
                        "test3.txt",
                    ]
                ],
                [],
                True,
            ),
            (
                TEST_MANIFEST_FILENAME_NOT_IN_VIEW,
                [
                    [
                        "4",
                        "Filename",
                        "The file path 'test_bad.txt' on row 4 does not exist in the file view.",
                        "test_bad.txt",
                    ]
                ],
                [],
                True,
            ),
            (
                TEST_MANIFEST_ENTITY_ID_NOT_IN_VIEW,
                [
                    [
                        "4",
                        "Filename",
                        "The entityId for file path 'test3.txt' on row 4 does not exist in the file view.",
                        "test3.txt",
                    ]
                ],
                [],
                True,
            ),
            (
                TEST_MANIFEST_ENTITY_ID_MISMATCH,
                [
                    [
                        "4",
                        "Filename",
                        "The entityId for file path 'test3.txt' on row 4 does not match "
                        "the entityId for the file in the file view.",
                        "test3.txt",
                    ]
                ],
                [],
                True,
            ),
        ],
        ids=[
            "valid_manifest",
            "missing_entity_id",
            "bad_filename",
            "bad_entity_id",
            "entity_id_mismatch",
        ],
    )
    def test_filename_validation(
        self,
        va_obj: ValidateAttribute,
        manifest: DataFrame,
        expected_errors: list,
        expected_warnings: list,
        generates_error: bool,
    ):
        mock_synapse_storage = Mock()
        mock_synapse_storage.storageFileviewTable = TEST_DF_FILEVIEW
        va_obj.synStore = mock_synapse_storage
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_login",
        ), patch.object(
            mock_synapse_storage, "reset_index", return_value=TEST_DF_FILEVIEW
        ), patch.object(
            schematic.models.validate_attribute.GenerateError,
            "generate_filename_error",
            return_value=(
                expected_errors if len(expected_errors) < 1 else expected_errors[0],
                expected_warnings,
            ),
        ) as mock_generate_filename_error:
            actual_errors, actual_warnings = va_obj.filename_validation(
                val_rule="filenameExists syn61682648",
                manifest=manifest,
                access_token="test_access_token",
                dataset_scope="syn1",
            )
            mock_generate_filename_error.assert_called_once() if generates_error else mock_generate_filename_error.assert_not_called()
            assert (actual_errors, actual_warnings) == (
                expected_errors,
                expected_warnings,
            )

    def test_filename_validation_null_dataset_scope(self, va_obj: ValidateAttribute):
        with pytest.raises(
            ValueError,
            match="A dataset is required to be specified for filename validation",
        ):
            va_obj.filename_validation(
                val_rule="filenameExists syn61682648",
                manifest=TEST_MANIFEST_GOOD,
                access_token="test_access_token",
                dataset_scope=None,
            )

    #########################################
    # _run_validation_across_target_manifests
    #########################################

    @pytest.mark.parametrize("input_column", [(Series([])), (Series(["A"]))])
    @pytest.mark.parametrize("rule", ALL_SET_RULES)
    @pytest.mark.parametrize(
        "target_manifests", [({"syn1": TEST_DF_MISSING_PATIENT}), ({})]
    )
    def test__run_validation_across_target_manifests_return_false(
        self,
        va_obj: ValidateAttribute,
        input_column: Series,
        rule: str,
        target_manifests: dict[str, DataFrame],
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests that return False
        These tests show that when no target manifests are found to check against, or the target
        manifest is missing the target column, False is returned
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value=target_manifests,
        ):
            _, result = va_obj._run_validation_across_target_manifests(
                rule_scope="value",
                val_rule=rule,
                manifest_col=input_column,
                target_column=Series([]),
            )
            assert result is False

    @pytest.mark.parametrize("input_column", [(Series([])), (Series(["A"]))])
    @pytest.mark.parametrize("rule", ALL_SET_RULES)
    def test__run_validation_across_target_manifests_return_msg(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests that return a string
        These tests show that if at least one target manifest does'nt have

        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": TEST_DF1, "syn2": TEST_DF_EMPTY_COLS},
        ):
            _, result = va_obj._run_validation_across_target_manifests(
                rule_scope="value",
                val_rule=rule,
                manifest_col=input_column,
                target_column=Series([]),
            )
            assert result == "values not recorded in targets stored"

    @pytest.mark.parametrize("rule", ALL_VALUE_RULES)
    def test__run_validation_across_target_manifests_value_scope(
        self, va_obj: ValidateAttribute, test_df1: DataFrame, rule: str
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_target_manifests with value rule"""

        # This tests when an empty column is validated there are no missing values to be returned
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="value",
                val_rule=rule,
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert isinstance(validation_output[0], Series)
            assert validation_output[0].empty
            assert isinstance(validation_output[1], Series)
            assert validation_output[1].empty
            assert isinstance(validation_output[2], Series)
            assert validation_output[2].empty

    @pytest.mark.parametrize(
        "input_column, missing_ids, present_ids, repeat_ids",
        [
            ([], [], ["syn1"], []),
            (["A"], [], ["syn1"], []),
            (["A", "A"], [], ["syn1"], []),
            (["A", "B", "C"], [], ["syn1"], []),
            (["D"], ["syn1"], [], []),
            (["D", "D"], ["syn1"], [], []),
            (["D", "E"], ["syn1"], [], []),
            ([1], ["syn1"], [], []),
        ],
    )
    @pytest.mark.parametrize(
        "rule", MATCH_ATLEAST_ONE_SET_RULES + MATCH_EXACTLY_ONE_SET_RULES
    )
    def test__run_validation_across_target_manifests_match_atleast_exactly_with_one_target(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        input_column: list,
        missing_ids: list[str],
        present_ids: list[str],
        repeat_ids: list[str],
        rule: str,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests
          using matchAtleastOne set and matchExactlyOne rule.
        This shows that these rules behave the same.
        If all values in the column match the target manifest, the manifest id gets added
          to the present ids list.
        Otherwise the manifest id gets added to the missing ids list
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule=rule,
                manifest_col=Series(input_column),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert list(validation_output[0].keys()) == missing_ids
            assert validation_output[1] == present_ids
            assert list(validation_output[2].keys()) == repeat_ids

    @pytest.mark.parametrize(
        "input_column, missing_ids, present_ids, repeat_ids",
        [
            ([], [], ["syn1", "syn2"], []),
            (["A"], [], ["syn1", "syn2"], []),
            (["D"], ["syn1", "syn2"], [], []),
        ],
    )
    @pytest.mark.parametrize(
        "rule", MATCH_ATLEAST_ONE_SET_RULES + MATCH_EXACTLY_ONE_SET_RULES
    )
    def test__run_validation_across_target_manifests_match_atleast_exactly_with_two_targets(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        input_column: list,
        missing_ids: list[str],
        present_ids: list[str],
        repeat_ids: list[str],
        rule: str,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests
          using matchAtleastOne set and matchExactlyOne rule.
        This shows these rules behave the same.
        This also shows that when there are multiple target manifests they both get added to
          either the present of missing manifest ids
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1, "syn2": test_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule=rule,
                manifest_col=Series(input_column),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert list(validation_output[0].keys()) == missing_ids
            assert validation_output[1] == present_ids
            assert list(validation_output[2].keys()) == repeat_ids

    @pytest.mark.parametrize(
        "input_column, missing_ids, present_ids, repeat_ids",
        [
            ([], [], [], []),
            (["D"], [], [], []),
            (["D", "D"], [], [], []),
            (["D", "E"], [], [], []),
            ([1], [], [], []),
            (["A"], [], [], ["syn1"]),
            (["A", "A"], [], [], ["syn1"]),
            (["A", "B", "C"], [], [], ["syn1"]),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_NONE_SET_RULES)
    def test__run_validation_across_target_manifests_set_rules_match_none_with_one_target(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        input_column: list,
        missing_ids: list[str],
        present_ids: list[str],
        repeat_ids: list[str],
        rule: str,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests
          using matchNone set rule
        When there are nt matching values, no id get added
        When there are matching values the id gets added to the repeat ids
        """

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule=rule,
                manifest_col=Series(input_column),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert list(validation_output[0].keys()) == missing_ids
            assert validation_output[1] == present_ids
            assert list(validation_output[2].keys()) == repeat_ids

    @pytest.mark.parametrize(
        "input_column, missing_ids, present_ids, repeat_ids",
        [
            ([], [], [], []),
            (["D"], [], [], []),
            (["D", "D"], [], [], []),
            (["D", "E"], [], [], []),
            ([1], [], [], []),
            (["A"], [], [], ["syn1", "syn2"]),
            (["A", "A"], [], [], ["syn1", "syn2"]),
            (["A", "B", "C"], [], [], ["syn1", "syn2"]),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_NONE_SET_RULES)
    def test__run_validation_across_target_manifests_set_rules_match_none_with_two_targets(
        self,
        va_obj: ValidateAttribute,
        test_df1: DataFrame,
        input_column: list,
        missing_ids: list[str],
        present_ids: list[str],
        repeat_ids: list[str],
        rule: str,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests
          using matchNone set rule
        When there are nt matching values, no id get added
        When there are matching values the id gets added to the repeat ids
        """

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": test_df1, "syn2": test_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule=rule,
                manifest_col=Series(input_column),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert list(validation_output[0].keys()) == missing_ids
            assert validation_output[1] == present_ids
            assert list(validation_output[2].keys()) == repeat_ids

    ######################################
    # _run_validation_across_targets_value
    ######################################

    @pytest.mark.parametrize(
        "tested_column, target_column, missing, duplicated, repeat",
        [
            (["A", "B", "C"], ["A", "B", "C"], [], [], ["A", "B", "C"]),
            (["A", "B", "C", "C"], ["A", "B", "C"], [], [], ["A", "B", "C", "C"]),
            (["A", "B"], ["A", "B", "C"], [], [], ["A", "B"]),
            (["C"], ["C", "C"], [], ["C"], ["C"]),
            (["C"], ["C", "C", "C"], [], ["C"], ["C"]),
            (["A", "B", "C", "D"], ["A", "B", "C"], ["D"], [], ["A", "B", "C"]),
            (
                ["A", "B", "C", "D", "D"],
                ["A", "B", "C"],
                ["D", "D"],
                [],
                ["A", "B", "C"],
            ),
            (["D"], ["A", "B", "C"], ["D"], [], []),
        ],
    )
    def test__run_validation_across_targets_value(
        self,
        va_obj: ValidateAttribute,
        tested_column: list,
        target_column: list,
        missing: list,
        duplicated: list,
        repeat: list,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_targets_value
        These tests show:
        To get repeat values, a value must appear in both the tested and target column
        To get duplicated values, a value must appear more than once in the target column
        To get missing values, a value must appear in the tested column, but not the target column

        """
        validation_output = va_obj._run_validation_across_targets_value(
            manifest_col=Series(tested_column),
            concatenated_target_column=Series(target_column),
        )
        assert validation_output[0].to_list() == missing
        assert validation_output[1].to_list() == duplicated
        assert validation_output[2].to_list() == repeat

    ####################################
    # _run_validation_across_targets_set
    ####################################

    @pytest.mark.parametrize("tested_column", [(), ("A"), ("A", "A"), ("A", "B")])
    @pytest.mark.parametrize(
        "rule", MATCH_ATLEAST_ONE_SET_RULES + MATCH_EXACTLY_ONE_SET_RULES
    )
    @pytest.mark.parametrize(
        "target_id, present_log_input, present_log_expected",
        [
            ("syn1", [], ["syn1"]),
            ("syn2", ["syn1"], ["syn1", "syn2"]),
            ("syn3", ["syn1"], ["syn1", "syn3"]),
        ],
    )
    def test__run_validation_across_targets_set_match_exactly_atleast_one_no_missing_values(
        self,
        va_obj: ValidateAttribute,
        test_df_col_names: dict[str, str],
        test_df1: DataFrame,
        rule: str,
        tested_column: list,
        target_id: str,
        present_log_input: list[str],
        present_log_expected: list[str],
    ) -> None:
        """
        This test shows that for matchAtLeastOne and matchExactlyOne rules that as long as all
          values in the tested column are in the target manifest, only the present manifest list
          is updated

        """
        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule=rule,
            column_names=test_df_col_names,
            manifest_col=Series(tested_column),
            target_attribute="patientid",
            target_manifest=test_df1,
            target_manifest_id=target_id,
            missing_manifest_log={},
            present_manifest_log=present_log_input.copy(),
            repeat_manifest_log={},
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
        )
        assert output[0] == {}
        assert output[1] == present_log_expected
        assert output[2] == {}
        assert bool_list1 == [True]
        assert bool_list2 == [False]

    @pytest.mark.parametrize(
        "rule", MATCH_ATLEAST_ONE_SET_RULES + MATCH_EXACTLY_ONE_SET_RULES
    )
    @pytest.mark.parametrize(
        "tested_column, target_id, present_log_input, present_log_expected",
        [
            (["D"], "syn1", [], []),
            (["D", "D"], "syn2", [], []),
            (["D", "F"], "syn3", [], []),
        ],
    )
    def test__run_validation_across_targets_set_match_exactly_atleast_one_missing_values(
        self,
        va_obj: ValidateAttribute,
        test_df_col_names: dict[str, str],
        test_df1: DataFrame,
        rule: str,
        tested_column: list,
        target_id: str,
        present_log_input: list[str],
        present_log_expected: list[str],
    ) -> None:
        """
        This test shows that for matchAtLeastOne and matchExactlyOne rules,
          that missing values get added
        """
        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule=rule,
            column_names=test_df_col_names,
            manifest_col=Series(tested_column),
            target_attribute="patientid",
            target_manifest=test_df1,
            target_manifest_id=target_id,
            missing_manifest_log={},
            present_manifest_log=present_log_input.copy(),
            repeat_manifest_log={},
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
        )
        assert output[0][target_id].to_list() == tested_column
        assert output[1] == present_log_expected
        assert output[2] == {}
        assert bool_list1 == [True]
        assert bool_list2 == [False]

    def test__run_validation_across_targets_set_match_none(
        self,
        va_obj: ValidateAttribute,
        test_df_col_names: dict[str, str],
        test_df1: DataFrame,
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_targets_set for matchAtLeastOne"""

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule="matchNone, Patient.PatientID, set",
            column_names=test_df_col_names,
            manifest_col=Series(["A", "B", "C"]),
            target_attribute="patientid",
            target_manifest=test_df1,
            target_manifest_id="syn1",
            missing_manifest_log={},
            present_manifest_log=[],
            repeat_manifest_log={},
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
        )
        assert output[0] == {}
        assert output[1] == []
        assert list(output[2].keys()) == ["syn1"]
        assert output[2]["syn1"].to_list() == ["A", "B", "C"]
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule="matchNone, Patient.PatientID, set",
            column_names=test_df_col_names,
            manifest_col=Series(["A"]),
            target_attribute="patientid",
            target_manifest=test_df1,
            target_manifest_id="syn2",
            missing_manifest_log={},
            present_manifest_log=[],
            repeat_manifest_log={"syn1": Series(["A", "B", "C"])},
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
        )
        assert output[0] == {}
        assert output[1] == []
        assert list(output[2].keys()) == ["syn1", "syn2"]
        assert output[2]["syn1"].to_list() == ["A", "B", "C"]
        assert output[2]["syn2"].to_list() == ["A"]
        assert bool_list1 == [True]
        assert bool_list2 == [False]

    ###############################
    # _gather_value_warnings_errors
    ###############################

    @pytest.mark.parametrize(
        "rule, missing, duplicated, repeat",
        [
            ("matchAtLeastOne Patient.PatientID value error", [], [], []),
            ("matchAtLeastOne Patient.PatientID value error", [], ["A"], []),
            ("matchAtLeastOne Patient.PatientID value error", [], ["A", "A"], []),
            ("matchAtLeastOne Patient.PatientID value error", [], ["A", "B", "C"], []),
            ("matchExactlyOne Patient.PatientID value error", [], [], []),
            ("matchNone Patient.PatientID value error", [], [], []),
        ],
    )
    def test__gather_value_warnings_errors_passing(
        self,
        va_obj: ValidateAttribute,
        rule: str,
        missing: list,
        duplicated: list,
        repeat: list,
    ) -> None:
        """
        Tests for ValidateAttribute._gather_value_warnings_errors
        For matchAtLeastOne to pass there must be no missing values
        For matchExactlyOne there must be no missing or duplicated values
        For matchNone there must be no repeat values
        """
        assert va_obj._gather_value_warnings_errors(
            val_rule=rule,
            source_attribute="PatientID",
            value_validation_store=(
                Series(missing),
                Series(duplicated),
                Series(repeat),
            ),
        ) == ([], [])

    @pytest.mark.parametrize(
        "rule, missing, duplicated, repeat",
        [
            ("matchAtLeastOne Patient.PatientID value error", ["A"], [], []),
            ("matchAtLeastOne Patient.PatientID value warning", ["A"], [], []),
            ("matchExactlyOne Patient.PatientID value error", ["A"], [], []),
            ("matchExactlyOne Patient.PatientID value warning", ["A"], [], []),
            ("matchExactlyOne Patient.PatientID value error", [], ["B"], []),
            ("matchExactlyOne Patient.PatientID value warning", [], ["B"], []),
            ("matchNonePatient.PatientID value error", [], [], ["A"]),
            ("matchNone Patient.PatientID value warning", [], [], ["A"]),
        ],
    )
    def test__gather_value_warnings_errors_with_errors(
        self,
        va_obj: ValidateAttribute,
        rule: str,
        missing: list,
        duplicated: list,
        repeat: list,
    ) -> None:
        """Tests for ValidateAttribute._gather_value_warnings_errors"""

        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule=rule,
            source_attribute="PatientID",
            value_validation_store=(
                Series(missing, name="PatientID"),
                Series(duplicated, name="PatientID"),
                Series(repeat, name="PatientID"),
            ),
        )
        if rule.endswith("error"):
            assert warnings == []
            assert len(errors) == 1
        else:
            assert len(warnings) == 1
            assert errors == []

    #############################
    # _gather_set_warnings_errors
    #############################

    @pytest.mark.parametrize(
        "validation_tuple",
        [
            (({}, [], {})),
            (({}, ["syn1"], {})),
            (({"syn1": Series(["A"])}, ["syn1"], {"syn2": Series(["B"])})),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_SET_RULES)
    def test__gather_set_warnings_errors_match_atleast_one_passes(
        self,
        va_obj: ValidateAttribute,
        validation_tuple: tuple[dict[str, Series], list[str], dict[str, Series]],
        rule: str,
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchAtLeastOne"""

        assert va_obj._gather_set_warnings_errors(
            val_rule=rule,
            source_attribute="PatientID",
            set_validation_store=validation_tuple,
        ) == ([], [])

    def test__gather_set_warnings_errors_match_atleast_one_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchAtLeastOne"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({"syn1": Series(["A"])}, [], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0][0] == ["2"]
        assert errors[0][1] == "PatientID"
        assert errors[0][2] == (
            "Value(s) ['A'] from row(s) ['2'] of the attribute PatientID in the source "
            "manifest are missing. Manifest(s) ['syn1'] are missing the value(s)."
        )
        assert errors[0][3] == ["A"]

    @pytest.mark.parametrize(
        "validation_tuple",
        [
            (({}, [], {})),
            (({}, ["syn1"], {})),
            ({"syn1": Series(["A"])}, ["syn1"], {"syn2": Series(["B"])}),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_SET_RULES)
    def test__gather_set_warnings_errors_match_exactly_one_passes(
        self,
        va_obj: ValidateAttribute,
        validation_tuple: tuple[dict[str, Series], list[str], dict[str, Series]],
        rule: str,
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchExactlyOne"""
        assert va_obj._gather_set_warnings_errors(
            val_rule=rule,
            source_attribute="PatientID",
            set_validation_store=validation_tuple,
        ) == ([], [])

    @pytest.mark.parametrize(
        "input_store, expected_list",
        [
            (
                ({}, ["syn1", "syn2"], {}),
                [
                    [
                        None,
                        "PatientID",
                        (
                            "All values from attribute PatientID in the source manifest are "
                            "present in 2 manifests instead of only 1. Manifests ['syn1', 'syn2'] "
                            "match the values in the source attribute."
                        ),
                        None,
                    ]
                ],
            ),
            (
                ({}, ["syn1", "syn2", "syn3"], {}),
                [
                    [
                        None,
                        "PatientID",
                        (
                            "All values from attribute PatientID in the source manifest are "
                            "present in 3 manifests instead of only 1. Manifests "
                            "['syn1', 'syn2', 'syn3'] match the values in the source attribute."
                        ),
                        None,
                    ]
                ],
            ),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_SET_RULES)
    def test__gather_set_warnings_errors_match_exactly_one_errors(
        self,
        va_obj: ValidateAttribute,
        input_store: tuple[dict[str, Series], list[str], dict[str, Series]],
        expected_list: list[str],
        rule: str,
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchExactlyOne"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=rule,
            source_attribute="PatientID",
            set_validation_store=input_store,
        )
        if rule.endswith("error"):
            assert warnings == []
            assert errors == expected_list
        else:
            assert warnings == expected_list
            assert errors == []

    @pytest.mark.parametrize(
        "validation_tuple", [(({}, [], {})), (({"syn1": Series(["A"])}, ["syn1"], {}))]
    )
    @pytest.mark.parametrize("rule", MATCH_NONE_SET_RULES)
    def test__gather_set_warnings_errors_match_none_passes(
        self,
        va_obj: ValidateAttribute,
        validation_tuple: tuple[dict[str, Series], list[str], dict[str, Series]],
        rule: str,
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchNone"""

        assert va_obj._gather_set_warnings_errors(
            val_rule=rule,
            source_attribute="PatientID",
            set_validation_store=validation_tuple,
        ) == ([], [])

    @pytest.mark.parametrize(
        "input_store, expected_list",
        [
            (
                ({}, [], {"syn1": Series(["A"])}),
                [
                    [
                        ["2"],
                        "PatientID",
                        (
                            "Value(s) ['A'] from row(s) ['2'] for the attribute PatientID "
                            "in the source manifest are not unique. "
                            "Manifest(s) ['syn1'] contain duplicate values."
                        ),
                        ["A"],
                    ]
                ],
            ),
            (
                ({"x": Series(["A"])}, ["x"], {"syn1": Series(["A"])}),
                [
                    [
                        ["2"],
                        "PatientID",
                        (
                            "Value(s) ['A'] from row(s) ['2'] for the attribute PatientID "
                            "in the source manifest are not unique. "
                            "Manifest(s) ['syn1'] contain duplicate values."
                        ),
                        ["A"],
                    ]
                ],
            ),
            (
                ({}, [], {"syn2": Series(["B"])}),
                [
                    [
                        ["2"],
                        "PatientID",
                        (
                            "Value(s) ['B'] from row(s) ['2'] for the attribute PatientID "
                            "in the source manifest are not unique. "
                            "Manifest(s) ['syn2'] contain duplicate values."
                        ),
                        ["B"],
                    ]
                ],
            ),
        ],
    )
    @pytest.mark.parametrize("rule", MATCH_NONE_SET_RULES)
    def test__gather_set_warnings_errors_match_none_errors(
        self,
        va_obj: ValidateAttribute,
        input_store: tuple[dict[str, Series], list[str], dict[str, Series]],
        expected_list: list[str],
        rule: str,
    ) -> None:
        """
        Tests for ValidateAttribute._gather_set_warnings_errors for matchNone
        This test shows that only the repeat_manifest_log matters
        NOTE: when the repeat repeat_manifest_log is longer than one the order
        of the values and synapse ids in the msg are inconsistent, making that
        case hard to test
        """

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule=rule,
            source_attribute="PatientID",
            set_validation_store=input_store,
        )
        if rule.endswith("error"):
            assert warnings == []
            assert errors == expected_list
        else:
            assert warnings == expected_list
            assert errors == []

    ###################
    # _get_column_names
    ###################

    @pytest.mark.parametrize(
        "input_dict, expected_dict",
        [
            ({}, {}),
            ({"col1": []}, {"col1": "col1"}),
            ({"COL 1": []}, {"col1": "COL 1"}),
            ({"ColId": []}, {"colid": "ColId"}),
            ({"ColID": []}, {"colid": "ColID"}),
            (
                {"col1": [], "col2": []},
                {
                    "col1": "col1",
                    "col2": "col2",
                },
            ),
        ],
    )
    def test__get_column_names(
        self,
        va_obj: ValidateAttribute,
        input_dict: dict[str, list],
        expected_dict: dict[str, str],
    ) -> None:
        """Tests for ValidateAttribute._get_column_names"""
        assert va_obj._get_column_names(DataFrame(input_dict)) == expected_dict

    ##############
    # get_no_entry
    ##############

    @pytest.mark.parametrize(
        "input_entry, node_name, expected",
        [
            ("entry", "Check NA", False),
            ("entry", "Check Date", False),
            ("<NA>", "Check NA", False),
            ("<NA>", "Check Date", True),
        ],
    )
    def test_get_no_entry(
        self,
        va_obj: ValidateAttribute,
        input_entry: str,
        node_name: str,
        expected: bool,
    ) -> None:
        """
        This test shows that:
        - if the entry is a normal string the result is always False(not no entry),
        - if the entry is "<NA>" the result is False if the attribute has the "isNA" rule
        - if the entry is "<NA>" the result is True if the attribute does not have the "isNA" rule
        """
        assert va_obj.get_no_entry(input_entry, node_name) is expected

    #####################
    # get_entry_has_value
    #####################

    @pytest.mark.parametrize(
        "input_entry, node_name, expected",
        [
            ("entry", "Check NA", True),
            ("entry", "Check Date", True),
            ("<NA>", "Check NA", True),
            ("<NA>", "Check Date", False),
        ],
    )
    def test_get_entry_has_value(
        self,
        va_obj: ValidateAttribute,
        input_entry: str,
        node_name: str,
        expected: bool,
    ) -> None:
        """
        This test shows that:
        - if the entry is a normal string the result is always True,
        - if the entry is "<NA>" the result is True if the attribute has the "isNA" rule
        - if the entry is "<NA>" the result is False if the attribute does not have the "isNA" rule
        """
        assert va_obj.get_entry_has_value(input_entry, node_name) is expected

    #################
    # list_validation
    #################

    @pytest.mark.parametrize(
        "input_column, rule",
        [
            (Series(["x,x,x"], name="Check List"), "list like"),
            (Series(["x,x,x"], name="Check List"), "list strict"),
            (Series([], name="Check List"), "list like"),
            (Series([], name="Check List"), "list strict"),
            (Series(["x"], name="Check List"), "list like"),
            (Series(["xxx"], name="Check List"), "list like"),
            (Series(["1"], name="Check List"), "list like"),
            (Series([1], name="Check List"), "list like"),
            (Series([1.1], name="Check List"), "list like"),
            (Series([1, 1, 1], name="Check List"), "list like"),
            (Series([np.nan], name="Check List"), "list like"),
            (Series([True], name="Check List"), "list like"),
        ],
    )
    def test_list_validation_passing(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.list_validation
        This test shows that:
        - when using list like, just about anything is validated
        - when using list strict, empty columns, and comma separated strings pass

        """
        errors, warnings, _ = va_obj.list_validation(rule, input_column)
        assert len(errors) == 0
        assert len(warnings) == 0

    @pytest.mark.parametrize(
        "input_column",
        [
            (Series(["x"], name="Check List")),
            (Series(["xxxx"], name="Check List")),
            (Series([1], name="Check List")),
            (Series([1.1], name="Check List")),
            (Series([1, 1, 1], name="Check List")),
            (Series([np.nan], name="Check List")),
            (Series([True], name="Check List")),
        ],
    )
    @pytest.mark.parametrize("rule", ["list strict", "list strict warning"])
    def test_list_validation_not_passing(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.list_validation
        This test shows what doesn't pass when using list strict
        """
        errors, warnings, _ = va_obj.list_validation(rule, input_column)
        if rule.endswith("warning"):
            assert len(errors) == 0
            assert len(warnings) > 0
        else:
            assert len(errors) > 0
            assert len(warnings) == 0

    ##################
    # regex_validation
    ##################

    @pytest.mark.parametrize(
        "input_column, rule",
        [
            (Series(["a"], name="Check List"), "regex match [a-f]"),
            (Series(["a,b,<NA>"], name="Check Regex List Strict"), "regex match [a-f]"),
        ],
    )
    def test_regex_validation_passing(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.regex_validation
        This test shows passing examples using the match rule
        """
        errors, warnings = va_obj.regex_validation(rule, input_column)
        assert len(errors) == 0
        assert len(warnings) == 0

    @pytest.mark.parametrize(
        "input_column, rule",
        [
            (Series(["g"], name="Check List"), "regex match [a-f]"),
            (Series(["a,b,c,g"], name="Check Regex List Strict"), "regex match [a-f]"),
        ],
    )
    def test_regex_validation_failing(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.regex_validation
        This test shows failing examples using the match rule
        """
        errors, warnings = va_obj.regex_validation(rule, input_column)
        assert len(errors) == 1
        assert len(warnings) == 0

    @pytest.mark.parametrize(
        "input_column, rule, exception",
        [
            (Series(["a"]), "", ValidationError),
            (Series(["a"]), "regex", ValidationError),
            (Series(["a"]), "regex match", ValidationError),
            (Series(["a"]), "regex match [a-f]", ValueError),
        ],
    )
    def test_regex_validation_exceptions(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str, exception
    ) -> None:
        """
        This tests ValidateAttribute.regex_validation
        This test shows that:
        - when the rule is malformed, a ValidationError is raised
        - when the input series has no name, a ValueError is raised

        """
        with pytest.raises(exception):
            va_obj.regex_validation(rule, input_column)

    @pytest.mark.parametrize(
        "input_column, rule",
        [
            (Series(["a,b,c"], name="Check Regex List"), "list::regex match [a-f]"),
            (
                Series(["a,b,c", "d,e,f"], name="Check Regex List"),
                "list::regex match [a-f]",
            ),
        ],
    )
    def test_regex_validation_with_list_column(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.regex_validation using a list column
        """
        errors, warnings = va_obj.regex_validation(rule, input_column)
        assert len(errors) == 0
        assert len(warnings) == 0

    #################
    # type_validation
    #################

    @pytest.mark.parametrize(
        "input_column, rule",
        [
            (Series(["a"], name="Check String"), "str"),
            (Series([1], name="Check Num"), "num"),
            (Series([1], name="Check Int"), "int"),
            (Series([1.1], name="Check Float"), "float"),
            (Series([np.nan], name="Check String"), "str"),
            (Series([np.nan], name="Check Num"), "num"),
            (Series([np.nan], name="Check Int"), "int"),
            (Series([np.nan], name="Check Float"), "float"),
        ],
    )
    def test_type_validation_passing(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.type_validation
        This test shows passing examples using the type rule
        """
        errors, warnings = va_obj.type_validation(rule, input_column)
        assert len(errors) == 0
        assert len(warnings) == 0

    @pytest.mark.parametrize(
        "input_column, rule",
        [
            (Series([1], name="Check String"), "str"),
            (Series([1], name="Check String"), "str error"),
            (Series(["a"], name="Check Num"), "num"),
            (Series(["a"], name="Check Num"), "num error"),
            (Series(["20"], name="Check Num"), "num"),
            (Series([1.1], name="Check Int"), "int"),
            (Series(["a"], name="Check Int"), "int"),
            (Series(["a"], name="Check Int"), "int error"),
            (Series([1], name="Check Float"), "float"),
            (Series(["a"], name="Check Float"), "float"),
            (Series(["a"], name="Check Float"), "float error"),
        ],
    )
    def test_type_validation_errors(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.type_validation
        This test shows failing examples using the type rule
        """
        errors, warnings = va_obj.type_validation(rule, input_column)
        assert len(errors) == 1
        assert len(warnings) == 0

    @pytest.mark.parametrize(
        "input_column, rule",
        [
            (Series([1], name="Check String"), "str warning"),
            (Series(["a"], name="Check Num"), "num warning"),
            (Series(["a"], name="Check Int"), "int warning"),
            (Series(["a"], name="Check Float"), "float warning"),
        ],
    )
    def test_type_validation_warnings(
        self, va_obj: ValidateAttribute, input_column: Series, rule: str
    ) -> None:
        """
        This tests ValidateAttribute.type_validation
        This test shows failing examples using the type rule
        """
        errors, warnings = va_obj.type_validation(rule, input_column)
        assert len(errors) == 0
        assert len(warnings) == 1

    @pytest.mark.parametrize(
        "input_column, rule, exception, msg",
        [
            (
                Series([1], name="Check String"),
                "",
                ValueError,
                "val_rule first component:  must be one of",
            ),
            (
                Series([1], name="Check String"),
                "x",
                ValueError,
                "val_rule first component: x must be one of",
            ),
            (
                Series([1], name="Check String"),
                "x x x",
                ValueError,
                "val_rule must contain no more than two components.",
            ),
        ],
    )
    def test_type_validation_exceptions(
        self,
        va_obj: ValidateAttribute,
        input_column: Series,
        rule: str,
        exception: Exception,
        msg: str,
    ) -> None:
        """
        This tests ValidateAttribute.type_validation
        This test shows failing examples using the type rule
        """
        with pytest.raises(exception, match=msg):
            va_obj.type_validation(rule, input_column)

    ################
    # url_validation
    ################

    @pytest.mark.parametrize(
        "input_column",
        [
            (Series([], name="Check URL")),
            (Series([np.nan], name="Check URL")),
            (
                Series(
                    ["https://doi.org/10.1158/0008-5472.can-23-0128"], name="Check URL"
                )
            ),
        ],
    )
    def test_url_validation_passing(
        self,
        va_obj: ValidateAttribute,
        input_column: Series,
    ) -> None:
        """
        This tests ValidateAttribute.url_validation
        This test shows passing examples using the url rule
        """
        errors, warnings = va_obj.url_validation("url", input_column)
        assert len(errors) == 0
        assert len(warnings) == 0

    @pytest.mark.parametrize(
        "input_column",
        [(Series([""], name="Check URL")), (Series(["xxx"], name="Check URL"))],
    )
    def test_url_validation_failing(
        self,
        va_obj: ValidateAttribute,
        input_column: Series,
    ) -> None:
        """
        This tests ValidateAttribute.url_validation
        This test shows failing examples using the url rule
        """
        errors, warnings = va_obj.url_validation("url", input_column)
        assert len(errors) > 0
        assert len(warnings) == 0

    #######################
    # _parse_validation_log
    #######################

    @pytest.mark.parametrize(
        "input_log, expected_invalid_rows, expected_invalid_entities, expected_manifest_ids",
        [
            ({}, [], [], []),
            ({"syn1": Series(["A"])}, ["2"], ["A"], ["syn1"]),
            ({"syn1": Series(["A"], index=[1])}, ["3"], ["A"], ["syn1"]),
            ({"syn1": Series(["A", "B"])}, ["2"], ["A"], ["syn1"]),
            (
                {"syn1": Series(["A"]), "syn2": Series(["B"])},
                ["2"],
                ["A", "B"],
                ["syn1", "syn2"],
            ),
        ],
    )
    def test__parse_validation_log(
        self,
        va_obj: ValidateAttribute,
        input_log: dict[str, Series],
        expected_invalid_rows: list[str],
        expected_invalid_entities: list[str],
        expected_manifest_ids: list[str],
    ) -> None:
        """
        This test shows that
        - an empty log returns empty values
        - only the first value in each series is returned as invalid entities
        - the index of the invalid entity is returned incremented by 2
        - each manifest entity is returned

        """
        invalid_rows, invalid_entities, manifest_ids = va_obj._parse_validation_log(
            input_log
        )
        assert invalid_rows == expected_invalid_rows
        assert sorted(invalid_entities) == expected_invalid_entities
        assert manifest_ids == expected_manifest_ids

    ###################################
    # _merge_format_invalid_rows_values
    ###################################

    @pytest.mark.parametrize(
        "input_series1, input_series2, expected_invalid_rows, expected_invalid_entry",
        [
            (Series([], name="x"), Series([], name="x"), [], []),
            (Series(["A"], name="x"), Series([], name="x"), ["2"], ["A"]),
            (Series([], name="x"), Series(["B"], name="x"), ["2"], ["B"]),
            (Series(["A"], name="x"), Series(["B"], name="x"), ["2"], ["A", "B"]),
            (Series(["A"], name="x"), Series(["C"], name="x"), ["2"], ["A", "C"]),
            (
                Series(["A", "B"], name="x"),
                Series(["C"], name="x"),
                ["2", "3"],
                ["A", "B", "C"],
            ),
        ],
    )
    def test__merge_format_invalid_rows_values(
        self,
        va_obj: ValidateAttribute,
        input_series1: Series,
        input_series2: Series,
        expected_invalid_rows: list[str],
        expected_invalid_entry: list[str],
    ) -> None:
        """
        This test shows that
        - the names of the series must match
        - the indices of both series get combined and incremented by 2
        - the values of both series are combined
        """
        invalid_rows, invalid_entry = va_obj._merge_format_invalid_rows_values(
            input_series1, input_series2
        )
        assert invalid_rows == expected_invalid_rows
        assert invalid_entry == expected_invalid_entry

    ############################
    # _format_invalid_row_values
    ############################

    @pytest.mark.parametrize(
        "input_series, expected_invalid_rows, expected_invalid_entry",
        [
            (Series([]), [], []),
            (Series(["A"]), ["2"], ["A"]),
            (Series(["A", "B"]), ["2", "3"], ["A", "B"]),
        ],
    )
    def test__format_invalid_row_values(
        self,
        va_obj: ValidateAttribute,
        input_series: Series,
        expected_invalid_rows: list[str],
        expected_invalid_entry: list[str],
    ) -> None:
        """
        This test shows that the indices of the input series is incremented by 2
        """
        invalid_rows, invalid_entry = va_obj._format_invalid_row_values(input_series)
        assert invalid_rows == expected_invalid_rows
        assert invalid_entry == expected_invalid_entry

    ###########################################
    # _remove_non_entry_from_invalid_entry_list
    ###########################################

    @pytest.mark.parametrize(
        "input_entry, input_row_num, input_name, expected_invalid_entry, expected_row_num",
        [
            # Cases where entry and row number remain unchanged
            ([], [], "", [], []),
            (None, None, "", None, None),
            (["A"], None, "", ["A"], None),
            (None, ["1"], "", None, ["1"]),
            (["A"], ["1"], "x", ["A"], ["1"]),
            (["A", "B"], ["1"], "x", ["A", "B"], ["1"]),
            (["A"], ["1", "2"], "x", ["A"], ["1", "2"]),
            # When there are missing values the value and the row number are removed
            (["<NA>"], ["1"], "x", [], []),
            (["<NA>", "<NA>"], ["1", "2"], "x", [], []),
            (["<NA>", "A"], ["1", "2"], "x", ["A"], ["2"]),
            # When there are more row numbers than values, and there are missing values
            # then the row number that corresponds to the missing value is removed
            (["<NA>"], ["1", "2"], "x", [], ["2"]),
            (["<NA>", "<NA>"], ["1", "2", "3", "4"], "x", [], ["3", "4"]),
            (["<NA>", "A"], ["1", "2", "3", "4"], "x", ["A"], ["2", "3", "4"]),
            (["A", "<NA>"], ["1", "2", "3", "4"], "x", ["A"], ["1", "3", "4"]),
        ],
    )
    def test__remove_non_entry_from_invalid_entry_list(
        self,
        va_obj: ValidateAttribute,
        input_entry: list[str],
        input_row_num: list[str],
        input_name: str,
        expected_invalid_entry: list[str],
        expected_row_num: list[str],
    ) -> None:
        """
        Tests for ValidateAttribute.remove_non_entry_from_invalid_entry_list
        """
        invalid_entry, row_num = va_obj._remove_non_entry_from_invalid_entry_list(
            input_entry, input_row_num, input_name
        )
        assert invalid_entry == expected_invalid_entry
        assert row_num == expected_row_num

    @pytest.mark.parametrize(
        "input_entry, input_row_num, input_name, exception",
        [
            # if first two inputs are not empty, an empty name string causes an IndexError
            (["A"], ["1"], "", IndexError),
            # if there are more invalid entries than row numbers, there is an IndexError
            (["<NA>", "<NA>"], ["1"], "x", IndexError),
        ],
    )
    def test__remove_non_entry_from_invalid_entry_list_exceptions(
        self,
        va_obj: ValidateAttribute,
        input_entry: list[str],
        input_row_num: list[str],
        input_name: str,
        exception: Exception,
    ) -> None:
        """
        Tests for ValidateAttribute.remove_non_entry_from_invalid_entry_list that cause
          exceptions
        """
        with pytest.raises(exception):
            va_obj._remove_non_entry_from_invalid_entry_list(
                input_entry, input_row_num, input_name
            )

    ####################################
    # _check_if_target_manifest_is_empty
    ####################################

    @pytest.mark.parametrize(
        "input_dataframe, input_bool_list, input_column_dict, output_bool_list",
        [
            # Dataframes with only required columns are always considered_empty
            (
                DataFrame({"component": [], "id": [], "entityid": []}),
                [],
                {"component": "component", "id": "id", "entityid": "entityid"},
                [True],
            ),
            (
                DataFrame({"component": ["xxx"], "id": ["xxx"], "entityid": ["xxx"]}),
                [],
                {"component": "component", "id": "id", "entityid": "entityid"},
                [True],
            ),
            # Dataframes with non-required columns whose only values are null are considered empty
            (
                DataFrame(
                    {
                        "component": ["xxx"],
                        "id": ["xxx"],
                        "entityid": ["xxx"],
                        "col1": [np.nan],
                    }
                ),
                [],
                {"component": "component", "id": "id", "entityid": "entityid"},
                [True],
            ),
            (
                DataFrame(
                    {
                        "component": ["xxx"],
                        "id": ["xxx"],
                        "entityid": ["xxx"],
                        "col1": [np.nan],
                        "col2": [np.nan],
                    }
                ),
                [],
                {"component": "component", "id": "id", "entityid": "entityid"},
                [True],
            ),
            # Dataframes with non-required columns who have non-null values are not considered empty
            (
                DataFrame(
                    {
                        "component": ["xxx"],
                        "id": ["xxx"],
                        "entityid": ["xxx"],
                        "col1": ["xxx"],
                    }
                ),
                [],
                {"component": "component", "id": "id", "entityid": "entityid"},
                [False],
            ),
            (
                DataFrame(
                    {
                        "component": ["xxx"],
                        "id": ["xxx"],
                        "entityid": ["xxx"],
                        "col1": [np.nan],
                        "col2": ["xxx"],
                    }
                ),
                [],
                {"component": "component", "id": "id", "entityid": "entityid"},
                [False],
            ),
        ],
    )
    def test__check_if_target_manifest_is_empty(
        self,
        va_obj: ValidateAttribute,
        input_dataframe: DataFrame,
        input_bool_list: list[bool],
        input_column_dict: dict[str, str],
        output_bool_list: list[bool],
    ) -> None:
        """
        Tests for ValidateAttribute._check_if_target_manifest_is_empty
        """
        bool_list = va_obj._check_if_target_manifest_is_empty(
            input_dataframe, input_bool_list, input_column_dict
        )
        assert bool_list == output_bool_list

    @pytest.mark.parametrize(
        "input_dataframe, input_bool_list, input_column_dict, exception",
        [
            # column name dict must have keys "component", "id", "entityid"
            (DataFrame({"component": [], "id": [], "entityid": []}), [], {}, KeyError),
            # dataframe must have columns "component", "id", "entityid"
            (
                DataFrame(),
                [],
                {"component": "component", "id": "id", "entityid": "entityid"},
                KeyError,
            ),
        ],
    )
    def test__check_if_target_manifest_is_empty_exceptions(
        self,
        va_obj: ValidateAttribute,
        input_dataframe: DataFrame,
        input_bool_list: list[bool],
        input_column_dict: dict[str, str],
        exception: Exception,
    ) -> None:
        """
        Tests for ValidateAttribute._check_if_target_manifest_is_empty that cause
          exceptions
        """
        with pytest.raises(exception):
            va_obj._check_if_target_manifest_is_empty(
                input_dataframe, input_bool_list, input_column_dict
            )

    #################
    # _get_rule_scope
    #################

    @pytest.mark.parametrize(
        "input_rule, output_scope",
        [
            # After splitting by spaces, the third element is returned
            ("a b c", "c"),
            ("a b c d", "c"),
        ],
    )
    def test__get_rule_scope(
        self, va_obj: ValidateAttribute, input_rule: str, output_scope: str
    ) -> None:
        """
        Tests for ValidateAttribute._get_rule_scope
        """
        assert va_obj._get_rule_scope(input_rule) == output_scope

    @pytest.mark.parametrize(
        "input_rule, exception",
        [
            # The rule must a string when split by spaces, have atleast three elements
            ("", IndexError),
            ("x", IndexError),
            ("x x", IndexError),
            ("x;x;x", IndexError),
        ],
    )
    def test__get_rule_scope_exceptions(
        self, va_obj: ValidateAttribute, input_rule: str, exception: Exception
    ) -> None:
        """
        Tests for ValidateAttribute._get_rule_scope that cause exceptions
        """
        with pytest.raises(exception):
            va_obj._get_rule_scope(input_rule)
