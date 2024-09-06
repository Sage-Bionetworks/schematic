"""Unit testing for the ValidateAttribute class"""

from typing import Generator
from unittest.mock import Mock, patch

import numpy as np
import pytest
from pandas import DataFrame, Series, concat

import schematic.models.validate_attribute
from schematic.models.validate_attribute import ValidateAttribute
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

TEST_MANIFEST_BAD_FILENAME = DataFrame(
    {
        "Component": ["Mockfilename", "Mockfilename", "Mockfilename"],
        "Filename": ["test1.txt", "test2.txt", "test_bad.txt"],
        "entityId": ["syn1", "syn2", "syn3"],
    }
)

TEST_MANIFEST_BAD_ENTITY_ID = DataFrame(
    {
        "Component": ["Mockfilename", "Mockfilename", "Mockfilename"],
        "Filename": ["test1.txt", "test2.txt", "test3.txt"],
        "entityId": ["syn1", "syn2", "syn_bad"],
    }
)


@pytest.fixture(name="va_obj")
def fixture_va_obj(
    dmge: DataModelGraphExplorer,
) -> Generator[ValidateAttribute, None, None]:
    """Yield a ValidateAttribute object"""
    yield ValidateAttribute(dmge)


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


class TestValidateAttributeObject:
    """Testing for ValidateAttribute class with all Synapse calls mocked"""

    ##################
    # cross_validation
    ##################

    @pytest.mark.parametrize("series", EXACTLY_ATLEAST_PASSING_SERIES)
    @pytest.mark.parametrize("rule", MATCH_ATLEAST_ONE_SET_RULES)
    def test_cross_validation_match_atleast_one_set_rules_passing(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
        ):
            assert va_obj.cross_validation(rule, series) == ([], [])

    @pytest.mark.parametrize("series", EXACTLY_ATLEAST_PASSING_SERIES)
    @pytest.mark.parametrize("rule", MATCH_EXACTLY_ONE_SET_RULES)
    def test_cross_validation_match_exactly_one_set_rules_passing(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_match_atleast_one_set_rules_errors(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_match_exactly_one_set_rules_errors(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
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
    def test_cross_validation_match_none_set_rules_passing(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_match_none_set_rules_errors(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, series)
            if rule.endswith("error"):
                assert warnings == []
                assert len(errors) == 1
            else:
                assert len(warnings) == 1
                assert errors == []

    @pytest.mark.parametrize("rule", MATCH_ATLEAST_ONE_VALUE_RULES)
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
    def test_cross_validation_value_match_atleast_one_rules_passing(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
        rule: str,
        tested_column: list,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns pass for matchAtLeastOne
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_value_match_atleast_one_rules_errors(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_match_exactly_one_value_rules_passing(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
        rule: str,
        tested_column: list,
    ):
        """
        Tests ValidateAttribute.cross_validation
        These tests show what columns pass for matchExactlyOne
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_value_match_exactly_one_rules_errors(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_match_none_value_rules_passing(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
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
    def test_cross_validation_value_match_none_rules_errors(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
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
            return_value={"syn1": cross_val_df1},
        ):
            errors, warnings = va_obj.cross_validation(rule, tested_column)
            if rule.endswith("error"):
                assert len(errors) == 1
                assert warnings == []
            else:
                assert errors == []
                assert len(warnings) == 1

    #########################################
    # filename_validation
    #########################################

    @pytest.mark.parametrize(
        "manifest_df, expected_errors, expected_warnings",
        [
            (TEST_MANIFEST_GOOD, [], []),
            (
                TEST_MANIFEST_BAD_FILENAME,
                [
                    [
                        "2",
                        "Filename",
                        "The file path 'test_bad.txt' on row 2 does not exist in the file view.",
                        "test_bad.txt",
                    ]
                ],
                [],
            ),
            (
                TEST_MANIFEST_BAD_ENTITY_ID,
                [
                    [
                        "2",
                        "Filename",
                        "The entityId for file path 'test3.txt' on row 2"
                        " does not match the entityId for the file in the file view",
                        "test3.txt",
                    ]
                ],
                [],
            ),
        ],
        ids=["valid_manifest", "bad_filename", "bad_entity_id"],
    )
    def test_filename_validation(
        self,
        va_obj: ValidateAttribute,
        manifest_df: DataFrame,
        expected_errors: list,
        expected_warnings: list,
    ):
        mock_synapse_storage = Mock()
        mock_synapse_storage.storageFileviewTable = TEST_DF_FILEVIEW
        va_obj.synStore = mock_synapse_storage
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_login",
        ), patch.object(
            mock_synapse_storage, "reset_index", return_value=TEST_DF_FILEVIEW
        ):
            assert va_obj.filename_validation(
                val_rule="filenameExists syn61682648",
                manifest=manifest_df,
                access_token="test_access_token",
            ) == (expected_errors, expected_warnings)

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
        self, va_obj: ValidateAttribute, cross_val_df1: DataFrame, rule: str
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_target_manifests with value rule"""

        # This tests when an empty column is validated there are no missing values to be returned
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
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
        cross_val_df1: DataFrame,
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
        Otherwise the maniferst id gets added to the missing ids list
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
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
        cross_val_df1: DataFrame,
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
        This also shows that when thare are multiple target mnaifests they both get added to
          either the present of missing manifest ids
        """
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
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
        cross_val_df1: DataFrame,
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
        When there are mathcing values the id gets added to the repeat ids
        """

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
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
        cross_val_df1: DataFrame,
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
        When there are mathcing values the id gets added to the repeat ids
        """

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
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
    def test__run_validation_across_targets_set_match_exactly_atleaset_one_no_missing_values(
        self,
        va_obj: ValidateAttribute,
        cross_val_col_names: dict[str, str],
        cross_val_df1: DataFrame,
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
            column_names=cross_val_col_names,
            manifest_col=Series(tested_column),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
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
    def test__run_validation_across_targets_set_match_exactly_atleaset_one_missing_values(
        self,
        va_obj: ValidateAttribute,
        cross_val_col_names: dict[str, str],
        cross_val_df1: DataFrame,
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
            column_names=cross_val_col_names,
            manifest_col=Series(tested_column),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
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
        cross_val_col_names: dict[str, str],
        cross_val_df1: DataFrame,
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_targets_set for matchAtLeastOne"""

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule="matchNone, Patient.PatientID, set",
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
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
            column_names=cross_val_col_names,
            manifest_col=Series(["A"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
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
        For matchAtLeastOne to pass there must be no mssing values
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
