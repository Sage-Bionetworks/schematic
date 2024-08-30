"""Unit testing for the ValidateAttribute class"""

from typing import Generator
from unittest.mock import patch

import pytest
from pandas import Series, DataFrame, concat

from schematic.models.validate_attribute import ValidateAttribute
from schematic.schemas.data_model_graph import DataModelGraphExplorer
import schematic.models.validate_attribute

# pylint: disable=protected-access


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


class TestValidateAttributeObject:
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
            assert va_obj.cross_validation(
                val_rule, Series(["A", "B"], index=[0, 1], name="PatientID")
            ) == (
                [],
                [],
            )
            assert va_obj.cross_validation(
                val_rule, Series([], index=[], name="PatientID")
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
                val_rule,
                Series(["A", "B", "C", "D"], index=[0, 1, 2, 3], name="PatientID"),
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

            assert va_obj.cross_validation(val_rule, Series(["A", "B"])) == ([], [])

            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C", "D"])) == (
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
            assert va_obj.cross_validation(val_rule, Series(["D"])) == (
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

            errors, _ = va_obj.cross_validation(
                val_rule, Series(["A", "B"], index=[0, 1], name="PatientID")
            )
            assert len(errors) == 1

    def test_cross_validation_value_match_atleast_one_rules_passing(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
        cross_val_df2: DataFrame,
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

        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df2},
        ):
            assert va_obj.cross_validation(val_rule, Series(["A", "B", "C", "D"])) == (
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

    def test_cross_validation_match_exactly_one_value_rules_passing(
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

    def test_cross_validation_match_exactly_one_value_rules_errors(
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
                rule_scope="value",
                val_rule="xxx Patient.PatientID xxx",
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
                rule_scope="value",
                val_rule="xxx Patient.PatientID xxx",
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
                rule_scope="value",
                val_rule="xxx Patient.PatientID xxx",
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
                rule_scope="value",
                val_rule="xxx Patient.PatientID xxx",
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
                rule_scope="value",
                val_rule="xxx Patient.PatientID xxx",
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

    def test__run_validation_across_target_manifests_set_rules_match_atleast_one(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests
          with matchAtleastOne set rule
        """
        # These tests show when a column that is empty or partial/full match to
        # the target manifest, the output only the present manifest log is updated
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchAtLeastOne Patient.PatientID set error",
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == ["syn1"]
            assert validation_output[2] == {}

            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchAtLeastOne Patient.PatientID set error",
                manifest_col=Series(["A", "B", "C"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == ["syn1"]
            assert validation_output[2] == {}

            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchAtLeastOne Patient.PatientID set error",
                manifest_col=Series(["A"]),
                target_column=Series([]),
            )

        # This test shows that if there is an extra value ("D") in the column being validated
        # it gets added to the missing_manifest_log
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchAtLeastOne Patient.PatientID set error",
                manifest_col=Series(["A", "B", "C", "D"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert list(validation_output[0].keys()) == ["syn1"]
            assert validation_output[0]["syn1"].to_list() == ["D"]
            assert validation_output[1] == []
            assert validation_output[2] == {}

    def test__run_validation_across_target_manifests_set_rules_match_none(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests
          with matchNone set rule
        """

        # This test shows nothing happens when the column is empty or has no shared values
        # witht the target manifest
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchNone Patient.PatientID set error",
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == []
            assert validation_output[2] == {}

            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchNone Patient.PatientID set error",
                manifest_col=Series(["D"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == []
            assert validation_output[2] == {}

        # These tests when any values match they are put into the repeat log
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchNone Patient.PatientID set error",
                manifest_col=Series(["A", "B", "C"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == []
            assert list(validation_output[2].keys()) == ["syn1"]
            assert validation_output[2]["syn1"].to_list() == ["A", "B", "C"]

            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchNone Patient.PatientID set error",
                manifest_col=Series(["A"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == []
            assert list(validation_output[2].keys()) == ["syn1"]
            assert validation_output[2]["syn1"].to_list() == ["A"]

    def test__run_validation_across_target_manifests_set_rules_exactly_one(
        self,
        va_obj: ValidateAttribute,
        cross_val_df1: DataFrame,
    ) -> None:
        """
        Tests for ValidateAttribute._run_validation_across_target_manifests with
        matchExactlyOne set rule
        """
        # These tests show when an empty a partial match or full match column is used,
        # the output only contains the targeted synapse id as a present value
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchExactlyOne Patient.PatientID set error",
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == ["syn1"]
            assert validation_output[2] == {}

            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchExactlyOne Patient.PatientID set error",
                manifest_col=Series(["A", "B", "C"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == ["syn1"]
            assert validation_output[2] == {}

            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchExactlyOne Patient.PatientID set error",
                manifest_col=Series(["A"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == ["syn1"]
            assert validation_output[2] == {}

        # These tests shows that if there is an extra value ("D") in the column being validated
        # it gets added to the missing manifest values dict
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchExactlyOne Patient.PatientID set error",
                manifest_col=Series(["A", "B", "C", "D"]),
                target_column=Series([]),
            )
            assert isinstance(validation_output, tuple)
            assert list(validation_output[0].keys()) == ["syn1"]
            assert validation_output[0]["syn1"].to_list() == ["D"]
            assert validation_output[1] == []
            assert validation_output[2] == {}

        # This tests shows when a manifest macthes more than one manifest, both are added
        # to the present manifest log
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df1, "syn2": cross_val_df1},
        ):
            _, validation_output = va_obj._run_validation_across_target_manifests(
                rule_scope="set",
                val_rule="matchExactlyOne Patient.PatientID set error",
                manifest_col=Series([]),
                target_column=Series(["A", "B", "C"]),
            )
            assert isinstance(validation_output, tuple)
            assert validation_output[0] == {}
            assert validation_output[1] == ["syn1", "syn2"]
            assert validation_output[2] == {}

    def test__run_validation_across_targets_value(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._run_validation_across_targets_value"""

        validation_output = va_obj._run_validation_across_targets_value(
            manifest_col=Series(["A", "B", "C"]),
            concatenated_target_column=Series(["A", "B", "C"]),
        )
        assert validation_output[0].empty
        assert validation_output[1].empty
        assert validation_output[2].to_list() == ["A", "B", "C"]

        validation_output = va_obj._run_validation_across_targets_value(
            manifest_col=Series(["C"]),
            concatenated_target_column=Series(["A", "B", "B", "C", "C"]),
        )
        assert validation_output[0].empty
        assert validation_output[1].to_list() == ["C"]
        assert validation_output[2].to_list() == ["C"]

        validation_output = va_obj._run_validation_across_targets_value(
            manifest_col=Series(["A", "B", "C"]),
            concatenated_target_column=Series(["A"]),
        )
        assert validation_output[0].to_list() == ["B", "C"]
        assert validation_output[1].empty
        assert validation_output[2].to_list() == ["A"]

    def test__gather_value_warnings_errors_passing(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_value_warnings_errors"""
        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID value error",
            source_attribute="PatientID",
            value_validation_store=(Series(), Series(["A", "B", "C"]), Series()),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID value error",
            source_attribute="PatientID",
            value_validation_store=(
                Series(),
                Series(["A", "B", "C"]),
                Series(["A", "B", "C"]),
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_value_warnings_errors(
            val_rule="matchAtLeastOne comp.att value error",
            source_attribute="att",
            value_validation_store=(Series(), Series(), Series()),
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
            value_validation_store=(Series(["A", "B", "C"]), Series(), Series()),
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
            val_rule="matchAtleastOne, Patient.PatientID, set",
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
        assert output[1] == ["syn1"]
        assert output[2] == {}
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule="matchAtleastOne, Patient.PatientID, set",
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn2",
            missing_manifest_log={},
            present_manifest_log=["syn1"],
            repeat_manifest_log={},
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
        )
        assert output[0] == {}
        assert output[1] == ["syn1", "syn2"]
        assert output[2] == {}
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule="matchAtleastOne, Patient.PatientID, set",
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C", "D"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn1",
            missing_manifest_log={},
            present_manifest_log=[],
            repeat_manifest_log={},
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
        )
        assert list(output[0].keys()) == ["syn1"]
        assert list(output[0].values())[0].to_list() == ["D"]
        assert output[1] == []
        assert output[2] == {}
        assert bool_list1 == [True]
        assert bool_list2 == [False]

        output, bool_list1, bool_list2 = va_obj._run_validation_across_targets_set(
            val_rule="matchAtleastOne, Patient.PatientID, set",
            column_names=cross_val_col_names,
            manifest_col=Series(["A", "B", "C", "E"]),
            target_attribute="patientid",
            target_manifest=cross_val_df1,
            target_manifest_id="syn2",
            missing_manifest_log={"syn1": Series(["D"])},
            present_manifest_log=[],
            repeat_manifest_log={},
            target_attribute_in_manifest_list=[],
            target_manifest_empty=[],
        )
        assert list(output[0].keys()) == ["syn1", "syn2"]
        assert output[0]["syn1"].to_list() == ["D"]
        assert output[0]["syn2"].to_list() == ["E"]
        assert output[1] == []
        assert output[2] == {}
        assert bool_list1 == [True]
        assert bool_list2 == [False]

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

    def test__gather_set_warnings_errors_match_atleast_one_passes(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchAtLeastOne"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, [], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, ["syn1"], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, ["syn1", "syn2"], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchAtLeastOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=(
                {"syn1": Series(["A"])},
                ["syn1"],
                {"syn2": Series(["B"])},
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

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

    def test__gather_set_warnings_errors_match_exactly_one_passes(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchExactlyOne"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchExactlyOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, [], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchExactlyOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, ["syn1"], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchExactlyOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=(
                {"syn1": Series(["A"])},
                ["syn1"],
                {"syn2": Series(["B"])},
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

    def test__gather_set_warnings_errors_match_exactly_one_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchExactlyOne"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchExactlyOne Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, ["syn1", "syn2"], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert not errors[0][0]
        assert errors[0][1] == "PatientID"
        assert errors[0][2] == (
            "All values from attribute PatientID in the source manifest are present in 2 "
            "manifests instead of only 1. Manifests ['syn1', 'syn2'] match the values in "
            "the source attribute."
        )
        assert not errors[0][3]

    def test__gather_set_warnings_errors_match_none_passes(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchNone"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchNone Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, [], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchNone Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({"syn1": Series(["A"])}, ["syn1"], {}),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

    def test__gather_set_warnings_errors_match_none_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchNone"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchNone Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=({}, [], {"syn1": Series(["A"])}),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0][0] == ["2"]
        assert errors[0][1] == "PatientID"
        assert errors[0][2] == (
            "Value(s) ['A'] from row(s) ['2'] for the attribute PatientID "
            "in the source manifest are not unique. "
            "Manifest(s) ['syn1'] contain duplicate values."
        )
        assert errors[0][3] == ["A"]

        errors, warnings = va_obj._gather_set_warnings_errors(
            val_rule="matchNone Patient.PatientID set error",
            source_attribute="PatientID",
            set_validation_store=(
                {},
                [],
                {"syn1": Series(["A"]), "syn2": Series(["B"])},
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 1
        assert errors[0][0] == ["2"]
        assert errors[0][1] == "PatientID"
        possible_errors = [
            (
                "Value(s) ['A', 'B'] from row(s) ['2'] for the attribute PatientID in the source "
                "manifest are not unique. Manifest(s) ['syn1', 'syn2'] contain duplicate values."
            ),
            (
                "Value(s) ['B', 'A'] from row(s) ['2'] for the attribute PatientID in the source "
                "manifest are not unique. Manifest(s) ['syn1', 'syn2'] contain duplicate values."
            ),
        ]
        assert errors[0][2] in possible_errors
        possible_missing_values = [["A", "B"], ["B", "A"]]
        assert errors[0][3] in possible_missing_values

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
