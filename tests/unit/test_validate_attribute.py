"""Unit testing for the ValidateAttribute class"""

from typing import Generator
from unittest.mock import patch

import pytest
from pandas import Series, DataFrame, concat

from schematic.models.validate_attribute import (
    ValidateAttribute,
    SetValidationOutput,
    ValueValidationOutput,
    ParsedCrossValidationRule,
)
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

    def test__parse_cross_validation_rule_string(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for alidateAttribute._parse_cross_validation_rule_string"""
        assert va_obj._parse_cross_validation_rule_string(
            "matchAtLeastOne Patient.PatientID value error"
        ) == ParsedCrossValidationRule(
            "matchAtLeastOne", "Patient.PatientID", "value", "error"
        )

        assert va_obj._parse_cross_validation_rule_string(
            "matchAtLeastOne Patient.PatientID value"
        ) == ParsedCrossValidationRule(
            "matchAtLeastOne", "Patient.PatientID", "value", "warning"
        )

    def test__parse_cross_validation_rule_string_exceptions(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for alidateAttribute._parse_cross_validation_rule_string"""

        with pytest.raises(
            ValueError, match="Rule string must have 3 to 4 parts separated by spaces"
        ):
            va_obj._parse_cross_validation_rule_string("matchAtLeastOne")

        with pytest.raises(
            ValueError, match="The first section of the rule must be a valid rule type"
        ):
            va_obj._parse_cross_validation_rule_string(
                "xxx Patient.PatientID value error"
            )

        with pytest.raises(
            ValueError, match="The third section of the rule must be a valid scope type"
        ):
            va_obj._parse_cross_validation_rule_string(
                "matchAtLeastOne Patient.PatientID xxx error"
            )

        with pytest.raises(
            ValueError,
            match="The 4th section of the rule must be a valid message level type",
        ):
            va_obj._parse_cross_validation_rule_string(
                "matchAtLeastOne Patient.PatientID value xxx"
            )

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
                validation_target="comp.att",
                manifest_col=Series([]),
                target_column=Series([]),
            )
            assert result is False

        # This shows that when the only target manifest is empty False is returned
        with patch.object(
            schematic.models.validate_attribute.ValidateAttribute,
            "_get_target_manifest_dataframes",
            return_value={"syn1": cross_val_df3},
        ):
            _, result = va_obj._run_validation_across_target_manifests(
                project_scope=None,
                rule_scope="value",
                access_token="xxx",
                validation_target="comp.att",
                manifest_col=Series([]),
                target_column=Series([]),
            )

            assert result is False

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
                validation_target="Patient.PatientID",
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
                validation_target="Patient.PatientID",
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
                validation_target="Patient.PatientID",
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
                validation_target="Patient.PatientID",
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
                validation_target="Patient.PatientID",
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
        errors, warnings = va_obj._gather_set_warnings_errors(
            rule="matchAtLeastOne",
            scope_type="set",
            msg_level="error",
            source_attribute="PatientID",
            validation_output=SetValidationOutput(
                target_manifests=["syn1"], matching_manifests=["syn1"]
            ),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

        errors, warnings = va_obj._gather_set_warnings_errors(
            rule="matchAtLeastOne",
            scope_type="set",
            msg_level="error",
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
        errors, warnings = va_obj._gather_set_warnings_errors(
            rule="matchAtLeastOne",
            scope_type="set",
            msg_level="error",
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
            rule="matchAtLeastOne",
            scope_type="set",
            msg_level="error",
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
        errors, warnings = va_obj._gather_set_warnings_errors(
            rule="matchExactlyOne",
            scope_type="set",
            msg_level="error",
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
        errors, warnings = va_obj._gather_set_warnings_errors(
            rule="matchExactlyOne",
            scope_type="set",
            msg_level="error",
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
            rule="matchExactlyOne",
            scope_type="set",
            msg_level="error",
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
            rule="matchExactlyOne",
            scope_type="set",
            msg_level="error",
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
        errors, warnings = va_obj._gather_set_warnings_errors(
            rule="matchNone",
            scope_type="set",
            msg_level="error",
            source_attribute="PatientID",
            validation_output=SetValidationOutput(target_manifests=["syn1"]),
        )
        assert len(warnings) == 0
        assert len(errors) == 0

    def test__gather_set_warnings_errors_match_none_errors(
        self, va_obj: ValidateAttribute
    ) -> None:
        """Tests for ValidateAttribute._gather_set_warnings_errors for matchNone"""

        errors, warnings = va_obj._gather_set_warnings_errors(
            rule="matchNone",
            scope_type="set",
            msg_level="error",
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
