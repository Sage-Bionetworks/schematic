"""Unit testing for the ValidateAttribute class"""

from typing import Generator, Any, Tuple, Iterable
import pytest

from networkx import MultiDiGraph  # type: ignore

from schematic.schemas.data_model_validator import (
    DataModelValidator,
    get_node_labels_from,
    get_missing_fields_from,
    check_characters_in_node_display_name,
    match_node_names_with_reserved_names,
    create_reserve_name_error_messages,
    create_blacklisted_characters_error_message,
    create_missing_fields_error_messages
)
from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph

# pylint: disable=protected-access


@pytest.fixture(name="test_dmv")
def fixture_test_dmv() -> Generator[DataModelValidator, None, None]:
    """Yield a DataModelValidator object using test data model"""
    path_to_data_model = "tests/data/validator_test.model.csv"
    data_model_parser = DataModelParser(path_to_data_model=path_to_data_model)
    parsed_data_model = data_model_parser.parse_model()

    # Convert parsed model to graph
    data_model_grapher = DataModelGraph(parsed_data_model)

    # Generate graph
    graph_data_model = data_model_grapher.graph

    yield DataModelValidator(graph_data_model)


@pytest.fixture(name="empty_dmv")
def fixture_empty_dmv() -> Generator[DataModelValidator, None, None]:
    """Yield an empty DataModelValidator object"""
    yield DataModelValidator(MultiDiGraph())


class TestDataModelValidatorHelpers:
    """Testing for DataModelValidator helper functions"""

    @pytest.mark.parametrize(
        "input_dict, expected_list",
        [
            # These return empty lists
            ({}, []),
            ({"x": "y"}, []),
            ({"x": {}}, []),
            # Only values that are dicts are parsed.
            # Any dicts with keys named node_label, the value is collected
            ({"x": {"node_label": "A"}}, ["A"]),
            ({"x": {"node_label": "A"}, "y": {"node_label": "B"}}, ["A", "B"]),
        ],
    )
    def test_get_node_labels_from(self, input_dict: dict, expected_list: list) -> None:
        """Tests for get_node_labels_from"""
        assert get_node_labels_from(input_dict) == expected_list

    @pytest.mark.parametrize(
        "input_nodes, input_fields, expected_list",
        [
            # If there are no nodes or no required fields, nothing will be returned
            ([], [], []),
            ([], ["field1"], []),
            ([("node1", {"field1": "x"})], [], []),
            # For each node, if it has all required fields, nothing will be returned
            ([("node1", {"field1": "x"})], ["field1"], []),
            (
                [("node1", {"field1": "x"}), ("node2", {"field1": "x", "field2": "y"})],
                ["field1"],
                [],
            ),
            # For each node, if it is missing a required field, it is returned
            ([("node1", {"field2": "x"})], ["field1"], [("node1", "field1")]),
            (
                [("node1", {"field2": "x"}), ("node2", {"field1": "x"})],
                ["field1"],
                [("node1", "field1")],
            ),
            # For each node, if it is missing a required field, it is returned
            (
                [("node1", {})],
                ["field1", "field2"],
                [("node1", "field1"), ("node1", "field2")],
            ),
            ([("node1", {"field1": "x"})], ["field1", "field2"], [("node1", "field2")]),
        ],
    )
    def test_get_missing_fields_from(
        self,
        input_nodes: list[Tuple[Any, dict]],
        input_fields: list,
        expected_list: list[Tuple[Any, Any]],
    ) -> None:
        """Tests for get_missing_fields_from"""
        assert get_missing_fields_from(input_nodes, input_fields) == expected_list

    @pytest.mark.parametrize(
        "input_tuples, expected_msgs",
        [
            # If there are either no nodes, or no reserved names, nothing is returned
            ([], []),

            (
                [("node1", "field1")],
                ["For entry: node1, the required field field1 is missing in the data model graph, please double check your model and generate the graph again."],
            ),
            (
                [("node1", "field1"), ("node1", "field2")],
                [
                    "For entry: node1, the required field field1 is missing in the data model graph, please double check your model and generate the graph again.",
                    "For entry: node1, the required field field2 is missing in the data model graph, please double check your model and generate the graph again."
                ],
            ),
        ],
    )
    def test_create_missing_fields_error_messages(
        self, input_tuples: list[Tuple[str, str]], expected_msgs: list[str]
    ) -> None:
        """Tests for create_missing_fields_error_messages"""
        assert create_missing_fields_error_messages(input_tuples) == expected_msgs



    @pytest.mark.parametrize(
        "input_nodes, input_chars",
        [
            # If there are no nodes or blacklisted characters, nothing will be returned
            ([], []),
            # If all nodes have are formatted correctly, and the 'displayName' field has
            # no black listed characters, nothing will be returned
            ([("node1", {"displayName": "x"})], []),
            ([("node1", {"displayName": "x"})], ["y"]),
        ],
    )
    def test_check_characters_in_node_display_name_no_output(
        self,
        input_nodes: list[Tuple[Any, dict]],
        input_chars: list[str],
    ) -> None:
        """Tests for check_characters_in_node_display_name"""
        assert not check_characters_in_node_display_name(input_nodes, input_chars)

    @pytest.mark.parametrize(
        "input_nodes, input_chars",
        [
            # If all nodes have are formatted correctly, and the 'displayName' field has
            # black listed characters, those will be returned
            ([("node1", {"displayName": "xyz"})], ["x"]),
            ([("node1", {"displayName": "xyz"})], ["x", "y"]),
            ([("node1", {"displayName": "xyz"})], ["x", "y", "a"]),
        ],
    )
    def test_check_characters_in_node_display_name_with_output(
        self,
        input_nodes: list[Tuple[Any, dict]],
        input_chars: list[str],
    ) -> None:
        """Tests for check_characters_in_node_display_name"""
        assert check_characters_in_node_display_name(input_nodes, input_chars)

    @pytest.mark.parametrize(
        "input_chars, input_name, expected_msg",
        [
            (
                [],
                "",
                "Node:  contains a blacklisted character(s): , they will be striped if used in Synapse annotations.",
            ),
            (
                ["x", "y"],
                "node1",
                "Node: node1 contains a blacklisted character(s): x,y, they will be striped if used in Synapse annotations.",
            ),
        ],
    )
    def test_create_blacklisted_characters_error_msg(
        self, input_chars: list[str], input_name: str, expected_msg: str
    ) -> None:
        """Tests for create_blacklisted_characters_error_msg"""
        assert (
            create_blacklisted_characters_error_message(input_chars, input_name)
            == expected_msg
        )

    @pytest.mark.parametrize(
        "input_nodes, input_chars, exception, msg",
        [
            # If any nodes do not have the 'displayName' field, or is 'None'or 'True'
            #  a ValueError is raised
            (
                [("node1", {"field1": "x"})],
                [],
                ValueError,
                "Node: node1 missing displayName field",
            ),
            (
                [("node1", {"displayName": "x"}), ("node2", {"field1": "x"})],
                [],
                ValueError,
                "Node: node2 missing displayName field",
            ),
        ],
    )
    def test_check_characters_in_node_display_name_exceptions(
        self,
        input_nodes: list[Tuple[Any, dict]],
        input_chars: list[str],
        exception: Exception,
        msg: str,
    ) -> None:
        """Tests for check_characters_in_node_display_name"""
        with pytest.raises(exception, match=msg):
            check_characters_in_node_display_name(input_nodes, input_chars)

    @pytest.mark.parametrize(
        "input_nodes, input_names, expected_list",
        [
            # If there are either no nodes, or no reserved names, nothing is returned
            ([], [], []),
            (["node1"], [], []),
            ([], ["x"], []),
            # If there are matches between a node name and a reserved name (after lowering
            #  the case of both) return any matches
            (["node1"], ["node1"], [("node1", "node1")]),
            (["Node1"], ["node1"], [("node1", "Node1")]),
            (["node1"], ["Node1"], [("Node1", "node1")]),
        ],
    )
    def test_match_node_names_with_reserved_names(
        self,
        input_nodes: Iterable[str],
        input_names: Iterable[str],
        expected_list: list[Tuple[str, str]],
    ) -> None:
        """Tests for match_node_names_with_reserved_names"""
        assert (
            match_node_names_with_reserved_names(input_nodes, input_names)
            == expected_list
        )

    @pytest.mark.parametrize(
        "input_tuples, expected_msgs",
        [
            # If there are either no nodes, or no reserved names, nothing is returned
            ([], []),
            (
                [("node1", "Node1")],
                [
                    "Your data model entry name: Node1 overlaps with the reserved name: node1. Please change this name in your data model."
                ],
            ),
            (
                [("node1", "Node1"), ("node2", "Node2")],
                [
                    "Your data model entry name: Node1 overlaps with the reserved name: node1. Please change this name in your data model.",
                    "Your data model entry name: Node2 overlaps with the reserved name: node2. Please change this name in your data model.",
                ],
            ),
        ],
    )
    def test_create_reserve_name_error_msgs(
        self, input_tuples: list[Tuple[str, str]], expected_msgs: list[str]
    ) -> None:
        """Tests for create_reserve_name_error_msgs"""
        assert create_reserve_name_error_messages(input_tuples) == expected_msgs


class TestDataModelValidator:
    """Testing for DataModelValidator class"""

    def test_run_checks(
        self, test_dmv: DataModelValidator, empty_dmv: DataModelValidator
    ) -> None:
        """Tests for DataModelValidator.run_checks"""
        errors, warnings = test_dmv.run_checks()
        assert errors
        assert warnings
        errors, warnings = empty_dmv.run_checks()
        assert not errors
        assert not warnings

    def test__run_cycles(
        self, test_dmv: DataModelValidator, empty_dmv: DataModelValidator
    ) -> None:
        """Tests for DataModelValidator._run_cycles"""
        test_dmv._run_cycles()
        empty_dmv._run_cycles()

    def test__check_is_dag(
        self, test_dmv: DataModelValidator, empty_dmv: DataModelValidator
    ) -> None:
        """Tests for DataModelValidator._check_is_dag"""
        errors = test_dmv._check_is_dag()
        assert not errors
        errors = empty_dmv._check_is_dag()
        assert not errors

    def test__check_graph_has_required_node_fields(
        self, test_dmv: DataModelValidator, empty_dmv: DataModelValidator
    ) -> None:
        """Tests for DataModelValidator._check_graph_has_required_node_fields"""
        errors = test_dmv._check_graph_has_required_node_fields()
        assert not errors
        errors = empty_dmv._check_graph_has_required_node_fields()
        assert not errors

    def test__check_blacklisted_characters(
        self, test_dmv: DataModelValidator, empty_dmv: DataModelValidator
    ) -> None:
        """Tests for DataModelValidator._check_blacklisted_characters"""
        errors = test_dmv._check_blacklisted_characters()
        assert errors
        errors = empty_dmv._check_blacklisted_characters()
        assert not errors

    def test__check_reserved_names(
        self, test_dmv: DataModelValidator, empty_dmv: DataModelValidator
    ) -> None:
        """Tests for DataModelValidator._check_reserved_names"""
        errors = test_dmv._check_reserved_names()
        assert errors
        errors = empty_dmv._check_reserved_names()
        assert not errors
