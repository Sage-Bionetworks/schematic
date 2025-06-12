"""
This contains unit test for the crate_json_schema function, and its helper classes and functions.
The helper classes tested are JSONSchema, Node, GraphTraversalState,
"""

from typing import Any, Optional
import os
import json
import uuid
from shutil import rmtree

import pytest
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from schematic.schemas.data_model_graph import DataModelGraphExplorer
from schematic.schemas.create_json_schema import (
    ValidationRule,
    _get_validation_rule_based_fields,
    _get_range_from_in_range_rule,
    _get_pattern_from_regex_rule,
    _get_type_rule_from_rule_list,
    _get_rule_from_rule_list,
    JSONSchema,
    Node,
    GraphTraversalState,
    create_json_schema,
    _write_data_model,
    _set_conditional_dependencies,
    _set_property,
    _create_enum_array_property,
    _create_array_property,
    _create_enum_property,
    _create_simple_property,
    _set_type_specific_keywords,
)
from tests.utils import json_files_equal

# pylint: disable=protected-access
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments


@pytest.fixture(name="test_directory", scope="session")
def fixture_test_directory(request) -> str:
    """Returns a directory for creating test jSON Schemas in"""
    test_folder = f"tests/data/create_json_schema_{str(uuid.uuid4())}"

    def delete_folder():
        rmtree(test_folder)

    request.addfinalizer(delete_folder)
    os.makedirs(test_folder, exist_ok=True)
    return test_folder


@pytest.fixture(name="test_nodes")
def fixture_test_nodes(
    dmge: DataModelGraphExplorer,
) -> dict[str, Node]:
    """Yields dict of Nodes"""
    nodes = [
        "NoRules",
        "NoRulesNotRequired",
        "String",
        "StringNotRequired",
        "Enum",
        "EnumNotRequired",
        "Range",
        "Regex",
        "List",
        "ListNotRequired",
        "ListEnum",
        "ListEnumNotRequired",
        "ListString",
        "ListInRange",
    ]
    nodes = {node: Node(node, "JSONSchemaComponent", dmge) for node in nodes}
    return nodes


class TestJSONSchema:
    """Tests for JSONSchema"""

    def test_init(self) -> None:
        """Test the JSONSchema.init method"""
        schema = JSONSchema()
        assert schema.schema_id == ""
        assert schema.title == ""
        assert schema.schema == "http://json-schema.org/draft-07/schema#"
        assert schema.type == "object"
        assert schema.description == "TBD"
        assert not schema.properties
        assert not schema.required
        assert not schema.all_of

    def test_as_json_schema_dict(self) -> None:
        """Test the JSONSchema.as_json_schema_dict method"""
        schema = JSONSchema()
        assert schema.as_json_schema_dict() == {
            "$id": "",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "TBD",
            "properties": {},
            "required": [],
            "title": "",
            "type": "object",
        }

    def test_add_required_property(self) -> None:
        """Test the JSONSchema.add_required_property method"""
        # GIVEN a JSONSchema instance
        schema = JSONSchema()
        # WHEN adding a required property
        schema.add_required_property("name1")
        # THEN that property should be retrievable
        assert schema.required == ["name1"]
        # WHEN adding a second required property
        schema.add_required_property("name2")
        # THEN both properties should be retrievable
        assert schema.required == ["name1", "name2"]

    def test_add_to_all_of_list(self) -> None:
        """Test the JSONSchema.add_to_all_of_list method"""
        # GIVEN a JSONSchema instance
        schema = JSONSchema()
        # WHEN adding a dict to the all of list
        schema.add_to_all_of_list({"if": {}, "then": {}})
        # THEN that dict should be retrievable
        assert schema.all_of == [{"if": {}, "then": {}}]
        # WHEN adding a second dict
        schema.add_to_all_of_list({"if2": {}, "then2": {}})
        # THEN both dicts should be retrievable
        assert schema.all_of == [{"if": {}, "then": {}}, {"if2": {}, "then2": {}}]

    def test_update_property(self) -> None:
        """Test the JSONSchema.update_property method"""
        # GIVEN a JSONSchema instance
        schema = JSONSchema()
        # WHEN updating the properties dict
        schema.update_property({"name1": "property1"})
        # THEN that dict should be retrievable
        assert schema.properties == {"name1": "property1"}
        # WHEN updating the properties dict with a new key
        schema.update_property({"name2": "property2"})
        # THEN the new key and old key should be retrievable
        assert schema.properties == {"name1": "property1", "name2": "property2"}


@pytest.mark.parametrize(
    "node_name, expected_type, expected_is_array, expected_min, expected_max, expected_pattern",
    [
        # If there are no type validation rules the type is None
        ("NoRules", None, False, None, None, None),
        # If there is one type validation rule the type is set to the
        #  JSON Schema equivalent of the validation rule
        ("String", "string", False, None, None, None),
        # If there are any list type validation rules then is_array is set to True
        ("List", None, True, None, None, None),
        # If there are any list type validation rules and one type validation rule
        #  then is_array is set to True, and the type is set to the
        #  JSON Schema equivalent of the validation rule
        ("ListString", "string", True, None, None, None),
        # If there is an inRange rule the min and max will be set
        ("Range", "number", False, 50, 100, None),
        # If there is a regex rule, then the pattern should be set
        ("Regex", "string", False, None, None, "[a-f]"),
    ],
    ids=["None", "String", "List", "ListString", "Range", "Regex"],
)
def test_node_init(
    node_name: str,
    expected_type: Optional[str],
    expected_is_array: bool,
    expected_min: Optional[float],
    expected_max: Optional[float],
    expected_pattern: Optional[str],
    test_nodes: dict[str, Node],
) -> None:
    """Tests for Node class"""
    node = test_nodes[node_name]
    assert node.type == expected_type
    assert node.is_array == expected_is_array
    assert node.minimum == expected_min
    assert node.maximum == expected_max
    assert node.pattern == expected_pattern


@pytest.mark.parametrize(
    "validation_rules, expected_type, expected_is_array, expected_min, expected_max, expected_pattern",
    [
        # If there are no type validation rules the type is None
        ([], None, False, None, None, None),
        # If there is one type validation rule the type is set to the
        #  JSON Schema equivalent of the validation rule
        (["str"], "string", False, None, None, None),
        # If there are any list type validation rules then is_array is set to True
        (["list"], None, True, None, None, None),
        # If there are any list type validation rules and one type validation rule
        #  then is_array is set to True, and the type is set to the
        #  JSON Schema equivalent of the validation rule
        (["list", "str"], "string", True, None, None, None),
        # If there is an inRange rule the min and max will be set
        (["inRange 50 100"], "number", False, 50, 100, None),
        # If there is a regex rule, then the pattern should be set
        (["regex search [a-f]"], "string", False, None, None, "[a-f]"),
    ],
    ids=["No rules", "String", "List", "ListString", "InRange", "Regex"],
)
def test_get_validation_rule_based_fields(
    validation_rules: list[str],
    expected_type: Optional[str],
    expected_is_array: bool,
    expected_min: Optional[float],
    expected_max: Optional[float],
    expected_pattern: Optional[str],
) -> None:
    """Tests for _get_validation_rule_based_fields"""
    (
        prop_type,
        is_array,
        minimum,
        maximum,
        pattern,
    ) = _get_validation_rule_based_fields(validation_rules)
    assert prop_type == expected_type
    assert is_array == expected_is_array
    assert minimum == expected_min
    assert maximum == expected_max
    assert pattern == expected_pattern


@pytest.mark.parametrize(
    "input_rule, expected_tuple",
    [
        ("", (None, None)),
        ("inRange", (None, None)),
        ("inRange x x", (None, None)),
        ("inRange 0", (0, None)),
        ("inRange 0 x", (0, None)),
        ("inRange 0 1", (0, 1)),
        ("inRange 0 1 x", (0, 1)),
    ],
    ids=[
        "No rules",
        "inRange with no params",
        "inRange with bad params",
        "inRange with minimum",
        "inRange with minimum, bad maximum",
        "inRange with minimum, maximum",
        "inRange with minimum, maximum, extra param",
    ],
)
def test_get_range_from_in_range_rule(
    input_rule: str,
    expected_tuple: tuple[Optional[str], Optional[str]],
) -> None:
    """Test for _get_range_from_in_range_rule"""
    result = _get_range_from_in_range_rule(input_rule)
    assert result == expected_tuple


@pytest.mark.parametrize(
    "input_rule, expected_pattern",
    [
        ("", None),
        ("regex search [a-f]", "[a-f]"),
        ("regex match [a-f]", "^[a-f]"),
        ("regex match ^[a-f]", "^[a-f]"),
        ("regex split ^[a-f]", None),
    ],
    ids=[
        "No rules, None returned",
        "Search module, Pattern returned",
        "Match module, Pattern returned with carrot added",
        "Match module, Pattern returned with no carrot added",
        "Unallowed module, None returned",
    ],
)
def test_get_pattern_from_regex_rule(
    input_rule: str,
    expected_pattern: Optional[str],
) -> None:
    """Test for _get_pattern_from_regex_rule"""
    result = _get_pattern_from_regex_rule(input_rule)
    assert result == expected_pattern


@pytest.mark.parametrize(
    "rule, input_rules, expected_rule",
    [
        (ValidationRule.IN_RANGE, [], None),
        (ValidationRule.IN_RANGE, ["regex match [a-f]"], None),
        (ValidationRule.IN_RANGE, ["inRange 0 1"], "inRange 0 1"),
        (ValidationRule.IN_RANGE, ["str error", "inRange 0 1"], "inRange 0 1"),
        (ValidationRule.REGEX, ["inRange 0 1"], None),
        (ValidationRule.REGEX, ["regex match [a-f]"], "regex match [a-f]"),
    ],
    ids=[
        "inRange: No rules",
        "inRange: No inRange rules",
        "inRange: Rule present",
        "inRange: Rule present, multiple rules",
        "regex: No regex rules",
        "regex: Rule present",
    ],
)
def test_get_rule_from_rule_list(
    rule: ValidationRule,
    input_rules: list[str],
    expected_rule: Optional[str],
) -> None:
    """Test for _get_rule_from_rule_list"""
    result = _get_rule_from_rule_list(rule, input_rules)
    assert result == expected_rule


@pytest.mark.parametrize(
    "rule, input_rules",
    [
        (ValidationRule.IN_RANGE, ["inRange", "inRange"]),
        (ValidationRule.IN_RANGE, ["inRange 0", "inRange 0"]),
    ],
    ids=["Multiple inRange rules", "Multiple inRange rules with params"],
)
def test_get_rule_from_rule_list_exceptions(
    rule: ValidationRule,
    input_rules: list[str],
) -> None:
    """Test for __get_rule_from_rule_list with exceptions"""
    with pytest.raises(
        ValueError, match="Found more than one 'inRange' rule in validation rules"
    ):
        _get_rule_from_rule_list(rule, input_rules)


@pytest.mark.parametrize(
    "input_rules, expected_rule",
    [([], None), (["list strict"], None), (["str"], "str"), (["str error"], "str")],
    ids=["No rules", "List", "String", "String with error param"],
)
def test_get_type_rule_from_rule_list(
    input_rules: list[str],
    expected_rule: Optional[str],
) -> None:
    """Test for _get_type_rule_from_rule_list"""
    result = _get_type_rule_from_rule_list(input_rules)
    assert result == expected_rule


@pytest.mark.parametrize(
    "input_rules",
    [(["str", "int"]), (["str", "str", "str"]), (["str", "str error", "str warning"])],
    ids=[
        "Multiple type rules",
        "Repeated str rules",
        "Repeated str rules with parameters",
    ],
)
def test_get_type_rule_from_rule_list_exceptions(
    input_rules: list[str],
) -> None:
    """Test for _get_type_rule_from_rule_list with exceptions"""
    with pytest.raises(
        ValueError, match="Found more than one type rule in validation rules"
    ):
        _get_type_rule_from_rule_list(input_rules)


class TestGraphTraversalState:
    """Tests for GraphTraversalState class"""

    def test_init(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.__init__"""
        # GIVEN a GraphTraversalState instance with 5 nodes
        gts = GraphTraversalState(dmge, "Patient")
        # THEN the current_node, current_node_display_name, and first item in
        #  root dependencies should be "Component"
        assert gts.current_node.name == "Component"
        assert gts._root_dependencies[0] == "Component"
        assert gts.current_node.display_name == "Component"
        # THEN
        #  - root_dependencies should be 5 items long
        #  - nodes to process should be the same minus "Component"
        #  - _processed_nodes, _reverse_dependencies, and _valid_values_map should be empty
        assert gts._root_dependencies == [
            "Component",
            "Diagnosis",
            "PatientID",
            "Sex",
            "YearofBirth",
        ]
        assert gts._nodes_to_process == ["Diagnosis", "PatientID", "Sex", "YearofBirth"]
        assert not gts._processed_nodes
        assert not gts._reverse_dependencies
        assert not gts._valid_values_map

    def test_move_to_next_node(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.move_to_next_node"""
        # GIVEN a GraphTraversalState instance with 2 nodes
        gts = GraphTraversalState(dmge, "Patient")
        gts._nodes_to_process = ["YearofBirth"]
        # THEN the current_node should be "Component" and node to process has 1 node
        assert gts.current_node.name == "Component"
        assert gts.current_node.display_name == "Component"
        assert gts._nodes_to_process == ["YearofBirth"]
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current_node should now be YearofBirth and no nodes to process
        assert gts.current_node.name == "YearofBirth"
        assert gts.current_node.display_name == "Year of Birth"
        assert not gts._nodes_to_process

    def test_are_nodes_remaining(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.are_nodes_remaining"""
        # GIVEN a GraphTraversalState instance with 1 node
        gts = GraphTraversalState(dmge, "Patient")
        gts._nodes_to_process = []
        # THEN there should be nodes_remaining
        assert gts.are_nodes_remaining()
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN there should not be nodes_remaining
        assert not gts.are_nodes_remaining()

    def test_is_current_node_processed(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.is_current_node_processed"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient")
        # THEN the current node should not have been processed yet.
        assert not gts.is_current_node_processed()
        # WHEN adding a the current node to the processed list
        gts.update_processed_nodes_with_current_node()
        # THEN the current node should be listed as processed.
        assert gts.is_current_node_processed()

    def test_is_current_node_a_property(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.is_current_node_a_property"""
        # GIVEN a GraphTraversalState instance where the first node is Component and second is Male
        gts = GraphTraversalState(dmge, "Patient")
        gts._nodes_to_process = ["Male"]
        # THEN the current node should be a property
        assert gts.is_current_node_a_property()
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current node should not be a property, as the Male node is a valid value
        assert not gts.is_current_node_a_property()

    def test_is_current_node_in_reverse_dependencies(
        self, dmge: DataModelGraphExplorer
    ) -> None:
        """Test GraphTraversalState.is_current_node_in_reverse_dependencies"""
        # GIVEN a GraphTraversalState instance where
        # - the first node is Component
        # - the second node is FamilyHistory
        # - FamilyHistory has a reverse dependency of Cancer
        gts = GraphTraversalState(dmge, "Patient")
        gts._nodes_to_process = ["FamilyHistory"]
        gts._reverse_dependencies = {"FamilyHistory": ["Cancer"]}
        # THEN the current should not have reverse dependencies
        assert not gts.is_current_node_in_reverse_dependencies()
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current node should have reverse dependencies
        assert gts.is_current_node_in_reverse_dependencies()

    def test_update_processed_nodes_with_current_node(
        self, dmge: DataModelGraphExplorer
    ) -> None:
        """Test GraphTraversalState.update_processed_nodes_with_current_node"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient")
        # WHEN the node has been processed
        gts.update_processed_nodes_with_current_node()
        # THEN the node should be listed as processed
        assert gts._processed_nodes == ["Component"]

    def test_get_conditional_properties(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.get_conditional_properties"""
        # GIVEN a GraphTraversalState instance where
        # - the first node is Component
        # - the second node is FamilyHistory
        # - FamilyHistory has a reverse dependency of Cancer
        # - Cancer is a valid value of Diagnosis
        gts = GraphTraversalState(dmge, "Patient")
        gts._nodes_to_process = ["FamilyHistory"]
        gts._reverse_dependencies = {"FamilyHistory": ["Cancer"]}
        gts._valid_values_map = {"Cancer": ["Diagnosis"]}
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current node should have conditional properties
        assert gts.get_conditional_properties() == [("Diagnosis", "Cancer")]

    def test_update_valid_values_map(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState._update_valid_values_map"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient")
        # THEN the valid_values_map should be empty to start with
        assert not gts._valid_values_map
        # WHEN the map is updated with one node and two values
        gts._update_valid_values_map("Diagnosis", ["Healthy", "Cancer"])
        # THEN valid values map should have one entry for each valid value,
        #  with the node as the value
        assert gts._valid_values_map == {
            "Healthy": ["Diagnosis"],
            "Cancer": ["Diagnosis"],
        }

    def test_update_reverse_dependencies(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState._update_reverse_dependencies"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient")
        # THEN the reverse_dependencies should be empty to start with
        assert not gts._reverse_dependencies
        # WHEN the map is updated with one node and two reverse_dependencies
        gts._update_reverse_dependencies("Cancer", ["CancerType", "FamilyHistory"])
        # THEN reverse_dependencies should have one entry for each valid value,
        #  with the node as the value
        assert gts._reverse_dependencies == {
            "CancerType": ["Cancer"],
            "FamilyHistory": ["Cancer"],
        }

    def test_update_nodes_to_process(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState._update_nodes_to_process"""
        # GIVEN a GraphTraversalState instance with 5 nodes
        gts = GraphTraversalState(dmge, "Patient")
        # THEN the GraphTraversalState should have 4 nodes in nodes_to_process
        assert len(gts._nodes_to_process) == 4
        # WHEN adding a node to nodes_to_process
        gts._update_nodes_to_process(["NewNode"])
        # THEN that node should be in nodes_to_process as the last item
        assert len(gts._nodes_to_process) == 5
        assert gts._nodes_to_process[4] == "NewNode"


@pytest.mark.parametrize(
    "datatype",
    [
        ("Biospecimen"),
        ("BulkRNA-seqAssay"),
        ("JSONSchemaComponent"),
        ("MockComponent"),
        ("MockFilename"),
        ("MockRDB"),
        ("Patient"),
    ],
    ids=[
        "Biospecimen",
        "BulkRNA-seqAssay",
        "JSONSchemaComponent",
        "MockComponent",
        "MockFilename",
        "MockRDB",
        "Patient",
    ],
)
def test_create_json_schema_with_class_label(
    dmge: DataModelGraphExplorer, datatype: str, test_directory: str
) -> None:
    """Tests for JSONSchemaGenerator.create_json_schema"""
    test_file = f"test.{datatype}.schema.json"
    test_path = os.path.join(test_directory, test_file)
    expected_path = f"tests/data/expected_jsonschemas/expected.{datatype}.schema.json"
    create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=f"{datatype}_validation",
        schema_path=test_path,
        use_property_display_names=False,
    )
    assert json_files_equal(expected_path, test_path)


@pytest.mark.parametrize(
    "datatype",
    [
        ("BulkRNA-seqAssay"),
        ("Patient"),
    ],
    ids=["BulkRNA-seqAssay", "Patient"],
)
def test_create_json_schema_with_display_names(
    dmge: DataModelGraphExplorer, datatype: str, test_directory: str
) -> None:
    """Tests for JSONSchemaGenerator.create_json_schema"""
    test_file = f"test.{datatype}.display_names_schema.json"
    test_path = os.path.join(test_directory, test_file)
    expected_path = (
        f"tests/data/expected_jsonschemas/expected.{datatype}.display_names_schema.json"
    )
    create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=f"{datatype}_validation",
        schema_path=test_path,
    )
    assert json_files_equal(expected_path, test_path)


@pytest.mark.parametrize(
    "datatype",
    [
        ("BulkRNA-seqAssay"),
        ("Patient"),
    ],
)
def test_create_json_schema_with_display_names(
    dmge: DataModelGraphExplorer, datatype: str, test_directory: str
) -> None:
    """Tests for JSONSchemaGenerator.create_json_schema"""
    test_file = f"test.{datatype}.display_names_schema.json"
    test_path = os.path.join(test_directory, test_file)
    expected_path = (
        f"tests/data/expected_jsonschemas/expected.{datatype}.display_names_schema.json"
    )
    create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=f"{datatype}_validation",
        schema_path=test_path,
    )
    assert json_files_equal(expected_path, test_path)


@pytest.mark.parametrize(
    "instance_path, datatype",
    [
        ("tests/data/json_instances/valid_biospecimen1.json", "Biospecimen"),
        ("tests/data/json_instances/valid_bulk_rna1.json", "BulkRNA-seqAssay"),
        ("tests/data/json_instances/valid_bulk_rna2.json", "BulkRNA-seqAssay"),
        ("tests/data/json_instances/valid_patient1.json", "Patient"),
        ("tests/data/json_instances/valid_patient2.json", "Patient"),
    ],
    ids=[
        "Biospecimen",
        "BulkRNASeqAssay, FileFormat is BAM",
        "BulkRNASeqAssay, FileFormat is CRAM",
        "Patient, Diagnosis is Healthy",
        "Patient, Diagnosis is Cancer",
    ],
)
def test_validate_valid_instances(
    instance_path: str,
    datatype: str,
) -> None:
    """Validates instances using expected JSON Schemas"""
    schema_path = f"tests/data/expected_jsonschemas/expected.{datatype}.schema.json"
    with open(schema_path, encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    validator = Draft7Validator(schema)
    validator.validate(instance)


@pytest.mark.parametrize(
    "instance_path, datatype",
    [
        (
            "tests/data/json_instances/bulk_rna_missing_conditional_dependencies.json",
            "BulkRNA-seqAssay",
        ),
        (
            "tests/data/json_instances/patient_missing_conditional_dependencies.json",
            "Patient",
        ),
    ],
    ids=[
        "BulkRNA, FileFormat is CRAM, missing conditional dependencies",
        "Patient, Diagnosis is Cancer, missing conditional dependencies",
    ],
)
def test_validate_invalid_instances(
    instance_path: str,
    datatype: str,
) -> None:
    """Raises a ValidationError validating invalid instances using expected JSON Schemas"""
    schema_path = f"tests/data/expected_jsonschemas/expected.{datatype}.schema.json"
    with open(schema_path, encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    validator = Draft7Validator(schema)
    with pytest.raises(ValidationError):
        validator.validate(instance)


def test_write_data_model_with_schema_path(test_directory: str) -> None:
    """Test for _write_data_model with the path provided."""
    schema_path = os.path.join(test_directory, "test_write_data_model1.json")
    _write_data_model(json_schema_dict={}, schema_path=schema_path)
    assert os.path.exists(schema_path)


def test_write_data_model_with_name_and_jsonld_path(test_directory: str) -> None:
    """
    Test for _write_data_model with a name and the data model path used to create it.
    The name of the file should be "<jsonld_path_prefix>.<name>.schema.json"
    """
    json_ld_path = os.path.join(test_directory, "fake_model.jsonld")
    schema_path = os.path.join(
        test_directory, "fake_model.test_write_data_model2.schema.json"
    )
    _write_data_model(
        json_schema_dict={}, name="test_write_data_model2", jsonld_path=json_ld_path
    )
    assert os.path.exists(schema_path)


def test_write_data_model_exception() -> None:
    """
    Test for _write_data_model where neither the path, the name, or JSONLD path are provided.
    This should return a ValueError
    """
    with pytest.raises(ValueError):
        _write_data_model(json_schema_dict={})


@pytest.mark.parametrize(
    "reverse_dependencies, valid_values_map",
    [
        # If the input node has no reverse dependencies, nothing gets added
        ({"CancerType": []}, {}),
        # If the input node has reverse dependencies,
        #  but none of them are in the valid values map, nothing gets added
        ({"CancerType": ["Cancer"]}, {}),
    ],
    ids=[
        "No reverse dependencies",
        "No valid values",
    ],
)
def test_set_conditional_dependencies_nothing_added(
    reverse_dependencies: dict[str, list[str]],
    valid_values_map: dict[str, list[str]],
    dmge: DataModelGraphExplorer,
) -> None:
    """
    Tests for _set_conditional_dependencies
      were the schema doesn't change
    """
    json_schema = {"allOf": []}
    gts = GraphTraversalState(dmge, "Patient")
    gts._reverse_dependencies = reverse_dependencies
    gts._valid_values_map = valid_values_map
    gts.current_node.name = "CancerType"
    gts.current_node.display_name = "Cancer Type"
    _set_conditional_dependencies(
        json_schema=json_schema, graph_state=gts, use_property_display_names=False
    )
    assert json_schema == {"allOf": []}


@pytest.mark.parametrize(
    "reverse_dependencies, valid_values_map, expected_schema",
    [
        (
            {"CancerType": ["Cancer"]},
            {"Cancer": ["Diagnosis"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {"properties": {"Diagnosis": {"enum": ["Cancer"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    }
                ]
            ),
        ),
        (
            {"CancerType": ["Cancer"]},
            {"Cancer": ["Diagnosis1", "Diagnosis2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {"properties": {"Diagnosis1": {"enum": ["Cancer"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                    {
                        "if": {"properties": {"Diagnosis2": {"enum": ["Cancer"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                ]
            ),
        ),
        (
            {"CancerType": ["Cancer1", "Cancer2"]},
            {"Cancer1": ["Diagnosis1"], "Cancer2": ["Diagnosis2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {"properties": {"Diagnosis1": {"enum": ["Cancer1"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                    {
                        "if": {"properties": {"Diagnosis2": {"enum": ["Cancer2"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                ]
            ),
        ),
    ],
    ids=["one rev dep, one enum", "two rev deps, one enum", "two rev deps, two enums"],
)
def test_set_conditional_dependencies(
    reverse_dependencies: dict[str, list[str]],
    valid_values_map: dict[str, list[str]],
    expected_schema: JSONSchema,
    dmge: DataModelGraphExplorer,
) -> None:
    """Tests for _set_conditional_dependencies"""
    json_schema = JSONSchema()
    gts = GraphTraversalState(dmge, "Patient")
    gts._reverse_dependencies = reverse_dependencies
    gts._valid_values_map = valid_values_map
    gts.current_node.name = "CancerType"
    gts.current_node.display_name = "Cancer Type"
    _set_conditional_dependencies(
        json_schema=json_schema, graph_state=gts, use_property_display_names=False
    )
    assert json_schema == expected_schema


@pytest.mark.parametrize(
    "node_name, expected_schema",
    [
        # Array with an enum
        (
            "ListEnum",
            JSONSchema(
                properties={
                    "ListEnum": {
                        "description": "TBD",
                        "title": "List Enum",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["ab", "cd", "ef", "gh"]},
                            },
                        ],
                    }
                },
                required=["ListEnum"],
            ),
        ),
        # Array with an enum, required list should be empty
        (
            "ListEnumNotRequired",
            JSONSchema(
                properties={
                    "ListEnumNotRequired": {
                        "description": "TBD",
                        "title": "List Enum Not Required",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["ab", "cd", "ef", "gh"]},
                            },
                            {"type": "null", "title": "null"},
                        ],
                    }
                },
                required=[],
            ),
        ),
        # Enum, not array
        (
            "Enum",
            JSONSchema(
                properties={
                    "Enum": {
                        "description": "TBD",
                        "title": "Enum",
                        "oneOf": [{"enum": ["ab", "cd", "ef", "gh"], "title": "enum"}],
                    }
                },
                required=["Enum"],
            ),
        ),
        #  Array not enum
        (
            "List",
            JSONSchema(
                properties={
                    "List": {
                        "oneOf": [
                            {"type": "array", "title": "array"},
                        ],
                        "description": "TBD",
                        "title": "List",
                    }
                },
                required=["List"],
            ),
        ),
        # Not array or enum
        (
            "String",
            JSONSchema(
                properties={
                    "String": {
                        "description": "TBD",
                        "type": "string",
                        "title": "String",
                    }
                },
                required=["String"],
            ),
        ),
    ],
    ids=["Array, enum", "Array, enum, not required", "Enum", "Array", "String"],
)
def test_set_property(
    node_name: str, expected_schema: dict[str, Any], test_nodes: dict[str, Node]
) -> None:
    """Tests for set_property"""
    schema = JSONSchema()
    _set_property(schema, test_nodes[node_name], use_property_display_names=False)
    assert schema == expected_schema


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        (
            "ListEnum",
            {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array",
                        "items": {"enum": ["ab", "cd", "ef", "gh"]},
                    }
                ],
            },
            [[], ["ab"]],
            [[None], ["x"], None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            "ListEnumNotRequired",
            {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array",
                        "items": {"enum": ["ab", "cd", "ef", "gh"]},
                    },
                    {"type": "null", "title": "null"},
                ],
            },
            [[], ["ab"], None],
            [[None], ["x"]],
        ),
    ],
    ids=["Required", "Not required"],
)
def test_create_enum_array_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes: dict[str, Node],
) -> None:
    """Test for _create_enum_array_property"""
    schema = _create_enum_array_property(test_nodes[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        (
            "List",
            {"oneOf": [{"type": "array", "title": "array"}]},
            [[], [None], ["x"]],
            ["x", None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            "ListNotRequired",
            {
                "oneOf": [
                    {"type": "array", "title": "array"},
                    {"type": "null", "title": "null"},
                ],
            },
            [None, [], [None], ["x"]],
            ["x"],
        ),
        # If item_type is given, it is set in the schema
        (
            "ListString",
            {
                "oneOf": [
                    {"type": "array", "title": "array", "items": {"type": "string"}}
                ],
            },
            [[], ["x"]],
            [None, [None], [1]],
        ),
        # If property_data has range_min or range_max, they are set in the schema
        (
            "ListInRange",
            {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array",
                        "items": {"type": "number", "minimum": 50.0, "maximum": 100},
                    }
                ],
            },
            [[], [50]],
            [None, [None], [2], ["x"]],
        ),
    ],
    ids=[
        "Required, no item type",
        "Not required, no item type",
        "Required, string item type",
        "Required, integer item type",
    ],
)
def test_create_array_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes: dict[str, Node],
) -> None:
    """Test for _create_array_property"""
    schema = _create_array_property(test_nodes[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        # If is_required is True, no type is added
        (
            "Enum",
            {"oneOf": [{"enum": ["ab", "cd", "ef", "gh"], "title": "enum"}]},
            ["ab"],
            [1, "x", None],
        ),
        # If is_required is False, "null" is added as a type
        (
            "EnumNotRequired",
            {
                "oneOf": [
                    {"enum": ["ab", "cd", "ef", "gh"], "title": "enum"},
                    {"type": "null", "title": "null"},
                ],
            },
            ["ab", None],
            [1, "x"],
        ),
    ],
    ids=["Required", "Not required"],
)
def test_create_enum_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes: dict[str, Node],
) -> None:
    """Test for _create_enum_property"""
    schema = _create_enum_property(test_nodes[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        ("NoRulesNotRequired", {}, [None, 1, ""], []),
        # If property_type is given, it is added to the schema
        (
            "String",
            {"type": "string"},
            [""],
            [1, None],
        ),
        # If property_type is given, and is_required is False,
        # type is set to given property_type and "null"
        (
            "StringNotRequired",
            {
                "oneOf": [
                    {"type": "string", "title": "string"},
                    {"type": "null", "title": "null"},
                ],
            },
            [None, "x"],
            [1],
        ),
        # If is_required is True '"not": {"type":"null"}' is added to schema if
        # property_type is not given
        (
            "NoRules",
            {"not": {"type": "null"}},
            ["x", 1],
            [None],
        ),
        (
            "Range",
            {
                "type": "number",
                "minimum": 50,
                "maximum": 100,
            },
            [50, 75, 100],
            [None, 0, 49, 101],
        ),
    ],
    ids=[
        "Not required, no type",
        "Required, string type",
        "Not required, string type",
        "Required, no type",
        "Required, number type",
    ],
)
def test_create_simple_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes: dict[str, Node],
) -> None:
    """Test for _create_simple_property"""
    schema = _create_simple_property(test_nodes[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema",
    [
        ("NoRules", {}),
        ("Range", {"minimum": 50, "maximum": 100}),
        ("Regex", {"pattern": "[a-f]"}),
    ],
    ids=[
        "NoRules",
        "Range",
        "Regex",
    ],
)
def test_set_type_specific_keywords(
    node_name: str,
    expected_schema: dict[str, Any],
    test_nodes: dict[str, Node],
) -> None:
    """Test for _set_type_specific_keywords"""
    schema = {}
    _set_type_specific_keywords(schema, test_nodes[node_name])
    assert schema == expected_schema
