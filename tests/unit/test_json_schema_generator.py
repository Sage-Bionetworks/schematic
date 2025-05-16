"""Tests for JSON Schema generation"""

from typing import Generator, Any, Union, Optional
import os
import json
from shutil import rmtree

import pytest
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from schematic.models.metadata import MetadataModel
from schematic.schemas.json_schema_generator import (
    PropertyData,
    _get_ranges_from_range_rule,
    _get_in_range_rule_from_rule_list,
    _get_type_rule_from_rule_list,
    JSONSchema,
    NodeProcessor,
    JSONSchemaGenerator,
    _write_data_model,
    _set_conditional_dependencies,
    _set_property,
    _create_enum_array_property,
    _create_array_property,
    _create_enum_property,
    _create_simple_property,
)
from tests.utils import json_files_equal

# pylint: disable=protected-access
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments


@pytest.fixture(name="js_generator")
def fixture_js_generator() -> Generator[JSONSchemaGenerator, None, None]:
    """Yields a DataModelJSONSchema2 with the example data model"""
    metadata_model = MetadataModel(
        inputMModelLocation="tests/data/example.model.jsonld",
        inputMModelLocationType="local",
        data_model_labels="class_label",
    )
    data_model_js = JSONSchemaGenerator(
        jsonld_path=metadata_model.inputMModelLocation,
        graph=metadata_model.graph_data_model,
    )
    yield data_model_js


@pytest.fixture(name="test_directory", scope="session")
def fixture_test_directory():
    """Yields a directory for creating test jSON Schemas in"""
    test_folder = "tests/data/test_jsonschemas"
    os.makedirs(test_folder, exist_ok=True)
    yield test_folder
    rmtree(test_folder)


@pytest.mark.parametrize(
    "validation_rules, expected_type, expected_is_array, expected_min, expected_max",
    [
        # If there are no type validation rules the property_type is None
        ([], None, False, None, None),
        (["xxx"], None, False, None, None),
        # If there is one type validation rule the property_type is set to the
        #  JSON Schema equivalent of the validation rule
        (["str"], "string", False, None, None),
        (["bool"], "boolean", False, None, None),
        # If there are any list type validation rules the property_type is set to "array"
        (["list like"], None, True, None, None),
        (["list strict"], None, True, None, None),
        # If there are any list type validation rules and one type validation rule
        #  the property_type is set to "array", and the item_type is set to the
        #  JSON Schema equivalent of the validation rule
        (["list like", "str"], "string", True, None, None),
        # If there are any inRange rules the min and max will be set
        (["inRange 0 1", "int"], "integer", False, 0, 1),
        # If there are any inRange rules but no type rule, or a non-numeric type rule
        #  the type will be set to number
        (["inRange 0 1"], "number", False, 0, 1),
        (["inRange 0 1", "str"], "number", False, 0, 1),
    ],
    ids=[
        "No validation rules",
        "No actual validation rules",
        "String type rule",
        "Boolean type rule",
        "list like rule",
        "list strict rule",
        "list like and string type rules",
        "inRange and integer type rules",
        "inRange rule",
        "inRange and string type rules",
    ],
)
def test_property_data(
    validation_rules: list[str],
    expected_type: Optional[str],
    expected_is_array: bool,
    expected_min: Optional[float],
    expected_max: Optional[float],
) -> None:
    """Tests for PropertyData class"""
    result = PropertyData(validation_rules)
    assert result.type == expected_type
    assert result.is_array == expected_is_array
    assert result.minimum == expected_min
    assert result.maximum == expected_max


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
        schema.update_property({"name1": "property"})
        # THEN that dict should be retrievable
        assert schema.properties == {"name1": "property"}
        # WHEN updating the properties with a key that exists
        schema.update_property({"name1": "property2"})
        # THEN the updated dict should be retrievable
        assert schema.properties == {"name1": "property2"}
        # WHEN updating the properties dict with a new key
        schema.update_property({"name3": "property3"})
        # THEN the new key and old key should be retrievable
        assert schema.properties == {"name1": "property2", "name3": "property3"}


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
)
def test_get_ranges_from_range_rule(
    input_rule: str,
    expected_tuple: tuple[Union[str, None], Union[str, None]],
) -> None:
    """Test for _get_ranges_from_range_rule"""
    result = _get_ranges_from_range_rule(input_rule)
    assert result == expected_tuple


@pytest.mark.parametrize(
    "input_rules, expected_rule",
    [
        ([], None),
        (["list strict"], None),
        (["inRange 0 1"], "inRange 0 1"),
        (["str error", "inRange 0 1"], "inRange 0 1"),
    ],
)
def test_get_in_range_rule_from_rule_list(
    input_rules: list[str],
    expected_rule: Union[str, None],
) -> None:
    """Test for _get_in_range_rule_from_rule_list"""
    result = _get_in_range_rule_from_rule_list(input_rules)
    assert result == expected_rule


@pytest.mark.parametrize(
    "input_rules",
    [(["inRange", "inRange"]), (["inRange 0", "inRange 0"])],
)
def test_get_in_range_rule_from_rule_list_exceptions(
    input_rules: list[str],
) -> None:
    """Test for _get_in_range_rule_from_rule_list with exceptions"""
    with pytest.raises(
        ValueError, match="Found more than one inRange rule in validation rules"
    ):
        _get_in_range_rule_from_rule_list(input_rules)


@pytest.mark.parametrize(
    "input_rules, expected_rule",
    [([], None), (["list strict"], None), (["str"], "str"), (["str error"], "str")],
)
def test_get_type_rule_from_rule_list(
    input_rules: list[str],
    expected_rule: Union[str, None],
) -> None:
    """Test for _get_type_rule_from_rule_list"""
    result = _get_type_rule_from_rule_list(input_rules)
    assert result == expected_rule


@pytest.mark.parametrize(
    "input_rules",
    [(["str", "int"]), (["str", "str", "str"]), (["str", "str error", "str warning"])],
)
def test_get_type_rule_from_rule_list_exceptions(
    input_rules: list[str],
) -> None:
    """Test for _get_type_rule_from_rule_list with exceptions"""
    with pytest.raises(
        ValueError, match="Found more than one type rule in validation rules"
    ):
        _get_type_rule_from_rule_list(input_rules)


class TestNodeProcessor:
    """Tests for NodeProcessor class"""

    def test_init(self) -> None:
        """Test NodeProcessor.__init__"""
        # GIVEN a NodeProcessor instance with two nodes
        np = NodeProcessor(["node1", "node2"])
        # THEN the current_node should be first node in the input list
        assert np.current_node == "node1"
        # THEN nodes_to_process should be the rest of the nodes
        assert np.nodes_to_process == ["node2"]
        # THEN root_dependencies should be the entire list
        assert np.root_dependencies == ["node1", "node2"]

    def test_move_to_next_node(self) -> None:
        """Test NodeProcessor.move_to_next_node"""
        # GIVEN a NodeProcessor instance with two nodes
        np = NodeProcessor(["node1", "node2"])
        # THEN the current_node should be first node in the input list
        assert np.current_node == "node1"
        # WHEN using move_to_next_node
        np.move_to_next_node()
        # THEN the current_node should be second node in the input list
        assert np.current_node == "node2"
        # THEN nodes_to_process should be empty
        assert np.nodes_to_process == []

    def test_are_nodes_remaining(self) -> None:
        """Test NodeProcessor.are_nodes_remaining"""
        # GIVEN a NodeProcessor instance with one node
        np = NodeProcessor(["node1"])
        # THEN there should be (1) nodes_remaining
        assert np.are_nodes_remaining()
        # WHEN using move_to_next_node
        np.move_to_next_node()
        # THEN there should not be nodes_remaining
        assert not np.are_nodes_remaining()

    def test_is_current_node_processed(self) -> None:
        """Test NodeProcessor.is_current_node_processed"""
        # GIVEN a NodeProcessor instance with one node
        np = NodeProcessor(["node1"])
        # THEN the current node should not have been processed yet.
        assert not np.is_current_node_processed()
        # WHEN adding a the current node to the processed list
        np.processed_nodes += ["node1"]
        # THEN the current node should be listed as processed.
        assert np.is_current_node_processed()

    def test_update_range_domain_map(self) -> None:
        """Test NodeProcessor.update_range_domain_map"""
        # GIVEN a NodeProcessor instance
        np = NodeProcessor(["node1"])
        # THEN the valid_values_map should be empty to start with
        assert not np.valid_values_map
        # WHEN the map is updated with one node and two values
        np.update_valid_values_map("node1", ["value1", "value2"])
        # THEN valid values map should have one entry for each valid value,
        #  with the node as the value
        assert np.valid_values_map == {"value1": ["node1"], "value2": ["node1"]}

    def test_update_reverse_dependencies(self) -> None:
        """Test NodeProcessor.update_reverse_dependencies"""
        # GIVEN a NodeProcessor instance
        np = NodeProcessor(["node1"])
        # THEN the reverse_dependencies should be empty to start with
        assert not np.reverse_dependencies
        # WHEN the map is updated with one node and two reverse_dependencies
        np.update_reverse_dependencies("node1", ["nodeA", "nodeB"])
        # THEN reverse_dependencies should have one entry for each valid value,
        #  with the node as the value
        assert np.reverse_dependencies == {"nodeA": ["node1"], "nodeB": ["node1"]}

    def test_update_nodes_to_process(self) -> None:
        """Test NodeProcessor.update_nodes_to_process"""
        # GIVEN a NodeProcessor instance with one node
        np = NodeProcessor(["node1"])
        # THEN the NodeProcessor should have no nodes in nodes_to_process
        assert np.nodes_to_process == []
        # WHEN adding a node to nodes_to_process
        np.update_nodes_to_process(["node2"])
        # THEN that node should be in nodes_to_process
        assert np.nodes_to_process == ["node2"]

    def test_update_processed_nodes_with_current_node(self) -> None:
        """Test NodeProcessor.update_processed_nodes_with_current_node"""
        # GIVEN a NodeProcessor instance with one node
        np = NodeProcessor(["node1"])
        # THEN the NodeProcessor should have no nodes in nodes_to_process
        assert not np.processed_nodes
        # WHEN the node has been processed
        np.update_processed_nodes_with_current_node()
        # THEN the node should eb listed as processed
        assert np.processed_nodes == ["node1"]


@pytest.mark.parametrize(
    "datatype",
    [
        ("Biospecimen"),
        ("BulkRNA-seqAssay"),
        ("MockComponent"),
        ("MockFilename"),
        ("MockRDB"),
        ("Patient"),
    ],
)
def test_create_json_schema(
    js_generator: JSONSchemaGenerator, datatype: str, test_directory: str
) -> None:
    """Tests for JSONSchemaGenerator.create_json_schema"""
    test_file = f"test.{datatype}.schema.json"
    test_path = os.path.join(test_directory, test_file)
    expected_path = f"tests/data/expected_jsonschemas/expected.{datatype}.schema.json"
    title = f"{datatype}_validation"
    js_generator.create_json_schema(datatype, title, test_path)
    json_files_equal(test_path, expected_path)


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
        ({"property_name": []}, {}),
        # If the input node has reverse dependencies,
        #  but none of them are in the range domain map, nothing gets added
        ({"property_name": ["enum1"]}, {}),
        # If the input node has any reverse dependencies,
        #  and atleast one of them are in the range domain map,
        #  but the range domain map is empty for that node, nothing gets added
        ({"property_name": ["enum1"]}, {"enum1": []}),
    ],
)
def test_set_conditional_dependencies_nothing_added(
    reverse_dependencies: dict[str, list[str]],
    valid_values_map: dict[str, list[str]],
) -> None:
    """
    Tests for _set_conditional_dependencies
      were the schema doesn't change
    """
    json_schema = {"allOf": []}
    _set_conditional_dependencies(
        json_schema=json_schema,
        conditional_property="property_name",
        property_display_name="property_name",
        reverse_dependencies=reverse_dependencies,
        valid_values_map=valid_values_map,
    )
    assert json_schema == {"allOf": []}


@pytest.mark.parametrize(
    "reverse_dependencies, valid_values_map, expected_schema",
    [
        (
            {"property_name": ["enum1"]},
            {"enum1": ["rev_dep_property1"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {
                            "properties": {"rev_dep_property1": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    }
                ]
            ),
        ),
        (
            {"property_name": ["enum1"]},
            {"enum1": ["rev_dep_property1", "rev_dep_property2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {
                            "properties": {"rev_dep_property1": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    },
                    {
                        "if": {
                            "properties": {"rev_dep_property2": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    },
                ]
            ),
        ),
        (
            {"property_name": ["enum1", "enum2"]},
            {"enum1": ["rev_dep_property1"], "enum2": ["rev_dep_property2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {
                            "properties": {"rev_dep_property1": {"enum": ["enum1"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
                        },
                    },
                    {
                        "if": {
                            "properties": {"rev_dep_property2": {"enum": ["enum2"]}}
                        },
                        "then": {
                            "properties": {"property_name": {"not": {"type": "null"}}},
                            "required": ["property_name"],
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
) -> None:
    """Tests for _set_conditional_dependencies"""
    json_schema = JSONSchema()
    _set_conditional_dependencies(
        json_schema=json_schema,
        conditional_property="property_name",
        property_display_name="property_name",
        reverse_dependencies=reverse_dependencies,
        valid_values_map=valid_values_map,
    )
    assert json_schema == expected_schema


@pytest.mark.parametrize(
    "schema, enum_list, validation_rules, is_required, expected_schema",
    [
        # enum_list is not empty, validation rules contain a list rule, is_required is True
        # The property should be an array with an enum
        # required list should have "property_name"
        (
            JSONSchema(),
            ["enum1"],
            ["list"],
            True,
            JSONSchema(
                properties={
                    "property_name": {
                        "description": "TBD",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["enum1"]},
                            },
                        ],
                    }
                },
                required=["property_name"],
            ),
        ),
        # enum_list is not empty, validation rules contain a list rule, is_required is False
        # The property should be an array with an enum
        # required list should be empty
        (
            JSONSchema(),
            ["enum1"],
            ["list"],
            False,
            JSONSchema(
                properties={
                    "property_name": {
                        "description": "TBD",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["enum1"]},
                            },
                            {"type": "null", "title": "null"},
                        ],
                    }
                },
                required=[],
            ),
        ),
        # enum_list is not empty, validation rules do not contain a list rule
        # The property should be an enum
        (
            JSONSchema(),
            ["enum1"],
            [],
            False,
            JSONSchema(
                properties={
                    "property_name": {
                        "description": "TBD",
                        "oneOf": [
                            {"enum": ["enum1"], "title": "enum"},
                            {"type": "null", "title": "null"},
                        ],
                    }
                },
                required=[],
            ),
        ),
        # enum_list is empty, validation rules do contain a list rule
        # The property should be an array
        (
            JSONSchema(),
            [],
            ["list"],
            False,
            JSONSchema(
                properties={
                    "property_name": {
                        "oneOf": [
                            {"type": "array", "title": "array"},
                            {"type": "null", "title": "null"},
                        ],
                        "description": "TBD",
                    }
                },
                required=[],
            ),
        ),
        # enum_list is empty, validation rules do not contain a list rule
        # The property should be neither an array or enum
        (
            JSONSchema(),
            [],
            [],
            False,
            JSONSchema(
                properties={"property_name": {"description": "TBD"}},
                required=[],
            ),
        ),
    ],
)
def test_set_property(
    schema: JSONSchema,
    enum_list: list[str],
    expected_schema: dict[str, Any],
    validation_rules: list[str],
    is_required: bool,
) -> None:
    """Tests for set_property"""
    _set_property(
        json_schema=schema,
        name="property_name",
        enum_list=enum_list,
        validation_rules=validation_rules,
        is_required=is_required,
        description="TBD",
    )
    assert schema == expected_schema


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema, valid_values, invalid_values",
    [
        (
            ["enum1"],
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {
                            "type": "array",
                            "title": "array",
                            "items": {"enum": ["enum1"]},
                        }
                    ],
                }
            },
            [[], ["enum1"]],
            [[None], ["x"], None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            ["enum1"],
            False,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {
                            "type": "array",
                            "title": "array",
                            "items": {"enum": ["enum1"]},
                        },
                        {"type": "null", "title": "null"},
                    ],
                }
            },
            [[], ["enum1"], None],
            [[None], ["x"]],
        ),
    ],
    ids=["Required", "Not required"],
)
def test_create_enum_array_property(
    enum_list: list[str],
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_enum_array_property"""
    schema = _create_enum_array_property(
        name="name", enum_list=enum_list, is_required=is_required, description="TBD"
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "property_data, is_required, expected_schema, valid_values, invalid_values",
    [
        (
            PropertyData(["list"]),
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [{"type": "array", "title": "array"}],
                }
            },
            [[], [None], ["x"]],
            ["x", None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            PropertyData(["list"]),
            False,
            {
                "name": {
                    "oneOf": [
                        {"type": "array", "title": "array"},
                        {"type": "null", "title": "null"},
                    ],
                    "description": "TBD",
                }
            },
            [None, [], [None], ["x"]],
            ["x"],
        ),
        # If item_type is given, it is set in the schema
        (
            PropertyData(["list", "str"]),
            True,
            {
                "name": {
                    "oneOf": [
                        {"type": "array", "title": "array", "items": {"type": "string"}}
                    ],
                    "description": "TBD",
                }
            },
            [[], ["x"]],
            [None, [None], [1]],
        ),
        # If property_data has range_min or range_max, they are set in the schema
        (
            PropertyData(["num", "list", "inRange 0 1"]),
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {
                            "type": "array",
                            "title": "array",
                            "items": {"type": "number", "minimum": 0, "maximum": 1},
                        }
                    ],
                }
            },
            [[], [1]],
            [None, [None], [2], ["x"]],
        ),
    ],
    ids=[
        "Required, no item type",
        "Not required, no item type",
        "Required, string item type",
        "Required, number item type",
    ],
)
def test_create_array_property(
    property_data: PropertyData,
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_array_property"""
    schema = _create_array_property(
        name="name",
        property_data=property_data,
        is_required=is_required,
        description="TBD",
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "enum_list, is_required, expected_schema, valid_values, invalid_values",
    [
        # Empty enum list
        (
            [],
            True,
            {"name": {"description": "TBD", "oneOf": [{"enum": [], "title": "enum"}]}},
            [],
            [1, "x", None],
        ),
        # If is_required is True, no type is added
        (
            ["enum1"],
            True,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [{"enum": ["enum1"], "title": "enum"}],
                }
            },
            ["enum1"],
            [1, "x", None],
        ),
        # If is_required is False, "null" is added as a type
        (
            ["enum1"],
            False,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {"enum": ["enum1"], "title": "enum"},
                        {"type": "null", "title": "null"},
                    ],
                }
            },
            ["enum1", None],
            [1, "x"],
        ),
    ],
    ids=["empty enum list", "is required==True", "is required==False"],
)
def test_create_enum_property(
    enum_list: list[str],
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_enum_property"""
    schema = _create_enum_property(
        enum_list=enum_list, name="name", is_required=is_required, description="TBD"
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "property_data, is_required, expected_schema, valid_values, invalid_values",
    [
        (PropertyData(), False, {"name": {"description": "TBD"}}, [None, 1, ""], []),
        # If property_type is given, it is added to the schema
        (
            PropertyData(["str"]),
            True,
            {"name": {"type": "string", "description": "TBD"}},
            [""],
            [1, None],
        ),
        # If property_type is given, and is_required is False,
        # type is set to given property_type and "null"
        (
            PropertyData(["str"]),
            False,
            {
                "name": {
                    "description": "TBD",
                    "oneOf": [
                        {"type": "string", "title": "string"},
                        {"type": "null", "title": "null"},
                    ],
                }
            },
            [None, "x"],
            [1],
        ),
        # If is_required is True '"not": {"type":"null"}' is added to schema if
        # property_type is not given
        (
            PropertyData(),
            True,
            {"name": {"not": {"type": "null"}, "description": "TBD"}},
            ["x", 1],
            [None],
        ),
        (
            PropertyData(["num", "inRange 0 1"]),
            True,
            {
                "name": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "description": "TBD",
                }
            },
            [1],
            [None, 2],
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
    property_data: PropertyData,
    is_required: bool,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
) -> None:
    """Test for _create_simple_property"""
    schema = _create_simple_property(
        "name", property_data, is_required, description="TBD"
    )
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": schema, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})
