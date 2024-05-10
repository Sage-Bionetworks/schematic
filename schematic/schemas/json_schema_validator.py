"""Json Schema Validator"""

import os
from typing import Any

from jsonschema import validate

from schematic.utils.io_utils import load_schemaorg, load_json
from schematic.utils.general import str2list, dict2list, find_duplicates
from schematic.utils.curie_utils import (
    expand_curies_in_schema,
    extract_name_from_uri_or_curie,
)


class SchemaValidator:
    """Validate Schema against SchemaOrg standard

    Validation Criterias:
    1. Data Structure wise:
      > "@id", "@context", "@graph"
      > Each element in "@graph" should contain "@id", "@type", "rdfs:comment",
      "rdfs:label", "sms:displayName"
      > validate against JSON Schema
      > Should validate the whole structure, and also validate property and
      value separately
    2. Data Content wise:
      > "@id" field should match with "rdfs:label" field
      > all prefixes used in the file should be defined in "@context"
      > There should be no duplicate "@id"
      > Class specific
        > rdfs:label field should be capitalize the first character of each
          word for a class;
        > the value of "rdfs:subClassOf" should be present in the schema or in
          the core vocabulary
        > sms:displayName ideally should contain capitalized words separated by space,
          but that's not enforced by validation
      > Property specific
        > rdfs:label field should be cammelCase
        > the value of "schema:domainIncludes" should be present in the schema
          or in the core vocabulary
        > the value of "schema:rangeIncludes" should be present in the schema
          or in the core vocabulary
        > sms:displayName ideally should contain capitalized words separated
          by space, but that's not enforced by validation
        TODO: add dependencies and component dependencies to class
          structure documentation; as well as value range and required property

    """

    def __init__(self, schema: Any) -> None:
        self.schemaorg = {"schema": load_schemaorg(), "classes": [], "properties": []}
        for _schema in self.schemaorg["schema"]["@graph"]:
            for _record in _schema["@graph"]:
                if "@type" in _record:
                    _type = str2list(_record["@type"])
                    assert isinstance(_type, list)
                    if "rdfs:Property" in _type:
                        self.schemaorg["properties"].append(_record["@id"])
                    elif "rdfs:Class" in _type:
                        self.schemaorg["classes"].append(_record["@id"])
        self.extension_schema = {
            "schema": expand_curies_in_schema(schema),
            "classes": [],
            "properties": [],
        }
        for _record in self.extension_schema["schema"]["@graph"]:  # type: ignore
            _type = str2list(_record["@type"])
            assert isinstance(_type, list)
            if "rdfs:Property" in _type:
                self.extension_schema["properties"].append(_record["@id"])  # type: ignore
            elif "rdfs:Class" in _type:
                self.extension_schema["classes"].append(_record["@id"])  # type: ignore
        self.all_classes = self.schemaorg["classes"] + self.extension_schema["classes"]

    def validate_class_label(self, label_uri: str) -> None:
        """Check if the first character of class label is capitalized"""
        label = extract_name_from_uri_or_curie(label_uri)
        assert label[0].isupper()

    def validate_property_label(self, label_uri: str) -> None:
        """Check if the first character of property label is lower case"""
        label = extract_name_from_uri_or_curie(label_uri)
        assert label[0].islower()

    def validate_subclassof_field(self, subclassof_value: dict) -> None:
        """Check if the value of "subclassof" is included in the schema file"""
        subclassof_value_list = dict2list(subclassof_value)
        assert isinstance(subclassof_value_list, list)
        for record in subclassof_value:
            assert record["@id"] in self.all_classes

    def validate_domain_includes_field(self, domainincludes_value: dict) -> None:
        """Check if the value of "domainincludes" is included in the schema
        file
        """
        domainincludes_value_list = dict2list(domainincludes_value)
        assert isinstance(domainincludes_value_list, list)
        for record in domainincludes_value_list:
            assert (
                record["@id"] in self.all_classes
            ), f"value of domainincludes not recorded in schema: {domainincludes_value}"

    def validate_range_includes_field(self, rangeincludes_value: dict) -> None:
        """Check if the value of "rangeincludes" is included in the schema
        file
        """
        rangeincludes_value_list = dict2list(rangeincludes_value)
        assert isinstance(rangeincludes_value_list, list)
        for record in rangeincludes_value_list:
            assert record["@id"] in self.all_classes

    def check_whether_atid_and_label_match(self, record: dict) -> None:
        """Check if @id field matches with the "rdfs:label" field"""
        _id = extract_name_from_uri_or_curie(record["@id"])
        assert _id == record["rdfs:label"], f"id and label not match: {record}"

    def check_duplicate_labels(self) -> None:
        """Check for duplication in the schema"""
        labels = [
            _record["rdfs:label"]
            for _record in self.extension_schema["schema"]["@graph"]  # type: ignore
        ]
        duplicates = find_duplicates(labels)
        if len(duplicates) == 0:
            raise ValueError("Duplicates detected in graph: ", duplicates)

    def validate_schema(self, schema: Any) -> None:
        """Validate schema against SchemaORG standard"""
        json_schema_path = os.path.join("validation_schemas", "schema.json")
        json_schema = load_json(json_schema_path)
        return validate(schema, json_schema)

    def validate_property_schema(self, schema: Any) -> None:
        """Validate schema against SchemaORG property definition standard"""
        json_schema_path = os.path.join(
            "validation_schemas", "property_json_schema.json"
        )
        json_schema = load_json(json_schema_path)
        return validate(schema, json_schema)

    def validate_class_schema(self, schema: Any) -> None:
        """Validate schema against SchemaORG class definition standard"""
        json_schema_path = os.path.join("validation_schemas", "class_json_schema.json")
        json_schema = load_json(json_schema_path)
        return validate(schema, json_schema)

    def validate_full_schema(self) -> None:
        """validate full schema"""
        self.check_duplicate_labels()
        for record in self.extension_schema["schema"]["@graph"]:  # type: ignore
            self.check_whether_atid_and_label_match(record)
            if record["@type"] == "rdf:Class":
                self.validate_class_schema(record)
                self.validate_class_label(record["@id"])
            elif record["@type"] == "rdf:Property":
                self.validate_property_schema(record)
                self.validate_property_label(record["@id"])
                self.validate_domain_includes_field(
                    record["http://schema.org/domainIncludes"]
                )
                if "http://schema.org/rangeIncludes" in record:
                    self.validate_range_includes_field(
                        record["http://schema.org/rangeIncludes"]
                    )
