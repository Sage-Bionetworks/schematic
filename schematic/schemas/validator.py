import os
from jsonschema import validate

from schematic.utils.io_utils import load_schemaorg, load_json, load_default
from schematic.utils.general import str2list, dict2list, find_duplicates
from schematic.utils.curie_utils import expand_curies_in_schema, extract_name_from_uri_or_curie
from schematic.utils.validate_utils import validate_class_schema, validate_property_schema, validate_schema

from schematic.utils.config_utils import load_yaml

from definitions import CONFIG_PATH, DATA_PATH

config_data = load_yaml(CONFIG_PATH)

class SchemaValidator():
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
        > sms:displayName ideally should contain capitalized words separated by space, but that's not enforced by validation
      > Property specific
        > rdfs:label field should be cammelCase
        > the value of "schema:domainIncludes" should be present in the schema 
          or in the core vocabulary
        > the value of "schema:rangeIncludes" should be present in the schema 
          or in the core vocabulary
        > sms:displayName ideally should contain capitalized words separated by space, but that's not enforced by validation
        TODO: add dependencies and component dependencies to class structure documentation; as well as value range and required property
      
    """
    def __init__(self, schema):
        self.schemaorg = {'schema': load_schemaorg(),
                          'classes': [],
                          'properties': []}
        for _schema in self.schemaorg['schema']['@graph']:
            for _record in _schema["@graph"]:
                if "@type" in _record:
                    _type = str2list(_record["@type"])
                    if "rdfs:Property" in _type:
                        self.schemaorg['properties'].append(_record["@id"])
                    elif "rdfs:Class" in _type:
                        self.schemaorg['classes'].append(_record["@id"])
        self.extension_schema = {'schema': expand_curies_in_schema(schema),
                                 'classes': [],
                                 'properties': []}
        for _record in self.extension_schema['schema']["@graph"]:
            _type = str2list(_record["@type"])
            if "rdfs:Property" in _type:
                self.extension_schema['properties'].append(_record["@id"])
            elif "rdfs:Class" in _type:
                self.extension_schema['classes'].append(_record["@id"])
        self.all_classes = self.schemaorg['classes'] + self.extension_schema['classes']

    def validate_class_label(self, label_uri):
        """ Check if the first character of class label is capitalized
        """
        label = extract_name_from_uri_or_curie(label_uri)
        assert label[0].isupper()

    def validate_property_label(self, label_uri):
        """ Check if the first character of property label is lower case
        """
        label = extract_name_from_uri_or_curie(label_uri)
        assert label[0].islower()

    def validate_subclassof_field(self, subclassof_value):
        """ Check if the value of "subclassof" is included in the schema file
        """
        subclassof_value = dict2list(subclassof_value)
        for record in subclassof_value:
            assert record["@id"] in self.all_classes

    def validate_domainIncludes_field(self, domainincludes_value):
        """ Check if the value of "domainincludes" is included in the schema
        file
        """
        domainincludes_value = dict2list(domainincludes_value)
        for record in domainincludes_value:
            assert record["@id"] in self.all_classes, "value of domainincludes not recorded in schema: %r" % domainincludes_value

    def validate_rangeIncludes_field(self, rangeincludes_value):
        """ Check if the value of "rangeincludes" is included in the schema
        file
        """
        rangeincludes_value = dict2list(rangeincludes_value)
        for record in rangeincludes_value:
            assert record["@id"] in self.all_classes

    def check_whether_atid_and_label_match(self, record):
        """ Check if @id field matches with the "rdfs:label" field
        """
        _id = extract_name_from_uri_or_curie(record["@id"])
        assert _id == record["rdfs:label"], "id and label not match: %r" % record

    def check_duplicate_labels(self):
        """ Check for duplication in the schema
        """
        labels = [_record['rdfs:label'] for _record in self.extension_schema["schema"]["@graph"]]
        duplicates = find_duplicates(labels)
        try:
            assert len(duplicates) == 0
        except:
            raise Exception('Duplicates detected in graph: ', duplicates)

    def validate_schema(self, schema):
        """Validate schema against SchemaORG standard
        """
        json_schema_path = os.path.join(DATA_PATH, 'validation_schemas', 'schema.json')
        json_schema = load_json(json_schema_path)
        return validate(schema, json_schema)

    def validate_property_schema(self, schema):
        """Validate schema against SchemaORG property definition standard
        """
        json_schema_path = os.path.join(DATA_PATH, 'validation_schemas', 'property_json_schema.json')
        json_schema = load_json(json_schema_path)
        return validate(schema, json_schema)

    def validate_class_schema(self, schema):
        """Validate schema against SchemaORG class definition standard
        """
        json_schema_path = os.path.join(DATA_PATH, 'validation_schemas', 'class_json_schema.json')
        json_schema = load_json(json_schema_path)
        return validate(schema, json_schema)

    def validate_full_schema(self):
        self.check_duplicate_labels()
        for record in self.extension_schema['schema']['@graph']:
            self.check_whether_atid_and_label_match(record)
            if record['@type'] == "rdf:Class":
                self.validate_class_schema(record)
                self.validate_class_label(record["@id"])
            elif record['@type'] == "rdf:Property":
                self.validate_property_schema(record)
                self.validate_property_label(record["@id"])
                self.validate_domainIncludes_field(record["http://schema.org/domainIncludes"])
                if "http://schema.org/rangeIncludes" in record:
                    self.validate_rangeIncludes_field(record["http://schema.org/rangeIncludes"])