"""Data Model Relationships"""

from schematic.utils.schema_utils import (
    get_label_from_display_name,
    get_attribute_display_name_from_label,
    convert_bool_to_str,
    parse_validation_rules,
)


class DataModelRelationships:
    """Data Model Relationships"""

    def __init__(self) -> None:
        self.relationships_dictionary = self.define_data_model_relationships()

    def define_data_model_relationships(self) -> dict:
        """Define the relationships and their attributes so they can be accessed
          through other classes.
        The key is how it the relationship will be referenced througout Schematic.
        Note: Though we could use other keys to determine which keys define nodes and edges,
            edge_rel is used as an explicit definition, for easier code readablity.
        key:
            jsonld_key: Name for relationship in the JSONLD.
                        Include in all sub-dictionaries.
            csv_header: Str, name for this relationshp in the CSV data model.
                        Enter None if not part of the CSV data model.
            node_label: Name for relationship in the graph representation of the data model.
                        Do not include this key for edge relationships.
            type: type, type of expected to be read into graph creation.
            edge_rel: True, if this relationship defines an edge
                      False, if is a value relationship
                      Include in all sub-dictionaries.
            required_header: True, if relationship header is required for the csv
            jsonld_default:
                Defines default values to fill for JSONLD generation.
                Used during func DataModelJsonLD.clean_template(), to fill value with a default,
                  if not supplied in the data model.
            node_attr_dict: This is used to add information to nodes in the model.
                Only include for nodes not edges.
                set default values for this relationship
                key is the node relationship name, value is the default value.
                If want to set default as a function create a nested dictionary.
                    {'default': default_function,
                     'standard': alternative function to call if relationship is present for a node
                    }
                If adding new functions to node_dict will
                    need to modify data_model_nodes.generate_node_dict in
            edge_dir: str, 'in'/'out' is the edge an in or out edge. Define for edge relationships
            jsonld_dir: str, 'in'/out is the direction in or out in the JSONLD.

        TODO:
            - Use class inheritance to set up
        """
        map_data_model_relationships = {
            "displayName": {
                "jsonld_key": "sms:displayName",
                "csv_header": "Attribute",
                "node_label": "displayName",
                "type": str,
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {
                    "default": get_attribute_display_name_from_label,
                    "standard": get_attribute_display_name_from_label,
                },
            },
            "label": {
                "jsonld_key": "rdfs:label",
                "csv_header": None,
                "node_label": "label",
                "type": str,
                "edge_rel": False,
                "required_header": False,
                "node_attr_dict": {
                    "default": get_label_from_display_name,
                    "standard": get_label_from_display_name,
                },
            },
            "comment": {
                "jsonld_key": "rdfs:comment",
                "csv_header": "Description",
                "node_label": "comment",
                "type": str,
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {"default": "TBD"},
            },
            "rangeIncludes": {
                "jsonld_key": "schema:rangeIncludes",
                "csv_header": "Valid Values",
                "edge_key": "rangeValue",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "requiresDependency": {
                "jsonld_key": "sms:requiresDependency",
                "csv_header": "DependsOn",
                "edge_key": "requiresDependency",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "requiresComponent": {
                "jsonld_key": "sms:requiresComponent",
                "csv_header": "DependsOn Component",
                "edge_key": "requiresComponent",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "required": {
                "jsonld_key": "sms:required",
                "csv_header": "Required",
                "node_label": "required",
                "type": bool,
                "jsonld_default": "sms:false",
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {
                    "default": False,
                    "standard": convert_bool_to_str,
                },
            },
            "subClassOf": {
                "jsonld_key": "rdfs:subClassOf",
                "csv_header": "Parent",
                "edge_key": "parentOf",
                "jsonld_direction": "in",
                "edge_dir": "out",
                "jsonld_default": [{"@id": "bts:Thing"}],
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "validationRules": {
                "jsonld_key": "sms:validationRules",
                "csv_header": "Validation Rules",
                "node_label": "validationRules",
                "jsonld_direction": "out",
                "edge_dir": "out",
                "jsonld_default": [],
                "type": list,
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {
                    "default": [],
                    "standard": parse_validation_rules,
                },
            },
            "domainIncludes": {
                "jsonld_key": "schema:domainIncludes",
                "csv_header": "Properties",
                "edge_key": "domainValue",
                "jsonld_direction": "out",
                "edge_dir": "in",
                "type": list,
                "edge_rel": True,
                "required_header": True,
            },
            "isPartOf": {
                "jsonld_key": "schema:isPartOf",
                "csv_header": None,
                "node_label": "isPartOf",
                "type": dict,
                "edge_rel": False,
                "required_header": False,
                "node_attr_dict": {
                    "default": {"@id": "http://schema.biothings.io"},
                },
            },
            "id": {
                "jsonld_key": "@id",
                "csv_header": "Source",
                "node_label": "uri",
                "type": str,
                "edge_rel": False,
                "required_header": True,
                "node_attr_dict": {
                    "default": get_label_from_display_name,
                    "standard": get_label_from_display_name,
                },
            },
        }

        return map_data_model_relationships

    def define_required_csv_headers(self) -> list:
        """
        Helper function to retrieve required CSV headers, alert if required header was
          not provided.
        Returns:
            required_headers: lst, Required CSV headers.
        """
        required_headers = []
        for key, value in self.relationships_dictionary.items():
            try:
                if value["required_header"]:
                    required_headers.append(value["csv_header"])
            except KeyError:
                print(
                    (
                        "Did not provide a 'required_header' key, value pair for the "
                        f"nested dictionary {key} : {value}"
                    )
                )

        return required_headers

    def retreive_rel_headers_dict(self, edge: bool) -> dict[str, str]:
        """
        Helper function to retrieve CSV headers for edge and non-edge relationships
          defined by edge_type.

        Args:
            edge, bool: True if looking for edge relationships
        Returns:
            rel_headers_dict: dict, key: csv_header if the key represents an edge relationship.
        """
        rel_headers_dict = {}
        for rel, rel_dict in self.relationships_dictionary.items():
            if "edge_rel" in rel_dict:
                if rel_dict["edge_rel"] and edge:
                    rel_headers_dict.update({rel: rel_dict["csv_header"]})
                elif not rel_dict["edge_rel"] and not edge:
                    rel_headers_dict.update({rel: rel_dict["csv_header"]})
            else:
                raise ValueError(f"Did not provide a 'edge_rel' for relationship {rel}")

        return rel_headers_dict
