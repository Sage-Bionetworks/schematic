"""Curie utils"""

import logging
from typing import Any, Union

Context = dict[str, str]
Record = dict[str, Union[str, list, dict, None]]
Graph = list[Record]
Schema = dict[str, Any]

logger = logging.getLogger(__name__)


def extract_name_from_uri_or_curie(item: str) -> str:
    """Extract name from uri or curie"""
    if "http" not in item and len(item.split(":")) == 2:
        return item.split(":")[-1]
    if len(item.split("//")[-1].split("/")) > 1:
        return item.split("//")[-1].split("/")[-1]
    raise ValueError("Error extracting name from URI or Curie.")


def expand_curie_to_uri(curie: str, context_info: Context) -> str:
    """Expand curie to uri based on the context given

    parmas
    ======
    curie: curie to be expanded (e.g. bts:BiologicalEntity)
    context_info: jsonld context specifying prefix-uri relation (e.g. {"bts":
    "http://schema.biothings.io/"})
    """
    # as suggested in SchemaOrg standard file, these prefixes don't expand
    prefixes_not_expand = ["rdf", "rdfs", "xsd"]
    # determine if a value is curie
    if len(curie.split(":")) == 2:
        prefix, value = curie.split(":")
        if prefix in context_info and prefix not in prefixes_not_expand:
            return context_info[prefix] + value
        # if the input is not curie, return the input unmodified
        return curie
    return curie


def expand_curies_in_schema(schema: Schema) -> Schema:
    """Expand all curies in a SchemaOrg JSON-LD file into URI"""
    context: Context = schema["@context"]
    graph: Graph = schema["@graph"]
    new_schema = {"@context": context, "@graph": [], "@id": schema["@id"]}
    for record in graph:
        new_record: Record = {}
        for key, value in record.items():
            if isinstance(value, str):
                new_record[expand_curie_to_uri(key, context)] = expand_curie_to_uri(
                    value, context
                )
            elif isinstance(value, list):
                uri = expand_curie_to_uri(key, context)
                if isinstance(value[0], dict):
                    lst: list[dict[str, str]] = []
                    new_record[uri] = lst
                    for _item in value:
                        lst.append({"@id": expand_curie_to_uri(_item["@id"], context)})
                else:
                    new_record[uri] = [
                        expand_curie_to_uri(_item, context) for _item in value
                    ]
            elif isinstance(value, dict) and "@id" in value:
                new_record[expand_curie_to_uri(key, context)] = {
                    "@id": expand_curie_to_uri(value["@id"], context)
                }
            elif value is None:
                new_record[expand_curie_to_uri(key, context)] = None
        new_schema["@graph"].append(new_record)
    return new_schema


def uri2label(uri: str, schema: Schema) -> list:
    """Given a URI, return the label"""
    return [
        record["rdfs:label"] for record in schema["@graph"] if record["@id"] == uri
    ][0]
