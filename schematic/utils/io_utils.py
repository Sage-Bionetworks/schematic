"""io utils"""

from typing import Any
import json
import urllib.request
from schematic import LOADER


def load_json(file_path: str) -> Any:
    """Load json document from file path or url

    :arg str file_path: The path of the url doc, could be url or file path
    """
    if file_path.startswith("http"):
        with urllib.request.urlopen(file_path) as url:
            data = json.loads(url.read().decode())
            return data
    # handle file path
    else:
        with open(file_path, encoding="utf8") as fle:
            data = json.load(fle)
            return data


def export_json(json_doc: Any, file_path: str) -> None:
    """Export JSON doc to file"""
    with open(file_path, "w", encoding="utf8") as fle:
        json.dump(json_doc, fle, sort_keys=True, indent=4, ensure_ascii=False)


def load_default() -> Any:
    """Load biolink vocabulary"""
    data_path = "data_models/biothings.model.jsonld"
    biothings_path = LOADER.filename(data_path)
    return load_json(biothings_path)


def load_schemaorg() -> Any:
    """Load SchemaOrg vocabulary"""
    data_path = "data_models/schema_org.model.jsonld"
    schema_org_path = LOADER.filename(data_path)
    return load_json(schema_org_path)
