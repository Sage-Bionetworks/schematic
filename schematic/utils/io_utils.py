import os
import json
import urllib.request

from schematic import CONFIG, LOADER


def load_json(file_path):
    """Load json document from file path or url

    :arg str file_path: The path of the url doc, could be url or file path
    """
    if file_path.startswith("http"):
        with urllib.request.urlopen(file_path) as url:
            data = json.loads(url.read().decode())
            return data
    # handle file path
    else:
        with open(file_path) as f:
            data = json.load(f)
            return data


def export_json(json_doc, file_path):
    """Export JSON doc to file"""
    with open(file_path, "w") as f:
        json.dump(json_doc, f, sort_keys=True, indent=4, ensure_ascii=False)


def load_default():
    """Load biolink vocabulary"""
    data_path = "data_models/biothings.model.jsonld"
    biothings_path = LOADER.filename(data_path)

    return load_json(biothings_path)


def load_schemaorg():
    """Load SchemOrg vocabulary"""
    data_path = "data_models/schema_org.model.jsonld"
    schemaorg_path = LOADER.filename(data_path)

    return load_json(schemaorg_path)
