import os
import json
import urllib.request

from definitions import DATA_PATH

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
    """Export JSON doc to file
    """
    with open(file_path, 'w') as f:
        json.dump(json_doc, f, sort_keys=True,
                  indent=4, ensure_ascii=False)


def load_default():
    """Load biolink vocabulary
    """
    biothings_path = os.path.join(DATA_PATH, 'schema_org_schemas', 'biothings.jsonld')
    return load_json(biothings_path)


def load_schemaorg():
    """Load SchemOrg vocabulary
    """
    schemaorg_path = os.path.join(DATA_PATH, 'schema_org_schemas', 'all_layer.jsonld')
    return load_json(schemaorg_path)