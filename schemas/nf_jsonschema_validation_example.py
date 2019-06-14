import json
import jsonschema
from jsonschema import validate

with open("./nf_schema.json", "r") as nf_f:
    nf_schema = json.load(nf_f)
print(nf_schema)

with open("./test_annotations.json", "r") as a_f:
    test_annotations = json.load(a_f)

resolver = jsonschema.RefResolver("./nf_schema.json", None)

validate(instance = test_annotations, schema = nf_schema, resolver = resolver)
