def extract_name_from_uri_or_curie(item):
    """Extract name from uri or curie
    """
    if 'http' not in item and len(item.split(":")) == 2:
        return item.split(":")[-1]
    elif len(item.split("//")[-1].split('/')) > 1:
        return item.split("//")[-1].split('/')[-1]
    else:
        print("error")

def expand_curie_to_uri(curie, context_info):
    """Expand curie to uri based on the context given

    parmas
    ======
    curie: curie to be expanded (e.g. bts:BiologicalEntity)
    context_info: jsonld context specifying prefix-uri relation (e.g. {"bts":
    "http://schema.biothings.io/"})
    """
    # as suggested in SchemaOrg standard file, these prefixes don't expand
    PREFIXES_NOT_EXPAND = ["rdf", "rdfs", "xsd"]
    # determine if a value is curie
    if len(curie.split(':')) == 2:
        prefix, value = curie.split(":")
        if prefix in context_info and prefix not in PREFIXES_NOT_EXPAND:
            return context_info[prefix] + value
    # if the input is not curie, return the input unmodified
        else:
            return curie
    else:
        return curie

def expand_curies_in_schema(schema):
    """Expand all curies in a SchemaOrg JSON-LD file into URI
    """
    context = schema["@context"]
    graph = schema["@graph"]
    new_schema = {"@context": context,
                  "@graph": [],
                  "@id": schema["@id"]}
    for record in graph:
        new_record = {}
        for k, v in record.items():
            if type(v) == str:
                new_record[expand_curie_to_uri(k, context)] =  expand_curie_to_uri(v, context)
            elif type(v) == list:
                if type(v[0]) == dict:
                    new_record[expand_curie_to_uri(k, context)] = []
                    for _item in v:
                        new_record[expand_curie_to_uri(k, context)].append({"@id": expand_curie_to_uri(_item["@id"], context)})
                else:
                    new_record[expand_curie_to_uri(k, context)] = [expand_curie_to_uri(_item, context) for _item in v]
            elif type(v) == dict and "@id" in v:
                new_record[expand_curie_to_uri(k, context)] = {"@id": expand_curie_to_uri(v["@id"], context)}
            elif v == None:
                new_record[expand_curie_to_uri(k, context)] = None
        new_schema["@graph"].append(new_record)
    return new_schema


def uri2label(uri, schema):
    """Given a URI, return the label
    """
    return [record["rdfs:label"] for record in schema["@graph"] if record['@id'] == uri][0]