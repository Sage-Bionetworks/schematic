# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text

import pandas as pd

def update_df(existing_df:pd.DataFrame, new_df:pd.DataFrame, idx_key:str) -> pd.DataFrame:
    """ Updates an existing data frame with the entries of another dataframe on a given primary index.

    Args: 
        existing_df : data frame to be updated
        new_df: data frame to update with
        idx_key: name of primary index column

    Returns: an updated data frame if the index column is present in both the existing and new data frames; ow returns the existing data frame w/o changes; the column set of the existing data frame are not updated (i.e. schema is preserved)
    """

    if not (idx_key in existing_df.columns and idx_key in new_df):
        return existing_df

    # merge the two data frames keeping all the resulting data in new and existing columns
    updated_df = pd.merge(existing_df, new_df, how = "outer", on = idx_key, suffixes = ("_existing", "_new"))

    # filter to keep only updated values when available across all columns in the schema
    for col in existing_df.columns:
        if col != idx_key:
            existing_col = col + "_existing"
            new_col = col + "_new"

            updated_df[col] = updated_df[existing_col].where(updated_df[new_col].isnull(), updated_df[new_col])

    # remove unnecessary columns and keep existing schema
    updated_df = updated_df[existing_df.columns]

    return updated_df


def execute_google_api_requests(service, requests_body, **kwargs):
    """
    Execute google API requests batch; attempt to execute in parallel.

    Args:
        service: google api service; for now assume google sheets service that is instantiated and authorized
        service_type: default batchUpdate; TODO: add logic for values update
        kwargs: google API service parameters
    Return: google API response
    """

    if "spreadsheet_id" in kwargs and "service_type" in kwargs and kwargs["service_type"] == "batch_update":
        # execute all requests
        response = service.spreadsheets().batchUpdate(spreadsheetId=kwargs["spreadsheet_id"], body = requests_body).execute()
        
        return response


# utilities for schema explorer methods

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


def find_duplicates(_list):
    """Find duplicate items in a list
    """
    return set([x for x in _list if _list.count(x) > 1])