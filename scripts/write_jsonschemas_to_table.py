import synapseclient
from synapseclient.models import Table

import pandas as pd


def main():
    """
    The main entry point for this script.

    This script logs in to Synapse, finds all the JSONschema organizations
    and versions in the given list, and writes them to the Synapse table
    with Synapse ID syn69735275.

    The table will have the following columns:
        - org: the name of the JSONschema organization
        - name: the full name of the JSONschema version
        - dcc: the name of the DCC
        - datatype: the name of the datatype
        - uri: the URI of the JSONschema version
        - version: the semantic version of the JSONschema version
        - link: a link to the JSONschema version in the Synapse repo
    """
    syn = synapseclient.login()
    json_schema_organizations = ["sage.schemas.v2571", "sage.schemas.v2581"]
    js = syn.service("json_schema")
    to_write_schemas = []
    for organization_name in json_schema_organizations:
        org = js.JsonSchemaOrganization(organization_name)
        schemas = org.list_json_schemas()
        for schema in schemas:
            versions = schema.list_versions()
            for version in versions:
                to_write_schemas.append(
                    {
                        "org": organization_name,
                        "name": version.name,
                        "dcc": version.name.split(".")[0],
                        "datatype": version.name.split(".")[1],
                        "uri": version.uri,
                        "version": version.semantic_version,
                        "link": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{version.uri}",
                    }
                )

    df = pd.DataFrame(to_write_schemas)
    # exclude HTAN1, HTAN2, and NF schemas as they have their own JSONschema organizations
    df = df[~df["dcc"].isin(["htan", "htan2", "nf"])]
    table = Table(id="syn69735275").get(include_columns=True)
    table.upsert_rows(values=df, primary_keys=["uri"])


if __name__ == "__main__":
    main()
