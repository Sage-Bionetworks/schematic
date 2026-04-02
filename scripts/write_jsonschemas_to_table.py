import synapseclient
from synapseclient.models import Table
from synapseclient.models import SchemaOrganization

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
    # json_schema_organizations = ["sage.schemas.v2571", "sage.schemas.v2581"]
    json_schema_organizations = [
        "sage.schemas.v2571",
        "sage.schemas.v2581",
        "MultiConsortiaCoordinatingCenter",
        "org.synapse.nf",
    ]
    # js = syn.service("json_schema")
    to_write_schemas = []
    for organization_name in json_schema_organizations:
        org = SchemaOrganization(name=organization_name).get()
        # org = js.JsonSchemaOrganization(organization_name)
        schemas = org.get_json_schemas()
        for schema in schemas:
            print(schema)
            # versions = schema.list_versions()
            versions = schema.get_versions()
            try:
                for version in versions:
                    print(version)
                    if (
                        (
                            version.schema_name.startswith("ad")
                            and version.semantic_version
                            in ["0.1.0", "1.99.9999", "1.99.99", None]
                        )
                        or (
                            version.schema_name.startswith("el")
                            and version.semantic_version == "0.0.1"
                        )
                        or ".validation." in version.schema_name
                    ):
                        continue
                    if organization_name == "MultiConsortiaCoordinatingCenter":
                        # only include the latest version of MCC schemas
                        dcc = "MC2"
                        datatype = version.schema_name
                    elif organization_name == "org.synapse.nf":
                        dcc = "NF-OSI"
                        datatype = version.schema_name
                    else:
                        dcc = version.schema_name.split(".")[0]
                        datatype = version.schema_name.split(".")[1]

                    to_write_schemas.append(
                        {
                            "org": organization_name,
                            "name": version.schema_name,
                            "dcc": dcc,
                            "datatype": datatype,
                            "uri": version.id,
                            "version": version.semantic_version,
                            "link": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{version.id}",
                        }
                    )
            except Exception as e:
                print(e)

    df = pd.DataFrame(to_write_schemas)
    # exclude HTAN1, HTAN2, and NF schemas as they have their own JSONschema organizations
    df = df[~df["dcc"].isin(["htan", "htan2", "nf"])]
    table = Table(id="syn69735275").get(include_columns=True)
    table.upsert_rows(values=df, primary_keys=["uri"])


if __name__ == "__main__":
    main()
