"""
Stores config variables to be used in other modules
"""

storage = {
    "Synapse": {
        "masterFileview": "syn20446927",    # synapse ID of fileview containing administrative storage metadata
        "manifestFilename": "./data/synapse_storage_manifest.csv",  # default manifest name
        "username": "",
        "password": "",
        "api_creds": "syn21088684"
    }
}

model = {
    "input": {
        "model_location": "./data/schema_org_schemas/HTAN.jsonld",
        "model_file_type": "local"
    },
    "demo": {
        "htapp_location": "./data/schema_org_schemas/HTAPP.jsonld",
        "htapp_file_type": "local",
        "htapp_validation_file_location": "./data/validation_schemas/minimalHTAPPJSONSchema.json",
        "valid_manifest": "./data/manifests/HTAPP_manifest_valid.csv"
    }
}

schema = {
    "schemaLocation": "./schemas/HTAN.jsonld"
}

# app and manifest style (colors, alignments, etc.)
style = {
        "googleManifest": {
            "reqBgColor": {                 # required columns/cells background color
                        'red': 235.0/255,
                        'green': 248.0/255,
                        'blue': 250.0/255
            },
            "optBgColor": {                 # optional columns/cells background color
                        'red': 255.0/255,
                        'green': 255.0/255,
                        'blue': 230.0/255
            },
            # google sheet ID of spreadsheet to be used as a template for all generated manifests
            # if empty, an empty google sheet will be created for each manifest
            "masterTemplateId":"1LYS5qE4nV9jzcYw5sXwCza25slDfRA1CIg3cs-hCdpU"
        }
}
