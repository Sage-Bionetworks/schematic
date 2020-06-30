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
            }
        }
}
