"""
Stores config variables to be used in other modules
"""

# asset store properties
storage = {
    "Synapse":{
      #synapse ID of fileview containing administrative storage metadata; 
      #"masterFileview":"syn21893840",
      "masterFileview":"syn20446927",
      # default manifest name 
      "manifestFilename":"synapse_storage_manifest.csv"
    }
}



# schema properties
schema = {
    "schemaLocation": "./schemas/HTAN.jsonld"
}


#manifest properties
manifest = {
}

# app and manifest style properties (colors, alignments, etc.)
style = {
        "googleManifest":{
            #required columns/cells background color
            "reqBgColor":{
                        'red': 235.0/255,
                        'green': 248.0/255,
                        'blue': 250.0/255
            },
            #optional columns/cells background color
            "optBgColor":{
                        'red': 255.0/255,
                        'green': 255.0/255,
                        'blue': 230.0/255
            },
            # google sheet ID of spreadsheet to be used as a template for all generated manifests
            # if empty, an empty google sheet will be created for each manifest
            "masterTemplateId":"1LYS5qE4nV9jzcYw5sXwCza25slDfRA1CIg3cs-hCdpU"
        }
}
