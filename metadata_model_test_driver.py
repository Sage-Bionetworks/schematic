import json
import os

from MetadataModel import MetadataModel
from ManifestGenerator import ManifestGenerator

#inputMModelLocation = "./schemas/exampleSchemaReq.jsonld"
#inputMModelLocation = "./schemas/scRNASeq.jsonld"
inputMModelLocation = "./schemas/HTAPP.jsonld"
#inputMModelLocation = "./data/NFSchemaReq.jsonld"
inputMModelLocationType = "local"
#datasetType = "scRNASeq"
#datasetType = "Thing"
datasetType = "HTAPP"

mm = MetadataModel(inputMModelLocation, inputMModelLocationType)

print("*****************************************************")
print("Testing metamodel-based manifest generation")
print("*****************************************************")

# testing manifest generation; manifest is generated based on a jsonSchema parsed from Schema.org schema; this generates a google sheet spreadsheet; 

# to generate the sheet, the backend requires Google API credentials in a file credentials.json stored locally in the same directory as this file

# this credentials file is also stored on Synapse and can be retrieved given sufficient permissions to the Synapse project

# Google API credentials file stored on Synapse 
credentials_syn_file = "syn21088684"

# try downloading credentials file, if needed 

if not os.path.exists("./credentials.json"):
    
    print("Retrieving Google API credentials from Synapse")
    import synapseclient

    syn = synapseclient.Synapse()
    syn.login()
    syn.get(credentials_syn_file, downloadLocation = "./")
    print("Stored Google API credentials")

print("Google API credentials successfully located")

print("Testing manifest generation based on a provided Schema.org schema")
manifestURL = mm.getModelManifest("HTAN_" + datasetType, datasetType, filenames = ["1.txt", "2.txt", "3.txt"])

print(manifestURL)

print("Testing manifest generation based on a provided jsonSchema")
jsonSchemaFile = "./schemas/minimalHTAPPJSONSchema.json"
with open(jsonSchemaFile, "r") as f:
    jsonSchema = json.load(f)

mg = ManifestGenerator("HTAPP manifest", additional_metadata = {"Filename" : ["1.txt", "2.txt", "3.txt"]})

manifestURL = mg.get_manifest(jsonSchema)

print(manifestURL)


print("*****************************************************")
print("Testing metamodel-based validation")
print("*****************************************************")
manifestPath = "./HTAPP_manifest_valid.csv"

print("Testing validation with jsonSchema generation from Schema.org schema")
annotationErrors = mm.validateModelManifest(manifestPath, datasetType)
print(annotationErrors)

print("Testing validation with provided jsonSchema")
jsonSchemaFile = "./schemas/minimalHTAPPJSONSchema.json"
with open(jsonSchemaFile, "r") as f:
    jsonSchema = json.load(f)
annotationErrors = mm.validateModelManifest(manifestPath, datasetType, jsonSchema)
print(annotationErrors)


print("*****************************************************")
print("Testing metamodel-based manifest population")
print("*****************************************************")

print("Get a sheet prepopulated with an existing manifest; this is an example scRNASeq manifest")
prepopulatedManifestURL = mm.populateModelManifest("HTAN_" + datasetType, manifestPath, datasetType)
print(prepopulatedManifestURL)
