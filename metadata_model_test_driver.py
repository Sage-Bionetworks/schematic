import json
from MetadataModel import MetadataModel
from manifest_generator import ManifestGenerator

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

# testing manifest generation; manifest is generated based on a jsonSchema parsed from Schema.org schema
manifestURL = mm.getModelManifest("HTAN_" + datasetType, datasetType, filenames = ["1.txt", "2.txt", "3.txt"])

print(manifestURL)

# testing manifest generation based on a provided jsonSchema
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

# testing validation with jsonSchema generation from Schema.org schema
annotationErrors = mm.validateModelManifest(manifestPath, datasetType)
print(annotationErrors)

# testing validation with provided jsonSchema
jsonSchemaFile = "./schemas/minimalHTAPPJSONSchema.json"
with open(jsonSchemaFile, "r") as f:
    jsonSchema = json.load(f)
annotationErrors = mm.validateModelManifest(manifestPath, datasetType, jsonSchema)
print(annotationErrors)


print("*****************************************************")
print("Testing metamodel-based manifest population")
print("*****************************************************")

# get a sheet prepopulated with an existing manifest; returns a url to a google sheet; (this is an example scRNASeq manifest
prepopulatedManifestURL = mm.populateModelManifest("HTAN_" + datasetType, manifestPath, datasetType)
print(prepopulatedManifestURL)
