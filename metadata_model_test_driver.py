import json
import os
import pprint

from MetadataModel import MetadataModel
from ManifestGenerator import ManifestGenerator

pp = pprint.PrettyPrinter(indent = 3)

#inputMModelLocation = "./schemas/exampleSchemaReq.jsonld"
#inputMModelLocation = "./schemas/scRNASeq.jsonld"
#inputMModelLocation = "./schemas/HTAPP.jsonld"
#inputMModelLocation = "./schemas/HTAN.jsonld"
#inputMModelLocation = "./data/NFSchemaReq.jsonld"
inputMModelLocation = "./schemas/Test.jsonld"
inputMModelLocationType = "local"
#modelType = "scRNASeq"
#modelType = "Thing"
#modelType = "ScRNA-seq"

modelType = "TableA" 

mm = MetadataModel(inputMModelLocation, inputMModelLocationType)
"""
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
manifestURL = mm.getModelManifest("Test_" + modelType, modelType, filenames = ["1.txt", "2.txt", "3.txt"])

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
annotationErrors = mm.validateModelManifest(manifestPath, modelType)
pp.pprint(annotationErrors)

print("Testing validation with provided jsonSchema")
jsonSchemaFile = "./schemas/minimalHTAPPJSONSchema.json"
with open(jsonSchemaFile, "r") as f:
    jsonSchema = json.load(f)
annotationErrors = mm.validateModelManifest(manifestPath, modelType, jsonSchema)
pp.pprint(annotationErrors)


print("*****************************************************")
print("Testing metamodel-based manifest population")
print("*****************************************************")

print("Get a sheet prepopulated with an existing manifest; this is an example manifest")
prepopulatedManifestURL = mm.populateModelManifest("Test_" + modelType, manifestPath, modelType)
print(prepopulatedManifestURL)
"""

"""
print("*****************************************************")
print("Testing metamodel-based object dependency generation")
print("*****************************************************")

print("Generating dependency graph and ordering dependencies")
dependencies = mm.getOrderedModelNodes(modelType, "requiresDependency")
pp.pprint(dependencies)

with open(modelType + "_dependencies.json", "w") as f:
    json.dump(dependencies, f, indent = 3)

print("Dependencies stored: " + modelType + "_dependencies.json")
"""

print("*****************************************************")
print("Testing metamodel-based component dependency generation")
print("*****************************************************")

print("Generating dependency graph and ordering dependencies")
modelType = "CoordinationCenterProject"
dependencies = mm.getOrderedModelNodes(modelType, "requiresComponent")
pp.pprint(dependencies)

with open(modelType + "component_dependencies.json", "w") as f:
    json.dump(dependencies, f, indent = 3)

print("Component dependencies stored: " + modelType + "component_dependencies.json")

