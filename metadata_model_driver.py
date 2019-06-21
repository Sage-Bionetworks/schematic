from MetadataModel import MetadataModel

#inputMModelLocation = "./schemas/exampleSchemaReq.jsonld"
inputMModelLocation = "./schemas/scRNASeq.jsonld"
inputMModelLocationType = "local"
datasetType = "scRNASeq"

mm = MetadataModel(inputMModelLocation, inputMModelLocationType)

manifest_url = mm.getModelManifest(datasetType, additionalMetadata = {"Filename":["1.txt", "2.txt", "3.txt"]})

print(manifest_url)


'''
manifest_path = "./manifest.csv"

annotation_status = mm.validateModelManifest(manifest_path, datasetType)

print(annotation_status)
'''
