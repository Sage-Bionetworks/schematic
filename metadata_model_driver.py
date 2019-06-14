from MetadataModel import MetadataModel

inputMModelLocation = "./schemas/exampleSchemaReq.jsonld"
inputMModelLocationType = "local"
datasetType = "Thing"

mm = MetadataModel(inputMModelLocation, inputMModelLocationType)

manifest_url = mm.getModelManifest(datasetType)

print(manifest_url)
