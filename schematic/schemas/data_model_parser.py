class DataModelParser():
	def __init__(
		path_to_data_model: str,
		):

		model_type = self.get_model_type(path_to_data_model)
		parse_model(model_type)

	def get_model_type(self, path_to_data_model):
		
		return

	def parse_model(self, model_type)
		if model_type == 'csv'
			DataModelCSVParser.parse_csv_model()
		elif model_type == 'jsonld'
			DataModelJSONLDParser.parse_jsonld_model()
		return

class DataModelCSVParser():
	def __init__():

class DataModelJSONLDParser():
	def __init__():

