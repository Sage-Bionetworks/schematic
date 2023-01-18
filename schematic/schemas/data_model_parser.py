#import numpy as np
import json
import logging
import pandas as pd
import pathlib
from typing import Any, Dict, Optional, Text, List

from schematic.utils.df_utils import load_df
from schematic.utils.io_utils import load_json

logger = logging.getLogger(__name__)


class DataModelParser():
	'''
	This class takes in a path to a data model (either CSV for JSONLD for now)
	and will convert it to attributes and relationships that can then
	be further converted into a graph data model. Other data model types
	may be added in the future.

	'''
	def __init__(
		self,
		path_to_data_model: str,
		) -> None:

		self.path_to_data_model = path_to_data_model
		self.model_type = self.get_model_type(path_to_data_model)

	def get_model_type(self, path_to_data_model):
		'''
		Parses the path to the data model to extract the extension and determine the data model type.
		'''
		model_type = pathlib.Path(path_to_data_model).suffix.replace('.', '').upper()
		return model_type

	def parse_model(self):
		'''
		Given a data model type, instantiate and call the appropriate data model parser.
		'''
		if self.model_type == 'CSV':
			csv_parser = DataModelCSVParser()
			csv_parser.parse_csv_model(self.path_to_data_model)
		elif self.model_type == 'JSONLD':
			jsonld_parser = DataModelJSONLDParser()
			jsonld_parser.parse_jsonld_model(self.path_to_data_model)
		return

class DataModelCSVParser():
	'''
	
	'''

	def __init__(
		self
		):

		self.required_headers = set(
				[
					"Attribute",
					"Description",
					"Valid Values",
					"DependsOn",
					"Required",
					"Parent",
					"Properties",
					"DependsOn Component",
					"Source",
					"Validation Rules",
				]
			)

	def check_schema_definition(self, model_df: pd.DataFrame) -> bool:

		"""Checks if a schema definition data frame contains the right required headers.

		See schema definition guide for more details
		TODO: post and link schema definition guide

		Args:
			schema_definition: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0
		Raises: Exception
		"""
		try:
			if self.required_headers.issubset(set(list(model_df.columns))):
				return
			elif "Requires" in list(model_df.columns) or "Requires Component" in list(
				model_df.columns
			):
				raise ValueError(
					"The input CSV schema file contains the 'Requires' and/or the 'Requires "
					"Component' column headers. These columns were renamed to 'DependsOn' and "
					"'DependsOn Component', respectively. Switch to the new column names."
				)
			logger.debug("Schema definition csv ready for processing!")
		except:
			raise ValueError(
				f"Schema extension headers: {set(list(model_df.columns))} "
				f"do not match required schema headers: {self.required_headers}"
			)
		return


	def gather_csv_attributes_relationships(self, model_df):
		'''
		Note: Modeled after the current df_parser.create_nx_schema_objects but without reliance
		on the SE. Will just try to gather all the attributes and their relationships to one another.
		They will be loaded into a graph at a later stage. 
		'''

		# Check csv schema follows expectations.
		self.check_schema_definition(model_df)

		#load into format that can be read by validator.py
		
		# get attributes from Attribute column
		attributes = model_df[list(self.required_headers)].to_dict("records")
		
		# Build attribute/relationship dictionary
		relationship_types = ['Parent', 'DependsOn', 'DependsOn Component']
		#Does not include anything like valid values or properties...
		#Need to add these.

		attr_rel_dictionary = {}
		for attr in attributes:
			attr_rel_dictionary.update({attr['Attribute']: {'Relationships': {}}})
			for relationship in relationship_types:
				if not pd.isnull(attr[relationship]):
					rels = attr[relationship].strip().split(',')
					attr_rel_dictionary[attr['Attribute']]['Relationships'].update({relationship:rels})

		return attr_rel_dictionary


	def parse_csv_model(
		self,
		path_to_data_model: str,
		):

		'''
		Note:
			Leave out loading the base schema for now. Add it later at the 
			model graph stage.

		'''

		# Load the csv data model to DF
		model_df = load_df(path_to_data_model, data_model=True)

		# Gather info from the model

		model_dict = self.gather_csv_attributes_relationships(model_df)

		return model_dict

class DataModelJSONLDParser():
	def __init__(
		self,
		):
		'''
		Does not include anything like valid values or properties...
		Need to add these.

		'''

		self.relationship_types = {
				'sms:requiresDependency': 'DependsOn',
				'sms:requiresComponent': 'DependsOn Component',
				'rdfs:subClassOf': 'Parent',
				'sms:validationRules': 'Validation Rules',
				'schema:rangeIncludes': 'Valid Values',
				}

	

	def gather_jsonld_attributes_relationships(
		self,
		model_jsonld):
		'''
		Note: unlike a CSV the JSONLD might already have the biothings schema attached to it.
		So the output may not initially look identical.
		'''
		model_ids = [v['@id'] for v in model_jsonld]
		attr_rel_dictionary = {}

		# For each entry in the jsonld model
		for entry in model_jsonld:

			# Check to see if it has been assigned as a subclass as an attribute or parent.
			if 'rdfs:subClassOf' in entry.keys():

				# Checking if subclass type is list, actually gets rid of Biothings.
				if type(entry['rdfs:subClassOf']) == list:
					
					# Determine if the id the entry has been assigned as a sublcass of is also recoreded
					# as a model id. If it is, then the entry is not an attribute itself, but a valid value.
					subclass_id = entry['rdfs:subClassOf'][0]['@id']
					if not subclass_id in model_ids:
						
						# Get the id of the entry
						entry_id = entry['@id'].split(':')[1]

						# If the entry is an attribute that has not already been added to the dictionary, add it.
						if entry_id not in attr_rel_dictionary.keys():
							attr_rel_dictionary.update({entry_id: {'Relationships': {}}})
						
						
						for relationship in self.relationship_types.keys():
							if relationship in entry.keys():
								if entry[relationship] != []:
									if type(entry[relationship][0]) == dict:
										rels = [r['@id'].split(':')[1] for r in entry[relationship]]
									else:
										rels = entry[relationship]
									attr_rel_dictionary[
										entry_id]['Relationships'].update(
												{self.relationship_types[relationship]:rels})
				
		return attr_rel_dictionary

	def parse_jsonld_model(
		self,
		path_to_data_model:str,
		):
		'''


		'''
		# Load the json_ld model to df

		json_load = load_json(path_to_data_model)
		model_dict = self.gather_jsonld_attributes_relationships(json_load['@graph'])
		breakpoint()

		return model_dict


