#import numpy as np
import json
import logging
import pandas as pd
import pathlib
from typing import Any, Dict, Optional, Text, List

from schematic.utils.df_utils import load_df
from schematic.utils.io_utils import load_json

from schematic.schemas.data_model_relationships import (
    DataModelRelationships
    )

from schematic import LOADER

logger = logging.getLogger(__name__)


class DataModelParser():
    '''
    This class takes in a path to a data model (either CSV for JSONLD for now)
    and will convert it to attributes and relationships that can then
    be further converted into a graph data model. Other data model types
    may be added in the future.

    TODO:
    Change all naming to fit what we will be using with the graph later. Dictionary in data_model_edges.

    Make sure to build with namespace contexts in mind!

    '''
    def __init__(
        self,
        path_to_data_model: str,
        base_schema_path: None,
        ) -> None:

        self.path_to_data_model = path_to_data_model
        self.model_type = self.get_model_type(path_to_data_model)
        self.base_schema_path = base_schema_path

    def _get_base_schema_path(self, base_schema: str = None) -> str:
        """Evaluate path to base schema.

        Args:
            base_schema: Path to base data model. BioThings data model is loaded by default.

        Returns:
            base_schema_path: Path to base schema based on provided argument.
        """
        biothings_schema_path = LOADER.filename("data_models/biothings.model.jsonld")
        base_schema_path = biothings_schema_path if base_schema is None else base_schema

        return base_schema_path

    def get_model_type(self, path_to_data_model):
        '''
        Parses the path to the data model to extract the extension and determine the data model type.
        '''
        model_type = pathlib.Path(path_to_data_model).suffix.replace('.', '').upper()
        return model_type

    def parse_base_model(self):
        '''
        Add biothings to both models for consistency.

        Do separately from both parsers for clarity.

        Should this be its own class?

        Input: Base model path, if None do not add base model.

        '''

        if self.base_schema_path == 'No base model':
            return
        else:
            # determine base schema path
            base_model_path = self._get_base_schema_path(self.base_schema_path)

            # parse
            jsonld_parser = DataModelJSONLDParser()
            base_model = jsonld_parser.parse_jsonld_model(base_model_path)
            return base_model

    def parse_model(self):
        '''
        Given a data model type, instantiate and call the appropriate data model parser.
        '''
        if self.model_type == 'CSV':
            csv_parser = DataModelCSVParser()
            model_dict = csv_parser.parse_csv_model(self.path_to_data_model)
        elif self.model_type == 'JSONLD':
            jsonld_parser = DataModelJSONLDParser()
            model_dict = jsonld_parser.parse_jsonld_model(self.path_to_data_model)

        base_model = self.parse_base_model()
        return model_dict

class DataModelCSVParser():
    '''
    
    '''

    def __init__(
        self
        ):
        self.dmr = DataModelRelationships()
        self.required_headers = self.dmr.define_required_csv_headers()


    def check_schema_definition(self, model_df: pd.DataFrame) -> bool:

        """Checks if a schema definition data frame contains the right required headers.

        See schema definition guide for more details
        TODO: post and link schema definition guide

        Args:
            schema_definition: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0
        Raises: Exception
        """
        try:
            if set(self.required_headers).issubset(set(list(model_df.columns))):
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

        # Load relationships dictionary.
        self.rel_dict = self.dmr.define_data_model_relationships()
        
        # Get the type for each value that needs to be submitted.
        # using csv_headers as keys to match required_headers/relationship_types
        self.rel_val_types = {v['csv_header']:v['type']for k, v in self.rel_dict.items() if 'type' in v.keys()}

        #load into format that can be read by validator.py
        
        # get attributes from Attribute column
        attributes = model_df[list(self.required_headers)].to_dict("records")
        
        # Build attribute/relationship dictionary
        relationship_types = self.required_headers
        #relationship_types.remove("Attribute")

        # TODO: using an attr_rel_dictionary will strip the order that attributes were submitted from
        # the user. Will need to account for ordering later so the JSONLD fields are in the correct order.
        # This will ensure the manifest dependencies are in the correct order.
        # For now, just record order with a counter.
        position  = 0
        attr_rel_dictionary = {}
        for attr in attributes:
            # For each attribute, record its position in the data model and its relationships.
            
            attr_rel_dictionary.update({
                                        attr['Attribute']: {
                                                            'Position': position,
                                                            'Relationships': {},
                                                            },

                                        }
                                       )
            for relationship in relationship_types:
                rel_val_type = self.rel_val_types[relationship]
                if not pd.isnull(attr[relationship]):
                    # Fill in relationships based on type:
                    if rel_val_type == bool and type(attr[relationship]) == bool:
                        rels = attr[relationship]
                    # Add other value types and adjust as needed.
                    elif rel_val_type == list:
                        # Move strings to list if they are comma separated.
                        # Order from CSV is preserved here.
                        rels = attr[relationship].strip().split(',')
                        rels = [r.strip() for r in rels]
                        # Extract string from list if necessary.
                        # TODO Catch situation where len does not equal 1. Throw error.
                    elif rel_val_type == str:
                        rels = str(attr[relationship]).strip()
                        #rels = attr[relationship].strip()
                    attr_rel_dictionary[attr['Attribute']]['Relationships'].update({relationship:rels})
            position += 1

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

        '''

        self.data_model_relationships = DataModelRelationships()

    def gather_jsonld_attributes_relationships(
        self,
        model_jsonld):
        '''
        Note: unlike a CSV the JSONLD might already have the biothings schema attached to it.
        So the output may not initially look identical.
        TODO Check relationship attribute types like in CSV
        
        Make sure we can take in list of types.
        '''
        model_ids = [v['rdfs:label'] for v in model_jsonld]
        attr_rel_dictionary = {}
        # For each entry in the jsonld model
        for entry in model_jsonld:
            # Check to see if it has been assigned as a subclass as an attribute or parent.
            if 'rdfs:subClassOf' in entry.keys():

                # Checking if subclass type is list, actually gets rid of Biothings.
                # TODO: Allow biothings in future.
                if type(entry['rdfs:subClassOf']) == list:
                    
                    # Determine if the id the entry has been assigned as a sublcass of is also recoreded
                    # as a model id. If it is, then the entry is not an attribute itself, but a valid value.
                    subclass_id = entry['rdfs:subClassOf'][0]['rdfs:label']
                    if not subclass_id in model_ids:
                        
                        # Get the label of the entry
                        ## To allow for contexts split by the delimiter
                        entry_id = entry['rdfs:label'].split(':')[1]

                        # If the entry is an attribute that has not already been added to the dictionary, add it.
                        if entry_id not in attr_rel_dictionary.keys():
                            attr_rel_dictionary.update({entry_id: {'Relationships': {}}})
                        
                        for relationship in self.data_model_relationships.keys():
                            if relationship in entry.keys():
                                if entry[relationship] != []:
                                    if type(entry[relationship][0]) == dict:
                                        rels = [r['rdfs:label'].split(':')[1] for r in entry[relationship]]
                                    else:
                                        rels = entry[relationship]
                                    attr_rel_dictionary[
                                        entry_id]['Relationships'].update(
                                                {k: rels for k in self.data_model_relationships[relationship].keys()})
                
        return attr_rel_dictionary

    def parse_jsonld_model(
        self,
        path_to_data_model:str,
        ):
        '''
        Note: Converting JSONLD to look *Exactly* like the csv output would get rid
        of a lot of information. Will need to decide later if we want to
        preserve this information in some way.

        '''
        # Load the json_ld model to df

        json_load = load_json(path_to_data_model)
        model_dict = self.gather_jsonld_attributes_relationships(json_load['@graph'])
        return model_dict


