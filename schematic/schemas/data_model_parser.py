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
        base_schema_path: str = None,
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

        if not self.base_schema_path:
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
        # Instantiate DataModelRelationships
        self.dmr = DataModelRelationships()
        # Load relationships dictionary.
        self.rel_dict = self.dmr.define_data_model_relationships()
        self.edge_relationships_dictionary = self.dmr.define_edge_relationships()
        # Load required csv headers
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

        # Instantiate DataModelRelationships
        self.dmr = DataModelRelationships()
        # Load relationships dictionary.
        self.rel_dict = self.dmr.define_data_model_relationships()

    def gather_jsonld_attributes_relationships(
        self,
        model_jsonld):
        '''
        Note: unlike a CSV the JSONLD might already have the biothings schema attached to it.
        So the output may not initially look identical.
        TODO Check relationship attribute types like in CSV

        It is also just about impossible to extract attributes explicitly. Using a dictionary should avoid duplications.
        
        This is a close approximation to finding attributes and relationships but will not be convertable between csv and jsonld
        since jsonld does not have the concept of attributes. 
        
        TODO: Simplify or change this dictionary capture.
        '''
        

        # TODO: define this within the relationships class
        jsonld_keys_to_extract = ['label', 'subClassOf', 'id']
        label_jsonld_key, subclassof_jsonld_key, id_jsonld_key = [self.rel_dict[key]['jsonld_key']
                                                    for key in jsonld_keys_to_extract ]
        
        model_ids = [v[label_jsonld_key] for v in model_jsonld]
        attr_rel_dictionary = {}
        # For each entry in the jsonld model
        for entry in model_jsonld:
            # Check to see if it has been assigned as a subclass as an attribute or parent.
            if subclassof_jsonld_key in entry.keys():

                # Checking if subclass type is list, actually gets rid of Biothings.
                # TODO: Allow biothings in future (would need to handle as a dictionary)
                if type(entry[subclassof_jsonld_key]) == list and entry[subclassof_jsonld_key]:
                    
                    # Determine if the id the entry has been assigned as a sublcass of is also recoreded
                    # as a model id. If it is, then the entry is not an attribute itself, but a valid value.
                    subclass_id = entry[subclassof_jsonld_key][0][id_jsonld_key]

                    if not subclass_id in model_ids:
                        
                        # Get the label of the entry
                        entry_id = entry[label_jsonld_key]

                        # If the entry is an attribute that has not already been added to the dictionary, add it.
                        if entry_id not in attr_rel_dictionary.keys():
                            attr_rel_dictionary.update({entry_id: {'Relationships': {}}})
                        
                        # Add relationships for each attribute
                        # Right now, here we are stripping contexts, will need to track them in the future.
                        for key, val in self.rel_dict.items():
                            if val['jsonld_key'] in entry.keys() and 'csv_header' in val.keys():
                                rel_entry = entry[val['jsonld_key']]
                                if rel_entry != []:
                                    try:
                                        # add dictionary entry by itself.
                                        if type(rel_entry) == dict:
                                            rels = entry.get(val['jsonld_key'])['@id']
                                         # parse list of dictionaries to make a list of entries with context stripped (will update this section when contexts added.)
                                        elif type(rel_entry[0]) == dict:
                                            rels = [r[id_jsonld_key].split(':')[1] for r in rel_entry]
                                        elif type(rel_entry) == str:
                                            if ':' in rel_entry and 'http:' not in rel_entry:
                                                rels = rel_entry.split(':')[1]
                                                # Convert true/false strings to boolean
                                                if rels.lower() =='true':
                                                    rels = True
                                                elif rels.lower == 'false':
                                                    rels == False
                                            else:
                                                rels = rel_entry
                                        else:
                                            rels = rel_entry
                                    except:
                                        breakpoint()

                                    attr_rel_dictionary[
                                        entry_id]['Relationships'].update(
                                                {self.rel_dict[key]['csv_header']: rels})
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
