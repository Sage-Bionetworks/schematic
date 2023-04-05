import gc
import json
import logging
import numpy as np
import os
import pandas as pd
from typing import Any, Dict, Optional, Text, List

from schematic.schemas import SchemaGenerator
from schematic.utils.io_utils import load_json

logger = logging.getLogger(__name__)

class AttributesExplorer():
    def __init__(self,
                 path_to_jsonld: str,
                 )-> None:
        
        self.path_to_jsonld = path_to_jsonld
        self.json_data_model = load_json(self.path_to_jsonld)
        self.jsonld = load_json(self.path_to_jsonld)

        # instantiate a schema generator to retrieve db schema graph from metadata model graph
        self.sg = SchemaGenerator(self.path_to_jsonld)

        self.output_path = self.create_output_path('merged_csv')
    
    def create_output_path(self, terminal_folder):
        ''' Create output path to store Observable visualization data if it does not already exist.
        
        Args: self.path_to_jsonld
        
        Returns: output_path (str): path to store outputs 
        '''
        base_dir = os.path.dirname(self.path_to_jsonld)
        self.schema_name = self.path_to_jsonld.split('/')[-1].split('.model.jsonld')[0]
        output_path = os.path.join(base_dir, 'visualization', self.schema_name, terminal_folder)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        return output_path

    def convert_string_cols_to_json(self, df: pd.DataFrame, cols_to_modify: list):
        """Converts values in a column from strings to JSON list 
        for upload to Synapse.
        """
        for col in df.columns:
            if col in cols_to_modify:
                df[col] = df[col].apply(lambda x: json.dumps([y.strip() for y in x]) if x != "NaN" and x  and x == np.nan else x)
        return df

    def parse_attributes(self, save_file=True):
        '''
        Args: save_file (bool):
                True: merged_df is saved locally to output_path.
                False: merged_df is returned.

        Returns:
            merged_df (pd.DataFrame): dataframe containing data relating to attributes
                for the provided data model for all components in the data model. 
                Dataframe is saved locally as a csv if save_file == True, or returned if
                save_file == False. 
                
        '''
        # get all components
        component_dg = self.sg.se.get_digraph_by_edge_type('requiresComponent')
        components = component_dg.nodes()
        
        # For each data type to be loaded gather all attribtes the user would
        # have to provide.
        return self._parse_attributes(components, save_file)
    
    def parse_component_attributes(self, component=None, save_file=True, include_index=True):
        '''
        Args: save_file (bool):
                True: merged_df is saved locally to output_path.
                False: merged_df is returned.
              include_index (bool):
                Whether to include the index in the returned dataframe (True) or not (False)

        Returns:
            merged_df (pd.DataFrame): dataframe containing data relating to attributes
                for the provided data model for the specified component in the data model. 
                Dataframe is saved locally as a csv if save_file == True, or returned if
                save_file == False. 
        '''        

        if not component:
            raise ValueError("You must provide a component to visualize.")
        else:
            return self._parse_attributes([component], save_file, include_index)

    def _parse_attributes(self, components, save_file=True, include_index=True):
        '''
        Args: save_file (bool):
                True: merged_df is saved locally to output_path.
                False: merged_df is returned.
              components (list):
                list of components to parse attributes for
              include_index (bool):
                Whether to include the index in the returned dataframe (True) or not (False)

        Returns:
            merged_df (pd.DataFrame): dataframe containing data relating to attributes
                for the provided data model for specified components in the data model. 
                Dataframe is saved locally as a csv if save_file == True, or returned if
                save_file == False. 
        Raises:
            ValueError:
                If unable hits an error while attempting to get conditional requirements. 
                This error is likely to be found if there is a mismatch in naming.
        '''
        
        # For each data type to be loaded gather all attribtes the user would
        # have to provide.
        df_store = []
        for component in components:
            data_dict = {}
            # get the json schema
            json_schema = self.sg.get_json_schema_requirements(
                source_node=component, schema_name=self.path_to_jsonld)

            # Gather all attribues, their valid values and requirements
            for key, value in json_schema['properties'].items():
                data_dict[key] = {}
                for k, v in value.items():
                    if k == 'enum':
                        data_dict[key]['Valid Values'] = value['enum']
                if key in json_schema['required']:
                    data_dict[key]['Required'] = True
                else:
                    data_dict[key]['Required'] = False
                data_dict[key]['Component'] = component
            # Add additional details per key (from the JSON-ld)
            for dic in self.jsonld['@graph']:
                if 'sms:displayName' in dic.keys():
                    key = dic['sms:displayName']
                    if key in data_dict.keys():
                        data_dict[key]['Attribute'] = dic['sms:displayName']
                        data_dict[key]['Label'] = dic['rdfs:label']
                        data_dict[key]['Description'] = dic['rdfs:comment']
                        if 'validationRules' in dic.keys():
                            data_dict[key]['Validation Rules'] = dic['validationRules']
            # Find conditional dependencies
            if 'allOf' in json_schema.keys():
                for conditional_dependencies in json_schema['allOf']:
                    key = list(conditional_dependencies['then']['properties'])[0]
                    try:
                        if key in data_dict.keys():
                            if 'Cond_Req' not in data_dict[key].keys():
                                data_dict[key]['Cond_Req'] = []
                                data_dict[key]['Conditional Requirements'] = []
                            attribute = list(conditional_dependencies['if']['properties'])[0]
                            value = conditional_dependencies['if']['properties'][attribute]['enum']
                            # Capitalize attribute if it begins with a lowercase letter, for aesthetics.
                            if attribute[0].islower():
                                attribute = attribute.capitalize()

                            # Remove "Type" (i.e. turn "Biospecimen Type" to "Biospcimen")
                            if "Type" in attribute: 
                                attribute = attribute.split(" ")[0]
                            
                            # Remove "Type" (i.e. turn "Tissue Type" to "Tissue")
                            if "Type" in value[0]:
                                value[0] = value[0].split(" ")[0]

                            conditional_statement = f'{attribute} is "{value[0]}"'
                            if conditional_statement not in data_dict[key]['Conditional Requirements']:
                                data_dict[key]['Cond_Req'] = True
                                data_dict[key]['Conditional Requirements'].extend([conditional_statement])
                    except:
                        raise ValueError(
                            f"There is an error getting conditional requirements related "
                            "to the attribute: {key}. The error is likely caused by naming inconsistencies (e.g. uppercase, camelcase, ...)"
                        )

            for key, value in data_dict.items():
                if 'Conditional Requirements' in value.keys():

                    ## reformat conditional requirement 

                    # get all attributes 
                    attr_lst = [i.split(" is ")[-1] for i in data_dict[key]['Conditional Requirements']]
                    
                    # join a list of attributes by using OR 
                    attr_str = " OR ".join(attr_lst)

                    # reformat the conditional requirement 
                    component_name = data_dict[key]['Conditional Requirements'][0].split(' is ')[0]
                    conditional_statement_str = f' If {component_name} is {attr_str} then "{key}" is required'

                    data_dict[key]['Conditional Requirements'] = conditional_statement_str
            df = pd.DataFrame(data_dict)
            df = df.T
            cols = ['Attribute', 'Label', 'Description', 'Required', 'Cond_Req', 'Valid Values', 'Conditional Requirements', 'Validation Rules', 'Component']
            cols = [col for col in cols if col in df.columns]
            df = df[cols]
            df = self.convert_string_cols_to_json(df, ['Valid Values'])
            #df.to_csv(os.path.join(csv_output_path, data_type + '.vis_data.csv'))
            df_store.append(df)

        merged_attributes_df = pd.concat(df_store, join='outer')
        cols = ['Attribute', 'Label', 'Description', 'Required', 'Cond_Req', 'Valid Values', 'Conditional Requirements', 'Validation Rules', 'Component']
        cols = [col for col in cols if col in merged_attributes_df.columns]

        merged_attributes_df = merged_attributes_df[cols]
        if save_file == True:
            return merged_attributes_df.to_csv(os.path.join(self.output_path, self.schema_name + 'attributes_data.vis_data.csv'), index=include_index)
        elif save_file == False:
            return merged_attributes_df.to_csv(index=include_index)
