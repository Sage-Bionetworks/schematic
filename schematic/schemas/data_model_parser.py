import logging
import pandas as pd
import pathlib
from typing import Any, Dict, Optional, Text, List, Union

from schematic.utils.df_utils import load_df
from schematic.utils.io_utils import load_json
from schematic.utils.schema_utils import attr_dict_template

from schematic.schemas.data_model_relationships import DataModelRelationships

from schematic import LOADER

logger = logging.getLogger("Synapse storage")


class DataModelParser:
    """
    This class takes in a path to a data model and will convert it to an
    attributes:relationship dictionarythat can then be further converted into a graph data model.
    Other data model types may be added in the future.
    """

    def __init__(
        self,
        path_to_data_model: str,
    ) -> None:
        """
        Args:
            path_to_data_model, str: path to data model.
        """

        self.path_to_data_model = path_to_data_model
        self.model_type = self.get_model_type()
        self.base_schema_path = None

    def _get_base_schema_path(self, base_schema: str = None) -> str:
        """Evaluate path to base schema.

        Args:
            base_schema: Path to base data model. BioThings data model is loaded by default.

        Returns:
            base_schema_path: Path to base schema based on provided argument.
        """
        biothings_schema_path = LOADER.filename("data_models/biothings.model.jsonld")
        self.base_schema_path = (
            biothings_schema_path if base_schema is None else base_schema
        )

        return self.base_schema_path

    def get_model_type(self) -> str:
        """Parses the path to the data model to extract the extension and determine the data model type.
        Args:
            path_to_data_model, str: path to data model
        Returns:
            str: uppercase, data model file extension.
        Note: Consider moving this to Utils.
        """
        return pathlib.Path(self.path_to_data_model).suffix.replace(".", "").upper()

    def parse_base_model(self) -> Dict:
        """Parse base data model that new model could be built upon.
        Returns:
            base_model, dict:
                    {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
        Note: Not configured yet to successfully parse biothings.
        """

        # Determine base schema path
        base_model_path = self._get_base_schema_path(self.base_schema_path)

        # Parse
        jsonld_parser = DataModelJSONLDParser()
        base_model = jsonld_parser.parse_jsonld_model(base_model_path)
        return base_model

    def parse_model(self) -> Dict[str, dict[str, Any]]:
        """Given a data model type, instantiate and call the appropriate data model parser.
        Returns:
            model_dict, dict:
                {Attribute Display Name: {
                        Relationships: {
                                    CSV Header: Value}}}
        Raises:
            Value Error if an incorrect model type is passed.
        Note: in future will add base model parsing in this step too and extend new model off base model.
        """
        # base_model = self.parse_base_model()

        # Call appropriate data model parser and return parsed model.
        if self.model_type == "CSV":
            csv_parser = DataModelCSVParser()
            model_dict = csv_parser.parse_csv_model(self.path_to_data_model)
        elif self.model_type == "JSONLD":
            jsonld_parser = DataModelJSONLDParser()
            model_dict = jsonld_parser.parse_jsonld_model(self.path_to_data_model)
        else:
            raise ValueError(
                f"Schematic only accepts models of type CSV or JSONLD, you provided a model type {self.model_type}, please resubmit in the proper format."
            )
        return model_dict


class DataModelCSVParser:
    def __init__(self):
        # Instantiate DataModelRelationships
        self.dmr = DataModelRelationships()
        # Load relationships dictionary.
        self.rel_dict = self.dmr.define_data_model_relationships()
        # Get edge relationships
        self.edge_relationships_dictionary = self.dmr.retreive_rel_headers_dict(
            edge=True
        )
        # Load required csv headers
        self.required_headers = self.dmr.define_required_csv_headers()
        # Get the type for each value that needs to be submitted.
        # using csv_headers as keys to match required_headers/relationship_types
        self.rel_val_types = {
            v["csv_header"]: v["type"]
            for k, v in self.rel_dict.items()
            if "type" in v.keys()
        }

    def check_schema_definition(self, model_df: pd.DataFrame) -> bool:
        """Checks if a schema definition data frame contains the right required headers.
        Args:
            model_df: a pandas dataframe containing schema definition; see example here: https://docs.google.com/spreadsheets/d/1J2brhqO4kpeHIkNytzlqrdIiRanXDr6KD2hqjOTC9hs/edit#gid=0
        Raises: Exception if model_df does not have the required headers.
        """
        if set(self.required_headers).issubset(set(list(model_df.columns))):
            logger.debug("Schema definition csv ready for processing!")
            return
        elif "Requires" in list(model_df.columns) or "Requires Component" in list(
            model_df.columns
        ):
            raise ValueError(
                "The input CSV schema file contains the 'Requires' and/or the 'Requires "
                "Component' column headers. These columns were renamed to 'DependsOn' and "
                "'DependsOn Component', respectively. Switch to the new column names."
            )
        elif not set(self.required_headers).issubset(set(list(model_df.columns))):
            raise ValueError(
                f"Schema extension headers: {set(list(model_df.columns))} "
                f"do not match required schema headers: {self.required_headers}"
            )
        return

    def parse_entry(self, attr: dict, relationship: str) -> Any:
        """Parse attr entry baed on type
        Args:
            attr, dict: single row of a csv model in dict form, where only the required headers are keys. Values are the entries under each header.
            relationship, str: one of the header relationships to parse the entry of.
        Returns:
            parsed_rel_entry, any: parsed entry for downstream processing based on the entry type.
        """

        rel_val_type = self.rel_val_types[relationship]
        # Parse entry based on type:
        # If the entry should be preserved as a bool dont convert to str.
        if rel_val_type == bool and type(attr[relationship]) == bool:
            parsed_rel_entry = attr[relationship]
        # Move strings to list if they are comma separated. Schema order is preserved, remove any empty strings added by trailing commas
        elif rel_val_type == list:
            parsed_rel_entry = attr[relationship].strip().split(",")
            parsed_rel_entry = [r.strip() for r in parsed_rel_entry if r]
        # Convert value string if dictated by rel_val_type, strip whitespace.
        elif rel_val_type == str:
            parsed_rel_entry = str(attr[relationship]).strip()
        else:
            raise ValueError(
                "The value type recorded for this relationship, is not currently supported for CSV parsing. Please check with your DCC."
            )
        return parsed_rel_entry

    def gather_csv_attributes_relationships(
        self, model_df: pd.DataFrame
    ) -> Dict[str, dict[str, Any]]:
        """Parse csv into a attributes:relationshps dictionary to be used in downstream efforts.
        Args:
            model_df: pd.DataFrame, data model that has been loaded into pandas DataFrame.
        Returns:
            attr_rel_dictionary: dict,
                {Attribute Display Name: {
                    Relationships: {
                                    CSV Header: Value}}}
        """
        # Check csv schema follows expectations.
        self.check_schema_definition(model_df)

        # get attributes from Attribute column
        attributes = model_df[list(self.required_headers)].to_dict("records")

        # Build attribute/relationship dictionary
        relationship_types = self.required_headers
        attr_rel_dictionary = {}

        for attr in attributes:
            attribute_name = attr["Attribute"]
            # Add attribute to dictionary
            attr_rel_dictionary.update(attr_dict_template(attribute_name))
            # Fill in relationship info for each attribute.
            for relationship in relationship_types:
                if not pd.isnull(attr[relationship]):
                    parsed_rel_entry = self.parse_entry(
                        attr=attr, relationship=relationship
                    )
                    attr_rel_dictionary[attribute_name]["Relationships"].update(
                        {relationship: parsed_rel_entry}
                    )
        return attr_rel_dictionary

    def parse_csv_model(
        self,
        path_to_data_model: str,
    ):
        """Load csv data model and parse into an attributes:relationships dictionary
        Args:
            path_to_data_model, str: path to data model
        Returns:
            model_dict, dict:{Attribute Display Name: {
                                                Relationships: {
                                                        CSV Header: Value}}}
        """
        # Load the csv data model to DF
        model_df = load_df(path_to_data_model, data_model=True)

        # Gather info from the model
        model_dict = self.gather_csv_attributes_relationships(model_df)

        return model_dict


class DataModelJSONLDParser:
    def __init__(
        self,
    ):
        # Instantiate DataModelRelationships
        self.dmr = DataModelRelationships()
        # Load relationships dictionary.
        self.rel_dict = self.dmr.define_data_model_relationships()

    def parse_entry(self, rel_entry: any, id_jsonld_key: str) -> Any:
        """Parse an input entry based on certain attributes
        Args:
            rel_entry: Given a single entry and relationship in a JSONLD data model, the recorded value
            id_jsonld_key, str: the jsonld key for id
        Returns:
            parsed_rel_entry: an entry that has been parsed base on its input type and characteristics.
        """
        # Retrieve ID from single value dictionary
        if type(rel_entry) == dict and len(rel_entry.keys()) == 1:
            parsed_rel_entry = rel_entry["@id"]
        # Parse list of dictionaries to make a list of entries with context stripped (will update this section when contexts added.)
        elif type(rel_entry) == list and type(rel_entry[0]) == dict:
            parsed_rel_entry = [r[id_jsonld_key].split(":")[1] for r in rel_entry]
        # Strip context from string and convert true/false to bool
        elif type(rel_entry) == str:
            # Remove contexts and treat strings as appropriate.
            if ":" in rel_entry and "http:" not in rel_entry:
                parsed_rel_entry = rel_entry.split(":")[1]
                # Convert true/false strings to boolean
                if parsed_rel_entry.lower() == "true":
                    parsed_rel_entry = True
                elif parsed_rel_entry.lower == "false":
                    parsed_rel_entry = False
            else:
                parsed_rel_entry = rel_entry
        # For anything else get that
        else:
            parsed_rel_entry = rel_entry
        return parsed_rel_entry

    '''
    def get_display_name_from_label(self, label, model_jsonld):
        jsonld_keys_to_extract = ["label", "displayName"]
        label_jsonld_key, dn_jsonld_key = [
            self.rel_dict[key]["jsonld_key"] for key in jsonld_keys_to_extract
        ]
        breakpoint()
        for entry in model_jsonld:
            # Get the attr key for the dictionary
            if dn_jsonld_key in entry:
                # The attr_key is the entry display name if one was recorded
                attr_key = entry[dn_jsonld_key]
            else:
                # If not we wil use the get the label.
                attr_key = entry[label_jsonld_key]
        return attr_key
    '''
    def label_to_dn_dict(self, model_jsonld: list[dict]):
        """ Generate a dictionary of labels to display name, so can easily look up display names using the label.
        Args:
            model_jsonld: list of dictionaries, each dictionary is an entry in the jsonld data model
        Returns:
            dn_label_dict: dict of model labels to display names
        """
        jsonld_keys_to_extract = ["label", "displayName"]
        label_jsonld_key, dn_jsonld_key = [
            self.rel_dict[key]["jsonld_key"] for key in jsonld_keys_to_extract
        ]
        dn_label_dict = {}
        for entry in model_jsonld:
            dn_label_dict[entry[label_jsonld_key]]=entry[dn_jsonld_key]
        return dn_label_dict

    def convert_entry_to_dn_label(self, parsed_rel_entry:Union[str,list], model_jsonld:list[dict]) -> Union[str,list]:
        """Convert a parsed entry to display name, taking into account the entry type
        Args:
            parsed_rel_entry: an entry that has been parsed base on its input type and characteristics.
            model_jsonld: list of dictionaries, each dictionary is an entry in the jsonld data model
        Returns:
            parsed_rel_entry: an entry that has been parsed based on its input type and characteristics, and converted to display names.
        """
        # Get a dictionary of display_names mapped to labels
        dn_label_dict = self.label_to_dn_dict(model_jsonld=model_jsonld)

        # Handle if using the display name as the label
        if type(parsed_rel_entry) == list:
            parsed_rel_entry = [dn_label_dict.get(entry) if dn_label_dict.get(entry) else entry for entry in parsed_rel_entry ]
        elif type(parsed_rel_entry) == str:
            converted_label = dn_label_dict.get(parsed_rel_entry)
            if converted_label:
                parsed_rel_entry = dn_label_dict.get(parsed_rel_entry)
        return parsed_rel_entry

    def gather_jsonld_attributes_relationships(self, model_jsonld: List[dict]) -> Dict:
        """
        Args:
            model_jsonld: list of dictionaries, each dictionary is an entry in the jsonld data model
        Returns:
            attr_rel_dictionary: dict,
                {Node Display Name:
                    {Relationships: {
                                     CSV Header: Value}}}
        Notes:
            - Unlike a CSV the JSONLD might already have a base schema attached to it.
              So the attributes:relationship dictionary for importing a CSV vs JSONLD may not match.
            - It is also just about impossible to extract attributes explicitly. Using a dictionary should avoid duplications.
            - This is a promiscuous capture and will create an attribute for each model entry.
            - Currently only designed to capture the same information that would be encoded in CSV,
                can be updated in the future.
        TODO:
            - Find a way to delete non-attribute keys, is there a way to reliable distinguish after the fact?
            - Right now, here we are stripping contexts, will need to track them in the future.
        """

        # Retrieve relevant JSONLD keys.
        jsonld_keys_to_extract = ["label", "subClassOf", "id", "displayName"]
        label_jsonld_key, subclassof_jsonld_key, id_jsonld_key, dn_jsonld_key = [
            self.rel_dict[key]["jsonld_key"] for key in jsonld_keys_to_extract
        ]

        # Get a dictionary of display names to labels to identify values explicitly recorded
        dn_label_dict = self.label_to_dn_dict(model_jsonld=model_jsonld)

        # Build the attr_rel_dictionary
        attr_rel_dictionary = {}
        # Move through each entry in the jsonld model
        for entry in model_jsonld:
            # Get the attr key for the dictionary
            if dn_jsonld_key in entry:
                # The attr_key is the entry display name if one was recorded
                attr_key = entry[dn_jsonld_key]
            else:
                # If not we wil use the get the label.
                attr_key = entry[label_jsonld_key]

            # If the entry has not already been added to the dictionary, add it.
            if attr_key not in attr_rel_dictionary.keys():
                attr_rel_dictionary.update(attr_dict_template(attr_key))

            # Add relationships for each entry
            # Go through each defined relationship type (rel_key) and its attributes (rel_vals)
            for rel_key, rel_vals in self.rel_dict.items():
                # Determine if current entry in the for loop, can be described by the current relationship that is being cycled through.
                # used to also check "csv_header" in rel_vals.keys() which allows all JSONLD values through even if it does not have a CSV counterpart, will allow other values thorough in the else statement now
                if (
                    rel_vals["jsonld_key"] in entry.keys()
                    and rel_vals["csv_header"]
                ):
                    # Retrieve entry value associated with the given relationship
                    rel_entry = entry[rel_vals["jsonld_key"]]
                    # If there is an entry parse it by type and add to the attr:relationships dictionary.
                    if rel_entry:
                        parsed_rel_entry = self.parse_entry(
                            rel_entry=rel_entry, id_jsonld_key=id_jsonld_key
                        )
                        rel_csv_header = self.rel_dict[rel_key]["csv_header"]

                        #if rel_key == 'domainIncludes' or rel_key == 'subClassOf':
                        if rel_key == 'domainIncludes':
                            # In the JSONLD the domain includes field contains the ids of attributes that the current attribute is the property/parent of.
                            # Because of this we need to handle these values differently.
                            # We will get the values in the field (parsed_val), then add the current attribute as to the property key in the attr_rel_dictionary[p_attr_key].
                            for parsed_val in parsed_rel_entry:
                                attr_in_dict = False
                                #Get propert/parent key (displayName)
                                p_attr_key=''
                                # Check if the parsed value is already a part of the attr_rel_dictionary
                                for attr_dn, rels in attr_rel_dictionary.items():
                                    if parsed_val == rels["Relationships"].get('label'):
                                        p_attr_key = attr_dn
                                        attr_in_dict = True
                                # If it is part of the dictionary update add current attribute as a property of the parsed value
                                if attr_in_dict == True:

                                    if not rel_csv_header in attr_rel_dictionary[p_attr_key]["Relationships"]:
                                        attr_rel_dictionary[p_attr_key]["Relationships"].update({rel_csv_header:[entry[label_jsonld_key]]})
                                    else:
                                        attr_rel_dictionary[p_attr_key]["Relationships"][rel_csv_header].append(entry[label_jsonld_key])
                                # If the parsed_val is not already recorded in the dictionary, add it
                                elif attr_in_dict == False:
                                    # Get the display name for the parsed value
                                    p_attr_key = self.convert_entry_to_dn_label(parsed_val, model_jsonld)
                                    
                                    attr_rel_dictionary.update(attr_dict_template(p_attr_key))
                                    attr_rel_dictionary[p_attr_key]["Relationships"].update({rel_csv_header:[entry[label_jsonld_key]]})
                        else:
                            attr_rel_dictionary[attr_key]["Relationships"].update(
                                {rel_csv_header: parsed_rel_entry}
                            )
                        
                elif (
                    rel_vals["jsonld_key"] in entry.keys()
                    and not rel_vals["csv_header"]
                ):
                    # Retrieve entry value associated with the given relationship
                    rel_entry = entry[rel_vals["jsonld_key"]]
                    # If there is an entry parset it by type and add to the attr:relationships dictionary.
                    if rel_entry:
                        parsed_rel_entry = self.parse_entry(
                            rel_entry=rel_entry, id_jsonld_key=id_jsonld_key
                        )
                        # Add relationships for each attribute and relationship to the dictionary
                        attr_rel_dictionary[attr_key]["Relationships"].update(
                            {rel_key: parsed_rel_entry}
                        )
        return attr_rel_dictionary

    def parse_jsonld_model(
        self,
        path_to_data_model: str,
    ):
        """Convert raw JSONLD data model to attributes relationship dictionary.
        Args:
            path_to_data_model: str, path to JSONLD data model
        Returns:
            model_dict: dict,
                {Node Display Name:
                    {Relationships: {
                                     CSV Header: Value}}}
        """
        # Log warning that JSONLD parsing is in beta mode.
        logger.warning(
            "JSONLD parsing is in Beta Mode. Please inspect outputs carefully and report any errors."
        )
        # Load the json_ld model to df
        json_load = load_json(path_to_data_model)
        # Convert dataframe to attributes relationship dictionary.
        model_dict = self.gather_jsonld_attributes_relationships(json_load["@graph"])
        return model_dict
