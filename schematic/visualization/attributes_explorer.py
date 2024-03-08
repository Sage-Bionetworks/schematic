"""Attributes Explorer Class"""
import json
import logging
import os
from typing import Optional, no_type_check
import numpy as np
import pandas as pd

from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.utils.schema_utils import DisplayLabelType
from schematic.utils.io_utils import load_json

logger = logging.getLogger(__name__)


class AttributesExplorer:
    """AttributesExplorer class"""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        path_to_jsonld: str,
        data_model_labels: DisplayLabelType,
        data_model_grapher: Optional[DataModelGraph] = None,
        data_model_graph_explorer: Optional[DataModelGraphExplorer] = None,
        parsed_data_model: Optional[dict] = None,
    ) -> None:
        self.path_to_jsonld = path_to_jsonld

        self.jsonld = load_json(self.path_to_jsonld)

        # Parse Model
        if not parsed_data_model:
            data_model_parser = DataModelParser(
                path_to_data_model=self.path_to_jsonld,
            )
            parsed_data_model = data_model_parser.parse_model()

        # Instantiate DataModelGraph
        if not data_model_grapher:
            data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

        # Generate graph
        self.graph_data_model = data_model_grapher.graph

        # Instantiate Data Model Graph Explorer
        if not data_model_graph_explorer:
            self.dmge = DataModelGraphExplorer(self.graph_data_model)
        else:
            self.dmge = data_model_graph_explorer

        # Instantiate Data Model Json Schema
        self.data_model_js = DataModelJSONSchema(
            jsonld_path=self.path_to_jsonld, graph=self.graph_data_model
        )

        self.output_path = self.create_output_path("merged_csv")

    def create_output_path(self, terminal_folder: str) -> str:
        """Create output path to store Observable visualization data if it does not already exist.

        Args: self.path_to_jsonld

        Returns: output_path (str): path to store outputs
        """
        base_dir = os.path.dirname(self.path_to_jsonld)
        self.schema_name = self.path_to_jsonld.split("/")[-1].split(".model.jsonld")[0]
        output_path = os.path.join(
            base_dir, "visualization", self.schema_name, terminal_folder
        )
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        return output_path

    def _convert_string_cols_to_json(
        self, dataframe: pd.DataFrame, cols_to_modify: list[str]
    ) -> pd.DataFrame:
        """Converts values in a column from strings to JSON list
        for upload to Synapse.
        """
        for col in dataframe.columns:
            if col in cols_to_modify:
                dataframe[col] = dataframe[col].apply(
                    lambda x: json.dumps([y.strip() for y in x])
                    if x != "NaN" and x and x == np.nan
                    else x
                )
        return dataframe

    def parse_attributes(self, save_file: bool = True) -> Optional[str]:
        """
        Args:
            save_file (bool, optional):
              True: merged_df is saved locally to output_path.
              False: merged_df is returned as a string
              Defaults to True.

        Returns:
            Optional[str]: if save_file=False, the dataframe as a string, otherwise None

        """
        # get all components
        component_dg = self.dmge.get_digraph_by_edge_type("requiresComponent")
        components = component_dg.nodes()

        # For each data type to be loaded gather all attributes the user would
        # have to provide.
        return self._parse_attributes(components, save_file)

    def _parse_component_attributes(
        self,
        component: Optional[str] = None,
        save_file: bool = True,
        include_index: bool = True,
    ) -> Optional[str]:
        """

        Args:
            component (Optional[str], optional): A component. Defaults to None.
            save_file (bool, optional):
              True: merged_df is saved locally to output_path.
              False: merged_df is returned as a string
              Defaults to True.
            include_index (bool, optional):
              Whether to include the index in the returned dataframe (True) or not (False)
              Defaults to True.

        Raises:
            ValueError: If Component is None

        Returns:
            Optional[str]: if save_file=False, the dataframe as a string, otherwise None
        """
        if not component:
            raise ValueError("You must provide a component to visualize.")
        return self._parse_attributes([component], save_file, include_index)

    @no_type_check
    def _parse_attributes(
        self, components: list[str], save_file: bool = True, include_index: bool = True
    ) -> Optional[str]:
        """
        Args: save_file (bool):
                True: merged_df is saved locally to output_path.
                False: merged_df is returned.
              components (list[str]):
                list of components to parse attributes for
              include_index (bool):
                Whether to include the index in the returned dataframe (True) or not (False)

        Returns:
            Optional[str]:
              if save_file=False, the dataframe as a string, otherwise None

        Raises:
            ValueError:
                If unable hits an error while attempting to get conditional requirements.
                This error is likely to be found if there is a mismatch in naming.
        """
        # This function needs to be refactored, temporarily disabling some pylint errors
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-nested-blocks
        # pylint: disable=too-many-branches
        # pylint: disable=too-many-statements
        # type

        # For each data type to be loaded gather all attributes the user would
        # have to provide.
        df_store = []
        for component in components:
            data_dict: dict = {}

            # get the json schema
            json_schema = self.data_model_js.get_json_validation_schema(
                source_node=component, schema_name=self.path_to_jsonld
            )

            # Gather all attributes, their valid values and requirements
            for key, value in json_schema["properties"].items():
                data_dict[key] = {}
                for inner_key in value.keys():
                    if inner_key == "enum":
                        data_dict[key]["Valid Values"] = value["enum"]
                if key in json_schema["required"]:
                    data_dict[key]["Required"] = True
                else:
                    data_dict[key]["Required"] = False
                data_dict[key]["Component"] = component
            # Add additional details per key (from the JSON-ld)
            for dic in self.jsonld["@graph"]:
                if "sms:displayName" in dic:
                    key = dic["sms:displayName"]
                    if key in data_dict:
                        data_dict[key]["Attribute"] = dic["sms:displayName"]
                        data_dict[key]["Label"] = dic["rdfs:label"]
                        data_dict[key]["Description"] = dic["rdfs:comment"]
                        if "validationRules" in dic.keys():
                            data_dict[key]["Validation Rules"] = dic["validationRules"]
            # Find conditional dependencies
            if "allOf" in json_schema:
                for conditional_dependencies in json_schema["allOf"]:
                    key = list(conditional_dependencies["then"]["properties"])[0]
                    try:
                        if key in data_dict:
                            if "Cond_Req" not in data_dict[key].keys():
                                data_dict[key]["Cond_Req"] = []
                                data_dict[key]["Conditional Requirements"] = []
                            attribute = list(
                                conditional_dependencies["if"]["properties"]
                            )[0]
                            value = conditional_dependencies["if"]["properties"][
                                attribute
                            ]["enum"]
                            # Capitalize attribute if it begins with a lowercase
                            # letter, for aesthetics.
                            if attribute[0].islower():
                                attribute = attribute.capitalize()

                            # Remove "Type" (i.e. turn "Biospecimen Type" to "Biospecimen")
                            if "Type" in attribute:
                                attribute = attribute.split(" ")[0]

                            # Remove "Type" (i.e. turn "Tissue Type" to "Tissue")
                            if "Type" in value[0]:
                                value[0] = value[0].split(" ")[0]

                            conditional_statement = f'{attribute} is "{value[0]}"'
                            if (
                                conditional_statement
                                not in data_dict[key]["Conditional Requirements"]
                            ):
                                data_dict[key]["Cond_Req"] = True
                                data_dict[key]["Conditional Requirements"].extend(
                                    [conditional_statement]
                                )
                    except Exception as exc:
                        raise ValueError(
                            (
                                "There is an error getting conditional requirements related "
                                f"to the attribute: {key}. The error is likely caused by naming "
                                "inconsistencies (e.g. uppercase, camelcase, ...)"
                            )
                        ) from exc

            for outer_dict_key, inner_dict in data_dict.items():
                if "Conditional Requirements" in inner_dict.keys():
                    ## reformat conditional requirement
                    conditional_requirements = inner_dict["Conditional Requirements"]

                    # get all attributes
                    attr_lst = [i.split(" is ")[-1] for i in conditional_requirements]

                    # join a list of attributes by using OR
                    attr_str = " OR ".join(attr_lst)

                    # reformat the conditional requirement
                    component_name = conditional_requirements[0].split(" is ")[0]

                    conditional_statement_str = (
                        f" If {component_name} is {attr_str} then "
                        f'"{outer_dict_key}" is required'
                    )
                    conditional_requirements = conditional_statement_str

            data_dict_df = pd.DataFrame(data_dict)
            data_dict_df = data_dict_df.T
            cols = [
                "Attribute",
                "Label",
                "Description",
                "Required",
                "Cond_Req",
                "Valid Values",
                "Conditional Requirements",
                "Validation Rules",
                "Component",
            ]
            cols = [col for col in cols if col in data_dict_df.columns]
            data_dict_df = data_dict_df[cols]
            data_dict_df = self._convert_string_cols_to_json(
                data_dict_df, ["Valid Values"]
            )
            df_store.append(data_dict_df)

        merged_attributes_df = pd.concat(df_store, join="outer")
        cols = [
            "Attribute",
            "Label",
            "Description",
            "Required",
            "Cond_Req",
            "Valid Values",
            "Conditional Requirements",
            "Validation Rules",
            "Component",
        ]
        cols = [col for col in cols if col in merged_attributes_df.columns]

        merged_attributes_df = merged_attributes_df[cols]
        if save_file:
            return merged_attributes_df.to_csv(
                os.path.join(
                    self.output_path, self.schema_name + "attributes_data.vis_data.csv"
                ),
                index=include_index,
            )
        return merged_attributes_df.to_csv(index=include_index)
