"JSON schema file generator"

# pylint: disable=line-too-long

import os
from pathlib import Path
from typing import Any, Optional
import pandas as pd
import click

from schematic.models.metadata import MetadataModel
from schematic.schemas.json_schema_generator import JSONSchemaGenerator
from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.utils.io_utils import export_json


class JsonSchemaGeneratorDirector:
    """
    Directs the generation of JSON schemas for one or more components from a specified data model.

    Attributes:
        data_model_location (str): Path or URL to the data model file.
        parsed_model (dict): Parsed representation of the data model.
        components (list[str]): List of component names to generate schemas for.
        output_directory (Path): Directory where generated JSON schema files will be saved.
    """

    def __init__(
        self,
        data_model_location: str,
        components: Optional[list[str]] = None,
        output_directory: Optional[str] = None,
    ):
        """
        Initialize the JsonSchemaGeneratorDirector with data model location, components, and output directory.

        Args:
            data_model_location (str): Path or URL to the data model JSON-LD file.
            components (Optional[list[str]]): List of component names. If None, components will be gathered automatically from the data model.
            output_directory (Optional[str]): Directory where JSON schema files will be saved. Defaults to a subdirectory named 'component_jsonschemas' in the current working directory.

        Attributes Updated:
            self.data_model_location
            self.parsed_model
            self.components
            self.output_directory
        """
        self.data_model_location = data_model_location
        self.parsed_model = self._parse_model()
        self.components = components if components else self.gather_components()

        if output_directory is None:
            self.output_directory = Path(os.getcwd(), "component_jsonschemas")
        else:
            self.output_directory = Path(output_directory)

    def generate_jsonschema(
        self,
    ) -> list[dict[str, Any]]:
        """
        Generate JSON schemas for all specified components.

        Returns:
            list[dict[str, Any]]: A list of JSON schema dictionaries, each corresponding to a component.
        """
        json_schemas = []

        for component in self.components:
            json_schemas.append(self._generate_jsonschema(component))

        return json_schemas

    def gather_components(
        self,
    ) -> list[str]:
        """
        Identify all components in the data model based on the 'DependsOn' attribute containing 'Component'.

        Returns:
            list[str]: List of component names extracted from the data model.
        """

        # To represent each attribute of the nested model dictionary as a column in a dataframe,
        # it must be unpacked and the index reset
        unpacked_model_dict = {}

        for top_key, nested_dict in self.parsed_model.items():
            for nested_key, value in nested_dict.items():
                unpacked_model_dict[top_key, nested_key] = value

        attrs = pd.DataFrame.from_dict(
            unpacked_model_dict,
            orient="index",
        ).reset_index(drop=True)

        # Get a series of boolean values that can be used to identify which attributes (rows) have 'Component' in the DependsOn column
        string_depends_on = attrs.DependsOn
        string_depends_on = string_depends_on.astype(str)
        depends_on_component = string_depends_on.str.contains("Component")

        # select this subset and return as list of all components in the data model
        components = attrs.loc[depends_on_component, "Attribute"]

        return list(components)

    def _parse_model(
        self,
    ) -> dict[str, dict[str, Any]]:
        """
        Parse the data model file into a structured dictionary representation.

        Returns:
            dict[str, dict[str, Any]]: The parsed model as a nested dictionary.

        Attributes Updated:
            self.parsed_model
        """

        data_model_parser = DataModelParser(self.data_model_location)

        return data_model_parser.parse_model()

    def _generate_jsonschema(self, component: str) -> dict[str, Any]:
        """
        Generate the JSON schema for a single specified component.

        Args:
            component (str): The name of the component for which the JSON schema is to be generated.

        Returns:
            dict[str, Any]: The generated JSON schema dictionary for the specified component.

        Side Effects:
            Writes the generated JSON schema to a file via the JsonSchemaComponentGenerator.
        """

        # Direct the generation of the jsonschema for a single component
        generator = JsonSchemaComponentGenerator(
            data_model_location=self.data_model_location,
            component=component,
            output_directory=self.output_directory,
            parsed_model=self.parsed_model,
        )

        generator.get_component_json_schema()
        generator.add_description_to_json_schema()
        generator.write_json_schema_to_file()

        return generator.component_json_schema


class JsonSchemaComponentGenerator:
    """
    Responsible for generating the JSON schema for a specific component and writing it to a file.

    Attributes:
        data_model_location (str): Path or URL to the data model.
        parsed_model (dict): Parsed representation of the data model.
        dmge (DataModelGraphExplorer): Graph explorer for navigating the data model.
        component (str): The class label of the target component.
        output_path (Path): Path where the generated JSON schema file will be saved.
        incomplete_component_json_schema (dict): JSON schema before adding the component description.
        component_json_schema (dict): Final JSON schema with the description added.
    """

    def __init__(
        self,
        data_model_location: str,
        component: str,
        output_directory: Path,
        parsed_model: dict[str, Any],
    ):
        """
        Initialize the JsonSchemaComponentGenerator.

        Args:
            data_model_location (str): Path or URL to the data model JSON-LD file.
            component (str): Component name (class label or display name).
            output_directory (Path): Output directory for saving the JSON schema file.
            parsed_model (dict[str, Any]): The parsed model dictionary.

        Attributes Updated:
            self.data_model_location
            self.parsed_model
            self.dmge
            self.component
            self.output_path
            self.incomplete_component_json_schema
            self.component_json_schema
        """
        self.data_model_location = data_model_location
        self.parsed_model = parsed_model
        self.dmge = self._get_data_model_graph_explorer()

        # the component can be provided as either a class label or display name
        # internally all the work is done with the class label
        component_class_label = self.dmge.get_node_label(component)
        self.component = component_class_label if component_class_label else component

        self.output_path = self._build_output_path(output_directory)

        self.incomplete_component_json_schema: dict[str, Any] = {}
        self.component_json_schema: dict[str, Any] = {}

    def _build_output_path(self, output_directory: Path) -> Path:
        """
        Construct the file path where the JSON schema file will be saved.

        Args:
            output_directory (Path): Directory for saving the JSON schema.

        Returns:
            Path: Full path to the JSON schema file.
        """

        stripped_component = self.component.replace(" ", "")
        data_model_basename = Path(self.data_model_location).stem
        return Path(
            output_directory,
            data_model_basename,
            f"{stripped_component}_validation_schema.json",
        )

    def _get_data_model_graph_explorer(
        self,
    ) -> DataModelGraphExplorer:
        """
        Instantiate and return a DataModelGraphExplorer to navigate the data model graph.

        Returns:
            DataModelGraphExplorer: An instance for exploring the data model graph.
        """
        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(self.parsed_model, "class_label")

        # Generate graph
        graph_data_model = data_model_grapher.graph

        return DataModelGraphExplorer(graph_data_model)

    def get_component_json_schema(
        self,
    ) -> None:
        """
        Generate the initial JSON schema for the specified component without the description.

        Attributes Updated:
            self.incomplete_component_json_schema

        Raises:
            May raise errors if the component is not found in the data model graph.
        """
        schema_name = self.component + "_validation"

        metadata_model = MetadataModel(
            inputMModelLocation=self.data_model_location,
            inputMModelLocationType="local",
            data_model_labels="class_label",
        )
        data_model_json_schema = JSONSchemaGenerator(
            jsonld_path=metadata_model.inputMModelLocation,
            graph=metadata_model.graph_data_model,
        )

        self.incomplete_component_json_schema = (
            data_model_json_schema.get_json_validation_schema(
                source_node=self.component, schema_name=schema_name
            )
        )

    def add_description_to_json_schema(
        self,
    ) -> None:
        """
        Add the description of the component from the data model graph to the JSON schema.

        Attributes Updated:
            self.component_json_schema
        """
        component_description = self.dmge.get_node_comment(node_label=self.component)

        description_dict = {"description": component_description}

        self.component_json_schema.update(self.incomplete_component_json_schema)
        self.component_json_schema.update(description_dict)

        if "properties" not in self.component_json_schema:
            raise ValueError(
                f"component: {self.component} is malformed, missing properties"
            )

        if not isinstance(self.component_json_schema["properties"], dict):
            raise ValueError(
                f"component: {self.component} is malformed, properties should be an object"
            )

        for attribute, value in self.component_json_schema["properties"].items():
            if isinstance(value, dict) and "description" not in value:
                # https://sagebionetworks.jira.com/browse/SCHEMATIC-284
                # https://sagebionetworks.jira.com/browse/SCHEMATIC-283
                value["description"] = self.dmge.get_node_comment(
                    node_display_name=attribute
                )

        click.echo(f"Validation JSONschema generated for {self.component}.")

    def write_json_schema_to_file(
        self,
    ) -> None:
        """
        Write the finalized JSON schema with the description to the designated file path.

        Side Effects:
            Creates directories if they do not exist.
            Writes the JSON schema to the file system.
            Outputs status messages via Click.
        """
        output_directory = os.path.dirname(self.output_path)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        export_json(
            json_doc=self.component_json_schema,
            file_path=str(self.output_path),
            indent=2,
        )
        click.echo(
            f"Validation JSONschema file for {self.component} saved to {self.output_path}."
        )
