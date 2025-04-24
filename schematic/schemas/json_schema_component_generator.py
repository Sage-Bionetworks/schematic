"JSON schema file generator"

# pylint: disable=line-too-long

import os
from pathlib import Path
from typing import Any, Dict, Optional
import pandas as pd
import click

from schematic.models.metadata import MetadataModel
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.utils.io_utils import export_json


class JsonSchemaGeneratorDirector:
    """
    A class that directs the generation of a JSON schema component.
    """

    def __init__(
        self,
        data_model_location: str,
        components: Optional[list[str]] = None,
        output_directory: Optional[str] = None,
    ):
        self.data_model_location = data_model_location
        self.parsed_model = self._parse_model()
        self.components = components if components else self.gather_components()

        if output_directory is None:
            self.output_directory = Path(os.getcwd(), "component_jsonschemas")
        else:
            self.output_directory = Path(output_directory)

    def generate_jsonschema(
        self,
    ) -> list[Dict[str, Any]]:
        """
        Gather necessary components and generate JSON schema for each component.
        Returns:
            json_schemas: A list of JSON schemas for each component.
        """
        json_schemas = []

        for component in self.components:
            json_schemas.append(self._generate_jsonschema(component))

        return json_schemas

    def gather_components(
        self,
    ) -> list[str]:
        """
        Gather all components from the data model and store as list in the components attribute.
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
        Parse the data model to extract the components and their attributes and later for JSON schema generation.
        Stores:
            parsed_model: A dictionary representation of the data model.
        """

        data_model_parser = DataModelParser(self.data_model_location)

        return data_model_parser.parse_model()

    def _generate_jsonschema(self, component: str) -> dict[str, Any]:
        """
        Execute the steps to generate the JSON schema for a single component.
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
    Used to generate a jsonschema for a given component and write it to a file.
    """

    def __init__(
        self,
        data_model_location: str,
        component: str,
        output_directory: Path,
        parsed_model: Dict[str, Any],
    ):
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
        Build the output path for the JSON schema file.
        Args:
            output_directory: The directory where the JSON schema file will be saved.
        Returns:
            output_path: The path to the JSON schema file.
        """

        stripped_component = self.component.replace(" ", "")
        return Path(output_directory, f"{stripped_component}_validation_schema.json")

    def _get_data_model_graph_explorer(
        self,
    ) -> DataModelGraphExplorer:
        """'
        Create a DataModelGraphExplorer object to explore the data model graph.
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
        Generate the JSON schema for the component using the DataModelJSONSchema instance.
        The jsonschema is incomplete and will lack the description of the component.
        """
        schema_name = self.component + "_validation"

        metadata_model = MetadataModel(
            inputMModelLocation=self.data_model_location,
            inputMModelLocationType="local",
            data_model_labels="class_label",
        )
        data_model_json_schema = DataModelJSONSchema(
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
        Add the description of the component to the JSON schema.
        """
        component_description = self.dmge.get_node_comment(node_label=self.component)

        description_dict = {"description": component_description}

        description_dict |= self.incomplete_component_json_schema
        self.component_json_schema = description_dict
        click.echo(f"Validation JSONschema generated for {self.component}.")

    def write_json_schema_to_file(
        self,
    ) -> None:
        """
        Write the JSON schema to a file.
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
