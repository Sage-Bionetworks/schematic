"JSON schema file generator"

# pylint: disable=line-too-long

import os
from pathlib import Path
from typing import Any, Optional
import click

from schematic.models.metadata import MetadataModel
from schematic.schemas.create_json_schema import create_json_schema
from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.utils.io_utils import export_json
from schematic.utils.schema_utils import parsed_model_as_dataframe, DisplayLabelType


class JsonSchemaGeneratorDirector:
    """
    Directs the generation of JSON schemas for one or more components from a specified data model.

    Attributes:
        data_model_source (str): Path or URL to the data model file.
        parsed_model (dict): Parsed representation of the data model.
        components (list[str]): List of component names to generate schemas for.
        output_directory (Path): Directory where generated JSON schema files will be saved.
    """

    def __init__(
        self,
        data_model_source: str,
        components: Optional[list[str]] = None,
        output_directory: Optional[str] = None,
    ):
        """
        Initialize the JsonSchemaGeneratorDirector with data model location, components, and output directory.

        Args:
            data_model_source (str): Path or URL to the data model JSON-LD file.
            components (Optional[list[str]]): List of component names. If None, components will be gathered automatically from the data model.
            output_directory (Optional[str]): Directory where JSON schema files will be saved. Defaults to a subdirectory named 'component_jsonschemas' in the current working directory.

        Attributes Updated:
            self.data_model_source
            self.parsed_model
            self.components
            self.output_directory
        """
        self.data_model_source = data_model_source
        self.parsed_model = self._parse_model()
        self.components = components if components else self.gather_components()

        if output_directory is None:
            self.output_directory = Path(os.getcwd(), "component_jsonschemas")
        else:
            self.output_directory = Path(output_directory)

    def generate_jsonschema(
        self,
        data_model_labels: DisplayLabelType = "class_label",
    ) -> list[dict[str, Any]]:
        """
        Generate JSON schemas for all specified components.

        Returns:
            list[dict[str, Any]]: A list of JSON schema dictionaries, each corresponding to a component.
        """
        json_schemas = []

        for component in self.components:
            json_schemas.append(self._generate_jsonschema(component, data_model_labels))

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
        attrs = parsed_model_as_dataframe(self.parsed_model)

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

        data_model_parser = DataModelParser(self.data_model_source)

        return data_model_parser.parse_model()

    def _generate_jsonschema(
        self, component: str, data_model_labels: DisplayLabelType
    ) -> dict[str, Any]:
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
            data_model_source=self.data_model_source,
            component=component,
            output_directory=self.output_directory,
            parsed_model=self.parsed_model,
        )

        generator.get_component_json_schema(data_model_labels=data_model_labels)
        generator.write_json_schema_to_file()

        return generator.component_json_schema


class JsonSchemaComponentGenerator:
    """
    Responsible for generating the JSON schema for a specific component and writing it to a file.

    Attributes:
        data_model_source (str): Path or URL to the data model.
        parsed_model (dict): Parsed representation of the data model.
        dmge (DataModelGraphExplorer): Graph explorer for navigating the data model.
        component (str): The class label of the target component.
        output_path (Path): Path where the generated JSON schema file will be saved.
        component_json_schema (dict): Final JSON schema.
    """

    def __init__(
        self,
        data_model_source: str,
        component: str,
        output_directory: Path,
        parsed_model: dict[str, Any],
    ):
        """
        Initialize the JsonSchemaComponentGenerator.

        Args:
            data_model_source (str): Path or URL to the data model JSON-LD file.
            component (str): Component name (class label or display name).
            output_directory (Path): Output directory for saving the JSON schema file.
            parsed_model (dict[str, Any]): The parsed model dictionary.

        Attributes Updated:
            self.data_model_source
            self.parsed_model
            self.dmge
            self.component
            self.output_path
            self.component_json_schema
        """
        self.data_model_source = data_model_source
        self.parsed_model = parsed_model
        self.dmge = self._get_data_model_graph_explorer()

        # the component can be provided as either a class label or display name
        # internally all the work is done with the class label
        component_class_label = self.dmge.get_node_label(component)
        self.component = component_class_label if component_class_label else component

        self.output_path = self._build_output_path(output_directory)

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
        data_model_basename = Path(self.data_model_source).stem
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
        data_model_labels: DisplayLabelType = "class_label",
    ) -> None:
        """
        Generate JSON schema for the specified component.

        Attributes Updated:
            self.component_json_schema

        Raises:
            May raise errors if the component is not found in the data model graph.
        """
        metadata_model = MetadataModel(
            inputMModelLocation=self.data_model_source,
            inputMModelLocationType="local",
            data_model_labels=data_model_labels,
        )

        use_display_names = data_model_labels == "display_label"

        json_schema = create_json_schema(
            dmge=self.dmge,
            datatype=self.component,
            schema_name=self.component + "_validation",
            jsonld_path=metadata_model.inputMModelLocation,
            use_property_display_names=use_display_names,
        )
        self.component_json_schema = json_schema

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
