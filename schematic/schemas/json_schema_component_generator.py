import os
import pandas as pd
import json
from pathlib import Path

from typing import Any, Dict, List, Optional, Text

from schematic.models.metadata import MetadataModel
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_parser import DataModelParser
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer


class GeneratorDirector:
    """
    A class that directs the generation of a JSON schema component.
    """

    def __init__(
        self,
        data_model: str,
        components: Optional[list[str]] = None,
        output_directory: Optional[str] = None,
    ):
        self.data_model = data_model
        self.components = components
        self.output_directory = output_directory
        self.parsed_model = None
        if output_directory is None:
            self.output_directory = Path(os.getcwd(), "component_jsonschemas")

    def generate_jsonschema(
        self,
    ) -> list[Dict[str, Any]]:
        """
        call steps in jsonschemagenerator to generate the jsonschema
        """
        json_schemas = []

        if self.components is None:
            self.gather_components()

        for component in self.components:
            json_schemas.append(self._generate_jsonschema(component))

        return json_schemas

    def gather_components(
        self,
    ) -> None:
        """
        In cases where no components are provided, gather all components from the data model
        """

        self.components = self._extract_components()

        return

    def _extract_components(
        self,
    ):
        self._parse_model()

        attrs = pd.DataFrame.from_dict(
            {
                (i, j): self.parsed_model[i][j]
                for i in self.parsed_model.keys()
                for j in self.parsed_model[i].keys()
            },
            orient="index",
        ).reset_index(drop=True)

        depends_on_component = (
            attrs["DependsOn"].astype("str").str.contains("Component")
        )
        components = attrs["Attribute"].loc[depends_on_component]

        return list(components)

    def _parse_model(
        self,
    ):
        data_model_parser = DataModelParser(self.data_model)

        self.parsed_model = data_model_parser.parse_model()

        return

    def _generate_jsonschema(self, component: str) -> Dict[str, Any]:
        if not self.parsed_model:
            self._parse_model()

        generator = JsonSchemaComponentGenerator(
            data_model=self.data_model,
            component=component,
            output_directory=self.output_directory,
            parsed_model=self.parsed_model,
        )
        generator.get_data_model_json_schema()
        generator.get_component_json_schema()
        generator.add_description_to_json_schema()
        generator.write_json_schema_to_file()

        return generator.component_json_schema


class JsonSchemaComponentGenerator:
    """
    Generates a component's JSONschema based on the provided schema.
    """

    def __init__(
        self,
        data_model: str,
        component: str,
        output_directory: Optional[str],
        parsed_model: Dict[str, Any],
    ):
        self.data_model = data_model
        self.parsed_model = parsed_model
        self._get_data_model_graph_explorer()

        component_class_label = self.dmge.get_node_label(component)
        self.component = component_class_label if component_class_label else component

        self.output_path = self._build_output_path(output_directory)

        return

    def _build_output_path(self, output_directory: str) -> None:
        return Path(output_directory, f"{self.component}_validation_schema.json")

    def _get_data_model_graph_explorer(
        self,
    ):
        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(self.parsed_model, "class_label")

        # Generate graph
        graph_data_model = data_model_grapher.graph

        self.dmge = DataModelGraphExplorer(graph_data_model)

    def get_data_model_json_schema(
        self,
    ):
        metadata_model = MetadataModel(
            inputMModelLocation=self.data_model,
            inputMModelLocationType="local",
            data_model_labels="class_label",
        )
        self.data_model_json_schema = DataModelJSONSchema(
            jsonld_path=metadata_model.inputMModelLocation,
            graph=metadata_model.graph_data_model,
        )

        return

    def get_component_json_schema(
        self,
    ):
        schema_name = self.component + "_validation"

        self.incomplete_component_json_schema = (
            self.data_model_json_schema.get_json_validation_schema(
                source_node=self.component, schema_name=schema_name
            )
        )

        return

    def add_description_to_json_schema(
        self,
    ):
        component_description = self.dmge.get_node_comment(node_label=self.component)

        description_dict = {"description": component_description}

        description_dict |= self.incomplete_component_json_schema
        self.component_json_schema = description_dict

        return

    def write_json_schema_to_file(
        self,
    ):
        output_directory = os.path.dirname(self.output_path)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        with open(self.output_path, "w") as json_file:
            json.dump(self.component_json_schema, json_file, indent=4)

        return
