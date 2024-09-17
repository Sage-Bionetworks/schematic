import logging
import os
from os.path import exists

# allows specifying explicit variable types
from typing import Any, Dict, List, Optional, Text

import networkx as nx
from jsonschema import ValidationError
from opentelemetry import trace

from schematic.manifest.generator import ManifestGenerator
from schematic.models.validate_manifest import validate_all
from schematic.schemas.data_model_graph import DataModelGraph, DataModelGraphExplorer
from schematic.schemas.data_model_json_schema import DataModelJSONSchema
from schematic.schemas.data_model_parser import DataModelParser

# TODO: This module should only be aware of the store interface
# we shouldn't need to expose Synapse functionality explicitly
from schematic.store.synapse import SynapseStorage
from schematic.utils.df_utils import load_df

logger = logging.getLogger(__name__)

tracer = trace.get_tracer("Schematic")


class MetadataModel(object):
    """Metadata model wrapper around schema.org specification graph.

    Provides basic utilities to:

    1) manipulate the metadata model
    2) generate metadata model views:
        - generate manifest view of the metadata model
        - generate validation schema view of the metadata model
    """

    def __init__(
        self,
        inputMModelLocation: str,
        inputMModelLocationType: str,
        data_model_labels: str,
    ) -> None:
        """Instantiates a MetadataModel object.

        Args:
            inputMModelLocation: local path, uri, synapse entity id (e.g. gs://, syn123, /User/x/…); present location
            inputMModelLocationType: specifier to indicate where the metadata model resource can be found (e.g. 'local' if file/JSON-LD is on local machine)
        """
        # extract extension of 'inputMModelLocation'
        # ensure that it is necessarily pointing to a '.jsonld' file

        logger.debug(
            f"Initializing DataModelGraphExplorer object from {inputMModelLocation} schema."
        )

        # self.inputMModelLocation remains for backwards compatibility
        self.inputMModelLocation = inputMModelLocation
        self.path_to_json_ld = inputMModelLocation

        data_model_parser = DataModelParser(path_to_data_model=self.inputMModelLocation)
        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(parsed_data_model, data_model_labels)

        # Generate graph
        self.graph_data_model = data_model_grapher.graph

        self.dmge = DataModelGraphExplorer(self.graph_data_model)

        # check if the type of MModel file is "local"
        # currently, the application only supports reading from local JSON-LD files
        if inputMModelLocationType == "local":
            self.inputMModelLocationType = inputMModelLocationType
        else:
            raise ValueError(
                f"The type '{inputMModelLocationType}' is currently not supported."
            )

    def getModelSubgraph(self, rootNode: str, subgraphType: str) -> nx.DiGraph:
        """Gets a schema subgraph from rootNode descendants based on edge/node properties of type subgraphType.

        Args:
            rootNode: a schema node label (i.e. term).
            subgraphType: the kind of subgraph to traverse (i.e. based on node properties or edge labels).

        Returns:
            A directed subgraph (networkx DiGraph) of the metadata model with vertex set root node descendants.

        Raises:
            ValueError: rootNode not found in metadata model.
        """
        pass

    def getOrderedModelNodes(self, rootNode: str, relationshipType: str) -> List[str]:
        """Get a list of model objects ordered by their topological sort rank in a model subgraph on edges of a given relationship type.

        Args:
            rootNode: a schema object/node label (i.e. term)
            relationshipType: edge label type of the schema subgraph (e.g. requiresDependency)

        Returns:
            An ordered list of objects, that are all descendants of rootNode.

        Raises:
            ValueError: rootNode not found in metadata model.
        """
        ordered_nodes = self.dmge.get_descendants_by_edge_type(
            rootNode, relationshipType, connected=True, ordered=True
        )

        ordered_nodes.reverse()

        return ordered_nodes

    def getModelManifest(
        self,
        title: str,
        rootNode: str,
        datasetId: str = None,
        jsonSchema: str = None,
        filenames: list = None,
        useAnnotations: bool = False,
        sheetUrl: bool = True,
    ) -> str:
        """Gets data from the annotations manifest file.

        TBD: Does this method belong here or in manifest generator?

        Args:
            rootNode: a schema node label (i.e. term).
            useAnnotations: whether to populate manifest with current file annotations (True) or not (False, default).

        Returns:
            A manifest URI (assume Google doc for now).

        Raises:
            ValueError: rootNode not found in metadata model.
        """
        additionalMetadata = {}
        if filenames:
            additionalMetadata["Filename"] = filenames

        mg = ManifestGenerator(
            path_to_json_ld=self.inputMModelLocation,
            graph=self.graph_data_model,
            title=title,
            root=rootNode,
            additional_metadata=additionalMetadata,
            use_annotations=useAnnotations,
        )

        if datasetId:
            return mg.get_manifest(
                dataset_id=datasetId, json_schema=jsonSchema, sheet_url=sheetUrl
            )

        return mg.get_manifest(sheet_url=sheetUrl)

    def get_component_requirements(
        self, source_component: str, as_graph: bool = False
    ) -> List:
        """Given a source model component (see https://w3id.org/biolink/vocab/category for definnition of component), return all components required by it.
        Useful to construct requirement dependencies not only between specific attributes but also between categories/components of attributes;
        Can be utilized to track metadata completion progress across multiple categories of attributes.

        Args:
            source_component: an attribute label indicating the source component.
            as_graph: if False return component requirements as a list; if True return component requirements as a dependency graph (i.e. a DAG)

        Returns:
            A list of required components associated with the source component.
        """

        # get required components for the input/source component
        req_components = self.dmge.get_component_requirements(source_component)

        # retreive components as graph
        if as_graph:
            req_components_graph = self.dmge.get_component_requirements_graph(
                source_component
            )

            # serialize component dependencies DAG to a edge list of node tuples
            req_components = list(req_components_graph.edges())

            return req_components

        return req_components

    # TODO: abstract validation in its own module
    def validateModelManifest(
        self,
        manifestPath: str,
        rootNode: str,
        restrict_rules: bool = False,
        jsonSchema: Optional[str] = None,
        project_scope: Optional[List] = None,
        dataset_scope: Optional[str] = None,
        access_token: Optional[str] = None,
    ) -> tuple[list, list]:
        """Check if provided annotations manifest dataframe satisfies all model requirements.

        Args:
            rootNode: a schema node label (i.e. term).
            manifestPath: a path to the manifest csv file containing annotations.
            restrict_rules: bypass great expectations and restrict rule options to those implemented in house

        Returns:
            A validation status message; if there is an error the message.
            contains the manifest annotation record (i.e. row) that is invalid, along with the validation error associated with this record.

        Raises:
            ValueError: rootNode not found in metadata model.
        """
        # get validation schema for a given node in the data model, if the user has not provided input validation schema

        if not jsonSchema:
            # Instantiate Data Model Json Schema
            self.data_model_js = DataModelJSONSchema(
                jsonld_path=self.inputMModelLocation, graph=self.graph_data_model
            )

            jsonSchema = self.data_model_js.get_json_validation_schema(
                rootNode, rootNode + "_validation"
            )

        errors = []
        warnings = []

        load_args = {
            "dtype": "string",
        }
        # get annotations from manifest (array of json annotations corresponding to manifest rows)
        manifest = load_df(
            manifestPath,
            preserve_raw_input=False,
            allow_na_values=True,
            **load_args,
        )  # read manifest csv file as is from manifest path

        # handler for mismatched components/data types
        # throw TypeError if the value(s) in the "Component" column differ from the selected template type
        if ("Component" in manifest.columns) and (
            (len(manifest["Component"].unique()) > 1)
            or (manifest["Component"].unique()[0] != rootNode)
        ):
            logging.error(
                f"The 'Component' column value(s) {manifest['Component'].unique()} do not match the "
                f"selected template type '{rootNode}'."
            )

            # row indexes for all rows where 'Component' is rootNode
            row_idxs = manifest.index[manifest["Component"] != rootNode].tolist()
            # column index value for the 'Component' column
            col_idx = manifest.columns.get_loc("Component")
            # Series with index and 'Component' values from manifest
            mismatched_ser = manifest.iloc[row_idxs, col_idx]
            for index, component in mismatched_ser.items():
                errors.append(
                    [
                        index + 2,
                        "Component",
                        f"Component value provided is: '{component}', whereas the Template Type is: '{rootNode}'",
                        # tuple of the component in the manifest and selected template type
                        # check: R/Reticulate cannnot handle dicts? So returning tuple
                        (component, rootNode),
                    ]
                )

            return errors, warnings

        # check if suite has been created. If so, delete it
        if os.path.exists("great_expectations/expectations/Manifest_test_suite.json"):
            os.remove("great_expectations/expectations/Manifest_test_suite.json")

        errors, warnings, manifest = validate_all(
            self,
            errors=errors,
            warnings=warnings,
            manifest=manifest,
            manifestPath=manifestPath,
            dmge=self.dmge,
            jsonSchema=jsonSchema,
            restrict_rules=restrict_rules,
            project_scope=project_scope,
            dataset_scope=dataset_scope,
            access_token=access_token,
        )
        return errors, warnings

    def populateModelManifest(
        self, title, manifestPath: str, rootNode: str, return_excel=False
    ) -> str:
        """Populate an existing annotations manifest based on a dataframe.
            TODO: Remove this method; always use getModelManifest instead

        Args:
            rootNode: a schema node label (i.e. term).
            manifestPath: a path to the manifest csv file containing annotations.

        Returns:
            A link to the filled in model manifest (e.g. google sheet).

        Raises:
            ValueError: rootNode not found in metadata model.
        """
        mg = ManifestGenerator(
            path_to_data_model=self.inputMModelLocation,
            graph=self.graph_data_model,
            title=title,
            root=rootNode,
        )

        emptyManifestURL = mg.get_manifest()

        return mg.populate_manifest_spreadsheet(
            manifestPath, emptyManifestURL, return_excel=return_excel, title=title
        )

    @tracer.start_as_current_span("MetadataModel::submit_metadata_manifest")
    def submit_metadata_manifest(  # pylint: disable=too-many-arguments, too-many-locals
        self,
        manifest_path: str,
        dataset_id: str,
        manifest_record_type: str,
        restrict_rules: bool,
        access_token: Optional[str] = None,
        validate_component: Optional[str] = None,
        file_annotations_upload: bool = True,
        hide_blanks: bool = False,
        project_scope: Optional[list] = None,
        dataset_scope: Optional[str] = None,
        table_manipulation: str = "replace",
        table_column_names: str = "class_label",
        annotation_keys: str = "class_label",
    ) -> str:
        """
        Wrap methods that are responsible for validation of manifests for a given component,
          and association of the same manifest file with a specified dataset.

        Args:
            manifest_path (str): Path to the manifest file, which contains the metadata.
            dataset_id (str): Synapse ID of the dataset on Synapse containing the
              metadata manifest file.
            manifest_record_type (str): How the manifest is stored in Synapse
            restrict_rules (bool):
              If True: bypass great expectations and restrict rule options to
                those implemented in house
            access_token (Optional[str], optional): Defaults to None.
            validate_component (Optional[str], optional): Component from the schema.org
              schema based on which the manifest template has been generated.
            file_annotations_upload (bool, optional): Default to True. If false, do
              not add annotations to files. Defaults to True.
            hide_blanks (bool, optional): Defaults to False.
            project_scope (Optional[list], optional): Defaults to None.
            table_manipulation (str, optional): Defaults to "replace".
            table_column_names (str, optional): Defaults to "class_label".
            annotation_keys (str, optional): Defaults to "class_label".

        Raises:
            ValueError: When validate_component is provided, but it cannot be found in the schema.
            ValidationError: If validation against data model was not successful.

        Returns:
            str: If both validation and association were successful.
        """
        # TODO: avoid explicitly exposing Synapse store functionality
        # just instantiate a Store class and let it decide at runtime/config
        # the store type
        syn_store = SynapseStorage(
            access_token=access_token, project_scope=project_scope
        )
        manifest_id = None
        restrict_maniest = False
        censored_manifest_path = manifest_path.replace(".csv", "_censored.csv")
        # check if user wants to perform validation or not
        if validate_component is not None:
            try:
                # check if the component ("class" in schema) passed as argument is valid
                # (present in schema) or not
                self.dmge.is_class_in_schema(validate_component)
            except Exception as exc:
                # a KeyError exception is raised when validate_component fails in the
                # try-block above here, we are suppressing the KeyError exception and
                # replacing it with a more descriptive ValueError exception
                raise ValueError(
                    f"The component '{validate_component}' could not be found "
                    f"in the schema here '{self.path_to_json_ld}'"
                ) from exc

            # automatic JSON schema generation and validation with that JSON schema
            val_errors, _ = self.validateModelManifest(
                manifestPath=manifest_path,
                rootNode=validate_component,
                restrict_rules=restrict_rules,
                project_scope=project_scope,
                dataset_scope=dataset_scope,
                access_token=access_token,
            )

            # if there are no errors in validation process
            if val_errors == []:
                # upload manifest file from `manifest_path` path to entity with Syn ID `dataset_id`
                if os.path.exists(censored_manifest_path):
                    syn_store.associateMetadataWithFiles(
                        dmge=self.dmge,
                        metadataManifestPath=censored_manifest_path,
                        datasetId=dataset_id,
                        manifest_record_type=manifest_record_type,
                        hideBlanks=hide_blanks,
                        table_manipulation=table_manipulation,
                        table_column_names=table_column_names,
                        annotation_keys=annotation_keys,
                        file_annotations_upload=file_annotations_upload,
                    )
                    restrict_maniest = True

                manifest_id = syn_store.associateMetadataWithFiles(
                    dmge=self.dmge,
                    metadataManifestPath=manifest_path,
                    datasetId=dataset_id,
                    manifest_record_type=manifest_record_type,
                    hideBlanks=hide_blanks,
                    restrict_manifest=restrict_maniest,
                    table_manipulation=table_manipulation,
                    table_column_names=table_column_names,
                    annotation_keys=annotation_keys,
                    file_annotations_upload=file_annotations_upload,
                )

                logger.info("No validation errors occured during validation.")
                return manifest_id

            else:
                raise ValidationError(
                    "Manifest could not be validated under provided data model. "
                    f"Validation failed with the following errors: {val_errors}"
                )

        # no need to perform validation, just submit/associate the metadata manifest file
        if os.path.exists(censored_manifest_path):
            syn_store.associateMetadataWithFiles(
                dmge=self.dmge,
                metadataManifestPath=censored_manifest_path,
                datasetId=dataset_id,
                manifest_record_type=manifest_record_type,
                hideBlanks=hide_blanks,
                table_manipulation=table_manipulation,
                table_column_names=table_column_names,
                annotation_keys=annotation_keys,
                file_annotations_upload=file_annotations_upload,
            )
            restrict_maniest = True

        manifest_id = syn_store.associateMetadataWithFiles(
            dmge=self.dmge,
            metadataManifestPath=manifest_path,
            datasetId=dataset_id,
            manifest_record_type=manifest_record_type,
            hideBlanks=hide_blanks,
            restrict_manifest=restrict_maniest,
            table_manipulation=table_manipulation,
            table_column_names=table_column_names,
            annotation_keys=annotation_keys,
            file_annotations_upload=file_annotations_upload,
        )

        logger.debug(
            "Optional validation was not performed on manifest before association."
        )

        return manifest_id
