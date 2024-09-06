"""Help messages for CLI commands"""
# pylint: disable=line-too-long
#!/usr/bin/env python3

from typing import get_args

from schematic.utils.schema_utils import DisplayLabelType
from schematic.visualization.tangled_tree import FigureType, TextType

DATA_MODEL_LABELS_DICT = {
    "display_label": "use the display name as a label, if it is valid (contains no blacklisted characters) otherwise will default to class_label.",
    "class_label": "default, use standard class or property label.",
}
# Ensure that all DisplayLabelTypes have a description
assert sorted(DATA_MODEL_LABELS_DICT.keys()) == sorted(get_args(DisplayLabelType))

# Combine each label and its description into one string
DATA_MODEL_LABELS_LIST = [
    f"{label}, {description}" for label, description in DATA_MODEL_LABELS_DICT.items()
]

DATA_MODEL_LABELS_HELP = (
    "Choose how to set the label in the data model. "
    f"{' '.join(DATA_MODEL_LABELS_LIST)} "
    "Do not change from default unless there is a real need, using 'display_label' can have consequences if not used properly."
)


FIGURE_TYPES = " or ".join([f"'{item}'" for item in get_args(FigureType)])
TEXT_TYPES = " or ".join([f"'{item}'" for item in get_args(TextType)])

# `schematic manifest` related sub-commands description
manifest_commands = {
    "manifest": {
        "config": (
            "Specify the path to the `config.yml` using this option. This is a required argument."
        ),
        "get": {
            "short_help": (
                "Specify the path to the `config.yml` using this option. "
                "This is a required argument."
            ),
            "title": (
                "Specify the title of the manifest (or title prefix of multiple manifests) that "
                "will be created at the end of the run. You can either explicitly pass the "
                "title of the manifest here or provide it in the `config.yml` "
                "file as a value for the `(manifest > title)` key."
            ),
            "data_type": (
                "Specify the component(s) (data type) from the data model that is to be used "
                "for generating the metadata manifest file. To make all available manifests enter 'all manifests'. "
                "You can either explicitly pass the data type here or provide "
                "it in the `config.yml` file as a value for the `(manifest > data_type)` key."
            ),
            "jsonld": (
                "Specify the path to the JSON-LD data model (schema) using this option. You can either explicitly pass the "
                "schema here or provide a value for the `(model > input > location)` key."
            ),
            "dataset_id": (
                "Specify the synID of a dataset folder on Synapse. If there is an exisiting manifest already present "
                "in that folder, then it will be pulled with the existing annotations for further annotation/modification. "
            ),
            "sheet_url": (
                "This is a boolean flag. If flag is provided when command line utility is executed, result will be a link/URL "
                "to the metadata manifest file. If not it will produce a pandas dataframe for the same."
            ),
            "output_csv": ("Path to where the CSV manifest template should be stored."),
            "output_xlsx": (
                "Path to where the Excel manifest template should be stored."
            ),
            "use_annotations": (
                "This is a boolean flag. If flag is provided when command line utility is executed, it will prepopulate template "
                "with existing annotations from Synapse."
            ),
            "json_schema": (
                "Specify the path to the JSON Validation Schema for this argument. "
                "You can either explicitly pass the `.json` file here or provide it in the `config.yml` file "
                "as a value for the `(model > location)` key."
            ),
            "alphabetize_valid_values": (
                "Specify to alphabetize valid attribute values either ascending (a) or descending (d)."
                "Optional"
            ),
            "data_model_labels": DATA_MODEL_LABELS_HELP,
        },
        "migrate": {
            "short_help": (
                "Specify the path to the `config.yml` using this option. "
                "This is a required argument."
            ),
            "project_scope": (
                "Specify a comma-separated list of projects where manifest entities will be migrated to tables."
            ),
            "archive_project": (
                "Specify a single project where legacy manifest entities will be stored after migration to table."
            ),
            "return_entities": (
                "This is a boolean flag. If flag is provided when command line utility is executed, "
                "entities that have been transferred to an archive project will be returned to their original folders."
            ),
            "dry_run": (
                "This is a boolean flag. If flag is provided when command line utility is executed, "
                "a dry run will be performed. No manifests will be re-uploaded and no entities will be migrated, "
                "but archival folders will still be created. "
                "Migration information for testing purposes will be logged to the INFO level."
            ),
        },
        "download": {
            "short_help": ("Function to download manifest from asset store (Synapse)."),
            "dataset_id": (
                "Specify the synID of a dataset folder on Synapse. If there is an exisiting manifest already present "
                "in that folder, then it will be pulled with the existing annotations for further annotation/modification. "
            ),
            "new_manifest_name": (
                "Specify the new name to download the manifest file as."
            ),
        },
    }
}


# `schematic model` related sub-commands description
model_commands = {
    "model": {
        "config": (
            "Specify the path to the `config.yml` using this option. This is a required argument."
        ),
        "submit": {
            "short_help": ("Validation (optional) and submission of manifest files."),
            "manifest_path": (
                "Specify the path to the metadata manifest file that you want to submit to a dataset on Synapse. "
                "This is a required argument."
            ),
            "dataset_id": (
                "Specify the synID of the dataset folder on Synapse to which you intend to submit "
                "the metadata manifest file. This is a required argument."
            ),
            "validate_component": (
                "The component or data type from the data model which you can use to validate the "
                "data filled in your manifest template."
            ),
            "hide_blanks": (
                "This is a boolean flag. If flag is provided when command line utility is executed, annotations with blank values will be hidden from a dataset's annotation list in Synaspe."
                "If not, annotations with blank values will be displayed."
            ),
            "manifest_record_type": (
                "Specify the way the manifest should be store as on Synapse. Options are 'file_only', 'file_and_entities', 'table_and_file' and "
                "'table_file_and_entities'. 'file_and_entities' will store the manifest as a csv and create Synapse files for each row in the manifest. "
                "'table_and_file' will store the manifest as a table and a csv on Synapse. "
                "'file_only' will store the manifest as a csv only on Synapse."
                "'table_file_and_entities' will perform the options file_with_entites and table in combination."
                "Default value is 'table_file_and_entities'."
            ),
            "table_manipulation": (
                "Specify the way the manifest tables should be store as on Synapse when one with the same name already exists. Options are 'replace' and 'upsert'. "
                "'replace' will remove the rows and columns from the existing table and store the new rows and columns, preserving the name and synID. "
                "'upsert' will add the new rows to the table and preserve the exisitng rows and columns in the existing table. "
                "Default value is 'replace'. "
                "Upsert specific requirements: {\n}"
                "'upsert' should be used for initial table uploads if users intend to upsert into them at a later time."
                "Using 'upsert' at creation will generate the metadata necessary for upsert functionality."
                "Upsert functionality requires primary keys to be specified in the data model and manfiest as <component>_id."
                "Currently it is required to use --table_column_names = display_name with table upserts."
            ),
            "annotation_keys": (
                "Store attributes using the class label (default) or store attributes using the display label. "
                "Attribute display names in the schema must not only include characters that are "
                "not accepted by Synapse. Annotation names may only contain: letters, numbers, '_' and '.'"
            ),
            "table_column_names": (
                "class_label, display_label, display_name, default, class_label. When true annotations and table columns will be uploaded with the display name formatting with blacklisted characters removed. "
                "To use for tables, use in conjunction with the use_schema_label flag."
            ),
            "file_annotations_upload": (
                "This is a boolean flag. Default to True. If False, annotations will not be added to files during submission."
            ),
            "data_model_labels": DATA_MODEL_LABELS_HELP,
        },
        "validate": {
            "short_help": ("Validation of manifest files."),
            "manifest_path": (
                "Specify the path to the metadata manifest file that you want to submit to a dataset on Synapse. "
                "This is a required argument."
            ),
            "data_type": (
                "Specify the component (data type) from the data model that is to be used "
                "for validating the metadata manifest file. You can either explicitly pass the data type here or provide "
                "it in the `config.yml` file as a value for the `(manifest > data_type)` key."
            ),
            "json_schema": (
                "Specify the path to the JSON Validation Schema for this argument. "
                "You can either explicitly pass the `.json` file here or provide it in the `config.yml` file "
                "as a value for the `(model > input > validation_schema)` key."
            ),
            "restrict_rules": (
                "This is a boolean flag. If flag is provided when command line utility is executed, validation suite will only run with in-house validation rules, "
                "and Great Expectations rules and suite will not be utilized."
                "If not, the Great Expectations suite will be utilized and all rules will be available."
            ),
            "project_scope": (
                "Specify a comma-separated list of projects to search through for cross manifest validation."
            ),
            "dataset_scope": (
                "Specify a dataset to validate against for filename validation."
            ),
            "data_model_labels": DATA_MODEL_LABELS_HELP,
        },
    }
}


# `schematic schema` related sub-commands description
schema_commands = {
    "schema": {
        "convert": {
            "short_help": (
                "Convert specification from CSV data model to JSON-LD data model."
            ),
            "output_jsonld": (
                "Path to where the generated JSON-LD file needs to be outputted."
            ),
            "data_model_labels": DATA_MODEL_LABELS_HELP,
        }
    }
}

viz_commands = {
    "visualization": {
        "config": (
            "Specify the path to the `config.yml` using this option. This is a required argument."
        ),
        "tangled_tree": {
            "figure_type": (
                f"Specify the type of schema visualization to make. Either {FIGURE_TYPES}."
            ),
            "text_format": (
                f"Specify the type of text to gather for tangled tree visualization, either {TEXT_TYPES}."
            ),
            "data_model_labels": DATA_MODEL_LABELS_HELP,
        },
    }
}
