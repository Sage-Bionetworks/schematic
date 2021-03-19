#!/usr/bin/env python3

# `schematic manifest` related sub-commands description
manifest_commands = {
    "manifest": {
        "config": (
            "Specify the path to the `config.yml` using this option. This is a required argument."
            ),
        "get": {
            "short_help":(
                "Specify the path to the `config.yml` using this option. "
                "This is a required argument."
                ),
            "title": (
                "Specify the title of the manifest that will be created at the end of the run. "
                "You can either explicitly pass the title of the manifest here or provide it in the `config.yml` "
                "file as a value for the `(manifest > title)` key."
                ),
            "data_type": (
                "Specify the component (data type) from the data model that is to be used "
                "for generating the metadata manifest file. You can either explicitly pass the data type here or provide "
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
                "Takes `True` or `False` as argument values. If `True` then it will produce a Googlesheets URL/link "
                "to the metadata manifest file. If `False`, then it will produce a pandas dataframe for the same."
                ),
            "output_csv": (
                "Path to where the CSV manifest template should be stored."
                ),
            "use_annotations": (
                "Takes `True` or `False` as argument values. If `True` then it will prepopulate template with existing annotations "
                "associated with dataset files. If `False` it will skip populating template with Synapse annotations."
                ),
            "json_schema": (
                "Specify the path to the JSON Validation Schema for this argument. "
                "You can either explicitly pass the `.json` file here or provide it in the `config.yml` file "
                "as a value for the `(model > input > validation_schema)` key."
                )
        }
    }
}


# `schematic model` related sub-commands description
model_commands = {
    "model": {
        "config": (
            "Specify the path to the `config.yml` using this option. This is a required argument."
            ),
        "submit": {
            "short_help": (
                "Validation (optional) and submission of manifest files."
                ),
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
                )
        }
    }
}


# `schematic schema` related sub-commands description
schema_commands = {
    "schema": {
        "convert": {
            "short_help": (
                "Convert specification from CSV data model to JSON-LD data model."
            ),
            "base_schema": (
                "Path to base data model. BioThings data model is loaded by default."
            ),
            "output_jsonld": (
                "Path to where the generated JSON-LD file needs to be outputted."
            )
        }
    }
}
