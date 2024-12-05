Configure Schematic
===================

This is an example config for Schematic. All listed values are those that are the default if a config is not used. Remove any fields in the config you don't want to change.
If you remove all fields from a section, the entire section should be removed including the header.
Change the values of any fields you do want to change.  Please view the installation section for details on how to set some of this up.

.. code-block:: yaml

    # This describes where assets such as manifests are stored
    asset_store:
        # This is when assets are stored in a synapse project
        synapse:
            # Synapse ID of the file view listing all project data assets.
            master_fileview_id: "syn23643253"
            # Path to the synapse config file, either absolute or relative to this file
            config: ".synapseConfig"
            # Base name that manifest files will be saved as
            manifest_basename: "synapse_storage_manifest"

    # This describes information about manifests as it relates to generation and validation
    manifest:
        # Location where manifests will saved to
        manifest_folder: "manifests"
        # Title or title prefix given to generated manifest(s)
        title: "example"
        # Data types of manifests to be generated or data type (singular) to validate manifest against
        data_type:
            - "Biospecimen"
            - "Patient"

    # Describes the location of your schema
    model:
        # Location of your schema jsonld, it must be a path relative to this file or absolute
        location: "tests/data/example.model.jsonld"

    # This section is for using google sheets with Schematic
    google_sheets:
        # Path to the google service account creds, either absolute or relative to this file
        service_acct_creds: "schematic_service_account_creds.json"
        # When doing google sheet validation (regex match) with the validation rules.
        #   true is alerting the user and not allowing entry of bad values.
        #   false is warning but allowing the entry on to the sheet.
        strict_validation: true


This document will go into detail what each of these configurations mean.

Asset Store
-----------

Synapse
~~~~~~~
This describes where assets such as manifests are stored and the configurations of the asset store is described
under the asset store section.

* master_fileview_id: Synapse ID of the file view listing all project data assets.
* config: Path to the synapse config file, either absolute or relative to this file. Note, if you use `synapse config` command, you will have to provide the full path to the configuration file.
* manifest_basename: Base name that manifest files will be saved as on Synapse. The Component will be appended to it so for example: `synapse_storage_manifest_biospecimen.csv`

Manifest
--------
This describes information about manifests as it relates to generation and validation.  Note: some of these configurations can be overwritten by the CLI commands.

* manifest_folder: Location where manifests will saved to. This can be a relative or absolute path on your local machine.
* title: Title or title prefix given to generated manifest(s). This is used to name the manifest file saved locally.
* data_type: Data types of manifests to be generated or data type (singular) to validate manifest against. If you wanted all the available manifests, you can input "all manifests"


Model
-----
Describes the location of your schema

* location: This is the location of your schema jsonld, it must be a path relative to this file or absolute path.  Currently URL's are NOT supported, so you will have to download the jsonld data model.  Here is an example: https://raw.githubusercontent.com/ncihtan/data-models/v24.9.1/HTAN.model.jsonld

Google Sheets
-------------
Schematic leverages the Google API to generate manifests. This section is for using google sheets with Schematic

* service_acct_creds: Path to the google service account creds, either absolute or relative to this file. This is the path to the service account credentials file that you download from Google Cloud Platform.
* strict_validation: When doing google sheet validation (regex match) with the validation rules.

    * True is alerting the user and not allowing entry of bad values.
    * False is warning but allowing the entry on to the sheet.
