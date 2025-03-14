Submit a manifest to Synapse
============================

Prerequisites
-------------

**Obtain Synapse Credentials**:
Ensure you have a Synapse account and have obtained your Synapse credential by following the instructions here:
    `<https://python-docs.synapse.org/tutorials/authentication/#prerequisites>`_

**Before Using the Schematic CLI**

- **Install and Configure Schematic**:
  Ensure you have installed `schematic` and set up its dependencies.
  Refer to the **"Installation Guide for Users"** for detailed instructions.

- **Understand Important Concepts**:
  Familiarize yourself with key concepts outlined on the **home page** of the documentation.

- **Configuration File**:
  Learn more about each attribute in the configuration file by referring to the relevant documentation.

- **Obtain a manifest**:
  Please obtain a manifest by following the documentation of generating a manifest.


**Using the Schematic API in Production**

Visit the **Schematic API (Production Environment)**:
`<https://schematic.api.sagebionetworks.org/v1/ui/#/>`_

This will open the **Swagger UI**, where you can explore all available API endpoints.


Submit a Manifest File to Synapse
---------------------------------

Option 1: Use the CLI
~~~~~~~~~~~~~~~~~~~~~~

To submit a manifest file to Synapse, you will need to use the `schematic model submit` command.
This command will upload your manifest to Synapse and automatically validate it.

.. note::

    During submission, validation is optional. If you have finished validation in previous step, you could skip validation by removing `-vc <your data type>`


.. code-block:: bash

    schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse top level folder id> -vc <your data type> -mrt file_only

   - **`-c /path/to/config.yml`**: Specifies the configuration file containing the data model location and asset view (`master_fileview_id`).
   - **`-mp **: Your manifest file path.
   - **`-mrt **: The format of manifest submission. The options are: "table_and_file", "file_only", "file_and_entities", "table_file_and_entities". "file_only" option would submit the manifest as a file.
   - **`-vc <your_data_type>`**: Defines the data type/schema model for the manifest (e.g., `"Patient"`, `"Biospecimen"`).
   - **`-d <your_dataset_id>`**: Retrieves the existing manifest associated with a specific dataset on Synpase.


Option 2: Use the API
~~~~~~~~~~~~~~~~~~~~~~

1. Locate the **`model/submit`** endpoint in the **Swagger UI**.
2. Click **"Try it out"** to enable input fields.
3. Enter the required parameters and execute the request:

   - **`schema_url`**: The URL of your data model.
     - If your data model is hosted on **GitHub**, the URL should follow this format:
       - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
       - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **`data_type`**: The data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`). To skip validation, remove the default inputs.

   - **`dataset_id`**: The **top-level Synapse dataset ID**.
     - This can be a **Synapse Project ID** or a **Folder ID**.

   - **`asset_view`**: The **Synapse ID of the fileview** containing the top-level dataset for which you want to generate a manifest.

   - remove default inputs in dataset_scope and project_scope

   - set file_annotations_upload to false

   - table_manipulation is "replace" by default. You could keep it that way.

   - set **`manifest_record_type`**` to "file_only"


Submit a Manifest file and a Table to Synapse
---------------------------------------------

Submit a Manifest file and Add Annotations
-------------------------------------------
