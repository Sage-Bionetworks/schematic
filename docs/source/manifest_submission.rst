#############################
Submit a manifest to Synapse
#############################

*************
Prerequisites
*************

**Obtain Synapse Credentials**:
================================
Ensure you have a Synapse account and set up Synapse configuration file correctly. See the :ref:`setting up configuration files <installation:6. Set up configuration files>` section for more details.


**Using the Schematic API in Production**
=========================================

Visit the **Schematic API (Production Environment)**:
`<https://schematic.api.sagebionetworks.org/v1/ui/#/>`_

This will open the **Swagger UI**, where you can explore all available API endpoints.


**Before Using the Schematic CLI**
==================================

- **Install and Configure Schematic**:
  Ensure you have installed ``schematic`` and set up its dependencies.
  See the :ref:`installation:installation` section for more details.

- **Understand Important Concepts**:
  Familiarize yourself with key concepts outlined on the :ref:`index` of the documentation.

- **Configuration File**:
  For more details on configuring Schematic, refer to the :ref:`configuration:Configure Schematic` section.

- **Obtain a manifest**:
  Please obtain a manifest by following the documentation of :ref:`generating a manifest <manifest_generation>`.


Run help command
=================

You could run the following commands to learn about subcommands with manifest submission:

.. code-block:: bash

    schematic model -h

You could also run the following commands to learn about all the options with manifest submission:

.. code-block:: bash

    schematic model --config path/to/config.yml submit -h

**********************************
Submit a Manifest File to Synapse
**********************************

.. note::

  You can configure the format of the manifest being submitted by using the `-mrt flag` in the CLI or the `manifest_record_type` in the API.

  For table column names, here's a brief explanation of all the options:
   - display_name: use raw display name defined in the data model as the column name, no modifications to the name will be made.
   - display_label: use the display name formatting as the column name. Will strip blacklisted characters (including spaces) when present.
     The blacklisted characters are: "(", ")", ".", " ", "-"
   - class_label: default, use standard class label and strip any blacklisted characters (including spaces) when present. A schematic class label is UpperCamelCase.

.. note::

   Manifests should be submitted to the top-level dataset folder. Below are some examples demonstrating where the manifest file should go:

   .. code-block:: text

      syn12345678/
      ├── file1.csv
      ├── file2.csv
      ├── manifest.csv

   Here is the top-level folder ID: syn12345678

   Here's an example using subfolders:

   .. code-block:: text

      syn12345678/
      ├── subfolder1/
      │   └── file1
      ├── subfolder2/
      │   └── file2
      ├── file3
      ├── manifest.csv

   Here is the top-level folder ID: syn12345678

.. _submit_manifest_cli:

Option 1: Use the CLI
=====================

.. note::

    During submission, validation is optional. If you have finished validation in previous step, you could skip validation by removing `-vc <your data type>`


.. code-block:: bash

    schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse top level folder id> -vc <your data type> -mrt table_and_file -no-fa -tcn "class_label"

- **-c /path/to/config.yml**: Specifies the configuration file containing the data model location and asset view (`master_fileview_id`).
- **-mp**: Your manifest file path.
- **-mrt**: The format of manifest submission. The options are: "table_and_file", "file_only", "file_and_entities", "table_file_and_entities". "file_only" option would submit the manifest as a file.
- **-vc <your_data_type>**: Defines the data type/schema model for the manifest (e.g., `"Patient"`, `"Biospecimen"`). To skip validation, remove this flag.
- **-d <your_dataset_id>**: the top level dataset id that you want to submit the manifest to.
- **-no-fa**: Skips the file annotations upload.
- **-tcn**: Table Column Names: This is optional, and the available options are "class_label", "display_label", and "display_name". The default is "class_label", but you can change it based on your requirements.


.. _submit_manifest_api:

Option 2: Use the API
======================

.. note::

    During submission, validation is optional. If you have finished validation in previous step, you could skip validation by excluding the `data_type` and `dataset_scope` parameter values.


1. Visit the `**model/submit** endpoint <https://schematic.api.sagebionetworks.org/v1/ui/#/Model%20Operations/schematic_api.api.routes.submit_manifest_route>`_
2. Click **"Try it out"** to enable input fields.
3. Enter the required parameters and execute the request:

   - **schema_url**: The raw URL of your data model. If your data model is hosted on **GitHub**, use the following formats:
       - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
       - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **data_type**: Specify the data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`). To skip validation, exclude this parameter by removing the default inputs.

   - **dataset_id**: Provide the **top-level Synapse dataset ID**.
       - This can be either a **Synapse Project ID** or a **Folder ID**.

   - **asset_view**: Enter the **Synapse ID of the fileview** containing the top-level dataset for which you want to generate a manifest.

   - **dataset_scope** and **project_scope**: Remove the default inputs.

   - **file_annotations_upload**: Set this to `False`.

   - **table_manipulation**: The default is "replace". You can keep it as is.

   - **manifest_record_type**: Set this to "table_and_file" or adjust it based on your project requirements.

   - **table_column_names**: This is optional. Available options are "class_label", "display_label", and "display_name". The default is "class_label".


*******************************************
Submit a Manifest file and Add Annotations
*******************************************

.. note::

  Since annotations are enabled in the submission, if you are submitting a file-based manifest, you should see annotations attached to the entity IDs listed in the manifest.


.. _submit_manifest_add_annotations_cli:

Option 1: Use the CLI
=====================


.. note::

    During submission, validation is optional. If you have finished validation in previous step, you could skip validation by removing `-vc <your data type>`


.. code-block:: bash

    schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse top level folder id> -vc <your data type> -mrt table_and_file -fa -tcn "class_label"

- **-c /path/to/config.yml**: Specifies the configuration file containing the data model location and asset view (`master_fileview_id`).
- **-mp**: Your manifest file path.
- **-mrt**: The format of manifest submission. The options are: "table_and_file", "file_only", "file_and_entities", "table_file_and_entities". "file_only" option would submit the manifest as a file.
- **-vc <your_data_type>**: Defines the data type/schema model for the manifest (e.g., `"Patient"`, `"Biospecimen"`). To skip validation, remove this flag.
- **-d <your_dataset_id>**: the top level dataset id that you want to submit the manifest to.
- **-fa**: Enable file annotations upload.
- **-tcn**: Table Column Names: This is optional, and the available options are "class_label", "display_label", and "display_name". The default is "class_label", but you can change it based on your requirements.


.. _submit_manifest_add_annotations_api:

Option 2: Use the API
======================

.. note::

    During submission, validation is optional. If you have finished validation in previous step, you could skip validation by excluding the `data_type` and `dataset_scope` parameter values.


1. Visit the `**model/submit** endpoint <https://schematic.api.sagebionetworks.org/v1/ui/#/Model%20Operations/schematic_api.api.routes.submit_manifest_route>`_
2. Click **"Try it out"** to enable input fields.
3. Enter the required parameters and execute the request:

   - **schema_url**: The raw URL of your data model. If your data model is hosted on **GitHub**, the URL should follow this format:
       - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
       - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **data_type**: Specify the data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`). To skip validation, exclude this parameter by removing the default inputs.

   - **dataset_id**: The **top-level Synapse dataset ID**.
     - This can be a **Synapse Project ID** or a **Folder ID**.

   - **asset_view**: The **Synapse ID of the fileview** containing the top-level dataset for which you want to generate a manifest.

   - **dataset_scope** and **project_scope**: Remove any default inputs provided in these fields.

   - **file_annotations_upload**: Set this to `True`.

   - **table_manipulation**: The default is "replace". You can keep it as is or modify it if needed.

   - **manifest_record_type**: Set this to "table_and_file" or adjust it based on your project requirements.

   - **table_column_names**: This is optional. Available options are "class_label", "display_label", and "display_name". The default is "class_label".


**************************************
Expedite submission process (Optional)
**************************************

If your asset view contains multiple projects, it might take some time for the submission to finish.

You could expedite the submission process by specifying the project_scope parameter. This parameter allows you to specify the project(s) that you want to submit the manifest to.

To utilize this parameter, make sure that the projects listed there are part of the asset view.

.. _expedite_submission_cli:

Option 1: Use the CLI
=====================

.. code-block:: bash

    schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse top level folder id> -vc <your data type> -no-fa -ps "project_id1, project_id2"

- **-ps**: Specifies the project scope as a comma separated list of project IDs.


.. _expedite_submission_api:

Option 2: Use the API
======================

1. Visit the `**model/submit** endpoint <https://schematic.api.sagebionetworks.org/v1/ui/#/Model%20Operations/schematic_api.api.routes.submit_manifest_route>`_
2. Click **"Try it out"** to enable input fields.
3. Enter the required parameters and execute the request:

   - **schema_url**: The raw URL of your data model. If your data model is hosted on **GitHub**, use the following formats:
       - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
       - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **data_type**: Specify the data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`). To skip validation, exclude this parameter by removing the default inputs.

   - **dataset_id**: Provide the **top-level Synapse dataset ID**.
       - This can be either a **Synapse Project ID** or a **Folder ID**.

   - **asset_view**: Enter the **Synapse ID of the fileview** containing the top-level dataset for which you want to generate a manifest.

   - **project_scope**: Remove the default inputs. Add project IDs as string items.

   - **dataset_scope**: Remove default inputs.

   - **file_annotations_upload**: Set this to `false`.

   - **table_manipulation**: The default is "replace". You can keep it as is.

   - **manifest_record_type**: Set this to "file_only" or adjust it based on your project requirements.

   - **table_column_names**: This parameter is not applicable when uploading a manifest as a file. You can keep it as is and it will be ignored.


*************************************
Enable upsert for manifest submission
*************************************

By default, the CLI/API will replace the existing manifest and table with the new one. If you want to update the existing manifest and table, you could use the upsert option.


Pre-requisites
==============

1. Ensure that all your manifests, including both the initial manifests and those containing rows to be upserted, include a primary key: <YourComponentName_id>. For example, if your component name is "Patient", the primary key should be "Patient_id".
2. If you plan to use upsert in the future, select the upsert option during the initial table uploads.
3. Currently it is required to use -tcn "display_label" with table upserts.

.. _enable_upsert_cli:

Option 1: Use the CLI
======================

.. code-block:: bash

    schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse top level folder id> -mrt table_and_file -no-fa -tcn "display_label" -tm "upsert"

- **-tm**: The default option is "replace". Change it to "upsert" for enabling upsert.
- **-tcn**: Use display label for upsert.


.. _enable_upsert_api:

Option 2: Use the API
======================

1. Visit the `**model/submit** endpoint <https://schematic.api.sagebionetworks.org/v1/ui/#/Model%20Operations/schematic_api.api.routes.submit_manifest_route>`_
2. Click **"Try it out"** to enable input fields.
3. Enter the required parameters and execute the request:

   - **schema_url**: The raw URL of your data model. If your data model is hosted on **GitHub**, use the following formats:
       - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
       - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **data_type**: Specify the data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`). To skip validation, exclude this parameter by removing the default inputs.

   - **dataset_id**: Provide the **top-level Synapse dataset ID**.
       - This can be either a **Synapse Project ID** or a **Folder ID**.

   - **asset_view**: Enter the **Synapse ID of the fileview** containing the top-level dataset for which you want to generate a manifest.

   - **dataset_scope** and **project_scope**: Remove the default inputs.

   - **file_annotations_upload**: Set this to `False` if you do not want annotations to be uploaded.

   - **table_manipulation**: Update it to "upsert".

   - **manifest_record_type**: Set this to **"table_and_file"**

   - **table_column_names**:  Choose **"display_label"** for upsert.
