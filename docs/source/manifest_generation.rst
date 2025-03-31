Generate a manifest
===================
A **manifest** is a structured file containing metadata that adheres to a specific data model. This page covers different ways to generate a manifest.

Prerequisites
-------------

**Before Using the Schematic CLI**

- **Install and Configure Schematic**:
  Ensure you have installed `schematic` and set up its dependencies.
  See the :ref:`installation` section for more details.

- **Understand Important Concepts**:
  Understand Important Concepts: Familiarize yourself with key concepts outlined on the :ref:`index` of the documentation.

- **Configuration File**:
  Learn more about each attribute in the configuration file by referring to the relevant documentation.


**Using the Schematic API in Production**

Visit the **Schematic API (Production Environment)**:
`<https://schematic.api.sagebionetworks.org/v1/ui/#/>`_

This will open the **Swagger UI**, where you can explore all available API endpoints.


Generate an empty manifest
---------------------------------

Option 1: Use the CLI
~~~~~~~~~~~~~~~~~~~~~

   You can generate a manifest by running the following command:

   .. code-block:: bash

       schematic manifest -c /path/to/config.yml get -dt <your_data_type> -s

   - **-c /path/to/config.yml**: Specifies the configuration file containing your data model location.
   - **-dt <your_data_type>**: Defines the data type for the manifest (e.g., `"Patient"`, `"Biospecimen"`).
   - **-s**: Generates a manifest as a Google Sheet.

If you want to generate a manifest as an excel spreadsheet, you could do:

.. code-block:: bash

    schematic manifest -c /path/to/config.yml get -dt <your data type> --output-xlsx <your-output-manifest-path.xlsx>

And if you want to generate a manifest as a csv file, you could do:

.. code-block:: bash

    schematic manifest -c /path/to/config.yml get -dt <your data type> --output-csv <your-output-manifest-path.csv>

Option 2: Use the API
~~~~~~~~~~~~~~~~~~~~~

1. Locate the `manifest/generate` endpoint.
2. Click "Try it out" to enable input fields.
3. Enter the following parameters and execute the request:

   - **schema_url**: The URL of your data model.
     - If your data model is hosted on **GitHub**, the URL should follow this format:
       - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
       - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **data_type**: The data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`).
       - You can specify multiple data types or enter `"all manifests"` to generate manifests for all available data types.

   - **output_format**: The desired format for the generated manifest.
     - Options include `"excel"` or `"google_sheet"`.

This will generate a manifest directly from the API.


Generate a manifest using a dataset on synapse
----------------------------------------------

Option 1: Use the CLI
~~~~~~~~~~~~~~~~~~~~~~

.. note::

    Ensure your **Synapse credentials** are configured before running the command.
    You can obtain a **personal access token** from Synapse by following the instructions here:
    `<https://python-docs.synapse.org/tutorials/authentication/#prerequisites>`_


The **top-level dataset** can be either an empty folder or a folder containing files.

   .. code-block:: bash

       schematic manifest -c /path/to/config.yml get -dt <your_data_type> -s -d <synapse_dataset_id>

   - **-c /path/to/config.yml**: Specifies the configuration file containing the data model location and asset view (`master_fileview_id`).
   - **-dt <your_data_type>**: Defines the data type/schema model for the manifest (e.g., `"Patient"`, `"Biospecimen"`).
   - **-d <your_dataset_id>**: Retrieves the existing manifest associated with a specific dataset on Synpase.

Option 2: Use the API
~~~~~~~~~~~~~~~~~~~~~~

To generate a manifest using the **Schematic API**, follow these steps:

1. Locate the **`manifest/generate`** endpoint in the **Swagger UI**.
2. Click **"Try it out"** to enable input fields.
3. Enter the required parameters and execute the request:

   - **schema_url**: The URL of your data model.
       - If your data model is hosted on **GitHub**, the URL should follow this format:
           - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
           - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **output_format**: The desired format for the generated manifest.
       - Options include `"excel"` or `"google_sheet"`.

   - **data_type**: The data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`).
       - You can specify multiple data types or enter `"all manifests"` to generate manifests for all available data types.

   - **dataset_id**: The **top-level Synapse dataset ID**.
       - This can be a **Synapse Project ID** or a **Folder ID**.

   - **asset_view**: The **Synapse ID of the fileview** containing the top-level dataset for which you want to generate a manifest.

Generate a manifest using a dataset on synapse and pull annotations
--------------------------------------------------------------------

Option 1: Use the CLI
~~~~~~~~~~~~~~~~~~~~~~

.. note::

    Ensure your **Synapse credentials** are configured before running the command.
    You can obtain a **personal access token** from Synapse by following the instructions here:
    `<https://python-docs.synapse.org/tutorials/authentication/#prerequisites>`_


The **top-level dataset** can be either an empty folder or a folder containing files.

   .. code-block:: bash

       schematic manifest -c /path/to/config.yml get -dt <your_data_type> -s -d <synapse_dataset_id> -a

   - **-c /path/to/config.yml**: Specifies the configuration file containing the data model location and asset view (`master_fileview_id`).
   - **-a**: Pulls annotations from Synapse and fills out the manifest with the annotations.
   - **-dt <your_data_type>**: Defines the data type/schema model for the manifest (e.g., `"Patient"`, `"Biospecimen"`).
   - **-d <your_dataset_id>**: Retrieves the existing manifest associated with a specific dataset on Synpase.


Option 2: Use the API
~~~~~~~~~~~~~~~~~~~~~~

To generate a manifest using the **Schematic API**, follow these steps:

1. Locate the **manifest/generate** endpoint in the **Swagger UI**.
2. Click **"Try it out"** to enable input fields.
3. Enter the required parameters and execute the request:

   - **schema_url**: The URL of your data model.
       - If your data model is hosted on **GitHub**, the URL should follow this format:
           - JSON-LD: `https://raw.githubusercontent.com/<your-repo-path>/data-model.jsonld`
           - CSV: `https://raw.githubusercontent.com/<your-repo-path>/data-model.csv`

   - **output_format**: The desired format for the generated manifest.
       - Options include `"excel"` or `"google_sheet"`.

   - **data_type**: The data type or schema model for your manifest (e.g., `"Patient"`, `"Biospecimen"`).
       - You can specify multiple data types or enter `"all manifests"` to generate manifests for all available data types.

   - **dataset_id**: The **top-level Synapse dataset ID**.
       - This can be a **Synapse Project ID** or a **Folder ID**.

   - **asset_view**: The **Synapse ID of the fileview** containing the top-level dataset for which you want to generate a manifest.

   - **use_annotations**: A boolean value that determines whether to pull annotations from Synapse and fill out the manifest with the annotations.
       - Set this value to `true` to pull annotations.
