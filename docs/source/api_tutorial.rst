Contributing your manifest with the APIs
---------------------------------------

Prerequisites
~~~~~~~~~~~~~

1. **Prepare a data model**: Ensure you have a data model ready. If you don’t have one, you can use the following example models:
   * CSV data model [https://raw.githubusercontent.com/Sage-Bionetworks/schematic/refs/heads/main/tests/data/example.model.csv]
   * JSON-LD data model [https://raw.githubusercontent.com/Sage-Bionetworks/schematic/refs/heads/main/tests/data/example.model.jsonld]


The contribution process includes three main commands.
For information about the parameters of each of these commands, please refer to the CLI Reference section.

1. **Generate** a manifest to fill out
2. **Validate** the manifest (optional, since it's included in submission)
3. **Submit** the manifest to Synapse


Step 1: Generate a Manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Visit the Schematic API in the production environment: https://schematic.api.sagebionetworks.org/v1/ui/#/.

This will open the Swagger UI, where you can explore all available endpoints for the Schematic API.

To generate a manifest:

1. Locate the manifest/generate endpoint.
2. Click "Try it out" to enable input fields.
3. Enter the following parameters and execute the request:
    - **Schema_url**: The url of your data model. If you data model is hosted in Github, the url should looks like: `https://raw.githubusercontent.com/path-of-your-data-model.jsonld` or `https://raw.githubusercontent.com/path-of-your-data-model.csv`.
    - **data_type**: The data type or schema model for your manifest (e.g., "Patient", "Biospecimen"). Feel free to enter all data types or "all manifests" to get manifests for all data types.
    - **output_format**: The format in which you want to generate the manifest (e.g."excel", "google_sheet").

This will generate a manifest directly from the API.
