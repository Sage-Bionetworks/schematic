Generate a manifest
---------------------------------------
A **manifest** is a structured file containing metadata that adheres to a specific data model. This page covers different ways to generate a manifest.

Generate a manifest using the CLI
~~~~~~~~~~~~~~~~~~~~~~~~~~~

   You can generate a manifest using the **Schematic CLI** by running the following command:

   .. code-block:: bash

       schematic manifest -c /path/to/config.yml get -dt <your_data_type> -s

   - **`-c /path/to/config.yml`**: Specifies the configuration file containing your data model location.
   - **`-dt <your_data_type>`**: Defines the data type for the manifest (e.g., `"Patient"`, `"Biospecimen"`).
   - **`-s`**: Generates a manifest as a Google Sheet.

If you want to generate a manifest as an excel spreadsheet, you could do:

.. code-block:: bash

    schematic manifest -c /path/to/config.yml get -dt <your data type> --output-xlsx <your-output-manifest-path.xlsx>

And if you want to generate a manifest as a csv file, you could do:

.. code-block:: bash

    schematic manifest -c /path/to/config.yml get -dt <your data type> --output-csv <your-output-manifest-path.csv>


Generate a manifest using the API
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Visit the Schematic API in the production environment: https://schematic.api.sagebionetworks.org/v1/ui/#/.

This will open the Swagger UI, where you can explore all available endpoints for the Schematic API.

To generate a manifest:

1. Locate the `manifest/generate` endpoint.
2. Click "Try it out" to enable input fields.
3. Enter the following parameters and execute the request:
    - **Schema_url**: The url of your data model. If you data model is hosted in Github, the url should look like: `https://raw.githubusercontent.com/path-of-your-data-model.jsonld` or `https://raw.githubusercontent.com/path-of-your-data-model.csv`.
    - **data_type**: The data type or schema model for your manifest (e.g., "Patient", "Biospecimen"). Feel free to enter multiple data types or "all manifests" to get manifests for all data types.
    - **output_format**: The format in which you want to generate the manifest (e.g."excel", "google_sheet").

This will generate a manifest directly from the API.
