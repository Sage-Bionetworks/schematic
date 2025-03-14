Contributing your manifest with the CLI
---------------------------------------

In this tutorial, you'll learn how to contribute your metadata manifests to Synapse using the `CLI`. Following best practices,
we will cover generating, validating, and submitting your manifest in a structured workflow.

.. note::

    Whether you have submitted manifests before to your "Top Level Folder" (see important terminology) OR are submitting a new manifest, we **strongly recommend** you to follow this workflow.
    If you deviate from this workflow or upload files to Synapse directly without using schematic, you risk the errors outlined in the
    troubleshooting section of the documentation.

    Question: What if I've already gone through this workflow, can I download the manifest, modify it and upload it to Synapse without Schematic?

    Answer: Yes, but you risk running into errors when others use these commands.
    Updates may have been made to the data model by the DCC and these changes won't be reflected unless you regenerate your manfiest.
    We strongly recommend not doing that.


Prerequisites
~~~~~~~~~~~~~

1. **Install and configure Schematic**: Ensure that you have installed `schematic` and set up its dependencies. See "Installation Guide For: Users" for more information.
2. **Important Concepts**: Make sure you know the important concepts outlined on the home page of the doc site.
3. **Configuration**: Read more here about each of the attributes in the configuration file.
4. **Prepare a data model**: Ensure you have a data model ready. If not, please use the data model in `schematic/tests/data/example.model.csv` or `/Users/lpeng/Documents/schematic-git2/schematic/tests/data/example.model.jsonld`

Steps to Contribute a Manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The contribution process includes three main commands.
For information about the parameters of each of these commands, please refer to the CLI Reference section.

1. **Generate** a manifest to fill out
2. **Validate** the manifest (optional, since it's included in submission)
3. **Submit** the manifest to Synapse


Step 1: Generate a Manifest
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `schematic manifest get` command that creates a manifest template based on a data model and existing manifests.

.. note::

    This step is crucial for ensuring that your manifest includes all the necessary columns and headers. As of v24.10.2, you will
    want to generate the manifest to ensure the right Filenames are populated in your manifest. If you just uploaded your folder
    with data or files to a folder, you may see that this files are missing or get a `LookUp` error.  This is an artifact of Synapse
    fileviews, please run this command again.

Rename config_example.yml to config.yml, and update it to specify the location of your data model:

.. code-block:: text

    model:
        location: "/your data model location"

Your data model can be in either JSON-LD or CSV format.

Then, run the following command to generate a manifest as a google sheet.

.. code-block:: bash

    schematic manifest -c /path/to/config.yml get -dt <your data type> -s

- **Data Type**: The data type or schema model for your manifest (e.g., "Patient", "Biospecimen")..

This command will create a google sheet with the necessary columns and headers, which you can then fill with your metadata.


If you want to generate a manifest as an excel spreadsheet, you could do:

.. code-block:: bash

    schematic manifest -c /path/to/config.yml get -dt <your data type> --output-xlsx <your-output-manifest-path.xlsx>

And if you want to generate a manifest as a csv file, you could do:

.. code-block:: bash

    schematic manifest -c /path/to/config.yml get -dt <your data type> --output-csv <your-output-manifest-path.csv>

Step 2: Validate the Manifest (Optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Though optional, `schematic model validate`` is a useful step to ensure that your manifest meets the required standards before submission.
It checks for any errors, such as missing or incorrectly formatted values.

.. note::

    If your manifest has an empty Component column, you will need to fill it out before validation.

.. code-block:: bash

    schematic model -c /path/to/config.yml validate -dt <your data type> -mp <your csv manifest path>

If validation passes, you'll see a success message; if there are errors, `schematic` will list them. Correct any issues before proceeding to submission.

Step 3: Submit the Manifest to Synapse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `schematic model submit` command uploads your manifest to Synapse. This command will automatically validate
the manifest as part of the submission process, so if you prefer, you can skip the standalone validation step.

.. note::

    During the manifest submission, it will fill out the entityId column if it's missing.

.. code-block:: bash

    schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse top level folder id> -vc <your data type> -mrt file_only

This command will:

- Validate your manifest
- If validation is successful, submit it to the specified "Top Level Folder" (see important terminology) in Synapse.
