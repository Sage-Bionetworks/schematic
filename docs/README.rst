Schematic
=========

1. Introduction
-----------------

SCHEMATIC is an acronym for *Schema Engine for Manifest Ingress and
Curation*. The Python based infrastructure provides a *novel*
schema-based, data ingress ecosystem, that is meant to streamline the
process of metadata annotation and validation for various data
contributors.

2. Installation Requirements and Pre-requisites
-------------------------------------------------

Following are the tools or packages that you will need to set up
``schematic`` for your use:

-  Python 3.7.1 or higher

If you do not have a version of Python greater than 3.7.1, it is
recommended to use ``pyenv`` to be able to easily use and switch between
multiple Python versions.

-  `pyenv <https://github.com/pyenv/pyenv>`__

It is recommended that you install the ``poetry`` dependency manager if
you are a current (or potential) ``schematic`` contributor or a DCC
admin managing installations of the `Data Curator
App <https://github.com/Sage-Bionetworks/data_curator/>`__.

Note: If you're experiencing issues with `pyenv <https://github.com/pyenv/pyenv>`__
on ``macOS``, you can consider using `miniconda <https://docs.conda.io/en/latest/miniconda.html>`__.

-  `poetry <https://github.com/python-poetry/poetry>`__

**Note**: Make sure you are a registered and certified user on
`synapse.org <https://www.synapse.org/>`__, and also have all the
right permissions to download credentials files in the following steps.
Contact your DCC liaison to request for permission to access the
credentials files.

3. Package Installation and Setup
-------------------------------------

3.1. Virtual Environment Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    python -m venv .venv  # create a virtual environment

.. code:: bash

    source .venv/bin/activate # activate the `venv` virtual environment

3.2. Installing
~~~~~~~~~~~~~~~~~

.. code:: bash

    python -m pip install schematicpy  # install and upgrade package

3.3. Fill in Configuration File(s)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two main configuration files that need to be edited –
`config.yml <https://github.com/Sage-Bionetworks/schematic/blob/develop/config.yml>`__
and
`.synapseConfig <https://github.com/Sage-Bionetworks/synapsePythonClient/blob/v2.2.2-rc/synapseclient/.synapseConfig>`__.

Download a copy of the ``.synapseConfig`` file, open the file in the
editor of your choice and edit the
`authtoken <https://github.com/Sage-Bionetworks/synapsePythonClient/blob/ba42e2d35673d19ce2b5bbd089f4fc1e99aca178/synapseclient/.synapseConfig#L9>`__
attribute under the
`[authentication] <https://github.com/Sage-Bionetworks/synapsePythonClient/blob/ba42e2d35673d19ce2b5bbd089f4fc1e99aca178/synapseclient/.synapseConfig#L7>`__
section.

 Description of config.yml attributes

::

    definitions:
        synapse_config: "~/path/to/.synapseConfig"
        creds_path: "~/path/to/credentials.json"
        token_pickle: "~/path/to/token.pickle"
        service_acct_creds: "~/path/to/service_account_creds.json"

    synapse:
        master_fileview: "syn23643253" # fileview of project with datasets on Synapse
        manifest_folder: "~/path/to/manifest_folder/" # manifests will be downloaded to this folder
        manifest_filename: "filename.ext" # name of the manifest file in the project dataset
        token_creds: "syn23643259" # synapse ID of credentials.json file
        service_acct_creds: "syn25171627" # synapse ID of service_account_creds.json file

    manifest:
        title: "Patient Manifest " # title of metadata manifest file
        data_type: "Patient" # component or data type from the data model

    model:
        input:
            location: "data/schema_org_schemas/example.jsonld" # path to JSON-LD data model
            file_type: "local" # only type "local" is supported currently
            validation_schema: "~/path/to/validation_schema.json" # path to custom JSON Validation Schema JSON file
            log_location: "~/path/to/log_folder/" # auto-generated JSON Validation Schemas can be logged
        

Note: Paths can be specified relative to the `config.yml` file or as absolute paths.

3.4. Obtain Google Credentials File(s)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    schematic init --config ~/path/to/config.yml --auth [token|serivce_account] 

The ``credentials.json`` file is required when you are using
`OAuth2 <https://developers.google.com/identity/protocols/oauth2>`__
to authenticate with the Google APIs.

For details about the steps involved in the `OAuth2 authorization
flow <https://github.com/Sage-Bionetworks/schematic/blob/develop/schematic/utils/google_api_utils.py#L18>`__,
refer to the ``Credentials`` section in the
`docs/md/details <https://github.com/Sage-Bionetworks/schematic/blob/develop/docs/md/details.md#credentials>`__
document.

Use the ``schematic_service_account_creds.json`` file for the service
account mode of authentication (*for Google services/APIs*).

Note: The ``Selection Options`` dropdown which allows the user to select
multiple values in a cell during manifest annotation `does not
work <https://developers.google.com/apps-script/api/concepts>`__ with
the service account mode of authentication.

4. Command Line Interface
-------------------------------

4.1. Schematic Initialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Initialize `schematic` for use with the `init` command by selecting the 
mode of authentication of your choice:

.. code:: bash

    schematic init --config ~/path/to/config.yml

4.2. Metadata Manifest Generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To generate a metadata manifest template based on a data type that is
present in your data model:

.. code:: bash

    schematic manifest --config ~/path/to/config.yml get

4.3. Metadata Manifest Validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To simply validate the data filled in the manifest generated from the 
above step:

.. code:: bash

    schematic model --config validate --manifest_path ~/path/to/manifest.csv

4.4. Metadata Manifest Validation and Submission
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To submit (and optionally validate) your filled metadata manifest file:

.. code:: bash

    schematic model --config ~/path/to/config.yml submit --manifest_path ~/path/to/manifest.csv --dataset_id dataset_synapse_id

Note: To view a full list of all the arguments that can be supplied to
the command line interfaces, add a ``--help`` option at the end of each
of the commands.

5. Contributing
-----------------

Interested in contributing? Awesome! We follow the typical `GitHub
workflow <https://guides.github.com/introduction/flow/>`__ of forking a
repo, creating a branch, and opening pull requests. For more information
on how you can add or propose a change, visit our `contributing
guide <https://github.com/Sage-Bionetworks/schematic/blob/develop/CONTRIBUTION.md>`__.
To start contributing to the package, you can refer to the `Getting
Started <https://github.com/Sage-Bionetworks/schematic/blob/develop/CONTRIBUTION.md#getting-started>`__
section in our `contributing
guide <https://github.com/Sage-Bionetworks/schematic/blob/develop/CONTRIBUTION.md>`__.

6. Contributors
-----------------

Active contributors and maintainers:

-  `Milen Nikolov <https://github.com/milen-sage>`__
-  `Sujay Patil <https://github.com/sujaypatil96>`__
-  `Bruno Grande <https://github.com/BrunoGrandePhD>`__
-  `Xengie Doan <https://github.com/xdoan>`__
