Schematic
=========

1.1. Introduction
-----------------

SCHEMATIC is an acronym for *Schema Engine for Manifest Ingress and
Curation*. The Python based infrastructure provides a *novel*
schema-based, data ingress ecosystem, that is meant to streamline the
process of metadata annotation and validation for various data
contributors.

1.2. Installation Requirements and Pre-requisites
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

-  `poetry <https://github.com/python-poetry/poetry>`__

**Important**: Make sure you are a registered and certified user on
`synapse.org <https://www.synapse.org/>`__, and also have all the
right permissions to download credentials files in the following steps.
Contact your DCC liaison to request for permission to access the
credentials files.

1.3. Package Setup Instructions
-------------------------------

1.3.1. Clone Project Repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since the package isn't available on `PyPI <https://pypi.org/>`__
yet, to setup the package you need to ``clone`` the project repoository
from GitHub by running the following command:

.. code:: bash

    git clone --single-branch --branch develop `https://github.com/Sage-Bionetworks/schematic.git`

1.3.2. Virtual Environment Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    python -m venv .venv  # create a virtual environment

.. code:: bash

    source .venv/bin/activate # activate the `venv` virtual environment

1.3.3. Install Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~

After cloning the ``schematic`` project from GitHub and setting up your
virtual environment:

.. code:: bash

    cd schematic  # change directory to schematic
    git checkout develop  # switch to develop branch of schematic 
    poetry build # build source and wheel archives
    pip install dist/schematicpy-0.1.11-py3-none-any.whl  # install wheel file

1.3.4. Obtain Credentials File(s)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

    synapse get syn21088684 # download copy of credentials.json file

The ``credentials.json`` file is required when you are using
`OAuth2 <https://developers.google.com/identity/protocols/oauth2>`__
to authenticate with the Google APIs.

For details about the steps involved in the `OAuth2 authorization
flow <https://github.com/Sage-Bionetworks/schematic/blob/develop/schematic/utils/google_api_utils.py#L18>`__,
refer to the ``Credentials`` section in the
`docs/details <https://github.com/Sage-Bionetworks/schematic/blob/develop/docs/details.md#credentials>`__
document.

.. code:: bash

    synapse get syn24214983 # download copy of schematic_service_account_creds.json file

Use the ``schematic_service_account_creds.json`` file for the service
account mode of authentication (*for Google services/APIs*).

Note: The ``Selection Options`` dropdown which allows the user to select
multiple values in a cell during manifest annotation `does not
work <https://developers.google.com/apps-script/api/concepts>`__ with
the service account mode of authentication.

1.3.5. Fill in Configuration File(s)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two main configuration files that need to be edited –
`config.yml <https://github.com/Sage-Bionetworks/schematic/blob/develop/config.yml>`__
and
`.synapseConfig <https://github.com/Sage-Bionetworks/synapsePythonClient/blob/master/synapseclient/.synapseConfig>`__.

Download a copy of the ``.synapseConfig`` file, open the file in the
editor of your choice and edit the
`username <https://github.com/Sage-Bionetworks/synapsePythonClient/blob/master/synapseclient/.synapseConfig#L8>`__
and
`apikey <https://github.com/Sage-Bionetworks/synapsePythonClient/blob/master/synapseclient/.synapseConfig#L9>`__
attributes under the
`[authentication] <https://github.com/Sage-Bionetworks/synapsePythonClient/blob/master/synapseclient/.synapseConfig#L7>`__
section.

 Description of config.yml attributes

::

    definitions:
        synapse_config: "Path to .synapseConfig file"
        creds_path: "Path to credentials.json file"
        token_pickle: "Path to token.pickle file"
        service_acct_creds: "Path to service_account_creds.json file"

    synapse:
        master_fileview: "Fileview of project with datasets on Synapse"
        manifest_folder: "Path to folder where the manifest file should be downloaded to"
        manifest_filename: "Name of the manifest file in the Synapse project"
        api_creds: "syn23643259"

    manifest:
        title: "Name metadata manifest file"
        data_type: "Component or Data Type to be used for validation"

    model:
        input:
            location: "Path to data model JSON-LD file"
            file_type: "local"  # only this type is supported at the moment
            validation_schema: "Path to JSON Validation Schema JSON file"
            log_location: "Folder where auto-generated JSON Validation Schemas can be logged to"
        

Note: You can get your Synapse API key by: *logging into Synapse* >
*Settings* > *Synapse API Key* > *Show API Key*.

1.3.6. Command Line Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1.3.6.1. Metadata Manifest Generation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To generate a metadata manifest template based on a data type that is
present in your data model:

.. code:: bash

    schematic manifest --config ~/path/to/config.yml get

1.3.6.2. Metadata Manifest Validation and Submission
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: bash

    schematic model --config ~/path/to/config.yml submit --manifest_path ~/path/to/manifest.csv --dataset_id dataset_synapse_id

Refer to the
`docs <https://github.com/Sage-Bionetworks/schematic/tree/develop/docs>`__
for more details.

Note: To view a full list of all the arguments that can be supplied to
the command line interfaces, add a ``--help`` option at the end of each
of the commands.

1.4. Contributing
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

1.5. Contributors
-----------------

Active contributors and maintainers:

-  `Milen Nikolov <https://github.com/milen-sage>`__
-  `Sujay Patil <https://github.com/sujaypatil96>`__
-  `Bruno Grande <https://github.com/BrunoGrandePhD>`__
-  `Xengie Doan <https://github.com/xdoan>`__
