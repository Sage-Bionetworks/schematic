Installation
============

Installation Requirements
-------------------------

- Your installed python version must be 3.9.0 ≤ version < 3.11.0
- You need to be a registered and certified user on `synapse.org <https://www.synapse.org/>`_

.. note::
   To create Google Sheets files from Schematic, please follow our credential policy for Google credentials. You can find a detailed tutorial `Google Credentials Guide <https://scribehow.com/shared/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA>`_.  
   If you're using ``config.yml``, make sure to specify the path to ``schematic_service_account_creds.json`` (see the ``google_sheets > service_account_creds`` section for more information).

Installation Guide For: Users
-----------------------------

The instructions below assume you have already installed `python <https://www.python.org/downloads/>`_, with the release version meeting the constraints set in the `Installation Requirements`_ section, and do not have a Python environment already active.

1. Verify your python version
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ensure your python version meets the requirements from the `Installation Requirements`_ section using the following command:

.. code-block:: shell

   python3 --version

If your current Python version is not supported by Schematic, you can switch to the supported version using a tool like `pyenv <https://github.com/pyenv/pyenv?tab=readme-ov-file#switch-between-python-versions>`_. Follow the instructions in the pyenv documentation to install and switch between Python versions easily.

.. note::
   You can double-check the current supported python version by opening up the `pyproject.toml <https://github.com/Sage-Bionetworks/schematic/blob/main/pyproject.toml#L39>`_ file in this repository and finding the supported versions of python in the script.

2. Set up your virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you are working with a python version supported by `schematic`, you will need to activate a virtual environment within which you can install the package. Below we will show how to create your virtual environment either with ``venv`` or with ``conda``.

2a. Set up your virtual environment with ``venv``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Python 3 has built-in support for virtual environments with the ``venv`` module, so you no longer need to install ``virtualenv``:

.. code-block:: shell

   python3 -m venv .venv
   source .venv/bin/activate

2b. Set up your virtual environment with ``conda``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``conda`` is a powerful package and environment management tool that allows users to create isolated environments used particularly in data science and machine learning workflows. If you would like to manage your environments with ``conda``, continue reading:

1. **Download your preferred ``conda`` installer**: Begin by `installing conda <https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html>`_. We personally recommend working with Miniconda, which is a lightweight installer for ``conda`` that includes only ``conda`` and its dependencies.
2. **Execute the ``conda`` installer**: Once you have downloaded your preferred installer, execute it using ``bash`` or ``zsh``, depending on the shell configured for your terminal environment. For example:

   .. code-block:: shell

      bash Miniconda3-latest-MacOSX-arm64.sh

3. **Verify your ``conda`` setup**: Follow the prompts to complete your setup. Then verify your setup by running the ``conda`` command.
4. **Create your ``schematic`` environment**: Begin by creating a fresh ``conda`` environment for ``schematic`` like so:

   .. code-block:: shell

      conda create --name 'schematicpy' python=3.10

5. **Activate the environment**: Once your environment is set up, you can now activate your new environment with ``conda``:

   .. code-block:: shell

      conda activate schematicpy

3. Install ``schematic`` dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Install the package using `pip <https://pip.pypa.io/en/stable/quickstart/>`_:

.. code-block:: shell

   python3 -m pip install schematicpy

If you run into ``ERROR: Failed building wheel for numpy``, the error might be able to resolve by upgrading pip. Please try to upgrade pip by:

.. code-block:: shell

   pip3 install --upgrade pip

4. Get your data model as a ``JSON-LD`` schema file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now you need a schema file, e.g. ``model.jsonld``, to have a data model that schematic can work with. While you can download a super basic `example data model <https://raw.githubusercontent.com/Sage-Bionetworks/schematic/refs/heads/develop/tests/data/example.model.jsonld>`_, you’ll probably be working with a DCC-specific data model. For non-Sage employees/contributors using the CLI, you might care only about the minimum needed artifact, which is the  ``.jsonld``; locate and download only that from the right repo.

Here are some example repos with schema files:

- https://github.com/ncihtan/data-models/
- https://github.com/nf-osi/nf-metadata-dictionary/

5. Obtain Google credential files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any function that interacts with a Google sheet (such as ``schematic manifest get``) requires Google Cloud credentials.

1. **Option 1**: `Step-by-step <https://scribehow.com/shared/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA?referrer=workspace>`_ guide on how to create these credentials in Google Cloud.
   - Depending on your institution's policies, your institutional Google account may or may not have the required permissions to complete this. A possible workaround is to use a personal or temporary Google account.

.. warning::
   At the time of writing, Sage Bionetworks employees do not have the appropriate permissions to create projects with their Sage Bionetworks Google accounts. You would follow instructions using a personal Google account.

2. **Option 2**: Ask your DCC/development team if they have credentials previously set up with a service account.

Once you have obtained credentials, be sure that the json file generated is named in the same way as the ``service_acct_creds`` parameter in your ``config.yml`` file. You will find more context on the ``config.yml`` in section [6. Set up configuration files](#6-set-up-configuration-files).

.. note::
   Running ``schematic init`` is no longer supported due to security concerns. To obtain ``schematic_service_account_creds.json``, please follow the `instructions <https://scribehow.com/shared/Enable_Google_Drive_and_Google_Sheets_APIs_for_project__yqfcJz_rQVeyTcg0KQCINA>`_. Schematic uses Google’s API to generate Google sheet templates that users fill in to provide (meta)data. Most Google sheet functionality could be authenticated with service account. However, more complex Google sheet functionality requires token-based authentication. As browser support that requires the token-based authentication diminishes, we are hoping to deprecate token-based authentication and keep only service account authentication in the future.

.. note::
   Use the ``schematic_service_account_creds.json`` file for the service account mode of authentication (*for Google services/APIs*). Service accounts are special Google accounts that can be used by applications to access Google APIs programmatically via OAuth2.0, with the advantage being that they do not require human authorization.

6. Set up configuration files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following section will walk through setting up your configuration files with your credentials to allow for communication between ``schematic`` and the Synapse API.

There are two main configuration files that need to be created and modified:

- ``.synapseConfig``
- ``config.yml``

**Create and modify the ``.synapseConfig``**

The ``.synapseConfig`` file is what enables communication between ``schematic`` and the Synapse API using your credentials. You can automatically generate a ``.synapseConfig`` file by running the following in your command line and following the prompts.

.. tip::
   You can generate a new authentication token on the Synapse website by going to ``Account Settings`` > ``Personal Access Tokens``.

.. code-block:: shell

   synapse config

After following the prompts, a new ``.synapseConfig`` file and ``.synapseCache`` folder will be created in your home directory. You can view these hidden assets in your home directory with the following command:

.. code-block:: shell

   ls -a ~

The ``.synapseConfig`` is used to log into Synapse if you are not using an environment variable (i.e. ``SYNAPSE_ACCESS_TOKEN``) for authentication, and the ``.synapseCache`` is where your assets are stored if you are not working with the CLI and/or you have specified ``.synapseCache`` as the location in which to store your manifests, in your ``config.yml``.

**Create and modify the ``config.yml``**

In this repository there is a ``config_example.yml`` file with default configurations to various components that are required before running ``schematic``, such as the Synapse ID of the main file view containing all your project assets, the

Installation Guide For: Contributors
------------------------------------

The instructions below assume you have already installed `python <https://www.python.org/downloads/>`_, with the release version meeting the constraints set in the `Installation Requirements`_ section, and do not have an environment already active (e.g., with ``pyenv``). For development, we recommend working with versions > python 3.9 to avoid issues with ``pre-commit``'s default hook configuration.

When contributing to this repository, please first discuss the change you wish to make via the `service desk <https://sagebionetworks.jira.com/servicedesk/customer/portal/5/group/8>`_ so that we may track these changes.

Once you have finished setting up your development environment using the instructions below, please follow the guidelines in `CONTRIBUTION.md <https://github.com/Sage-Bionetworks/schematic/blob/develop-fds-2218-update-readme/CONTRIBUTION.md>`_ during your development.

Please note we have a `code of conduct <CODE_OF_CONDUCT.md>`_, please follow it in all your interactions with the project.

1. Clone the ``schematic`` package repository
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For development, you will be working with the latest version of ``schematic`` on the repository to ensure compatibility between its latest state and your changes. Ensure your current working directory is where you would like to store your local fork before running the following command:

.. code-block:: shell

   git clone https://github.com/Sage-Bionetworks/schematic.git

2. Install ``poetry``
~~~~~~~~~~~~~~~~~~~~~

Install ``poetry`` (version 1.3.0 or later) using either the `official installer <https://python-poetry.org/docs/#installing-with-the-official-installer>`_ or ``pip``. If you have an older installation of Poetry, we recommend uninstalling it first.

.. code-block:: shell

   pip install poetry

Check to make sure your version of poetry is > v1.3.0

.. code-block:: shell

   poetry --version

3. Start the virtual environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Change directory (``cd``) into your cloned ``schematic`` repository, and initialize the virtual environment using the following command with ``poetry``:

.. code-block:: shell

   poetry shell

To make sure your poetry version and python version are consistent with the versions you expect, you can run the following command:

.. code-block:: shell

   poetry debug info

4. Install ``schematic`` dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before you begin, make sure you are in the latest ``develop`` branch of the repository.

The following command will install the dependencies based on what we specify in the ``poetry.lock`` file of this repository (which is generated from the libraries listed in the ``pyproject.toml`` file). If this step is taking a long time, try to go back to Step 2 and check your version of ``poetry``. Alternatively, you can try deleting the lock file and regenerate it by running ``poetry lock`` (Note: this method should be used as a last resort because it may force other developers to change their development environment).

.. code-block:: shell

   poetry install --dev,doc

This command will install:
- The main dependencies required for running the package.
- Development dependencies for testing, linting, and code formatting.
- Documentation dependencies such as ``sphinx`` for building and maintaining documentation.

5. Set up configuration files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following section will walk through setting up your configuration files with your credentials to allow for communication between ``schematic`` and the Synapse API.

There are two main configuration files that need to be created and modified:
- ``.synapseConfig``
- ``config.yml``

**Create and modify the ``.synapseConfig``**

The ``.synapseConfig`` file is what enables communication between ``schematic`` and the Synapse API using your credentials. You can automatically generate a ``.synapseConfig`` file by running the following in your command line and following the prompts.

.. tip::
   You can generate a new authentication token on the Synapse website by going to ``Account Settings`` > ``Personal Access Tokens``.

.. code-block:: shell

   synapse config

After following the prompts, a new ``.synapseConfig`` file and ``.synapseCache`` folder will be created in your home directory. You can view these hidden assets in your home directory with the following command:

.. code-block:: shell

   ls -a ~

The ``.synapseConfig`` is used to log into Synapse if you are not using an environment variable (i.e., ``SYNAPSE_ACCESS_TOKEN``) for authentication, and the ``.synapseCache`` is where your assets are stored if you are not working with the CLI and/or you have specified ``.synapseCache`` as the location to store your manifests in your ``config.yml``.

.. important::
   When developing on ``schematic``, keep your ``.synapseConfig`` in your current working directory to avoid authentication errors.

**Create and modify the ``config.yml``**

In this repository, there is a ``config_example.yml`` file with default configurations to various components required before running ``schematic``, such as the Synapse ID of the main file view containing all your project assets, the base name of your manifest files, etc.

Copy the contents of the ``config_example.yml`` (located in the base directory of the cloned ``schematic`` repo) into a new file called ``config.yml``:

.. code-block:: shell

   cp config_example.yml config.yml

Once you've copied the file, modify its contents according to your use case. For example, if you wanted to change the folder where manifests are downloaded, your config should look like:

.. code-block:: text

   manifest:
     manifest_folder: "my_manifest_folder_path"

.. important::
   Be sure to update your ``config.yml`` with the location of your ``.synapseConfig`` created in the step above to avoid authentication errors. Paths can be specified relative to the ``config.yml`` file or as absolute paths.

.. note::
   ``config.yml`` is ignored by git.

6. Obtain Google credential files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any function that interacts with a Google Sheet (such as ``schematic manifest get``) requires Google Cloud credentials.

1. **Option 1**: Follow the step-by-step `guide <https://scribehow.com/shared/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA?referrer=workspace>`_ on how to create these credentials in Google Cloud.
   - Depending on your institution's policies, your institutional Google account may or may not have the required permissions to complete this. A possible workaround is to use a personal or temporary Google account.

.. warning::
   At the time of writing, Sage Bionetworks employees do not have the appropriate permissions to create projects with their Sage Bionetworks Google accounts. You would follow instructions using a personal Google account.

2. **Option 2**: Ask your DCC/development team if they have credentials previously set up with a service account.

Once you have obtained credentials, ensure that the JSON file generated is named in the same way as the ``service_acct_creds`` parameter in your ``config.yml`` file.

.. important::
   For testing, ensure there is no environment variable ``SCHEMATIC_SERVICE_ACCOUNT_CREDS``. Check the file ``.env`` to ensure this is not set. Also, verify that config files used for testing, such as ``config_example.yml``, do not contain ``service_acct_creds_synapse_id``.

.. note::
   Running ``schematic init`` is no longer supported due to security concerns. To obtain  ``schematic_service_account_creds.json``, please follow the `instructions <https://scribehow.com/shared/Enable_Google_Drive_and_Google_Sheets_APIs_for_project__yqfcJz_rQVeyTcg0KQCINA>`_. Schematic uses Google’s API to generate Google Sheet templates that users fill in to provide (meta)data.
   Most Google Sheet functionality could be authenticated with a service account. However, more complex Google Sheet functionality requires token-based authentication. As browser support that requires token-based authentication diminishes, we hope to deprecate token-based authentication and keep only service account authentication in the future.

.. note::
   Use the ``schematic_service_account_creds.json`` file for the service account mode of authentication (*for Google services/APIs*). Service accounts are special Google accounts that can be used by applications to access Google APIs programmatically via OAuth2.0, with the advantage being that they do not require human authorization.

7. Set up pre-commit hooks
~~~~~~~~~~~~~~~~~~~~~~~~~~~

This repository is configured to utilize pre-commit hooks as part of the development process. To enable these hooks, run the following command and look for the success message:

.. code-block:: shell

   pre-commit install

   pre-commit installed at .git/hooks/pre-commit

You can run ``pre-commit`` manually across the entire repository like so:

.. code-block:: shell

   pre-commit run --all-files

After running this step, your setup is complete, and you can test it in a Python instance or by running a command based on the examples in the command line usage section.

8. Verify your setup
~~~~~~~~~~~~~~~~~~~~

After running the steps above, your setup is complete, and you can test it in a ``python`` instance or by running a command based on the examples
