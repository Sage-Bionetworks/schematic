# Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![Documentation Status](https://readthedocs.org/projects/sage-schematic/badge/?version=develop)](https://sage-schematic.readthedocs.io/en/develop/?badge=develop) [![PyPI version](https://badge.fury.io/py/schematicpy.svg)](https://badge.fury.io/py/schematicpy)

# TL;DR

* `schematic` (Schema Engine for Manifest Ingress and Curation) is a python-based software tool that streamlines the retrieval, validation, and submission of metadata for biomedical datasets hosted on Sage Bionetworks' Synapse platform.
* Users can work with `schematic` in several ways, including through the CLI (see [Command Line Usage](#command-line-usage) for examples), through Docker (see [Docker Usage](#docker-usage) for examples), or with python.
* `schematic` needs to communicate with Synapse and Google Sheets in order for its processes to work. As such, users will need to set up their credentials for authentication with Synapse and the Google Sheets API.
* To get started with `schematic`, follow one of the Installation Guides depending on your use case:
   * [Installation Guide For: Schematic CLI users](#installation-guide-for-users)
   * [Installation Guide For: Contributors](#installation-guide-for-contributors)

# Table of Contents
- [Schematic](#schematic)
- [TL;DR](#tldr)
- [Table of Contents](#table-of-contents)
- [Introduction](#introduction)
- [Installation](#installation)
  - [Installation Requirements](#installation-requirements)
  - [Installation Guide For: Users](#installation-guide-for-users)
    - [1. Verify your python version](#1-verify-your-python-version)
    - [2. Set up your virtual environment](#2-set-up-your-virtual-environment)
      - [2a. Set up your virtual environment with `venv`](#2a-set-up-your-virtual-environment-with-venv)
      - [2b. Set up your virtual environment with `conda`](#2b-set-up-your-virtual-environment-with-conda)
    - [3. Install `schematic` dependencies](#3-install-schematic-dependencies)
    - [4. Get your data model as a `JSON-LD` schema file](#4-get-your-data-model-as-a-json-ld-schema-file)
    - [5. Obtain Google credential files](#5-obtain-google-credential-files)
    - [6. Set up configuration files](#6-set-up-configuration-files)
    - [7. Verify your setup](#7-verify-your-setup)
  - [Installation Guide For: Contributors](#installation-guide-for-contributors)
    - [1. Clone the `schematic` package repository](#1-clone-the-schematic-package-repository)
    - [2. Install `poetry`](#2-install-poetry)
    - [3. Start the virtual environment](#3-start-the-virtual-environment)
    - [4. Install `schematic` dependencies](#4-install-schematic-dependencies)
    - [5. Set up configuration files](#5-set-up-configuration-files)
    - [6. Obtain Google credential files](#6-obtain-google-credential-files-1)
    - [7. Set up pre-commit hooks](#7-set-up-pre-commit-hooks)
    - [8. Verify your setup](#8-verify-your-setup)
- [Command Line Usage](#command-line-usage)
- [Docker Usage](#docker-usage)
    - [Running the REST API](#running-the-rest-api)
      - [Example 1: Using the `config.yml` path](#example-1-using-the-configyml-path)
      - [Example 2: Use environment variables](#example-2-use-environment-variables)
    - [Running `schematic` to Validate Manifests](#running-schematic-to-validate-manifests)
      - [Example for macOS/Linux](#example-for-macoslinux)
      - [Example for Windows](#example-for-windows)
- [Exporting OpenTelemetry data from schematic](#exporting-opentelemetry-data-from-schematic)
  - [Exporting OpenTelemetry data for SageBionetworks employees](#exporting-opentelemetry-data-for-sagebionetworks-employees)
    - [Exporting data locally](#exporting-data-locally)
- [Contributors](#contributors)


# Introduction
SCHEMATIC is an acronym for _Schema Engine for Manifest Ingress and Curation_. The Python based infrastructure provides a _novel_ schema-based, metadata ingress ecosystem, that is meant to streamline the process of biomedical dataset annotation, metadata validation and submission to a data repository for various data contributors.

# Installation
## Installation Requirements
* Your installed python version must be 3.9.0 ≤ version < 3.11.0
* You need to be a registered and certified user on [`synapse.org`](https://www.synapse.org/)

> [!NOTE]  
> To create Google Sheets files from Schematic, please follow our credential policy for Google credentials. You can find a detailed tutorial [here](https://scribehow.com/shared/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA).
> If you're using config.yml, make sure to specify the path to `schematic_service_account_creds.json` (see the `google_sheets > service_account_creds` section for more information).

## Installation Guide For: Users

The instructions below assume you have already installed [python](https://www.python.org/downloads/), with the release version meeting the constraints set in the [Installation Requirements](#installation-requirements) section, and do not have a Python environment already active.

### 1. Verify your python version

Ensure your python version meets the requirements from the [Installation Requirements](#installation-requirements) section using the following command:
```
python3 --version
```
If your current Python version is not supported by Schematic, you can switch to the supported version using a tool like [pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#switch-between-python-versions). Follow the instructions in the pyenv documentation to install and switch between Python versions easily.

> [!NOTE]
> You can double-check the current supported python version by opening up the [pyproject.toml](https://github.com/Sage-Bionetworks/schematic/blob/main/pyproject.toml#L39) file in this repository and find the supported versions of python in the script.

### 2. Set up your virtual environment

Once you are working with a python version supported by `schematic`, you will need to activate a virtual environment within which you can install the package. Below we will show how to create your virtual environment either with `venv` or with `conda`.

#### 2a. Set up your virtual environment with `venv`

Python 3 has built-in support for virtual environments with the `venv` module, so you no longer need to install `virtualenv`:

```
python3 -m venv .venv
source .venv/bin/activate
```

#### 2b. Set up your virtual environment with `conda`

`conda` is a powerful package and environment management tool that allows users to create isolated environments used particularly in data science and machine learning workflows. If you would like to manage your environments with `conda`, continue reading:

1. **Download your preferred `conda` installer**: Begin by [installing `conda`](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html). We personally recommend working with `Miniconda` which is a lightweight installer for `conda` that includes only `conda` and its dependencies.

2. **Execute the `conda` installer**: Once you have downloaded your preferred installer, execute it using `bash` or `zsh`, depending on the shell configured for your terminal environment. For example:

   ```
   bash Miniconda3-latest-MacOSX-arm64.sh
   ```

3. **Verify your `conda` setup**: Follow the prompts to complete your setup. Then verify your setup by running the `conda` command.
   
4. **Create your `schematic` environment**: Begin by creating a fresh `conda` environment for `schematic` like so:

   ```
   conda create --name 'schematicpy' python=3.10
   ```

5. **Activate the environment**: Once your environment is set up, you can now activate your new environment with `conda`:

   ```
   conda activate schematicpy
   ```

### 3. Install `schematic` dependencies

Install the package using [pip](https://pip.pypa.io/en/stable/quickstart/):

```
python3 -m pip install schematicpy
```

If you run into `ERROR: Failed building wheel for numpy`, the error might be able to resolve by upgrading pip. Please try to upgrade pip by:

```
pip3 install --upgrade pip
```

### 4. Get your data model as a `JSON-LD` schema file

Now you need a schema file, e.g. `model.jsonld`, to have a data model that schematic can work with. While you can download a super basic example data model [here](https://raw.githubusercontent.com/Sage-Bionetworks/schematic/refs/heads/develop/tests/data/example.model.jsonld), you’ll probably be working with a DCC-specific data model. For non-Sage employees/contributors using the CLI, you might care only about the minimum needed artifact, which is the  `.jsonld`; locate and download only that from the right repo.

Here are some example repos with schema files:
* https://github.com/ncihtan/data-models/
* https://github.com/nf-osi/nf-metadata-dictionary/

### 5. Obtain Google credential files

Any function that interacts with a google sheet (such as `schematic manifest get`) requires google cloud credentials.

1. **Option 1**: [Here](https://scribehow.com/shared/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA?referrer=workspace)’s a step-by-step guide on how to create these credentials in Google Cloud.
   * Depending on your institution's policies, your institutional Google account may or may not have the required permissions to complete this. A possible workaround is to use a personal or temporary Google account.

> [!WARNING]
> At the time of writing, Sage Bionetworks employees do not have the appropriate permissions to create projects with their Sage Bionetworks Google accounts. You would follow instructions using a personal Google account. 

2. **Option 2**: Ask your DCC/development team if they have credentials previously set up with a service account.

Once you have obtained credentials, be sure that the json file generated is named in the same way as the `service_acct_creds` parameter in your `config.yml` file. You will find more context on the `config.yml` in section [6. Set up configuration files](#6-set-up-configuration-files).

> [!NOTE]
> Running `schematic init` is no longer supported due to security concerns. To obtain  `schematic_service_account_creds.json`, please follow the instructions [here](https://scribehow.com/shared/Enable_Google_Drive_and_Google_Sheets_APIs_for_project__yqfcJz_rQVeyTcg0KQCINA). 
schematic uses Google’s API to generate google sheet templates that users fill in to provide (meta)data.
Most Google sheet functionality could be authenticated with service account. However, more complex Google sheet functionality
requires token-based authentication. As browser support that requires the token-based authentication diminishes, we are hoping to deprecate
token-based authentication and keep only service account authentication in the future. 

> [!NOTE]
> Use the ``schematic_service_account_creds.json`` file for the service
> account mode of authentication (*for Google services/APIs*). Service accounts
> are special Google accounts that can be used by applications to access Google APIs
> programmatically via OAuth2.0, with the advantage being that they do not require
> human authorization. 

### 6. Set up configuration files

The following section will walk through setting up your configuration files with your credentials to allow for communication between `schematic` and the Synapse API.

There are two main configuration files that need to be created + modified:
- `.synapseConfig`
- `config.yml`

**Create and modify the `.synapseConfig`**

The `.synapseConfig` file is what enables communication between `schematic` and the Synapse API using your credentials.
You can automatically generate a `.synapseConfig` file by running the following in your command line and following the prompts.

>[!TIP]
>You can generate a new authentication token on the Synapse website by going to `Account Settings` > `Personal Access Tokens`.

```
synapse config
```

After following the prompts, a new `.synapseConfig` file and `.synapseCache` folder will be created in your home directory. You can view these hidden
assets in your home directory with the following command:

```
ls -a ~
```

The `.synapseConfig` is used to log into Synapse if you are not using an environment variable (i.e. `SYNAPSE_ACCESS_TOKEN`) for authentication, and the `.synapseCache` is where your assets are stored if you are not working with the CLI and/or you have specified `.synapseCache` as the location in which to store your manfiests, in your `config.yml` (more on the `config.yml` below).

**Create and modify the `config.yml`**

In this repository there is a `config_example.yml` file with default configurations to various components that are required before running `schematic`,
such as the Synapse ID of the main file view containing all your project assets, the base name of your manifest files, etc.

Download the `config_example.yml` as a new file called `config.yml` and modify its contents according to your use case.

For example, one of the components in this `config.yml` that will likely be modified is the location of your schema. After acquiring your schema file using the
instructions in step [4. Get your data model as a `JSON-LD` schema file](#4-get-your-data-model-as-a-json-ld-schema-file), your `config.yml` should contain something like:

```text
model:
  location: "path/to/your/model.jsonld"
```

> [!IMPORTANT]
> Please note that for the example above, your local working directory would typically have `model.jsonld` and `config.yml` side-by-side. The path to your data model should match what is in `config.yml`.

> [!IMPORTANT]
> Be sure to update your `config.yml` with the location of your `.synapseConfig` created in the step above, to avoid authentication errors. Paths can be specified relative to the `config.yml` file or as absolute paths.

> [!NOTE]
> `config.yml` is ignored by git.

### 7. Verify your setup
After running the steps above, your setup is complete, and you can test it on a `python` instance or by running a command based on the examples in the [Command Line Usage](#command-line-usage) section.

## Installation Guide For: Contributors

The instructions below assume you have already installed [python](https://www.python.org/downloads/), with the release version meeting the constraints set in the [Installation Requirements](#installation-requirements) section, and do not have an environment already active (e.g. with `pyenv`). For development, we recommend working with versions > python 3.9 to avoid issues with `pre-commit`'s default hook configuration.

When contributing to this repository, please first discuss the change you wish to make via the [service desk](https://sagebionetworks.jira.com/servicedesk/customer/portal/5/group/8) so that we may track these changes.

Once you have finished setting up your development environment using the instructions below, please follow the guidelines in [CONTRIBUTION.md](https://github.com/Sage-Bionetworks/schematic/blob/develop-fds-2218-update-readme/CONTRIBUTION.md) during your development.

Please note we have a [code of conduct](CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

### 1. Clone the `schematic` package repository

For development, you will be working with the latest version of `schematic` on the repository to ensure compatibility between its latest state and your changes. Ensure your current working directory is where
you would like to store your local fork before running the following command:

```
git clone https://github.com/Sage-Bionetworks/schematic.git
```

### 2. Install `poetry` 

Install `poetry` (version 1.3.0 or later) using either the [official installer](https://python-poetry.org/docs/#installing-with-the-official-installer) or `pip`. If you have an older installation of Poetry, we recommend uninstalling it first.

```
pip install poetry
```

Check to make sure your version of poetry is > v1.3.0

```
poetry --version
```

### 3. Start the virtual environment

`cd` into your cloned `schematic` repository, and initialize the virtual environment using the following command with `poetry`:

```
poetry shell
```

To make sure your poetry version and python version are consistent with the versions you expect, you can run the following command:

```
poetry debug info
```

### 4. Install `schematic` dependencies

Before you begin, make sure you are in the latest `develop` of the repository.

The following command will install the dependencies based on what we specify in the `poetry.lock` file of this repository (which is generated from the libraries listed in the `pyproject.toml` file). If this step is taking a long time, try to go back to Step 2 and check your version of `poetry`. Alternatively, you can try deleting the lock file and regenerate it by doing `poetry lock` (Please note this method should be used as a last resort because this would force other developers to change their development environment).

```
poetry install --with dev,doc
```

This command will install:
* The main dependencies required for running the package.
* Development dependencies for testing, linting, and code formatting.
* Documentation dependencies such as `sphinx` for building and maintaining documentation.

### 5. Set up configuration files

The following section will walk through setting up your configuration files with your credentials to allow for communication between `schematic` and the Synapse API.

There are two main configuration files that need to be created + modified:
- `.synapseConfig`
- `config.yml`

**Create and modify the `.synapseConfig`**

The `.synapseConfig` file is what enables communication between `schematic` and the Synapse API using your credentials.
You can automatically generate a `.synapseConfig` file by running the following in your command line and following the prompts.

>[!TIP]
>You can generate a new authentication token on the Synapse website by going to `Account Settings` > `Personal Access Tokens`.

```
synapse config
```

After following the prompts, a new `.synapseConfig` file and `.synapseCache` folder will be created in your home directory. You can view these hidden
assets in your home directory with the following command:

```
ls -a ~
```

The `.synapseConfig` is used to log into Synapse if you are not using an environment variable (i.e. `SYNAPSE_ACCESS_TOKEN`) for authentication, and the `.synapseCache` is where your assets are stored if you are not working with the CLI and/or you have specified `.synapseCache` as the location in which to store your manfiests, in your `config.yml` (more on the `config.yml` below).

> [!IMPORTANT]
> When developing on `schematic`, keep your `.synapseConfig` in your current working directory to avoid authentication errors.

**Create and modify the `config.yml`**

In this repository there is a `config_example.yml` file with default configurations to various components that are required before running `schematic`,
such as the Synapse ID of the main file view containing all your project assets, the base name of your manifest files, etc.

Copy the contents of the `config_example.yml` (located in the base directory of the cloned `schematic` repo) into a new file called `config.yml`

```
cp config_example.yml config.yml
```

Once you've copied the file, modify its contents according to your use case. For example, if you wanted to change the folder where manifests are downloaded your config should look like:

```text
manifest:
  manifest_folder: "my_manifest_folder_path"
```

> [!IMPORTANT]
> Be sure to update your `config.yml` with the location of your `.synapseConfig` created in the step above, to avoid authentication errors. Paths can be specified relative to the `config.yml` file or as absolute paths.

> [!NOTE]
> `config.yml` is ignored by git.

### 6. Obtain Google credential files

Any function that interacts with a google sheet (such as `schematic manifest get`) requires google cloud credentials.

1. **Option 1**: [Here](https://scribehow.com/shared/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA?referrer=workspace)’s a step-by-step guide on how to create these credentials in Google Cloud.
   * Depending on your institution's policies, your institutional Google account may or may not have the required permissions to complete this. A possible workaround is to use a personal or temporary Google account.

> [!WARNING]
> At the time of writing, Sage Bionetworks employees do not have the appropriate permissions to create projects with their Sage Bionetworks Google accounts. You would follow instructions using a personal Google account. 

2. **Option 2**: Ask your DCC/development team if they have credentials previously set up with a service account.

Once you have obtained credentials, be sure that the json file generated is named in the same way as the `service_acct_creds` parameter in your `config.yml` file.

> [!IMPORTANT]
> For testing, make sure there is no environment variable `SCHEMATIC_SERVICE_ACCOUNT_CREDS`. Check the file `.env` to ensure this is not set. Also, check that config files used for testing, such as `config_example.yml` do not contain service_acct_creds_synapse_id.

> [!NOTE]
> Running `schematic init` is no longer supported due to security concerns. To obtain  `schematic_service_account_creds.json`, please follow the instructions [here](https://scribehow.com/shared/Enable_Google_Drive_and_Google_Sheets_APIs_for_project__yqfcJz_rQVeyTcg0KQCINA). 
schematic uses Google’s API to generate google sheet templates that users fill in to provide (meta)data.
Most Google sheet functionality could be authenticated with service account. However, more complex Google sheet functionality
requires token-based authentication. As browser support that requires the token-based authentication diminishes, we are hoping to deprecate
token-based authentication and keep only service account authentication in the future. 

> [!NOTE]
> Use the ``schematic_service_account_creds.json`` file for the service
> account mode of authentication (*for Google services/APIs*). Service accounts
> are special Google accounts that can be used by applications to access Google APIs
> programmatically via OAuth2.0, with the advantage being that they do not require
> human authorization. 

### 7. Set up pre-commit hooks

This repository is configured to utilize pre-commit hooks as part of the development process. To enable these hooks, please run the following command and look for the following success message:
```
$ pre-commit install
pre-commit installed at .git/hooks/pre-commit
```

You can run `pre-commit` manually across the entire repository like so:

```
pre-commit run --all-files
```

After running this step, your setup is complete, and you can test it on a python instance or by running a command based on the examples in the [Command Line Usage](#command-line-usage) section.

### 8. Verify your setup
After running the steps above, your setup is complete, and you can test it on a `python` instance or by running a command based on the examples in the [Command Line Usage](#command-line-usage) section.

# Command Line Usage
1. Generate a new manifest as a google sheet

```
schematic manifest -c /path/to/config.yml get -dt <your data type> -s
```

2. Grab an existing manifest from synapse 

```
schematic manifest -c /path/to/config.yml get -dt <your data type> -d <your synapse dataset folder id> -s
```

3. Validate a manifest

```
schematic model -c /path/to/config.yml validate -dt <your data type> -mp <your csv manifest path>
```

4. Submit a manifest as a file

```
schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse dataset folder id> -vc <your data type> -mrt file_only
```

Please visit more documentation [here](https://sage-schematic.readthedocs.io/en/stable/cli_reference.html#) for more information. 

# Docker Usage

Here we will demonstrate how to run `schematic` with Docker, with different use-cases for running API endpoints, validating the manifests, and
using how to use `schematic` based on your OS (macOS/Linux).

### Running the REST API

Use the Docker image to run `schematic`s REST API. You can either use the file path for the `config.yml` created using the installation instructions,
or set up authentication with environment variables.

#### Example 1: Using the `config.yml` path 
```
docker run --rm -p 3001:3001 \
  -v $(pwd):/schematic -w /schematic --name schematic \
  -e SCHEMATIC_CONFIG=/schematic/config.yml \
  -e GE_HOME=/usr/src/app/great_expectations/ \
  sagebionetworks/schematic \
  python /usr/src/app/run_api.py
``` 

#### Example 2: Use environment variables
1. save content of `config.yml` as to environment variable `SCHEMATIC_CONFIG_CONTENT` by doing: `export SCHEMATIC_CONFIG_CONTENT=$(cat /path/to/config.yml)`

2. Similarly, save the content of `schematic_service_account_creds.json` as `SERVICE_ACCOUNT_CREDS` by doing: `export SERVICE_ACCOUNT_CREDS=$(cat /path/to/schematic_service_account_creds.json)`

3. Pass `SCHEMATIC_CONFIG_CONTENT` and `schematic_service_account_creds` as environment variables by using `docker run`

```
docker run --rm -p 3001:3001 \
  -v $(pwd):/schematic -w /schematic --name schematic \
  -e GE_HOME=/usr/src/app/great_expectations/ \
  -e SCHEMATIC_CONFIG_CONTENT=$SCHEMATIC_CONFIG_CONTENT \
  -e SERVICE_ACCOUNT_CREDS=$SERVICE_ACCOUNT_CREDS \
  sagebionetworks/schematic \
  python /usr/src/app/run_api.py
``` 
### Running `schematic` to Validate Manifests
You can also use Docker to run `schematic` commands like validating manifests. Below are examples for different platforms.

#### Example for macOS/Linux

1. Clone the repository:
```
git clone https://github.com/sage-bionetworks/schematic ~/schematic
```
2. Update the `.synapseConfig` with your credentials. See the installation instructions for how to do this.

3. Run Docker:
```
docker run \
  -v ~/schematic:/schematic \
  -w /schematic \
  -e SCHEMATIC_CONFIG=/schematic/config.yml \
  -e GE_HOME=/usr/src/app/great_expectations/ \
  sagebionetworks/schematic schematic model \
  -c /schematic/config.yml validate \
  -mp /schematic/tests/data/mock_manifests/Valid_Test_Manifest.csv \
  -dt MockComponent \
  -js /schematic/tests/data/example.model.jsonld
``` 

#### Example for Windows

Run the following command to validate manifests:
```
docker run -v %cd%:/schematic \
  -w /schematic \
  -e GE_HOME=/usr/src/app/great_expectations/ \
  sagebionetworks/schematic \
  schematic model \
  -c config.yml validate -mp tests/data/mock_manifests/inValid_Test_Manifest.csv -dt MockComponent -js /schematic/data/example.model.jsonld
```

# Exporting OpenTelemetry data from schematic
This section is geared towards the SageBionetworks specific deployment of schematic as
an API server running in the Sage specific AWS account.


Schematic is setup to produce and export OpenTelemetry data while requests are flowing
through the application code. This may be accomplished by setting a few environment
variables wherever the application is running. Those variables are:

- `TRACING_EXPORT_FORMAT`: Determines in what format traces will be exported. Supported values: [`otlp`].
- `LOGGING_EXPORT_FORMAT`: Determines in what format logs will be exported. Supported values: [`otlp`].
- `TRACING_SERVICE_NAME`: The name of the service to attach for all exported traces.
- `LOGGING_SERVICE_NAME`: The name of the service to attach for all exported logs.
- `DEPLOYMENT_ENVIRONMENT`: The name of the environment to attach for all exported telemetry data.
- `OTEL_EXPORTER_OTLP_ENDPOINT`: The endpoint to export telemetry data to.

Authentication (Oauth2 client credential exchange):

Used in cases where an intermediate opentelemetry collector is not, or can not be used.
This option is not preferred over using an intermediate opentelemetry collector, but is 
left in the code to show how we may export telemetry data with an authorization header 
deried from an oauth2 client credential exchange flow.

- `TELEMETRY_EXPORTER_CLIENT_ID`: The ID of the client to use when executing the OAuth2.0 "Client Credentials" flow.
- `TELEMETRY_EXPORTER_CLIENT_SECRET`: The Secret of the client to use when executing the OAuth2.0 "Client Credentials" flow.
- `TELEMETRY_EXPORTER_CLIENT_TOKEN_ENDPOINT`: The Token endpoint to use when executing the OAuth2.0 "Client Credentials" flow.
- `TELEMETRY_EXPORTER_CLIENT_AUDIENCE`: The ID of the API server to use when executing the OAuth2.0 "Client Credentials" flow.

Authentication (Static Bearer token)

- `OTEL_EXPORTER_OTLP_HEADERS`: Used for developers to set a static Bearer token to be used when exporting telemetry data.

The above configuration will work when the application is running locally, in a
container, running in AWS, or running via CLI. The important part is that the
environment variables are set before the code executes, as the configuration is setup
when the code runs.

## Exporting OpenTelemetry data for SageBionetworks employees
The DPE (Data Processing & Engineering) team is responsible for maintaining and giving
out the above sensitive information. Please reach out to the DPE team if a new ID/Secret
is needed in order to export telemetry data in a new environment, or locally during
development.

### Exporting data locally
In order to conserve the number of monthly token requests that can be made the following
process should be followed instead of setting the `TELEMETRY_EXPORTER_CLIENT_*`
environment variables above.

1) Request access to a unique client ID/Secret that identifies you from DPE.
2) Retrieve a token that must be refreshed every 24 hours via cURL. The specific values will be given when the token is requested. Example:
```
curl --request POST \
  --url https://TOKEN_URL.us.auth0.com/oauth/token \
  --header 'content-type: application/json' \
  --data '{"client_id":"...","client_secret":"...","audience":"...","grant_type":"client_credentials"}'
```
3) Set an environment variable in your `.env` file like: `OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer ey...`

If you fail to create a new access token after 24 hours you will see HTTP 403 JWT 
Expired messages when the application attempts to export telemetry data.

# Contributors

Main contributors and developers:

- [Milen Nikolov](https://github.com/milen-sage)
- [Mialy DeFelice](https://github.com/mialy-defelice)
- [Sujay Patil](https://github.com/sujaypatil96)
- [Bruno Grande](https://github.com/BrunoGrandePhD)
- [Robert Allaway](https://github.com/allaway)
- [Gianna Jordan](https://github.com/giajordan)
- [Lingling Peng](https://github.com/linglp)
