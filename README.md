# Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![Documentation Status](https://readthedocs.org/projects/sage-schematic/badge/?version=develop)](https://sage-schematic.readthedocs.io/en/develop/?badge=develop) [![PyPI version](https://badge.fury.io/py/schematicpy.svg)](https://badge.fury.io/py/schematicpy)

# Table of contents
- [Introduction](#introduction)
- [Installation](#installation)
  - [Installation Requirements](#installation-requirements)
  - [Installation guide for data curator app](#installation-guide-for-data-curator-app)
  - [Installation guide for developers/contributors](#installation-guide-for-developerscontributors)
- [Other Contribution Guidelines](#other-contribution-guidelines)
    - [Update readthedocs documentation](#update-readthedocs-documentation)
- [Command Line Usage](#command-line-usage)
- [Testing](#testing)
  - [Updating Synapse test resources](#updating-synapse-test-resources)
- [Code Style](#code-style)
- [Contributors](#contributors)

# Introduction
SCHEMATIC is an acronym for _Schema Engine for Manifest Ingress and Curation_. The Python based infrastructure provides a _novel_ schema-based, metadata ingress ecosystem, that is meant to streamline the process of biomedical dataset annotation, metadata validation and submission to a data repository for various data contributors.

# Installation
## Installation Requirements
* Python 3.7.1 or higher

Note: You need to be a registered and certified user on [`synapse.org`](https://www.synapse.org/), and also have the right permissions to download the Google credentials files from Synapse.


## Installation guide for data curator app

Create and activate a virtual environment within which you can install the package:

```
python3 -m venv .venv
source .venv/bin/activate
```

Note: Python 3 has a built-in support for virtual environment [venv](https://docs.python.org/3/library/venv.html#module-venv) so you no longer need to install virtualenv.

Install and update the package using [pip](https://pip.pypa.io/en/stable/quickstart/):

```
python3 -m pip install schematicpy
```

If you run into error: Failed building wheel for numpy, the error might be able to resolve by upgrading pip. Please try to upgrade pip by:

```
pip3 install --upgrade pip
```

## Installation guide for developers/contributors 

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

Please note we have a [code of conduct](CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

### General instructions
1. Clone this repository to your local machine so that you can begin making changes. 
2. Follow the [Github docs](https://docs.github.com/en/desktop/contributing-and-collaborating-using-github-desktop/making-changes-in-a-branch/managing-branches#creating-a-branch) to create a branch off the `develop` branch. Name the branch appropriately, either briefly summarizing the bug (ex., `spatil/add-restapi-layer`) or feature or simply use the issue number in the name (ex., `spatil/issue-414-fix`).
3. Push all your changes to your develop branch. 
4. When all changes are tested locally and ready to be merged, follow the [Github docs](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) and create a pull request in GitHub.

> A Sage Bionetworks engineer must review and accept your pull request. A code review (which happens with both the contributor and the reviewer present) is required for contributing.

*Note*: Make sure you have the latest version of the `develop` branch on your local machine.

### Development environment setup
1. Clone the `schematic` package repository.
```
git clone https://github.com/Sage-Bionetworks/schematic.git
```
2. Follow the [instructions](https://python-poetry.org/docs/) here to install `poetry`
3. Start the virtual environment by doing: 
```
poetry shell
```
4. Install the dependencies by doing: 
```
poetry install
```
This command will install the dependencies based on what we specify in poetry.lock

5. Fill in credential files: 
*Note*: If you won't interact with Synapse, please ignore this section.

There are two main configuration files that need to be edited :
[config.yml](https://github.com/Sage-Bionetworks/schematic/blob/develop/config.yml)
and [synapseConfig](https://raw.githubusercontent.com/Sage-Bionetworks/synapsePythonClient/v2.3.0-rc/synapseclient/.synapseConfig)

<strong>Configure .synapseConfig File</strong>

Download a copy of the ``.synapseConfig`` file, open the file in the
editor of your choice and edit the `username` and `authtoken` attribute under the `authentication` section 

*Note*: You could also visit [configparser](https://docs.python.org/3/library/configparser.html#module-configparser>) doc to see the format that `.synapseConfig` must have. For instance:
>[authentication]<br> username = ABC <br> authtoken = abc

<strong>Configure config.yml File</strong>

Description of `config.yml` attributes

    definitions:
        synapse_config: "~/path/to/.synapseConfig"
        creds_path: "~/path/to/credentials.json"
        token_pickle: "~/path/to/token.pickle"
        service_acct_creds: "~/path/to/service_account_creds.json"

    synapse:
        master_fileview: "syn23643253" # fileview of project with datasets on Synapse
        manifest_folder: "~/path/to/manifest_folder/" # manifests will be downloaded to this folder
        manifest_basename: "filename" # base name of the manifest file in the project dataset, without extension
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
            log_location: "~/path/to/log_folder/validation_schema.json" # auto-generated JSON Validation Schemas can be logged
        

*Note*: Paths can be specified relative to the `config.yml` file or as absolute paths.

6. Obtain Google credential Files

To obtain ``credentials.json`` and ``token.pickle``, please run:

```
schematic init --config ~/path/to/config.yml
```
This should prompt you with a URL that will take you through Google OAuth. Your `credentials.json` and `token.pickle` will get automatically downloaded the first time you run this command.

*Note* : The ``credentials.json`` file is required when you are using
[OAuth2](https://developers.google.com/identity/protocols/oauth2)
to authenticate with the Google APIs.

For details about the steps involved in the [OAuth2 authorization
flow](https://github.com/Sage-Bionetworks/schematic/blob/develop/schematic/utils/google_api_utils.py#L18)
refer to the ``Credentials`` section in the
[docs/md/details](https://github.com/Sage-Bionetworks/schematic/blob/develop/docs/md/details.md#credentials)
document.

To obtain  ``schematic_service_account_creds.json``, please run: 
```
schematic init --config ~/path/to/config.yml --auth service_account
```
*Notes*: Use the ``schematic_service_account_creds.json`` file for the service
account mode of authentication (*for Google services/APIs*). Service accounts 
are special Google accounts that can be used by applications to access Google APIs 
programmatically via OAuth2.0, with the advantage being that they do not require 
human authorization. 

*Background*: schematic uses Google’s API to generate google sheet templates that users fill in to provide (meta)data.
Most Google sheet functionality could be authenticated with service account. However, more complex Google sheet functionality
requires token-based authentication. As browser support that requires the token-based authentication diminishes, we are hoping to deprecate
token-based authentication and keep only service account authentication in the future. 

# Other Contribution Guidelines
## Update readthedocs documentation
1. `cd docs`
2. After making relevant changes, you could run the `make html` command to re-generate the `build` folder.
3. Please contact the dev team to publish your updates

*Other helpful resources*:

1. [Getting started with Sphinx](https://haha.readthedocs.io/en/latest/intro/getting-started-with-sphinx.html)
2. [Installing Sphinx](https://haha.readthedocs.io/en/latest/intro/getting-started-with-sphinx.html)

## Update toml file and lock file
If you install external libraries by using `poetry add <name of library>`, please make sure that you include `pyproject.toml` and `poetry.lock` file in your commit.

## Reporting bugs or feature requests
You can use the [`Issues`](https://github.com/Sage-Bionetworks/schematic/issues) tab to **create bug and feature requests**. Providing enough details to the developers to verify and troubleshoot your issue is paramount:
- **Provide a clear and descriptive title as well as a concise summary** of the issue to identify the problem.
- **Describe the exact steps which reproduce the problem** in as many details as possible.
- **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
- **Explain which behavior you expected to see** instead and why.
- **Provide screenshots of the expected or actual behaviour** where applicable.

# Command Line Usage
Please visit more documentation [here](https://sage-schematic.readthedocs.io/en/develop/cli_reference.html)



# Testing 

All code added to the client must have tests. The Python client uses pytest to run tests. The test code is located in the [tests](https://github.com/Sage-Bionetworks/schematic/tree/develop-docs-update/tests) subdirectory.

You can run the test suite in the following way:

```
pytest -vs tests/
```

## Updating Synapse test resources

1. Duplicate the entity being updated (or folder if applicable).
2. Edit the duplicates (_e.g._ annotations, contents, name).
3. Update the test suite in your branch to use these duplicates, including the expected values in the test assertions.
4. Open a PR as per the usual process (see above).
5. Once the PR is merged, leave the original copies on Synapse to maintain support for feature branches that were forked from `develop` before your update.
   - If the old copies are problematic and need to be removed immediately (_e.g._ contain sensitive data), proceed with the deletion and alert the other contributors that they need to merge the latest `develop` branch into their feature branches for their tests to work.

# Code style

* Please consult the [Google Python style guide](http://google.github.io/styleguide/pyguide.html) prior to contributing code to this project.
* Be consistent and follow existing code conventions and spirit.


# Contributors

Main contributors and developers:

- [Milen Nikolov](https://github.com/milen-sage)
- [Mialy DeFelice](https://github.com/mialy-defelice)
- [Sujay Patil](https://github.com/sujaypatil96)
- [Bruno Grande](https://github.com/BrunoGrandePhD)
- [Robert Allaway](https://github.com/allaway)
- [Gianna Jordan](https://github.com/giajordan)
- [Lingling Peng](https://github.com/linglp)
