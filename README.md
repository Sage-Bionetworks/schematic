# Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![Documentation Status](https://readthedocs.org/projects/sage-schematic/badge/?version=develop)](https://sage-schematic.readthedocs.io/en/develop/?badge=develop) [![PyPI version](https://badge.fury.io/py/schematicpy.svg)](https://badge.fury.io/py/schematicpy)

# Table of contents
- [Introduction](#introduction)
- [Installation](#installation)
  - [Installation Requirements](#installation-requirements)
  - [Installation guide for data curator app](#installation-guide-for-data-curator-app)
  - [Installation guide for developers/contributors](#installation-guide-for-developerscontributors)
- [Other Contribution Guidelines](#other-contribution-guidelines)
  - [Reporting bugs or feature requests](#reporting-bugs-or-feature-requests)
- [Release process](#release-process)
  - [Release to Test PyPI _(optional)_](#release-to-test-pypi-_optional_)
  - [Release to PyPI _(mandatory)_](#release-to-pypi-_mandatory_)
- [Testing](#testing)
  - [Updating Synapse test resources](#updating-synapse-test-resources)
- [Command Line Usage](#command-line-usage)
  - [Initialization](#initialization)
  - [Manifest](#manifest)
  - [Schema](#schema)
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
There are two main configuration files that need to be edited :
[config.yml](https://github.com/Sage-Bionetworks/schematic/blob/develop/config.yml)
and [synapseConfig](https://raw.githubusercontent.com/Sage-Bionetworks/synapsePythonClient/v2.3.0-rc/synapseclient/.synapseConfig)

Download a copy of the ``.synapseConfig`` file, open the file in the
editor of your choice and edit the `username` and `authtoken` attribute under the `authentication` section 

*Note*: You could also visit [configparser](https://docs.python.org/3/library/configparser.html#module-configparser>) doc to see the format that `.synapseConfig` must have. For instance:
>[authentication]<br> username = ABC <br> authtoken = abc


Description of `config.yml` attributes

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
            log_location: "~/path/to/log_folder/validation_schema.json" # auto-generated JSON Validation Schemas can be logged
        

*Note*: Paths can be specified relative to the `config.yml` file or as absolute paths.

6. Obtain Google credential Files

To obtain ``credentials.json`` and ``token.pickle``, please run:

```
schematic init --config ~/path/to/config.yml
```
This should prompt you with a URL that will take you through Google OAuth. Your `credential.json` and `token.pickle` will get automatically downloaded the first time you run this command.

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
## Reporting bugs or feature requests
You can use the [`Issues`](https://github.com/Sage-Bionetworks/schematic/issues) tab to **create bug and feature requests**. Providing enough details to the developers to verify and troubleshoot your issue is paramount:
- **Provide a clear and descriptive title as well as a concise summary** of the issue to identify the problem.
- **Describe the exact steps which reproduce the problem** in as many details as possible.
- **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
- **Explain which behavior you expected to see** instead and why.
- **Provide screenshots of the expected or actual behaviour** where applicable.

# Release process
Once the code has been merged into the `develop` branch on this repo, there are two processes that need to be completed to ensure a _release_ is complete.

- You should create a GitHub [tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging), with the appropriate version number. Typically, from `v21.06` onwards all tags are created following the Linux Ubuntu versioning convention which is the `YY.MM` format where `Y` is the year and `M` is the month of that year when that release was created.
- You should push the package to [PyPI](https://pypi.org/). Schematic is on PyPI as [schematicpy](https://pypi.org/project/schematicpy/). You can go through the following two sections for that.

## Release to Test PyPI _(optional)_

The purpose of this section is to verify that the package looks and works as intended, by viewing it on [Test PyPI](https://test.pypi.org/) and installing the test version in a separate virtual environment.

```
poetry build   # build the package
poetry config repositories.testpypi https://test.pypi.org/legacy/   # add Test PyPI as an alternate package repository
poetry publish -r testpypi   # publish the package to Test PyPI
```

Installing:

```
pip install --index-url https://test.pypi.org/simple/
```

## Release to PyPI _(mandatory)_

If the package looks great on Test PyPI and works well, the next step is to publish the package to PyPI:

```
poetry publish  # publish the package to PyPI
```

> You'll need to [register](https://pypi.org/account/register/) for a PyPI account before uploading packages to the package index. Similarly for [Test PyPI](https://test.pypi.org/account/register/) as well.

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

# Command Line Usage

## Initialization

```
schematic init --config ~/path/to/config.yml    # initialize mode of authentication
```
**Options**:

Required: 
* -c, --config: Specify the path to the `config.yml` using this option. 

Optional: 
* -v, --verbosity: Either CRITICAL, ERROR, WARNING, INFO or DEBUG
* -a, --auth: Specify the mode of authentication you want to use for Google accounts. You can use one of either `token` or `service_account`. The default mode of authentication is `token` which uses OAuth.


## Manifest
### Generate an empty manifest or Get an existing manifest
```
schematic manifest --config ~/path/to/config.yml get [OPTIONS]  # generate manifest based on data type
```

**Options**:

Required:
* -c, --config: Specify the path to the `config.yml` using this option. 

Optional: 
* -dt, --data_type: Specify the component (data type) from the data model that is to be used for generating the metadata manifest file. You can either explicitly pass the data type here or provide it in the config.yml file as a value for the (manifest > data_type) key.
* -p, --jsonld: Specify the path to the JSON-LD data model (schema) using this option. You can either explicitly pass the schema here or provide a value for the (model > input > location) key.
* -d, --dataset_id: Specify the synID of a dataset folder on Synapse. If there is an exisiting manifest already present in that folder, then it will be pulled with the existing annotations for further annotation/modification
* -v, --verbosity: Either CRITICAL, ERROR, WARNING, INFO or DEBUG
* -t, --title: Specify the title of the manifest that will be created at the end of the run. You can either explicitly pass the title of the manifest here or provide it in the config.yml file as a value for the (manifest > title) key.
* -s, --sheet_url: This is a boolean flag. If flag is provided when command line utility is executed, result will be a link/URL to the metadata manifest file. If not it will produce a pandas dataframe for the same.
* -o, --output_csv: Path to where the CSV manifest template should be stored.
* -a, --use_annotations: This is a boolean flag. If flag is provided when command line utility is executed, it will prepopulate template with existing annotations from Synapse.
* -oa, --oauth: This is a boolean flag. If flag is provided when command line utility is executed, OAuth will be used to authenticate your Google credentials. If not service account mode of authentication will be used.
* -j, --json_schema: Specify the path to the JSON Validation Schema for this argument. You can either explicitly pass the .json file here or provide it in the config.yml file as a value for the (model > input > validation_schema) key.

To get an existing manifest (as a Google Sheet URL) using `poetry`: 

Step 1: Obtain `credentials.json`, `token.pickle`, and `schematic_service_account_creds.json` by following the instructions above. 

Step 2:  Make sure you have credentials to download the desired manifest from Synapse. The "download" button should be disabled if you don't have credentials. 

Step 3: Update master_fileview in config.yml. Make sure that your config.yml points to the right master fileview. 

Step 4: Use parent id of the manifest for "dataset_id" parameter

*Note*: if the dataset_id you provided is invalid, it will generate an empty manifest based on the data model. 

```
poetry run schematic manifest -c ~/path/to/config.yml get -d <dataset id> -s -oauth
```
*Note*: If you want to get an existing manifest, the dataset id should be the parent id of your desired manifest. If your dataset id is incorrect, you will get an empty manifest 


### Validate a manifest
```
schematic manifest --config ~/path/to/config.yml validate [OPTIONS]   # validate manifest
```

**Options**:

Required:
* -c, --config: Specify the path to the `config.yml` using this option. 
* -mp,--manifest_path: Specify the path to the metadata manifest file that you want to submit to a dataset on Synapse. This is a required argument.

Optional: 
* -dt, --data_type: Specify the component (data type) from the data model that is to be used for generating the metadata manifest file. You can either explicitly pass the data type here or provide it in the config.yml file as a value for the (manifest > data_type) key.
* -js, --json_schema: Specify the path to the JSON Validation Schema for this argument. You can either explicitly pass the .json file here or provide it in the config.yml file as a value for the (model > input > validation_schema) key.


### Submit a  manifest
```
schematic model submit [OPTIONS]
```

**Options**:

Required:
* -c, --config: Specify the path to the `config.yml` using this option. 
* -mp, --manifest_path: Specify the path to the metadata manifest file that you want to submit to a dataset on Synapse. 
* -d, --dataset_id: Specify the synID of the dataset folder on Synapse to which you intend to submit the metadata manifest file.

Optional: 
* -v, --verbosity: Either CRITICAL, ERROR, WARNING, INFO or DEBUG
* -vc, --validate_component: The component or data type from the data model which you can use to validate the data filled in your manifest template.


## Schema
### Convert schema to JSON-LD format
```
schematic schema convert <options> <DATA_MODEL_CSV>
```
**Options**:

Optional: 
* -v, --verbosity: Either CRITICAL, ERROR, WARNING, INFO or DEBUG
* -b, --base_schema: Path to base data model. BioThings data model is loaded by default.
* -o, --output_jsonld: Path to where the generated JSON-LD file needs to be outputted.

*Note*: This command might take a few minutes to run. 

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