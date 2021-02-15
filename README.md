# 1. Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![GitHub stars](https://img.shields.io/github/stars/Sage-Bionetworks/schematic)](https://github.com/Sage-Bionetworks/schematic/stargazers) [![GitHub forks](https://img.shields.io/github/forks/Sage-Bionetworks/schematic)](https://github.com/Sage-Bionetworks/schematic/network)

- [1. Schematic](#1-schematic)
  - [1.1. Introduction](#11-introduction)
  - [1.2. Installation Requirements and Pre-requisites](#12-installation-requirements-and-pre-requisites)
  - [1.3. Package Setup Instructions](#13-package-setup-instructions)
    - [1.3.1. Clone Project Repository](#131-clone-project-repository)
    - [1.3.2. Virtual Environment Setup](#132-virtual-environment-setup)
    - [1.3.3. Install Dependencies](#133-install-dependencies)
    - [1.3.4. Obtain Credentials File(s)](#134-obtain-credentials-files)
    - [1.3.5. Fill in Configuration File(s)](#135-fill-in-configuration-files)
    - [1.3.6. Command Line Interface](#136-command-line-interface)
      - [1.3.6.1. Metadata Manifest Generation](#1361-metadata-manifest-generation)
      - [1.3.6.2. Metadata Manifest Validation and Submission](#1362-metadata-manifest-validation-and-submission)
  - [1.4. Contributing](#14-contributing)
  - [1.5. Contributors](#15-contributors)

## 1.1. Introduction

SCHEMATIC is an acronym for _Schema Engine for Manifest Ingress and Curation_. The Python based infrastructure provides a _novel_ schema-based, data ingress ecosystem, that is meant to streamline the process of metadata annotation and validation for various data contributors.

## 1.2. Installation Requirements and Pre-requisites

Following are the tools or packages that you will need to set up `schematic` for your use:

- Python 3.7.1 or higher

If you do not have a version of Python greater than 3.7.1, it is recommended to use `pyenv` to be able to easily use and switch between multiple Python versions.

- [`pyenv`](https://github.com/pyenv/pyenv)

It is recommended that you install the `poetry` dependency manager if you are a current (or potential) `schematic` contributor or a DCC admin managing installations of the [Data Curator App](https://github.com/Sage-Bionetworks/data_curator/).

- [`poetry`](https://github.com/python-poetry/poetry)


**Important**: Make sure you are a registered and certified user on [`synapse.org`](https://www.synapse.org/), and also have all the right permissions to download credentials files in the following steps.

## 1.3. Package Setup Instructions

For `schematic` Contributors and DCC Admins

### 1.3.1. Clone Project Repository

Since the package isn't available on [`PyPI`](https://pypi.org/) yet, to setup the package you need to `clone` the project repoository from GitHub by running the following command:

```bash
git clone --single-branch --branch develop https://github.com/Sage-Bionetworks/schematic.git
```

### 1.3.2. Virtual Environment Setup

If you are a DCC Admin:

You can explicitly create a virtual environment by running the following command:

```python
python -m venv .venv
```

To activate the `venv` virtual environment and also retreive the path to your virtual environment, run:

```bash
$ source $(poetry env info --path)/bin/activate
```

If you are a `schematic` package contributor you don't need to create a `venv` virtual environment, `poetry` will create a virtual environment by default, which you can use.

### 1.3.3. Install Dependencies

If you are a DCC Admin:

After cloning the `schematic` project from GitHub and setting up your virtual environment:
- Change directory to the `schematic` package:  `cd schematic`
- Switch to the development (`develop`) branch of the package: `git checkout develop`
- Use [`poetry`](https://python-poetry.org/docs/cli/#build) to build the source and wheel archives: `poetry build`

This will create a folder called `dist` in your root directory which contains the `.tar.gz` and `.whl` bundle files.
- Install wheel file: `pip install dist/schematicpy-0.1.11-py3-none-any.whl`

If you are a `schematic` contributor:

Running the following command reads the `pyproject.toml` file from the current project, resolves the dependencies and install them:

```bash
poetry install
```

### 1.3.4. Obtain Credentials File(s)

Download a copy of the `credentials.json` file stored on Synapse by running the following command:

```bash
$ synapse get syn21088684
```

The `credentials.json` file is required when you are using [`OAuth2`](https://developers.google.com/identity/protocols/oauth2) to authenticate with the Google APIs.

For details about the steps involved in the `OAuth2` [authorization flow](https://github.com/Sage-Bionetworks/schematic/blob/develop/schematic/utils/google_api_utils.py#L18), refer to the `Credentials` section in the `docs/details` document.

Alternatively, you can also download the `schematic_service_account_creds.json` file, which uses the service account mode of authentication (_for Google services_). To download this file, run the following:

```bash
$ synapse get syn24214983
```

Note: The `Selection Options` dropdown which allows the user to select multiple values in a cell during manifest annotation [does not work](https://developers.google.com/apps-script/api/concepts) with the service account mode of authentication.

### 1.3.5. Fill in Configuration File(s)

There are two main configuration files that need to be edited – [`config.yml`](https://github.com/Sage-Bionetworks/schematic/blob/develop/config.yml) and [`.synapseConfig`](https://github.com/Sage-Bionetworks/synapsePythonClient/blob/master/synapseclient/.synapseConfig).

Download a copy of the `.synapseConfig` file, open the file in the editor of your choice and edit the `username` and `apikey` attributes under the `[authentication]` section.

<details>
  <summary>Description of config.yml attributes</summary>
  
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
        
</details>

Note: You can get your Synapse API key by: _logging into Synapse_ > _Settings_ > _Synapse API Key_ > _Show API Key_.

### 1.3.6. Command Line Interface

The two main CLI utilities that are distributed as part of the package are:

#### 1.3.6.1. Metadata Manifest Generation

To generate a metadata manifest template based on a data type that is present in your data model:

```bash
$ schematic manifest --config /path/to/config.yml get
```

#### 1.3.6.2. Metadata Manifest Validation and Submission

```bash
$ schematic model --config /path/to/config.yml submit --manifest_path /path/to/manifest.csv --dataset_id dataset_synapse_id
```

Refer to the [docs](https://github.com/Sage-Bionetworks/schematic/tree/develop/docs) for more details.

Note: To view a full list of all the arguments that can be supplied to the command line interfaces, add a `--help` option at the end of each of the commands.

## 1.4. Contributing

Interested in contributing? Awesome! We follow the typical [GitHub workflow](https://guides.github.com/introduction/flow/) of forking a repo, creating a branch, and opening pull requests. For more information on how you can add or propose a change, visit our [contributing guide](https://github.com/Sage-Bionetworks/schematic/blob/develop/CONTRIBUTION.md).

## 1.5. Contributors

Active contributors and maintainers:

- [Milen Nikolov](https://github.com/milen-sage)
- [Sujay Patil](https://github.com/sujaypatil96)
- [Bruno Grande](https://github.com/BrunoGrandePhD)
- [Xengie Doan](https://github.com/xdoan)
