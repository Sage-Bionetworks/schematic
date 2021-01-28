# Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![GitHub stars](https://img.shields.io/github/stars/Sage-Bionetworks/schematic)](https://github.com/Sage-Bionetworks/schematic/stargazers) [![GitHub forks](https://img.shields.io/github/forks/Sage-Bionetworks/schematic)](https://github.com/Sage-Bionetworks/schematic/network)


## Introduction

SCHEMATIC is an acronym for _Schema Engine for Manifest Ingress and Curation_. The Python based infrastructure provides a _novel_ schema-based, data ingress ecosystem, that is meant to streamline the process of metadata annotation and validation for various data contributors.

## Installation Requirements and Pre-requisites

Following are the tools or packages that you will need to setup `schematic` for your use:

- [`pyenv`](https://github.com/pyenv/pyenv)
- Python 3.7.1 or higher
- [`poetry`](https://github.com/python-poetry/poetry)

Note: It is recommended to use `pyenv` to be able to easily switch between Python versions. Explicit switching between environments using `poetry` is possible as well.

Make sure you are a registered user on [`synapse.org`](https://www.synapse.org/), and also have all the right permissions to download credentials files in the following steps.

## Package Setup

### Clone Project Repository

Since the package isn't available on [`PyPI`](https://pypi.org/) yet, to setup the package you need to `clone` the project repoository from GitHub by running the following command:

```bash
$ git clone --single-branch --branch develop https://github.com/Sage-Bionetworks/schematic.git
```

### Virtual Environment Setup

You can explicitly create a virtual environment by running the following command:

```python
$ python -m venv .venv
```

If you don't want to create a `venv` virtual environment, `poetry` will create a virtual environment for you. To use `poetry`'s virtual environment, skip this step and move on to the next step.

### Install Dependencies

Running the following command reads the `pyproject.toml` file from the current project, resolves the dependencies and install them:

```bash
$ poetry install
```

### Obtain Credentials File(s)

Download a copy of the `credentials.json` file stored on Synapse by running the following command:

```bash
$ synapse get syn21088684
```

Alternatively, you can also download the `schematic_service_account_creds.json` file, which uses the service account mode of authentication (_for Google services_). To download this file, run the following:

```bash
$ synapse get syn24214983
```

Note: The service account mode of authentication is still being tested and certain functionalities which leverage Google Apps Script API are [not fully compatible with this mode](https://developers.google.com/apps-script/api/concepts). So we recommend you follow the first command in this section.

### Fill in Configuration File(s)

There are two main configuration files that need to be edited – `config.yml` and `.synapseConfig`.

First, open the `.synapseConfig` file in the editor of your choice and edit the `username` and `apikey` attributes under the `[authentication]` section.

Note: You can get your Synapse API key by: _logging into Synapse_ > _Settings_ > _Synapse API Key_ > _Show API Key_.

## Contributing

Interested in contributing? Awesome! We follow the typical [GitHub workflow](https://guides.github.com/introduction/flow/) of forking a repo, creating a branch, and opening pull requests. For more information on how you can add or propose a change, visit our [contributing guide](https://github.com/Sage-Bionetworks/schematic/blob/develop/CONTRIBUTION.md).

## Contributors

Active contributors and maintainers:

- [Milen Nikolov](https://github.com/milen-sage)
- [Sujay Patil](https://github.com/sujaypatil96)
- [Bruno Grande](https://github.com/BrunoGrandePhD)
- [Xengie Doan](https://github.com/xdoan)
