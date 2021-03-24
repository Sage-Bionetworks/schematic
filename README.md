# Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![GitHub stars](https://img.shields.io/github/stars/Sage-Bionetworks/schematic)](https://github.com/Sage-Bionetworks/schematic/stargazers) [![GitHub forks](https://img.shields.io/github/forks/Sage-Bionetworks/schematic)](https://github.com/Sage-Bionetworks/schematic/network)

- [Schematic](#schematic)
  - [1.1. Introduction](#11-introduction)
  - [1.2. Installation Requirements and Pre-requisites](#12-installation-requirements-and-pre-requisites)
  - [1.3. Package Setup Instructions](#13-package-setup-instructions)
  - [1.4. Command Line Interface](#14-command-line-interface)
  - [1.5. Contributing](#15-contributing)
  - [1.6. Contributors](#16-contributors)

## 1.1. Introduction

SCHEMATIC is an acronym for _Schema Engine for Manifest Ingress and Curation_. The Python based infrastructure provides a _novel_ schema-based, data ingress ecosystem, that is meant to streamline the process of metadata annotation and validation for various data contributors.

## 1.2. Installation Requirements and Pre-requisites

* Python 3.7.1 or higher
* [`pyenv`](https://github.com/pyenv/pyenv)
* [`poetry`](https://github.com/python-poetry/poetry)


**Important**: You need to be a registered and certified user on [`synapse.org`](https://www.synapse.org/), and also have the right permissions to download the Google credentials files from Synapse.

## 1.3. Package Setup Instructions

* [Clone Project Repository](https://sage-schematic.readthedocs.io/en/develop/README.html#clone-project-repository)
* [Virtual Environment Setup](https://sage-schematic.readthedocs.io/en/develop/README.html#virtual-environment-setup)
* [Install Dependencies](https://sage-schematic.readthedocs.io/en/develop/README.html#install-dependencies)
* [Obtain Credentials File(s)](https://sage-schematic.readthedocs.io/en/develop/README.html#obtain-credentials-file-s)
* [Fill in Configuration File(s)](https://sage-schematic.readthedocs.io/en/develop/README.html#fill-in-configuration-file-s)


## 1.4. Command Line Interface

* [Schematic Initialization](https://sage-schematic.readthedocs.io/en/develop/cli_reference.html#schematic-init) (_initialize mode of authentication_)

* [Metadata Manifest Validation](https://sage-schematic.readthedocs.io/en/develop/cli_reference.html#schematic-model-validate) (_validate metadata manifest (.csv) files_)

* [Metadata Manifest Generation](https://sage-schematic.readthedocs.io/en/develop/cli_reference.html#schematic-manifest-get) (_generate metadata manifest (.csv) files_)

* [Metadata Manifest Validation and Submission](https://sage-schematic.readthedocs.io/en/develop/cli_reference.html#schematic-model-submit) (_submission and optional validation of metadata manifest (.csv) files_)

Refer to the [docs](https://github.com/Sage-Bionetworks/schematic/tree/develop/docs/md/details.md) for more details.

## 1.5. Contributing

Interested in contributing? Awesome! We follow the typical [GitHub workflow](https://guides.github.com/introduction/flow/) of forking a repo, creating a branch, and opening pull requests. For more information on how you can add or propose a change, visit our [contributing guide](CONTRIBUTION.md). To start contributing to the package, you can refer to the [Getting Started](CONTRIBUTION.md#getting-started) section in our [contributing guide](CONTRIBUTION.md).

## 1.6. Contributors

Active contributors and maintainers:

- [Milen Nikolov](https://github.com/milen-sage)
- [Sujay Patil](https://github.com/sujaypatil96)
- [Bruno Grande](https://github.com/BrunoGrandePhD)
- [Xengie Doan](https://github.com/xdoan)
