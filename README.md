# Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![Documentation Status](https://readthedocs.org/projects/sage-schematic/badge/?version=develop)](https://sage-schematic.readthedocs.io/en/develop/?badge=develop) [![PyPI version](https://badge.fury.io/py/schematicpy.svg)](https://badge.fury.io/py/schematicpy)

## Introduction

SCHEMATIC is an acronym for _Schema Engine for Manifest Ingress and Curation_. The Python based infrastructure provides a _novel_ schema-based, metadata ingress ecosystem, that is meant to streamline the process of biomedical dataset annotation, metadata validation and submission to a data repository for various data contributors.

## Installation Requirements and Pre-requisites

* Python 3.7.1 or higher

Note: You need to be a registered and certified user on [`synapse.org`](https://www.synapse.org/), and also have the right permissions to download the Google credentials files from Synapse.

## Installing

Create and activate a virtual environment within which you can install the package:

```
python -m venv .venv
source .venv/bin/activate
```

Install and update the package using [pip](https://pip.pypa.io/en/stable/quickstart/):

```
python -m pip install schematicpy
```

## Command Line Client Usage

### Initialization

```
schematic init --config ~/path/to/config.yml    # initialize mode of authentication
```

### Manifest

```
schematic manifest --config ~/path/to/config.yml get    # generate manifest based on data type
```

```
schematic manifest --config ~/path/to/config.yml validate   # validate manifest
```

### Model

```
schematic model --config ~/path/to/config.yml submit    # validate and submit manifest
```

## Creating a Relational Database and Uploading to the Data to a local Database

 * For a detailed tutorial please see [SchemaHub documentation](https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/2490171418/Using+Schematic+to+Create+a+Relational+Database}). (Schema Hub is currently only accessable for internal users.)

 * Note: 
 	* make sure all config files (`config.yaml`, `sql_config.yaml`, `sql_query_config.yaml`) are in the same location.
 	* File and folder naming is very important for automated RDB
 		generation. See tutorial for detailed naming instructions
 		before proceeding.

 
### Creating local MySQL server

* Install MySQL Server:
 * For MacOS X:  follow [these](https://flaviocopes.com/mysql-how-to-install/) instructions.
 	* Note the username and password you set during installation.
* Install dependencies: `poetry install`
* Use poetry shell to access virtual environment: `poetry shell`
	* If having permissions issues, may have to use `sudo poetry shell`
* Ensure the mysql server process is running; if you haven't already run
`brew services start mysql`
(assuming you installed mysql via homebrew)
* Update `sql_config.yaml` file with your username (eg. `root`), password and host (e.g. `localhost`).
* Make sure `sql_config.yaml` file is in the same folder as `config.yaml`


### Creating a Relational Database

This script assumes you have already created the jsonld schema and manifests.

Note: The csv data model name must have the suffix `rdb.model.csv` for it to be properly parsed to create the manifests.

## Create SQL Database
```
python3 scripts/create_load_sql_db_nf.py -create_db_tables 
```



## Applying SQL Queries and Uploading tables to Synapse


### Uploading Data to a Relational Database

### 

## Contributing

Interested in contributing? Awesome! We follow the typical [GitHub workflow](https://guides.github.com/introduction/flow/) of forking a repo, creating a branch, and opening pull requests. For more information on how you can add or propose a change, visit our [contributing guide](CONTRIBUTION.md). To start contributing to the package, you can refer to the [Getting Started](CONTRIBUTION.md#getting-started) section in our [contributing guide](CONTRIBUTION.md).

## Contributors

Active contributors and maintainers:

- [Milen Nikolov](https://github.com/milen-sage)
- [Sujay Patil](https://github.com/sujaypatil96)
- [Bruno Grande](https://github.com/BrunoGrandePhD)
- [Robert Allaway](https://github.com/allaway)
