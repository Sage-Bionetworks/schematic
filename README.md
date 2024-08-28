# Schematic
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FSage-Bionetworks%2Fschematic%2Fbadge%3Fref%3Ddevelop&style=flat)](https://actions-badge.atrox.dev/Sage-Bionetworks/schematic/goto?ref=develop) [![Documentation Status](https://readthedocs.org/projects/sage-schematic/badge/?version=develop)](https://sage-schematic.readthedocs.io/en/develop/?badge=develop) [![PyPI version](https://badge.fury.io/py/schematicpy.svg)](https://badge.fury.io/py/schematicpy)

# Table of contents
- [Schematic](#schematic)
- [Table of contents](#table-of-contents)
- [Introduction](#introduction)
- [Installation](#installation)
  - [Installation Requirements](#installation-requirements)
  - [Installation guide for Schematic CLI users](#installation-guide-for-schematic-cli-users)
  - [Installation guide for developers/contributors](#installation-guide-for-developerscontributors)
    - [Development environment setup](#development-environment-setup)
    - [Development process instruction](#development-process-instruction)
    - [Example For REST API ](#example-for-rest-api-)
      - [Use file path of `config.yml` to run API endpoints:](#use-file-path-of-configyml-to-run-api-endpoints)
      - [Use content of `config.yml` and `schematic_service_account_creds.json`as an environment variable to run API endpoints:](#use-content-of-configyml-and-schematic_service_account_credsjsonas-an-environment-variable-to-run-api-endpoints)
    - [Example For Schematic on mac/linux ](#example-for-schematic-on-maclinux-)
    - [Example For Schematic on Windows ](#example-for-schematic-on-windows-)
- [Other Contribution Guidelines](#other-contribution-guidelines)
  - [Updating readthedocs documentation](#updating-readthedocs-documentation)
  - [Update toml file and lock file](#update-toml-file-and-lock-file)
  - [Reporting bugs or feature requests](#reporting-bugs-or-feature-requests)
- [Command Line Usage](#command-line-usage)
- [Testing](#testing)
  - [Updating Synapse test resources](#updating-synapse-test-resources)
- [Code style](#code-style)
- [Contributors](#contributors)

# Introduction
SCHEMATIC is an acronym for _Schema Engine for Manifest Ingress and Curation_. The Python based infrastructure provides a _novel_ schema-based, metadata ingress ecosystem, that is meant to streamline the process of biomedical dataset annotation, metadata validation and submission to a data repository for various data contributors.

# Installation
## Installation Requirements
* Python version 3.9.0≤x<3.11.0
* You need to be a registered and certified user on [`synapse.org`](https://www.synapse.org/)

Note: Our credential policy for Google credentials in order to create Google sheet files from Schematic, see tutorial ['HERE'](https://scribehow.com/shared/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA). If you plan to use `config.yml`, please ensure that the path of `schematic_service_account_creds.json` is indicated there (see `google_sheets > service_account_creds` section)

## Installation guide for Schematic CLI users
1. **Verifying Python Version Compatibility**

To ensure compatibility with Schematic, please follow these steps:

Check your own Python version:
```
python3 --version
```

Check the Supported Python Version: Open the pyproject.toml file in the Schematic repository to find the version of Python that is supported. You can view this file directly on GitHub [here](https://github.com/Sage-Bionetworks/schematic/blob/main/pyproject.toml#L39).

Switching Python Versions: If your current Python version is not supported by Schematic, you can switch to the supported version using tools like [pyenv](https://github.com/pyenv/pyenv?tab=readme-ov-file#switch-between-python-versions). Follow the instructions in the pyenv documentation to install and switch between Python versions easily.

2. **Setting Up the Virtual Environment**

After switching to the version of Python supported by Schematic, please activate a virtual environment within which you can install the package:
```
python3 -m venv .venv
source .venv/bin/activate
```
Note: Python 3 has built-in support for virtual environments with the venv module, so you no longer need to install virtualenv.

3. **Installing Schematic**

Install the package using [pip](https://pip.pypa.io/en/stable/quickstart/):

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

### Development environment setup
1. Clone the `schematic` package repository.
```
git clone https://github.com/Sage-Bionetworks/schematic.git
```
2. Install `poetry` (version 1.3.0 or later) using either the [official installer](https://python-poetry.org/docs/#installing-with-the-official-installer) or [pipx](https://python-poetry.org/docs/#installing-with-pipx). If you have an older installation of Poetry, we recommend uninstalling it first. 

3. Start the virtual environment by doing: 
```
poetry shell
```
4. Install the dependencies by doing: 
```
poetry install --all-extras
```
This command will install the dependencies based on what we specify in poetry.lock. If this step is taking a long time, try to go back to step 2 and check your version of poetry. Alternatively, you could also try deleting the lock file and regenerate it by doing `poetry install` (Please note this method should be used as a last resort because this would force other developers to change their development environment)


5. Fill in credential files: 
*Note*: If you won't interact with Synapse, please ignore this section.

There are two main configuration files that need to be edited:
- config.yml
- [synapseConfig](https://raw.githubusercontent.com/Sage-Bionetworks/synapsePythonClient/master/synapseclient/.synapseConfig)

<strong>Configure .synapseConfig File</strong>

Download a copy of the ``.synapseConfig`` file, open the file in the editor of your 
choice and edit the `username` and `authtoken` attribute under the `authentication` 
section. **Note:** You must place the file at the root of the project like
`{project_root}/.synapseConfig` in order for any authenticated tests to work.

*Note*: You could also visit [configparser](https://docs.python.org/3/library/configparser.html#module-configparser>) doc to see the format that `.synapseConfig` must have. For instance:
>[authentication]<br> username = ABC <br> authtoken = abc

<strong>Configure config.yml File</strong>

There are some defaults in schematic that can be configured. These fields are in ``config_example.yml``:

```text

# This is an example config for Schematic.
# All listed values are those that are the default if a config is not used.
# Save this as config.yml, this will be gitignored.
# Remove any fields in the config you don't want to change
# Change the values of any fields you do want to change


# This describes where assets such as manifests are stored
asset_store:
  # This is when assets are stored in a synapse project
  synapse:
    # Synapse ID of the file view listing all project data assets.
    master_fileview_id: "syn23643253"
    # Path to the synapse config file, either absolute or relative to this file
    config: ".synapseConfig"
    # Base name that manifest files will be saved as
    manifest_basename: "synapse_storage_manifest"

# This describes information about manifests as it relates to generation and validation
manifest:
  # Location where manifests will saved to
  manifest_folder: "manifests"
  # Title or title prefix given to generated manifest(s)
  title: "example"
  # Data types of manifests to be generated or data type (singular) to validate manifest against
  data_type:
    - "Biospecimen"
    - "Patient"

# Describes the location of your schema
model:
  # Location of your schema jsonld, it must be a path relative to this file or absolute
  location: "tests/data/example.model.jsonld"

# This section is for using google sheets with Schematic
google_sheets:
  # Path to the synapse config file, either absolute or relative to this file
  service_acct_creds: "schematic_service_account_creds.json"
  # When doing google sheet validation (regex match) with the validation rules.
  #   true is alerting the user and not allowing entry of bad values.
  #   false is warning but allowing the entry on to the sheet.
  strict_validation: true
```

If you want to change any of these copy ``config_example.yml`` to ``config.yml``, change any fields you want to, and remove any fields you don't.

For example if you wanted to change the folder where manifests are downloaded your config should look like:

```text

manifest:
  manifest_folder: "my_manifest_folder_path"
```

_Note_: `config.yml` is ignored by git.

_Note_: Paths can be specified relative to the `config.yml` file or as absolute paths.

6. Login to Synapse by using the command line
On the CLI in your virtual environment, run the following command: 
```
synapse login -u <synapse username> -p <synapse password> --rememberMe
```

7. Obtain Google credential Files
Running `schematic init` is no longer supported due to security concerns. To obtain  `schematic_service_account_creds.json`, please follow the instructions [here](https://scribehow.com/shared/Enable_Google_Drive_and_Google_Sheets_APIs_for_project__yqfcJz_rQVeyTcg0KQCINA). 

> As v22.12.1 version of schematic, using `token` mode of authentication (in other words, using `token.pickle` and `credentials.json`) is no longer supported due to Google's decision to move away from using OAuth out-of-band (OOB) flow. Click [here](https://developers.google.com/identity/protocols/oauth2/resources/oob-migration) to learn more. 

*Notes*: Use the ``schematic_service_account_creds.json`` file for the service
account mode of authentication (*for Google services/APIs*). Service accounts 
are special Google accounts that can be used by applications to access Google APIs 
programmatically via OAuth2.0, with the advantage being that they do not require 
human authorization. 

*Background*: schematic uses Google’s API to generate google sheet templates that users fill in to provide (meta)data.
Most Google sheet functionality could be authenticated with service account. However, more complex Google sheet functionality
requires token-based authentication. As browser support that requires the token-based authentication diminishes, we are hoping to deprecate
token-based authentication and keep only service account authentication in the future. 

8. Set up pre-commit hooks

This repository is configured to utilize pre-commit hooks as part of the development process. To enable these hooks, please run the following command and look for the following success message:
```
$ pre-commit install
pre-commit installed at .git/hooks/pre-commit
```

### Development process instruction

For new features, bugs, enhancements

1. Pull the latest code from [develop branch in the upstream repo](https://github.com/Sage-Bionetworks/schematic)
2. Checkout a new branch develop-<feature/fix-name> from the develop branch
3. Do development on branch develop-<feature/fix-name>
   a. may need to ensure that schematic poetry toml and lock files are compatible with your local environment
4. Add changed files for tracking and commit changes using [best practices](https://www.perforce.com/blog/vcs/git-best-practices-git-commit)
5. Have granular commits: not “too many” file changes, and not hundreds of code lines of changes
6. Commits with work in progress are encouraged:
   a. add WIP to the beginning of the commit message for “Work In Progress” commits
7. Keep commit messages descriptive but less than a page long, see best practices
8. Push code to develop-<feature/fix-name> in upstream repo
9. Branch out off develop-<feature/fix-name> if needed to work on multiple features associated with the same code base
10. After feature work is complete and before creating a PR to the develop branch in upstream
    a. ensure that code runs locally
    b. test for logical correctness locally
    c. wait for git workflow to complete (e.g. tests are run) on github
11. Create a PR from develop-<feature/fix-name> into the develop branch of the upstream repo
12. Request a code review on the PR
13. Once code is approved merge in the develop branch
14. Delete the develop-<feature/fix-name> branch

*Note*: Make sure you have the latest version of the `develop` branch on your local machine.

### Example For REST API <br>

#### Use file path of `config.yml` to run API endpoints: 
```
docker run --rm -p 3001:3001 \
  -v $(pwd):/schematic -w /schematic --name schematic \
  -e SCHEMATIC_CONFIG=/schematic/config.yml \
  -e GE_HOME=/usr/src/app/great_expectations/ \
  sagebionetworks/schematic \
  python /usr/src/app/run_api.py
``` 

#### Use content of `config.yml` and `schematic_service_account_creds.json`as an environment variable to run API endpoints: 
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


### Example For Schematic on mac/linux <br>
To run example below, first clone schematic into your home directory  `git clone https://github.com/sage-bionetworks/schematic ~/schematic` <br>
Then update .synapseConfig with your credentials
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

### Example For Schematic on Windows <br>
```
docker run -v %cd%:/schematic \
  -w /schematic \
  -e GE_HOME=/usr/src/app/great_expectations/ \
  sagebionetworks/schematic \
  schematic model \
  -c config.yml validate -mp tests/data/mock_manifests/inValid_Test_Manifest.csv -dt MockComponent -js /schematic/data/example.model.jsonld
```

# Other Contribution Guidelines
## Updating readthedocs documentation
1. `cd docs`
2. After making relevant changes, you could run the `make html` command to re-generate the `build` folder.
3. Please contact the dev team to publish your updates

*Other helpful resources*:

1. [Getting started with Sphinx](https://haha.readthedocs.io/en/latest/intro/getting-started-with-sphinx.html)
2. [Installing Sphinx](https://haha.readthedocs.io/en/latest/intro/getting-started-with-sphinx.html)

## Update toml file and lock file
If you install external libraries by using `poetry add <name of library>`, please make sure that you include `pyproject.toml` and `poetry.lock` file in your commit.

## Reporting bugs or feature requests
You can **create bug and feature requests** through [Sage Bionetwork's FAIR Data service desk](https://sagebionetworks.jira.com/servicedesk/customer/portal/5/group/8). Providing enough details to the developers to verify and troubleshoot your issue is paramount:
- **Provide a clear and descriptive title as well as a concise summary** of the issue to identify the problem.
- **Describe the exact steps which reproduce the problem** in as many details as possible.
- **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
- **Explain which behavior you expected to see** instead and why.
- **Provide screenshots of the expected or actual behaviour** where applicable.

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

Please visit more documentation [here](https://sage-schematic.readthedocs.io/en/develop/cli_reference.html) for more information. 



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
