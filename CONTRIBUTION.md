# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

Please note we have a [code of conduct](CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

## How to contribute

### Reporting bugs or feature requests

You can use the [`Issues`](https://github.com/Sage-Bionetworks/schematic/issues) tab to **create bug and feature requests**. Providing enough details to the developers to verify and troubleshoot your issue is paramount:
- **Provide a clear and descriptive title as well as a concise summary** of the issue to identify the problem.
- **Describe the exact steps which reproduce the problem** in as many details as possible.
- **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
- **Explain which behavior you expected to see** instead and why.
- **Provide screenshots of the expected or actual behaviour** where applicable.

### General contribution instructions

1. Follow the [Github docs](https://help.github.com/articles/fork-a-repo/) to make a copy (a fork) of the repository to your own Github account.
2. [Clone the forked repository](https://docs.github.com/en/github/creating-cloning-and-archiving-repositories/cloning-a-repository-from-github/cloning-a-repository) to your local machine so you can begin making changes.
3. Make sure this repository is set as the [upstream remote repository](https://docs.github.com/en/github/collaborating-with-pull-requests/working-with-forks/configuring-a-remote-for-a-fork) so you are able to fetch the latest commits.
4. Push all your changes to the `develop` branch of the forked repository.

*Note*: Make sure you have you have the latest version of the `develop` branch on your local machine.

```
git checkout develop
git pull upstream develop
```

5. Create pull requests to the upstream repository.

### The development lifecycle

1. Pull the latest content from the `develop` branch of this central repository (not your fork).
2. Create a branch off the `develop` branch. Name the branch appropriately, either briefly summarizing the bug (ex., `spatil/add-restapi-layer`) or feature or simply use the issue number in the name (ex., `spatil/issue-414-fix`).
3. After completing work and testing locally, push the code to the appropriate branch on your fork.
4. In Github, create a pull request from the bug/feature branch of your fork to the `develop` branch of the central repository.

> A Sage Bionetworks engineer must review and accept your pull request. A code review (which happens with both the contributor and the reviewer present) is required for contributing.

### Development environment setup

1. Install [package dependencies](https://sage-schematic.readthedocs.io/en/develop/README.html#installation-requirements-and-pre-requisites).
2. Clone the `schematic` package repository.

```
git clone https://github.com/Sage-Bionetworks/schematic.git
```

3. [Create and activate](https://sage-schematic.readthedocs.io/en/develop/README.html#virtual-environment-setup) a virtual environment.
4. Run the following commands to build schematic and install the package along with all of its dependencies:

```
cd schematic  # change directory to schematic
git checkout develop  # switch to develop branch of schematic
poetry build # build source and wheel archives
pip install dist/schematicpy-x.y.z-py3-none-any.whl  # install wheel file
```

*Note*: Use the appropriate version number (based on the version of the codebase you are pulling) while installing the wheel file above.

5. [Obtain](https://sage-schematic.readthedocs.io/en/develop/README.html#obtain-google-credentials-file-s) appropriate Google credentials file(s).
6. [Obtain and Fill in](https://sage-schematic.readthedocs.io/en/develop/README.html#fill-in-configuration-file-s) the `config.yml` file and the `.synapseConfig` file as well as described in the `Fill in Configuration File(s)` part of the documentation.
7. [Run](https://docs.pytest.org/en/stable/usage.html) the test suite.

*Note*: To ensure that all tests run successfully, contact your DCC liason and request to be added to the `schematic-dev` [team](https://www.synapse.org/#!Team:3419888) on Synapse.

8. To test new changes made to any of the modules within `schematic`, do the following:

```
# make changes to any files or modules
pip uninstall schematicpy  # uninstall package
poetry build
pip install dist/schematicpy-x.y.z-py3-none-any.whl  # install wheel file
```

### Using Poetry to set up development environment 
1. Start the virtual environment by doing: 
```
poetry shell
```
2. Install the dependencies by doing: 
```
poetry install
```
This command will install the dependencies based on what we specify in poetry.lock
3. Add additional package by doing: 
```
poetry add 
```
The `add` command adds required packages to your pyproject.toml and installs them.

*Note*: The run command executes the given command inside the project’s virtualenv. For example, to see the python version that you are using in the virtual environment, you could do: 
```
poetry run Python -V
```
Similarly, for checking the version of Pytest that you are using, you can simply do: 
```
poetry run which pytest
```
For running tests locally, you could do: 
```
poetry run pytest tests
```
You would still need to follow step 5-6 to obtain and fill in configuration files. But you won't need step 8 for testing out local changes. 

Note: if you are using synapse-token authentication and running into error: KeyError 'synapse_config', please consider adding `synapse_config` key to the `tests/data/test_config.yml`.


## Release process

Once the code has been merged into the `develop` branch on this repo, there are two processes that need to be completed to ensure a _release_ is complete.

- You should create a GitHub [tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging), with the appropriate version number. Typically, from `v21.06` onwards all tags are created following the Linux Ubuntu versioning convention which is the `YY.MM` format where `Y` is the year and `M` is the month of that year when that release was created.
- You should push the package to [PyPI](https://pypi.org/). Schematic is on PyPI as [schematicpy](https://pypi.org/project/schematicpy/). You can go through the following two sections for that.

### Release to Test PyPI _(optional)_

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

### Release to PyPI _(mandatory)_

If the package looks great on Test PyPI and works well, the next step is to publish the package to PyPI:

```
poetry publish  # publish the package to PyPI
```

> You'll need to [register](https://pypi.org/account/register/) for a PyPI account before uploading packages to the package index. Similarly for [Test PyPI](https://test.pypi.org/account/register/) as well.

## Testing

All code added to the client must have tests. The Python client uses pytest to run tests. The test code is located in the [tests](https://github.com/Sage-Bionetworks/schematic/tree/develop-docs-update/tests) subdirectory.

You can run the test suite in the following way:

```
pytest -vs tests/
```

### Updating Synapse test resources

1. Duplicate the entity being updated (or folder if applicable).
2. Edit the duplicates (_e.g._ annotations, contents, name).
3. Update the test suite in your branch to use these duplicates, including the expected values in the test assertions.
4. Open a PR as per the usual process (see above).
5. Once the PR is merged, leave the original copies on Synapse to maintain support for feature branches that were forked from `develop` before your update.
   - If the old copies are problematic and need to be removed immediately (_e.g._ contain sensitive data), proceed with the deletion and alert the other contributors that they need to merge the latest `develop` branch into their feature branches for their tests to work.

## Code style

* Please consult the [Google Python style guide](http://google.github.io/styleguide/pyguide.html) prior to contributing code to this project.
* Be consistent and follow existing code conventions and spirit.

