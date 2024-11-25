# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

Please note we have a [code of conduct](CODE_OF_CONDUCT.md), please follow it in all your interactions with the project.

## How to report bugs or feature requests

You can **create bug and feature requests** through [Sage Bionetwork's DPE schematic support](https://sagebionetworks.jira.com/servicedesk/customer/portal/5/group/7/create/225). Providing enough details to the developers to verify and troubleshoot your issue is paramount:
- **Provide a clear and descriptive title as well as a concise summary** of the issue to identify the problem.
- **Describe the exact steps which reproduce the problem** in as many details as possible.
- **Describe the behavior you observed after following the steps** and point out what exactly is the problem with that behavior.
- **Explain which behavior you expected to see** instead and why.
- **Provide screenshots of the expected or actual behaviour** where applicable.

## How to contribute code

### The development environment setup

For setting up your environment, please follow the instructions in the `README.md` under `Installation Guide For: Contributors`.

### The development workflow

For new features, bugs, enhancements:

#### 1. Branch Setup
* Pull the latest code from the develop branch in the upstream repository.
* Checkout a new branch formatted like so: `<JIRA-ID>-<feature/fix-name>` from the develop branch

#### 2. Development Workflow
* Develop on your new branch.
* Ensure pyproject.toml and poetry.lock files are compatible with your environment.
* Add changed files for tracking and commit changes using [best practices](https://www.perforce.com/blog/vcs/git-best-practices-git-commit)
* Have granular commits: not “too many” file changes, and not hundreds of code lines of changes
* You can choose to create a draft PR if you prefer to develop this way

#### 3. Branch Management
* Push code to `<JIRA-ID>-<feature/fix-name>` in upstream repo:
  ```
  git push <upstream> <JIRA-ID>-<feature/fix-name>
  ```
* Branch off `<JIRA-ID>-<feature/fix-name>` if you need to work on multiple features associated with the same code base
* After feature work is complete and before creating a PR to the develop branch in upstream
    a. ensure that code runs locally
    b. test for logical correctness locally
    c. run `pre-commit` to style code if the hook is not installed
    c. wait for git workflow to complete (e.g. tests are run) on github

#### 4. Pull Request and Review
* Create a PR from `<JIRA-ID>-<feature/fix-name>` into the develop branch of the upstream repo
* Request a code review on the PR
* Once code is approved merge in the develop branch. The **"Squash and merge"** strategy should be used for a cleaner commit history on the `develop` branch. The description of the squash commit should include enough information to understand the context of the changes that were made.
* Once the actions pass on the main branch, delete the `<JIRA-ID>-<feature/fix-name>` branch

### Updating readthedocs documentation
1. Navigate to the docs directory.
2. Run make html to regenerate the build after changes.
3. Contact the development team to publish the updates.

*Helpful resources*:

1. [Getting started with Sphinx](https://www.sphinx-doc.org/en/master/usage/quickstart.html)
2. [Installing Sphinx](https://www.sphinx-doc.org/en/master/usage/installation.html)

### Update toml file and lock file
If you install external libraries by using `poetry add <name of library>`, please make sure that you include `pyproject.toml` and `poetry.lock` file in your commit.

### Code style

To ensure consistent code formatting across the project, we use the `pre-commit` hook. You can manually run `pre-commit` across the respository before making a pull request like so:

```
pre-commit run --all-files
```

Further, please consult the [Google Python style guide](http://google.github.io/styleguide/pyguide.html) prior to contributing code to this project.
Be consistent and follow existing code conventions and spirit.

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

* All new code must include tests.

* Tests are written using pytest and are located in the [tests/](https://github.com/Sage-Bionetworks/schematic/tree/develop/tests) subdirectory.

* Run tests with:
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

