# Release Process
- Test usage of new features/bug-fixes (e.g. with CLI) locally from the develop branch
- when the develop branch has a feature (or a number of features) that need a release,  create a “release PR” from develop into main 
    - In the release PR link all PRs and/or issues that are included in the release 
    - when the release PR is merged in main all commits related to PRs and issues in it will be added to the changelog on release
- once merged into main, tag the latest commit - i.e. the merge commit - with a version number (as version number is defined below). To tag the latest commit, you could use the following command: 
```
git tag -a <Tag name> -m <commit message>
```
- push the tag to main branch: 
```
git push origin <Tag name>
```

A GitHub workflow would get auto-triggered (see `.github/workflows/publish.yml`) to publish the package on PyPI (Schematic is on PyPI as [schematicpy](https://pypi.org/project/schematicpy/)) and test PyPI. You can also go through the following two sections to manually publish the package.

*Note*: If your tag does not fit into the format of `v*.*.*`, it would only get published to testPyPI. Before pushing to `main`, you could check your tag by using `git show <tag name>` For more information related to `git tag`, please check out [here](https://git-scm.com/book/en/v2/Git-Basics-Tagging)

## Definition of version number
vYY.MM.N
* YY stands for year
* MM stands for month
* N is the Nth consecutive release in a month

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

> You'll need to [register](https://pypi.org/account/register/) for a PyPI account and get added as maintainer before uploading packages to the package index. Similarly for [Test PyPI](https://test.pypi.org/account/register/) as well. 