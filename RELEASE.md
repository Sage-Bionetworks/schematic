# Release Process

Once the code has been merged into the `develop` branch on this repo, there are two processes that need to be completed to ensure a _release_ is complete.

- You should create a GitHub [tag](https://git-scm.com/book/en/v2/Git-Basics-Tagging), with the appropriate version number. Typically, from `v21.06` onwards all tags are created following the Linux Ubuntu versioning convention which is the `YY.MM` format where `Y` is the year and `M` is the month of that year when that release was created.
- You should push the package to [PyPI](https://pypi.org/). Schematic is on PyPI as [schematicpy](https://pypi.org/project/schematicpy/). You can go through the following two sections for that.

## Steps 
- Step 1: Open a pull request to merge the `main` branch to the `develop` branch: 
    - Click on the "Pull Request" tab on Github
    - Click on the green button "New pull request"
    - Select `main` as "base" and `develop` as "compare"
    - Resolve conflicts

- Step 2: Create a tag
`git tag <tag version> -m '<message>'`

- Step 3: Push the tag to main branch (this step assumes that you have checked out the main branch locally)
`git push origin <tag version>`

This should trigger the PYPI release workflow and release a new version of schematic to PYPI. You could check by cliking on the GitHub action log and login to your PYPI account (and select project `schematicpy`. Please note that you have to obtain access to `schematicpy` to be able to see it.)

>Note: if you make some mistakes and would like to delete a tag, try the following commands: `git push --delete origin <version number>` for deleting a tag remotely and `git tag -d <version number>` for deleting a tag locally. 


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