# Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change. 

Please note we have a code of conduct, please follow it in all your interactions with the project.

## Getting started

### General Contribution Instructions

1. Fork the repository.
2. Clone the forked repository.
3. Push all your changes to the dev branch of the forked repository.
4. Create pull requests to the origin repository.

### Setup Project for Development and Testing

1. Install [package dependencies](https://sage-schematic.readthedocs.io/en/develop/README.html#installation-requirements-and-pre-requisites).
2. Clone the `schematic` package repository: `git clone https://github.com/Sage-Bionetworks/schematic.git`
3. [Create and activate](https://sage-schematic.readthedocs.io/en/develop/README.html#virtual-environment-setup) a virtual environment.
4. Run the following commands to build schematic and install the package along with all of its dependencies:
   ```python
   cd schematic  # change directory to schematic
   git checkout develop  # switch to develop branch of schematic
   poetry build # build source and wheel archives
   pip install dist/schematicpy-0.1.11-py3-none-any.whl  # install wheel file
5. [Obtain](https://sage-schematic.readthedocs.io/en/develop/README.html#obtain-google-credentials-file-s) appropriate Google credentials file(s).
6. [Obtain and Fill in](https://sage-schematic.readthedocs.io/en/develop/README.html#fill-in-configuration-file-s) the `config.yml` file and the `.synapseConfig` file as well as described in the `Fill in Configuration File(s)` part of the documentation.
8. [Import](https://docs.python.org/3/reference/simple_stmts.html#the-import-statement) `schematicpy` to leverage its functions directly as follows: 
   ```python
   from schematic import <sub-package>.<method-name> # import method from schematic core module

## Pull Request Process

1. Ensure any install or build dependencies are removed before the end of the layer when doing a 
   build.
2. If needed, update the README.md with details of changes to the interface, this includes new environment 
   variables, exposed ports, useful file locations and container parameters.
3. You may merge the Pull Request in once you have the sign-off of at least one other developer, or if you 
   do not have permission to do that, you may request the second reviewer to merge it for you.
4. When merging into dev into master follow release procedures (TODO: releas process to be determined)

## Updating Synapse Test Resources

1. Duplicate the entity being updated (or folder if applicable).
2. Edit the duplicates (_e.g._ annotations, contents, name).
3. Update the test suite in your branch to use these duplicates, including the expected values in the test assertions. 
4. Open a PR as per the usual process (see above). 
5. Once the PR is merged, leave the original copies on Synapse to maintain support for feature branches that were forked from `develop` before your update. 
   - If the old copies are problematic and need to be removed immediately (_e.g._ contain sensitive data), proceed with the deletion and alert the other contributors that they need to merge the latest `develop` branch into their feature branches for their tests to work. 

## Code style

* Please consult this code style guide prior contributing code to the project:
http://google.github.io/styleguide/pyguide.html

* Be consistent and follow existing code conventions and spirit.
