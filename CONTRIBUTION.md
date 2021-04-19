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

All the sections referenced here are from the [documentation](https://sage-schematic.readthedocs.io/en/develop/index.html) on ReadTheDocs.

1. Make sure you have all the packages and tools required as specified in the [`Installation Requirements and Pre-requisites`](https://sage-schematic.readthedocs.io/en/develop/README.html#installation-requirements-and-pre-requisites) section.
2. Clone the `schematic` package repository from GitHub onto your local machine by running the following command: `git clone --single-branch --branch develop https://github.com/Sage-Bionetworks/schematic.git`
3. Create and activate a virtual environment as described in the [`Virtual Environment Setup`](https://sage-schematic.readthedocs.io/en/develop/README.html#virtual-environment-setup) section of the documentation.
4. Run the following commands to build schematic and install the package along with all of its dependencies:
```python
cd schematic  # change directory to schematic
git checkout develop  # switch to develop branch of schematic
poetry build # build source and wheel archives
pip install dist/schematicpy-0.1.11-py3-none-any.whl  # install wheel file
```
5. Fetch the appropriate Google credentials files (OAuth credentials file or service account credentials files) by running the command as shown in the `Obtain Google Credentials File(s)` section of the documentation.
6. Obtain and fill in the `config.yml` file and the `.synapseConfig` file as well as described in the `Fill in Configuration File(s)` part of the documentation.
7. Run any of the CLI utilities specified in the [`CLI reference`](https://sage-schematic.readthedocs.io/en/develop/README.html#command-line-interface).
8. You can also import `schematicpy` like any other Python package and use the library of methods provided by the package as follows: 
```python
import schematic  # import schematicpy as a module
from schematic import <method-name> # import method from schematic module
```

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

## Code of Conduct

### Our Pledge

In the interest of fostering an open and welcoming environment, we as
contributors and maintainers pledge to making participation in our project and
our community a harassment-free experience for everyone, regardless of age, body
size, disability, ethnicity, gender identity and expression, level of experience,
nationality, personal appearance, race, religion, or sexual identity and
orientation.

### Our Standards

Examples of behavior that contributes to creating a positive environment
include:

* Using welcoming and inclusive language
* Being respectful of differing viewpoints and experiences
* Gracefully accepting constructive criticism
* Focusing on what is best for the community
* Showing empathy towards other community members

Examples of unacceptable behavior by participants include:

* The use of sexualized language or imagery and unwelcome sexual attention or
advances
* Trolling, insulting/derogatory comments, and personal or political attacks
* Public or private harassment
* Publishing others' private information, such as a physical or electronic
  address, without explicit permission
* Other conduct which could reasonably be considered inappropriate in a
  professional setting

### Our Responsibilities

Project maintainers are responsible for clarifying the standards of acceptable
behavior and are expected to take appropriate and fair corrective action in
response to any instances of unacceptable behavior.

Project maintainers have the right and responsibility to remove, edit, or
reject comments, commits, code, wiki edits, issues, and other contributions
that are not aligned to this Code of Conduct, or to ban temporarily or
permanently any contributor for other behaviors that they deem inappropriate,
threatening, offensive, or harmful.

### Scope

This Code of Conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community. Examples of
representing a project or community include using an official project e-mail
address, posting via an official social media account, or acting as an appointed
representative at an online or offline event. Representation of a project may be
further defined and clarified by project maintainers.

### Enforcement

Instances of abusive, harassing, or otherwise unacceptable behavior may be
reported by contacting the project team at [INSERT EMAIL ADDRESS]. All
complaints will be reviewed and investigated and will result in a response that
is deemed necessary and appropriate to the circumstances. The project team is
obligated to maintain confidentiality with regard to the reporter of an incident.
Further details of specific enforcement policies may be posted separately.

Project maintainers who do not follow or enforce the Code of Conduct in good
faith may face temporary or permanent repercussions as determined by other
members of the project's leadership.

### Attribution

This Code of Conduct is adapted from the [Contributor Covenant][homepage], version 1.4,
available at [http://contributor-covenant.org/version/1/4][version]

[homepage]: http://contributor-covenant.org
[version]: http://contributor-covenant.org/version/1/4/
