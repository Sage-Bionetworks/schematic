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

1. Follow the package setup instructions on the main README till the "[Clone Project Repository](https://github.com/Sage-Bionetworks/schematic/tree/develop#131-clone-project-repository)" step.
2. If you are a `schematic` package contributor you don't need to create a `venv` virtual environment, `poetry` will create a virtual environment by default, which you can use.
3. Running the following command reads the [`pyproject.toml`](https://github.com/Sage-Bionetworks/schematic/blob/develop/pyproject.toml) file from the current project, resolves the dependencies and installs them: `poetry install`
4. Obtain credentials file(s) by following same instructions as in the [`1.3.4. Obtain Credentials File(s)`](README.md#134-obtain-credentials-files) section.
5. Fill in configuration file(s) in the same way as specified in the [`1.3.5. Fill in Configuration File(s)`](README.md#135-fill-in-configuration-files).
6. To run any of the CLI utilities shown in the [`1.3.6. Command Line Interface`](README.md#136-command-line-interface), prefix the commands with `poetry run`.

## Pull Request Process

1. Ensure any install or build dependencies are removed before the end of the layer when doing a 
   build.
2. If needed, update the README.md with details of changes to the interface, this includes new environment 
   variables, exposed ports, useful file locations and container parameters.
3. You may merge the Pull Request in once you have the sign-off of at least one other developer, or if you 
   do not have permission to do that, you may request the second reviewer to merge it for you.
4. When merging into dev into master follow release procedures (TODO: releas process to be determined)

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
