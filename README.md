# HTAN Data Ingress Pipeline

## Usage

### Virtual Environment Setup

Python 3 has built-in support for virtual environments (using `venv` module). Perform the following steps:

_Note: It is assumed that you are running all the below commands from the main/root (`HTAN-data-pipeline`) directory._

After cloning the git repository, navigate into the `HTAN-data-pipeline` directory and run the command as below:

    python3 -m venv .venv

This creates a Python3 virtual environment, with its own site directories (isolated from the system site directories).

To activate the virtual environment, run:

    source .venv/bin/activate

Install all the necessary packages/dependencies (as specified in `requirements.txt` file), by running the following command:

    .venv/bin/pip3 install -r requirements.txt

Now, your environment is ready to test the modules within the application.

Once, you have finished testing the application within the virtual environment and want to deactivate it, simply run:

    deactivate

----

### Install Package

To install the package, run the command:

      .venv/bin/pip3 install -e .

----

## Contribution

Clone a copy of the repository here:
      
      git clone --single-branch --branch organized-into-packages https://github.com/sujaypatil96/HTAN-data-pipeline.git

Modify your files, add them to the staging area, use a descriptive commit message and push to the same branch as a pull request for review.

* Please consult [CONTRIBUTION.md](https://github.com/sujaypatil96/HTAN-data-pipeline/blob/organized-into-packages/CONTRIBUTION.md) for further reference.
