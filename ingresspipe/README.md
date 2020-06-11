## Setting up virtual environment (for testing)

Python 3 has a built-in support for virtual environments (using `venv` module). Perform the following steps:

_Note: It is assumed that you are running all the below commands from the `ingresspipe` directory._

After cloning the git repository, navigate into the `ingresspipe` directory and run the command as below:

    python3 -m venv .venv

This creates a Python3 virtual environment, with its own site directories (isolated from the system site directories).

To activate the virtual environment, run:

    source .venv/bin/activate

Install all the necessary packages/dependencies (as specified in `requirements.txt` file), by running the following command:

    .venv/bin/pip3 install -r ../requirements.txt

Now, your environment is ready to test the modules within the application.