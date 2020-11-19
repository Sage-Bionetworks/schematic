# Schematic

## Usage

### Virtual Environment Setup

Python 3 has built-in support for virtual environments (using `venv` module). Perform the following steps:

_Note: It is assumed that you are running all the below commands from the main/root (`schematic`) directory._

Clone this branch of the git repository 

```
git clone --single-branch --branch main https://github.com/Sage-Bionetworks/schematic.git
```

Navigate into the `schematic` directory and run the command as below:

```bash
python[3] -m venv .venv
```

This creates a Python3 virtual environment (within the `root` folder/package), with its own site directories (isolated from the system site directories).

To activate the virtual environment, run:

```bash
source .venv/bin/activate
```

_Note: You should now see the name of the virtual environment to the left of the prompt._

### Install App/Package

To install the package/bundle/application:

```bash
pip[3] install -e .
```

To verify that the package has been installed (as a `pip` package), check here:

```bash
pip[3] list
```

Now, your environment is ready to test the modules within the application.

Once, you have finished testing the application within the virtual environment and want to deactivate it, simply run:

```bash
deactivate
```

To run any of the example file(s), go to your root directory and execute/run python script in the following way:

Let's say you want to run the `metadata_usage` example - then do this:

```bash
python[3] schematic/models/examples/metadata_usage.py
```

### Configure Synapse Credentials

Download a copy of the `credentials.json` file (or the file needed for authentication using service account, called `quickstart-1560359685924-198a7114b6b5.json`) stored on Synapse, using the synapse client command line utility. The credentials file is necessary for authentication to use Google services/APIs. To do so:


_Note: Make sure you have `download` access/permissions to the above files before running the below commands._

For `credentials.json` file:
```bash
synapse get syn21088684
```

For `quickstart-1560359685924-198a7114b6b5.json` file:
```bash
synapse get syn22316486
```

Find the synapse configuration file (_`.synapseConfig`_) downloaded to the current source directory. Access it like this:

```bash
vi[m] .synapseConfig
```

Open the config file, and under the authentication section, replace _< username >_ and _< apikey >_ with your Synapse username and API key.

_Note: You can get your Synapse API key by: **logging into Synapse > Settings > Synapse API Key > Show API Key**_.

----

### Contribution

Clone a copy of the repository here:
      
```bash
git clone --single-branch --branch develop https://github.com/Sage-Bionetworks/schematic.git
```

Modify your files, add them to the staging area, use a descriptive commit message and push to the same branch as a pull request for review.

* Please consult [CONTRIBUTION.md](https://github.com/Sage-Bionetworks/schematic/blob/develop/CONTRIBUTION.md) for further reference.
