# `schematic` Data Ingress Pipeline

## Usage

### Data Curator App Setup (backend)

_Note: It is assumed that you are running all the below commands from the main/root (HTAN-data-pipeline) directory._

Clone this repository. 

      git clone https://github.com/nf-osi/schematic

If you haven't already, create a conda environment in the cloned directory from the `environment.yml` file which has all the required package dependencies. The conda environment name `data_curator_env` is already defined in the `environment.yml` file.:

    conda env create -f environment.yml

Activate the `data_curator_env` environment:

    conda activate data_curator_env
 
Install the package/bundle/application:

      pip install -e .

To verify that the package has been installed (as a `pip` package), check here:

      pip list

Now, your environment is ready to test the modules within the application.

Once, you have finished testing the application within the virtual environment and want to deactivate it, simply run:

      conda deactivate

### Configure Synapse Credentials

Download a copy of the `credentials.json` file (or the file needed for authentication using service account, called `quickstart-1560359685924-198a7114b6b5.json`) stored on Synapse, using the synapse client command line utility. The credentials file is necessary for authentication to use Google services/APIs. To do so:

_Note: Make sure you have `download` access/permissions to the above files before running the below commands._

For `credentials.json` file:

      synapse get syn21088684

For `quickstart-1560359685924-198a7114b6b5.json` file:

      synapse get syn22316486

Find the synapse configuration file (_`.synapseConfig`_) downloaded to the current source directory. Access it like this:

      vi .synapseConfig

Open the config file, and under the authentication section, replace _< username >_ and _< apikey >_ with your Synapse username and API key.

_Note: You can get your Synapse API key by: **logging into Synapse > Settings > Synapse API Key > Show API Key**_.

----

### Contribution

Clone a copy of the core `schematic` repository here:
 
       git clone --single-branch --branch develop https://github.com/Sage-Bionetworks/schematic.git

Modify your files, add them to the staging area, use a descriptive commit message and push to the same branch as a pull request for review.

* Please consult [CONTRIBUTION.md](https://github.com/Sage-Bionetworks/schematic/blob/develop/CONTRIBUTION.md) for further reference.
