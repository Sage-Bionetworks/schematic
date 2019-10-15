# Data Coordinating Center - Dataset Ingress

The Data Coordinating Center (DCC) dataset ingress process consists of three main stages

1. __Dataset transfer__ 
Depending on dataset size, this step may take anywhere from a few minutes up to multiple hours.
2. __Metadata upload__ 
Depending on amount and diversity of dataset files, this step could take from 10 minutes to a couple of hours.
3. __Metadata validation and dataset submission confirmation__ 
This step should take less than 30 seconds on a typical internet connection.

The chart below provides a high-level overview of the steps a dataset contributing center needs to complete in each stage. Software tools steamlining the process are linked and documented, as well as contacts of DCC liaisons that can provide additional information and help facilitate dataset submission.

## Data transfer

#### Selecting storage platform

The DCC can provide dataset storage on the cloud, hosted by __Amazon Web Services (AWS)__ or __Google Cloud (GC)__. 

Each center may decide where to store their datasets depending on existing contracts, dataset location, or other preferences. 

The center may decide to use different storage platform for different datasets.

Once a center determines their dataset storage platform, they need to contact their DCC liaison, who will boot-up the required cloud infrastructure and authorize the center to transfer data into a private storage location. The DCC liaison will provide the required cloud authentication credentials and data storage location.

#### Data upload

To upload data to their DCC-designated storage location, centers may use 
1. standard tools provided by cloud platforms 
2. the Synapse platform tools 

In either case, depending on dataset size and other center preferences, they may utilize web-based or programmatic data upload interfaces. Some of the more typical options are described below, along with links to relevant documentation for more detail.

__AWS data upload__

_AWS web console_: 

This option would typically be useful for upload of files residing on your local machine to AWS S3 storage location. You can follow the steps below to complete a data upload

* Login to AWS here: 
* Navigate to Upload
* Go through prompts and select your target bucket and location










