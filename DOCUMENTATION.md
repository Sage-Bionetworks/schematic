# Data Coordinating Center - Dataset Ingress

The Data Coordinating Center (DCC) dataset ingress process consists of three main stages

1. __Dataset transfer__: depending on dataset size, this step may take anywhere from a few minutes up to multiple hours.

2. __Metadata upload__: depending on amount and diversity of dataset files, this step could take from 10 minutes to a couple of hours.

3. __Metadata validation and dataset submission confirmation__: this step should take less than 30 seconds on a typical internet connection.

The chart below provides a high-level overview of the steps a dataset contributing center needs to complete in each stage. Software tools steamlining the process are linked and documented, as well as contacts of DCC liaisons that can provide additional information and help facilitate dataset submission.

![Dataset ingress flow](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/overall_ingress_flow.png)

## Data transfer

#### Selecting storage platform

The DCC can provide dataset storage on the cloud, hosted by __Amazon Web Services (AWS)__ or __Google Cloud (GC)__. 

Each center may decide where to store their datasets depending on existing contracts, dataset location, or other preferences. 

The center may decide to use different storage platform for different datasets.

Once a center determines their dataset storage platform, they need to contact their DCC liaison, who will boot-up the required cloud infrastructure and authorize the center to transfer data into a private storage location. The DCC liaison will provide the required cloud authentication credentials and data storage location. Centers do not need to follow a particular folder hierarchy in the provided cloud storage location.

#### Data upload

To upload data to their DCC-designated storage location, centers may use 
a. standard tools provided by cloud platforms 
or
b. the Synapse platform tools 

In either case, depending on dataset size and other center preferences, they may utilize web-based or programmatic data upload interfaces. Some of the more typical options are described below, along with links to relevant documentation for more detail.

__AWS data upload__

_AWS web console_: 

This option would typically be useful for upload of files residing on your local machine to a AWS S3 storage location. You can follow the steps below to complete a data upload:

* Login to AWS here: 
<img width="1429" alt="Screen Shot 2019-10-15 at 2 47 22 PM" src="https://user-images.githubusercontent.com/15043209/66873497-71b20580-ef5d-11e9-9891-835890835f36.png">

* Navigate to Upload
<img width="1412" alt="Screen Shot 2019-10-15 at 2 48 14 PM" src="https://user-images.githubusercontent.com/15043209/66873939-9490e980-ef5e-11e9-85ea-8af7e28d1271.png">

* Go through prompts and select your target bucket and location
<img width="1411" alt="Screen Shot 2019-10-15 at 3 17 23 PM" src="https://user-images.githubusercontent.com/15043209/66874075-fb160780-ef5e-11e9-9584-1f5279874570.png">

<img width="730" alt="Screen Shot 2019-10-15 at 2 49 05 PM" src="https://user-images.githubusercontent.com/15043209/66874080-ffdabb80-ef5e-11e9-9f3b-6973ab9cddf3.png"

<img width="655" alt="Screen Shot 2019-10-15 at 2 56 47 PM" src="https://user-images.githubusercontent.com/15043209/66874102-1254f500-ef5f-11e9-8392-1459d92b2cb3.png">

<img width="1406" alt="Screen Shot 2019-10-15 at 2 51 00 PM" src="https://user-images.githubusercontent.com/15043209/66874108-154fe580-ef5f-11e9-9fb5-43db8e8eda53.png">


_AWS client_:

This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

You can modify the Python code vignette below for your particular dataset upload. For equivalent functionality in other programming languages, please refer to the AWS documentation here. 

* Dataset upload from a local folder to a AWS S3 storage location:

```python
if (isAwesome){
  return true
}
```

* Dataset upload from an existing S3 bucket to another AWS S3 storage location:

```python
if (isAwesome){
  return true
}
```

* Dataset upload from an existing GC bucket to another AWS S3 storage location:

```python
if (isAwesome){
  return true
}
```

__Google Cloud data upload__

_GC web console_: 

This option would typically be useful for upload of files residing on your local machine to a Google Cloud Bucket (GCB) storage location. You can follow the steps below to complete a data upload:

* Navigate to the GC storage location provided by your DCC liaison, which would look like:
ht<span>tps://</span>storage.cloud.google.com/hta-x

![GC console project screenshot](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/gc_project_console.png)

* Click on the folder corresponding to your dataset, e.g. hta-x-dataset 
* Drag and drop files; or use the 'Upload files' (or 'Upload folder') buttons. 
* When your files have been uploaded successfully you should see them in your console:

![GC console project screenshot](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/gc_file_upload_complete.png)

_GC client_:

This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

You can modify the Python code vignette below for your particular dataset upload. For equivalent functionality in other programming languages, please refer to the GC documentation here. 

* Dataset upload from a local folder to a GCB storage location:

```python
if (isAwesome){
  return true
}
```

* Dataset upload from an existing GCB to another GCB storage location:

```python
if (isAwesome){
  return true
}
```

__Synapse data upload__

_Synapse web interface_: 

This option would typically be useful for upload of files residing on your local machine to a Synapse storage location. You can follow the steps below to complete a data upload:

* Login to Synapse here: 
* Navigate to your project
* Go through prompts and upload your files

_Synapse client_:

This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

You can modify the Python code vignette below for your particular dataset upload. For equivalent functionality in R or CLI, please refer to the Synapse documentation here. 

* Dataset upload from a local folder to a Synapse storage location:

```python
if (isAwesome){
  return true
}
```

* Dataset upload from an existing GCB to another GCB storage location:

```python
if (isAwesome){
  return true
}
```

* Dataset upload from an existing AWS S3 bucket to a GCB storage location:

```python
if (isAwesome){
  return true
}
```






