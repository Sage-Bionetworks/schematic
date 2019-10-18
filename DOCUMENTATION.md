# Data Coordinating Center - Dataset Ingress

The Data Coordinating Center (DCC) dataset ingress process consists of three main stages

1. [__Dataset transfer to DCC__](#data_transfer): depending on dataset size, this step may take you anywhere from a few minutes up to multiple hours.

2. [__Metadata upload__](#metadata_upload): depending on amount and diversity of dataset files, this step could take you from 10 minutes to a couple of hours.

3. [__Metadata validation and dataset submission confirmation__](#submission_confirmation): this step should take you less than 30 seconds on a typical internet connection.

The chart below provides a high-level overview of the steps a dataset contributing center needs to complete in each stage. Software tools steamlining the process are linked and documented, as well as contacts of DCC liaisons that can provide additional information and help facilitate dataset submission.

![Dataset ingress flow](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/overall_ingress_flow.png)

<a name = "data_transfer"></a>
## Dataset transfer

### Selecting storage platform

The DCC can provide dataset storage on the cloud, hosted by __Amazon Web Services (AWS)__ or __Google Cloud (GC)__. The Synapse platform can be used to store datasets on both clouds, as well.

Your center may decide where to store datasets depending on existing contracts, dataset location, or other preferences. 

Once your center determines their dataset storage platform, you would need to contact your center's DCC liaison, who will boot-up the required cloud infrastructure and authorize you to transfer data into a private storage location. __The DCC liaison will provide the required cloud authentication credentials and data storage location__. Centers do not need to follow a particular folder hierarchy in the provided cloud storage location.

Note that depending on your storage platform choice, you would need to provide AWS, GC, or a Synapse account information to your DCC liaison.

### Dataset upload

To upload data to your DCC-designated storage location, you may use 

1. the Synapse platform tools  
or
2. standard tools provided by cloud platforms

In either case, depending on dataset size and other preferences, you may utilize web-based or programmatic data upload interfaces. Some of the more typical options are described below, along with links to relevant documentation for more detail and the typical usecase for each.

<details><summary><b>Synapse data upload via web interface</b></summary>
<p>
This option would typically be useful for uploading files residing on your local machine to a Synapse cloud storage location. You can follow the steps below to complete a data upload:

  * Navigate to your project, following the Synapse link provided by your DCC liaison; if prompted, please login with your Synapse account (or an associated Google account).

  * <details><summary>Create a folder to store your first dataset.</summary>
  <p>
     * Go to the Files tab 
<img width="1419" alt="Screen Shot 2019-10-15 at 4 03 02 PM" src="https://user-images.githubusercontent.com/15043209/66940461-d7ec6600-eff9-11e9-9825-18b6b1e3f014.png">

     * Create a folder (click on Files Tools -> Add New folder) 
<img width="1420" alt="Screen Shot 2019-10-15 at 4 03 13 PM" src="https://user-images.githubusercontent.com/15043209/66940495-e20e6480-eff9-11e9-8119-0c867b36cc65.png">
  </p></details>


* Go to your folder and upload the files from your dataset (click on Folder tools -> Upload or Link to a File)
<details><summary style="font-size:smaller"><i>How to upload files</i></summary>
 <p>

<img width="1421" alt="Screen Shot 2019-10-15 at 4 03 22 PM" src="https://user-images.githubusercontent.com/15043209/66940511-ea669f80-eff9-11e9-9060-1095ed6682f9.png">

* Once uploaded you can preview your files:
<img width="1422" alt="Screen Shot 2019-10-15 at 4 03 55 PM" src="https://user-images.githubusercontent.com/15043209/66940539-f6eaf800-eff9-11e9-8988-57ad3c0b2ab6.png">
<img width="1436" alt="1" src="https://user-images.githubusercontent.com/15043209/66940841-81335c00-effa-11e9-99d8-9f0a5cf18b8c.png">
</p>
</details>
</p>
</details>

<details><summary><b>Synapse data upload via a programmatic client</b></summary>
<p>
This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

You can modify the Python code vignette below for your particular dataset upload. For equivalent functionality in R or CLI, please refer to the Synapse documentation [here](https://docs.synapse.org/articles/getting_started_clients.html). 

To get started, first install the Synapse Python client:

```
pip install synapseclient
```

* Dataset upload from a local folder to a Synapse storage location:

```python
# the python Synapse client module
import synapseclient

# Synapse will organize your data files in a folder within project
# these are the corresponding Synapse modules
from synapseclient import Project, Folder, File

# Name an dcreate the folder that will store your dataset; 
# you can use a name representative for your particular dataset, e.g. hta-x-dataset
# for the parent parameter, please enter the synapse project ID provided by your DCC liaison
data_folder = Folder('hta-x-dataset', parent='syn123')

# create the folder on Synapse
data_folder = syn.store(data_folder)

# point to files you'd like to upload in your dataset; note that the description field is optional
# the code below would upload two files to your folder
test_entity = File('/path/to/data/file1.txt', description='file 1', parent=data_folder)
test_entity = syn.store(test_entity)

test_entity = File('/path/to/data/file1.txt', description='file 2', parent=data_folder)
test_entity = syn.store(test_entity)
```

<!--
* Dataset upload from an existing S3 location to Synapse:

```python
if (isAwesome){
  return true
}
```
-->
</p>
</details>

<details><summary><b>AWS data upload via web interface</b></summary>
<p>
This option would typically be useful for upload of files residing on your local machine to an AWS S3 storage location. You can follow the steps below to complete a data upload:

* Login to your AWS console (using the AWS account you provided your DCC liaison)

* Navigate to the storage location provided by your DCC liaison (e.g. a bucket named 'hta-x')
<details><summary style="font-size:smaller"><i>How to navigate to a bucket on AWS</i></summary>
 <p>
<img width="1412" alt="Screen Shot 2019-10-15 at 2 48 14 PM" src="https://user-images.githubusercontent.com/15043209/66873939-9490e980-ef5e-11e9-85ea-8af7e28d1271.png">

<img width="1411" alt="Screen Shot 2019-10-15 at 3 17 23 PM" src="https://user-images.githubusercontent.com/15043209/66874075-fb160780-ef5e-11e9-9584-1f5279874570.png">

<img width="730" alt="Screen Shot 2019-10-15 at 2 49 05 PM" src="https://user-images.githubusercontent.com/15043209/66874080-ffdabb80-ef5e-11e9-9f3b-6973ab9cddf3.png">
</p>
</details>

* Create a folder to store your first dataset and upload your files there 
<details><summary style="font-size:smaller"><i>How to upload files to AWS</i></summary>
 <p>
<img width="655" alt="Screen Shot 2019-10-15 at 2 56 47 PM" src="https://user-images.githubusercontent.com/15043209/66874102-1254f500-ef5f-11e9-8392-1459d92b2cb3.png">

<img width="1406" alt="Screen Shot 2019-10-15 at 2 51 00 PM" src="https://user-images.githubusercontent.com/15043209/66874108-154fe580-ef5f-11e9-9fb5-43db8e8eda53.png">
</p>
</details>
</p>
</details>


<details><summary><b>AWS data upload via command line client</b></summary>
<p>
This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

You can modify the CLI code vignette below for your particular dataset upload. For equivalent functionality in other programming languages, please refer to the AWS documentation here. 

* Dataset upload from a local folder to a AWS S3 storage location:
```
CLI code
```

* Dataset upload from an existing S3 bucket to another AWS S3 storage location:
```
CLI code
```
</p>
</details>


<details><summary><b>Google Cloud (GC) data upload via web interface</b></summary>
<p>
This option would typically be useful for upload of files residing on your local machine to a Google Cloud Bucket (GCB) storage location. You can follow the steps below to complete a data upload:

* Navigate to the GC storage location provided by your DCC liaison, which would look like:
ht<span>tps://</span>storage.cloud.google.com/hta-x

![GC console project screenshot](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/gc_project_console.png)

* Click on the folder corresponding to your dataset, e.g. hta-x-dataset 
* Drag and drop files; or use the 'Upload files' (or 'Upload folder') buttons. 
* When your files have been uploaded successfully you should see them in your console:

![GC console project screenshot](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/gc_file_upload_complete.png)
</p>
</details>

<details><summary><b>Google Cloud (GC) data upload via a programmatic client</b></summary>
<p>
This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

To get started with the Python Google Cloud client library, if you have not already, on your command line please run

```
pip install --upgrade google-cloud-storage google-auth oauthlib
```

You can modify the Python code vignettes below for your particular dataset upload. For equivalent functionality in other programming languages, or for more details on installing Python, please refer to the GC documentation [here](https://cloud.google.com/storage/docs/reference/libraries).


* Dataset upload from a local folder to a GCB storage location:

```python

# library that allows interacting with Google CLoud Buckets
from google.cloud import storage

# Explicitly use service account credentials by specifying the private key
# file provided by your DCC liaison
client = storage.Client.from_service_account_json('DCC_hta-x_credentials.json')
        
# specify GC bucket provided by your DCC liaison
bucket = client.get_bucket('hta-x')

# prepare a location for the uploaded file (e.g. the dataset folder in the DCC provided bucket)
blob = bucket.blob('hta-x-dataset/file1.txt')
# upload the file to the bucket by specifying the path to the local file you want to upload
blob.upload_from_filename('./file1.txt')

# note that the GC storage client supports various options (e.g. returniung signed url to uploaded objects; please refer to more detailed documentation here: https://googleapis.dev/python/storage/latest/client.html)
```

* Dataset copy from an existing GCB bucket to a GCB storage location provided by a DCC liaison:

```python
"""Copies a blob from your storage bucket to a DCC bucket."""
client = storage.Client.from_service_account_json('DCC_hta-x_credentials.json')

# specify your (source) bucket
source_bucket = client.get_bucket('your bucket name')
# specify the source file (including path to the source file in your bucket, if the file is in a folder in your bucket)
source_blob = source_bucket.blob('path to file in your bucket')

# specify the DCC provided bucket name
destination_bucket = storage_client.get_bucket('hta-x')

# prepare a location for the copied file on the DCC provided bucket (e.g. the dataset folder in the DCC provided bucket)
new_blob = source_bucket.copy_blob(source_blob, destination_bucket, 'hta-x/hta-x-dataset')

 print('File {} in bucket {} copied to file {} in bucket {}.'.format(
        source_blob.name, source_bucket.name, new_blob.name,
        destination_bucket.name))
```
</p>
</details>

<a name="metadata_upload"></a>
## Metadata upload

### Access the Data Curator by logging onto Synapse and going to this [link](https://www.synapse.org/#!Wiki:syn20681266/ENTITY)
#### A. Starting from a fresh template
* From the first tab select your project (corresponds to your bucket name) and your dataset( corresponds to your folder name).
<img width="1416" alt="2" src="https://user-images.githubusercontent.com/15043209/66961237-0af71f80-f023-11e9-85d3-244b0be1ee01.png">

* Navigate to the second tab "Get Metadata Template"
<img width="1419" alt="3" src="https://user-images.githubusercontent.com/15043209/66961248-10546a00-f023-11e9-8cc0-fd5e4f07dd08.png">

* Click the Link to Google Sheets Template 
<img width="1418" alt="4" src="https://user-images.githubusercontent.com/15043209/66961254-15b1b480-f023-11e9-872b-2e7d6521b898.png">

* When you click the link it will take you to the sheet with the filenames pre-populated.

<img width="1430" alt="5" src="https://user-images.githubusercontent.com/15043209/66961318-41349f00-f023-11e9-9107-466bdab77034.png">

* Fill out the sheet using the dropdowns with the allowed values.
<img width="1434" alt="Screen Shot 2019-10-15 at 4 06 43 PM" src="https://user-images.githubusercontent.com/15043209/66962305-86f26700-f025-11e9-92dc-254a75ef41f9.png">

* Save as a CSV 
<img width="1428" alt="Screen Shot 2019-10-15 at 4 07 06 PM" src="https://user-images.githubusercontent.com/15043209/66962318-8fe33880-f025-11e9-8426-4ce26de5a2c9.png">

* Navigate to the third tab "Submit & Validate Metadata"
<img width="1422" alt="Screen Shot 2019-10-15 at 4 07 36 PM" src="https://user-images.githubusercontent.com/15043209/66962329-95d91980-f025-11e9-9fe4-7c44b0d13d42.png">

* Upload your saved CSV 
<img width="1417" alt="Screen Shot 2019-10-15 at 4 08 00 PM" src="https://user-images.githubusercontent.com/15043209/66962344-9e315480-f025-11e9-9547-9d5ca3d713ca.png">

 * You will see your entries in the Metadata Preview 
<img width="1402" alt="Screen Shot 2019-10-15 at 4 08 14 PM" src="https://user-images.githubusercontent.com/15043209/66962357-a5586280-f025-11e9-8eb8-7acfc48a54ef.png">

* Click "Validate Metadata". If your metadata is valid a "Submit" button will appear.
<img width="1404" alt="Screen Shot 2019-10-15 at 4 08 39 PM" src="https://user-images.githubusercontent.com/15043209/66962370-aab5ad00-f025-11e9-890b-8a2b3209c202.png">

* Click the "Submit" button and if it is successful you will receive a link to your manifest on Synapse. 
<img width="1413" alt="Screen Shot 2019-10-15 at 4 08 50 PM" src="https://user-images.githubusercontent.com/15043209/66962379-b1442480-f025-11e9-9407-34dc6e33952d.png">

* Now your metadata will appear on the in the "Files and Metadata" Table in your Synapse Project. 
<img width="1426" alt="Screen Shot 2019-10-15 at 4 13 12 PM" src="https://user-images.githubusercontent.com/15043209/66963842-98d60900-f029-11e9-83d9-cb81d0842624.png">

#### B. Fixing an unvalidated template
* If you have chosen your project, gotten the template, and filled out the template with an error and uploaded it, e.g. this CSV
<img width="1407" alt="Screen Shot 2019-10-15 at 4 27 04 PM" src="https://user-images.githubusercontent.com/15043209/66964015-29144e00-f02a-11e9-904d-319ac5c11680.png">

* You will receive an error upon pressing the "Validate Metadata" button that highlights the cell and lists the error in detail. 
<img width="1401" alt="Screen Shot 2019-10-15 at 4 28 03 PM" src="https://user-images.githubusercontent.com/15043209/66964059-4ea15780-f02a-11e9-96ad-cf7e236f0012.png">

* You can edit your file on Google Sheet and re-download it as a CSV or edit the CSV locally. 
<img width="1130" alt="Screen Shot 2019-10-15 at 4 28 34 PM" src="https://user-images.githubusercontent.com/15043209/66964181-bbb4ed00-f02a-11e9-95ef-2b8e8c3053fe.png">

* Upload your file and see your metadata reflected.
<img width="1417" alt="Screen Shot 2019-10-15 at 4 28 53 PM" src="https://user-images.githubusercontent.com/15043209/66964212-d38c7100-f02a-11e9-9ce4-68bbac611bfc.png">

* Press the "Validate Metadata" button again. 
<img width="1398" alt="Screen Shot 2019-10-15 at 4 29 02 PM" src="https://user-images.githubusercontent.com/15043209/66964227-e010c980-f02a-11e9-99f1-b7f06c42c3e5.png">

* Now you can submit your validated metadata. 
<img width="1397" alt="Screen Shot 2019-10-15 at 4 29 14 PM" src="https://user-images.githubusercontent.com/15043209/66964257-f1f26c80-f02a-11e9-90d7-18f9459dab85.png">

<a name="submission_confirmation"></a>
## Metadata and dataset submission confirmation

You can verify that both your dataset and metadata have been successfully submitted to the DCC by navigating to the Synapse project containing you dataset (the link to the project was provided by your DCC liaison in stage 1; the link is also generated by the DataCurator app above, when your metadata submission is successful). 

If your dataset has been successfully submitted, under the Table tab of your project, there would be a table named 'hta-x-dataset', containing the list of files in your dataset and their metadata. 
