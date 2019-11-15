# Data Coordinating Center - Dataset Ingress

The Data Coordinating Center (DCC) dataset ingress process consists of three main stages

1. [__Dataset transfer to DCC__](#data_transfer): *Transfer your experimental data files to a cloud storage bucket.* Depending on dataset size, this step may take you anywhere from a few minutes up to multiple hours.

2. [__Metadata upload__](#metadata_upload): *Upload a spreadsheet of your metadata annotations for your data files.* Depending on the number and diversity of dataset files, this step could take you from 10 minutes to a couple of hours.

3. [__Metadata validation and dataset submission confirmation__](#submission_confirmation): *Verify that your metadata meets requirements.* This step should take you less than 30 seconds on a typical internet connection, and completes your submission to the DCC.

A dataset is a set of experimental data files derived from a single type of experimental platform, such as single-cell RNA sequencing. The chart below provides a high-level overview of the steps an HTAN Center needs to complete in each stage. Software tools steamlining the process are linked and documented, as well as contacts of DCC liaisons that can provide additional information and help facilitate data submission.

![Dataset ingress flow](https://user-images.githubusercontent.com/41303818/68252636-e1566600-ffda-11e9-8ab9-28b58e1be628.png)

<a name = "data_transfer"></a>
## Dataset transfer to DCC

### Selecting storage platform

The DCC provides dataset storage on the cloud, hosted by __Amazon Web Services (AWS)__ or __Google Cloud (GC)__. 
Your center may decide where to store datasets depending on existing contracts, dataset location, or other preferences. 

The DCC supports dataset transfers to both clouds via the Synapse platform. Please create a Synapse account <a href="https://www.synapse.org/" target = "_blank">here</a>, if you do not already have an existing account. 

Next, please provide your Synapse username and indicate your cloud platform preference to your DCC liaison. You can indicate a cloud platform of your choice, however the DCC recommends the following options:

- if your center's data is already stored on premises/local machines, select AWS as your storage option

- if your center's data is already stored on AWS, select AWS as your storage option and also provide your AWS storage region

- if your center's data is already stored on GC, select GC as your storage option

__Once you determine your dataset storage platform and provide Synapse username(s), your DCC liaison will boot-up the required cloud infrastructure and authorize you to transfer data into a private storage location.__

### Dataset upload

Centers do not need to follow a particular folder hierarchy in the provided cloud storage location.

To upload data to your DCC-designated storage location, please use the Synapse platform tools.

Depending on dataset size and other preferences, you may utilize web-based or programmatic data upload interfaces. Some of the more typical options are described below, along with links to relevant documentation for more detail and the typical usecase for each.

<details><summary><b>Synapse data upload via web interface</b></summary>
<blockquote>
This option would typically be useful for uploading files residing on your local machine to a Synapse cloud storage location. You can follow the steps below to complete a data upload:

  <details><summary>Navigate to your project, following the Synapse link provided by your DCC liaison</summary> 
  <blockquote> If prompted, please login with your Synapse account (or an associated Google account).

  ![synapseLogin](https://user-images.githubusercontent.com/15043209/67429375-e0faab80-f594-11e9-9353-1ab2fcd4976e.png)
  </blockqoute>
  </details>

  <details><summary>Create a folder to store your first dataset.</summary>
  <blockquote>
    
  - Go to the Files tab 
     
![htax FilesTab](https://user-images.githubusercontent.com/15043209/67429855-df7db300-f595-11e9-88b4-dab59c3ec95a.png)
    
   - Create a folder (click on Files Tools -> Add New folder) 
    
![htax CreateFolder](https://user-images.githubusercontent.com/15043209/67429986-253a7b80-f596-11e9-8e71-52c6b946f676.png)
  </blockquote>
  </details>

  <details><summary>Go to your folder and upload the files from your dataset (click on Folder tools -> Upload or Link to a File)</summary>
  <blockquote>

![htax Folder](https://user-images.githubusercontent.com/15043209/67430041-4733fe00-f596-11e9-90fc-cd88cdef51c1.png)

![htax FileUpload](https://user-images.githubusercontent.com/15043209/67430069-531fc000-f596-11e9-811b-5c48ef1a8ded.png)

   * Once uploaded you can preview your files:

![htaxFilesUploaded](https://user-images.githubusercontent.com/15043209/67430086-5c109180-f596-11e9-9c58-dc7f36c5f134.png)
  </blockquote>
  </details>
</blockquote>
</details>

<details><summary><b>Synapse data upload via a programmatic client</b></summary>
<blockquote>
This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

You can modify the Python code vignette below for your particular dataset upload. For equivalent functionality in R or CLI, please refer to the Synapse documentation [here](https://docs.synapse.org/articles/getting_started_clients.html). 

To get started, first install the Synapse Python client:

```
pip install synapseclient
```

- To upload a dataset from a local folder to a Synapse storage location, you can modify the script below

```python
# the python Synapse client module
import synapseclient

# Synapse will organize your data files in a folder within project
# these are the corresponding Synapse modules
from synapseclient import Project, Folder, File

# Log in to synapse
syn = synapseclient.Synapse()

syn.login('my_username', 'my_password')

# Name and create the folder that will store your dataset; 
# you can use a name representative for your particular dataset, e.g. hta-x-dataset
# for the parent parameter, please enter the synapse project ID provided by your DCC liaison
data_folder = Folder('hta-x-dataset', parent='syn123')

# create the folder on Synapse
data_folder = syn.store(data_folder)

# point to files you'd like to upload in your dataset; note that the description field is optional
# the code below would upload two files to your folder, feel free to create a loop for more files
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
</blockquote>
</details>


<!--
<details><summary><b>AWS data upload via web interface</b></summary>
<blockquote>
This option would typically be useful for upload of files residing on your local machine to an AWS S3 storage location. You can follow the steps below to complete a data upload.

<details><summary>Login to your AWS console</summary> 
  <blockquote>Please login using the AWS account you have provided to your DCC liaison, in order to access the DCC AWS bucket.   </blockquote>
</details>

<details><summary>Navigate to the storage-bucket location provided by your DCC liaison</summary>
 <blockquote>
   
 * To find a bucket named 'hta-x' you can click on Services -> S3
 ![awsS3 Search](https://user-images.githubusercontent.com/15043209/67430745-c249e400-f597-11e9-876f-fc470b887aeb.png)
 
 * Locate the hta-x bucket in the list and click on it to various the bucket management options
  ![AWS S3 bucket](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/aws_bucket_view.png)
  
</blockquote>
</details>

<details><summary>Create a folder to store your first dataset and upload your files there</summary>
 <blockquote>

* Click on 'Create folder'; name your folder to reflect the dataset name you'd like (e.g. hta-x-dataset); you can proceed with the default bucket settings for the folder
![AWS S3 dataset](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/aws_create_dataset.png)

* Click on the folder that you have created; click 'Upload'; you can drag and drop or browse to the files you'd like to upload
![AWS S3 upload](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/aws_dataset_upload.png)

</blockquote>
</details>

</blockquote>
</details>


<details><summary><b>AWS data upload via command line client</b></summary>
<blockquote>
This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

You can modify the CLI code vignette below for your particular dataset upload. To get started, see this link: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html.  For equivalent functionality in Python, please refer to the AWS documentation [here](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html). 

- To upload a dataset from a local folder to a S3 storage location, you can modify the script below
```
aws s3 cp /hta-x/hta-x-dataset s3://hta-x/hta-x-dataset
```

- To copy a dataset from an existing S3 bucket to another AWS S3 storage location, you can modify the script below
```
aws s3 sync s3://SOURCE_BUCKET_NAME s3://hta-x/hta-x-dataset
```
</blockquote>
</details>


<details><summary><b>Google Cloud (GC) data upload via web interface</b></summary>
<blockquote>
This option would typically be useful for upload of files residing on your local machine to a Google Cloud storage location.

The contents of the bucket are usually best viewed using a URL that will be provided, which for the bucket `hta-x` would be ht<span>tps://</span>storage.cloud.google.com/hta-x

You can follow the steps below to complete a data upload:

* Enter the provided bucket URL into your browser

* The `hta-x` bucket will initially be empty, but you can use the 'Create folder' button to add a folder for a particular dataset, such as `hta-x-dataset`

![GC console project screenshot](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/gc_project_console.png)

* Click on the folder corresponding to your dataset, e.g. `hta-x-dataset` 
* Drag and drop files; or use the 'Upload files' (or 'Upload folder') buttons. 
* When your files have been uploaded successfully you should see them in your console:
![GC console project screenshot](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/gc_file_upload_complete.png)
</blockquote>
</details>

<details><summary><b>Google Cloud (GC) data upload via a command line client</b></summary>
<blockquote>
This option would typically be most suitable for upload of files residing on a cloud or your local machine; and in case of uploading large-number and/or large-size files.

Google Cloud provides the command line tool gsutil, with [extended documentation](https://cloud.google.com/storage/docs/gsutil) and a [Quickstart](https://cloud.google.com/storage/docs/quickstart-gsutil).  The bucket `hta-x` is referenced as `gs://hta-x`. 

* Dataset upload from a local folder to a GC storage location (the `hta-x-dataset` folder in the bucket `hta-x`)

```
gsutil cp file1.txt gs://hta-x/hta-x-dataset
```
 
* Dataset upload from an existing GC storage bucket to another GC bucket is similar, but with both locations referenced using `gs://` 

```
gsutil cp gs://SOURCE_BUCKET_NAME gs://hta-x/hta-x-dataset"
```

( Note that the -m option will multithread the copy and speed things up, and an -r will copy an entire directory tree, e.g.
`gsutil -m cp -r dir gs://hta-x/hta-x-dataset`. See [cp](https://cloud.google.com/storage/docs/gsutil/commands/cp) documentation.)


</blockquote>
</details>

<details><summary><b>Google Cloud (GC) data upload via a programmatic client</b></summary>

To get started with the Python Google Cloud client library, if you have not already, on your command line please run

```
pip install --upgrade google-cloud-storage google-auth oauthlib
```

You can modify the Python code vignettes below for your particular dataset upload. For equivalent functionality in other programming languages, or for more details on installing Python, please refer to the GC documentation [here](https://cloud.google.com/storage/docs/reference/libraries).


- To upload a dataset from a local folder to a GC storage location provided by a DCC liaison, you can modify the script below

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

- To copy a dataset from an existing GC storage bucket to a GC storage location provided by a DCC liaison, you can modify the script below

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
</blockquote>
</details>
-->
</hr>

<a name="metadata_upload"></a>
## Metadata upload

<details><summary>At present, the DCC supports a web-based metadata upload via the Data Curator web app in Synapse.</summary> 
 <blockquote>
  
   We are working on providing 
   
   1. a Python package for programmatic metadata upload and management; 
   and 
   2. an API for programmatic metadata upload and management. 
   
   These will be available in the next release of the DCC data pipeline. Please check with your DCC liaison on details.
 
 </blockquote>
</details>

<details><summary><b>Use the <a href = "https://www.synapse.org/#!Wiki:syn20681266/ENTITY">Data Curator app</a> to curate a dataset for a first time</b></summary>
  <blockquote>
    
   You have already transfered your dataset to the DCC - congratulations! If you have not, please follow the instructions [here](#data_transfer). 
    
   Please provide the metadata for your dataset using the Data Curator app. Here we assume your dataset is named `hta-x-dataset`.
    
  <details><summary>Access the <a href = "https://www.synapse.org/#!Wiki:syn20681266/ENTITY">Data Curator app</a></summary>
  <blockquote>
  
  If you are prompted to login to Synapse, please use your Synapse account (or associated Google account).
  
  </blockquote>
  </details>
  
  <details><summary>In the app, from the first tab, select your project. The project name corresponds to the bucket name (here `hta-x`).  Then select your dataset, which corresponds to the folder name in your bucket (`hta-x-dataset`).
Then select the metadata template you would like to use (e.g. scRNASeq if providing metadata for a scRNASeq dataset). If you don't see the correct template for your dataset, you can select the "Minimal Metadata" template and <i>contact your DCC liaison</i>.</summary>
  <blockquote>
    
![DataCurator project selection](https://user-images.githubusercontent.com/41303818/68252750-29758880-ffdb-11e9-80a9-dd1efa2174d4.png)   
   
  </blockquote>
  </summary>
</details>
  

<details><summary>Once you have selected your dataset and metadata template, navigate to the second tab "Get Metadata Template" and click on "Link to Google Sheets Template". This will generate a link to a Google spreadsheet containing an empty template for you to complete with metadata, for each of the files in your dataset. </summary>
  <blockquote>

![dataCurator MetadataTab](https://user-images.githubusercontent.com/15043209/66961248-10546a00-f023-11e9-8cc0-fd5e4f07dd08.png)
 
![dataCurator LinktoTemp](https://user-images.githubusercontent.com/15043209/66961254-15b1b480-f023-11e9-872b-2e7d6521b898.png)
 
 </blockquote>
  </details>

<details><summary>You can fill out the sheet on the web, using dropdowns with allowed values and other standard Google Sheet features.</summary>
  <blockquote>

![gtemplate Empty](https://user-images.githubusercontent.com/15043209/66961318-41349f00-f023-11e9-9107-466bdab77034.png)
 
![gtemplate Filled](https://user-images.githubusercontent.com/15043209/66962305-86f26700-f025-11e9-92dc-254a75ef41f9.png)

Note that you can also save the spreadsheet as a CSV file and use a method of your choice to fill it out. The metadata CSV will be validated by the Data Curator app before submission in any case.
  
 </blockquote>
 </details>
  

<details><summary>Once filled in, you can save your spreadsheet as a CSV (File -> Download -> Comma-separated Value...)</summary>
  <blockquote>
    
![gtemplateDLCSV](https://user-images.githubusercontent.com/15043209/66962318-8fe33880-f025-11e9-8426-4ce26de5a2c9.png)

  </blockquote>
</details>

<details><summary>Next: navigate to the third tab "Submit & Validate Metadata"</summary>
  <blockquote>

![dataCurator SubmitTab](https://user-images.githubusercontent.com/15043209/66962329-95d91980-f025-11e9-9fe4-7c44b0d13d42.png)

  </blockquote>
</details>

<details><summary>Upload your saved CSV.</summary>
  <blockquote>

![dataCurator UploadCSV](https://user-images.githubusercontent.com/15043209/66962344-9e315480-f025-11e9-9547-9d5ca3d713ca.png)


 * If upload was successful, you will see your  metadata entries in the Metadata Preview 

![dataCurator MetadataPrev](https://user-images.githubusercontent.com/15043209/66962357-a5586280-f025-11e9-8eb8-7acfc48a54ef.png)

  </blockquopte>
</details>

<details><summary>Click "Validate Metadata"</summary>
 <blockquote>
   
 * If your metadata is valid, you will see a corresponding message and a "Submit" button will become available.
 
![dataCurator ValidateSuccess](https://user-images.githubusercontent.com/15043209/66962370-aab5ad00-f025-11e9-890b-8a2b3209c202.png)

* Clicking the "Submit" button confirms that this dataset has been curated according to the relevant DCC  data model. You will receive a link to your metadata in the Synapse system.

![dataCurator SubmitSuccess](https://user-images.githubusercontent.com/15043209/66962379-b1442480-f025-11e9-9407-34dc6e33952d.png)

</blockquote>
</details>


<details><summary> <span style="color:green">If your metadata has been validated and submitted successfully, your metadata will appear in the "Files and Metadata" Table in your Synapse Project.</span></summary>
  <blockquote>

![Fileview NewAnno](https://user-images.githubusercontent.com/15043209/66963842-98d60900-f029-11e9-83d9-cb81d0842624.png)

  </blockquote>
</details>


<details><summary><span style="color:red"> If you receive an error upon pressing the "Validate Metadata" button, the metadata template-cells causing the error will be highlighted, along with a corresponding list of error details</span></summary>
  <blockquote>
  
![dataCurator ValidateError](https://user-images.githubusercontent.com/15043209/66964059-4ea15780-f02a-11e9-96ad-cf7e236f0012.png)

* You can edit your file in a Google spreadsheet (click the link following the errors) and re-download it as a CSV or edit your CSV locally, as shown here on Excel.

![excel TemplateFixed](https://user-images.githubusercontent.com/15043209/66964181-bbb4ed00-f02a-11e9-95ef-2b8e8c3053fe.png)

* Upload your file and see your metadata updates reflected

![dataCurator UploadFixedFile](https://user-images.githubusercontent.com/15043209/66964212-d38c7100-f02a-11e9-9ce4-68bbac611bfc.png)

* Press the "Validate Metadata" button again

![dataCurator ValidateFixedFile](https://user-images.githubusercontent.com/15043209/66964227-e010c980-f02a-11e9-99f1-b7f06c42c3e5.png)

* If all errors have been resolved, you can submit your validated metadata

![dataCurator SubmitFixedFile](https://user-images.githubusercontent.com/15043209/66964257-f1f26c80-f02a-11e9-90d7-18f9459dab85.png)

* Please contact your DCC liaison if you cannot resolve a metadata error; or have questions regarding metadata submission.

  </blockquote>
</details>

  </blockquote>
</details>

<details><summary><b>Use the <a href = "https://www.synapse.org/#!Wiki:syn20681266/ENTITY">Data Curator app</a> to update existing metadata</b></summary>
  <blockquote>
    
   You have already transfered your dataset to the DCC, and have provided metadata successfully - congratulations! 
     
   Now you'd like to update your metadata in order to 
   
   * correct mistake(s) 
   * provide further/change metadata to comply with a new iteration of the DCC data model affecting your datasets' metadata
   * provide metadata for files that have been added to your dataset
       
  <details><summary>Access the <a href = "https://www.synapse.org/#!Wiki:syn20681266/ENTITY">Data Curator app</a></summary>
  <blockquote>
  
  If you are prompted to login to Synapse, please use your Synapse account (or associated Google account).
  
  </blockquote>
  </details>
  
  <details><summary>In the app, from the first tab, select your project (e.g. hta-x, corresponds to your bucket name if you have uploaded your dataset directly to a AWS or GC bucket); your dataset (e.g. hta-x-dataset, corresponds to a folder name in your bucket); and the metadata template you would like to use (e.g. scRNASeq if providing metadata for a scRNASeq dataset); if you don't see the correct template for your dataset, you can select the "Minimal Metadata" template and <i>contact your DCC liaison</i>.</summary>
  <blockquote>
    
 ![DataCurator project selection](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/data_curator_project_selection.png)   
   
  </blockquote>
  </summary>
</details>
  

<details><summary>Once you have selected your dataset and metadata template, navigate to the second tab "Get Metadata Template" and under "Have Previously Submitted Metadata?" click on 'Link to Google Sheets'. This will generate a link to a Google spreadsheet containing the metadata available for each of the files in your dataset.</summary>
  <blockquote>

![dataCurator MetadataTab](https://user-images.githubusercontent.com/15043209/66961248-10546a00-f023-11e9-8cc0-fd5e4f07dd08.png)
 
 ![Data Curator metadata update google sheets link](https://github.com/Sage-Bionetworks/HTAN-data-pipeline/blob/dev/doc/img/data_curator_metadata_update.png)
 
 </blockquote>
  </details>

<details><summary>You can fill out the sheet on the web, using dropdowns with allowed values and other standard Google Sheet features.</summary>
  <blockquote>
 
![gtemplate Filled](https://user-images.githubusercontent.com/15043209/66962305-86f26700-f025-11e9-92dc-254a75ef41f9.png)

Note that you can also save the spreadsheet as a CSV file and use a method of your choice to fill it out. The metadata CSV will be validated by the Data Curator app before submission in any case.
  
 </blockquote>
 </details>
  

<details><summary>Once updated, you can save your spreadsheet as a CSV (File -> Download -> Comma-separated Value...)</summary>
  <blockquote>
    
![gtemplate dlCSV](https://user-images.githubusercontent.com/15043209/66962318-8fe33880-f025-11e9-8426-4ce26de5a2c9.png)

  </blockquote>
</details>

<details><summary>Next: navigate to the third tab "Submit & Validate Metadata"</summary>
  <blockquote>

![dataCurator SubmitTab](https://user-images.githubusercontent.com/15043209/66962329-95d91980-f025-11e9-9fe4-7c44b0d13d42.png)

  </blockquote>
</details>

<details><summary>Upload your saved CSV.</summary>
  <blockquote>

![dataCurator UploadCSV](https://user-images.githubusercontent.com/15043209/66962344-9e315480-f025-11e9-9547-9d5ca3d713ca.png)


 * If upload was successful, you will see your  metadata entries in the Metadata Preview 

![dataCurator MetadataPreview](https://user-images.githubusercontent.com/15043209/66962357-a5586280-f025-11e9-8eb8-7acfc48a54ef.png)

  </blockquopte>
</details>

<details><summary>Click "Validate Metadata"</summary>
 <blockquote>
   
 * If your metadata is valid, you will see a corresponding message and a "Submit" button will become available.
 
![dataCurator ValidateSuccess](https://user-images.githubusercontent.com/15043209/66962370-aab5ad00-f025-11e9-890b-8a2b3209c202.png)

* Clicking the "Submit" button confirms that this dataset has been curated according to the latest DCC  data model. You will receive a link to your metadata in the Synapse system.

![dataCurator SubmitSuccess](https://user-images.githubusercontent.com/15043209/66962379-b1442480-f025-11e9-9407-34dc6e33952d.png)

</blockquote>
</details>


<details><summary> <span style="color:green">If your metadata has been validated and submitted successfully, your metadata will appear in the "Files and Metadata" Table in your Synapse Project.</span></summary>
  <blockquote>

![Filewview NewAnno](https://user-images.githubusercontent.com/15043209/66963842-98d60900-f029-11e9-83d9-cb81d0842624.png)

  </blockquote>
</details>


<details><summary><span style="color:red"> If you receive an error upon pressing the "Validate Metadata" button, the metadata template-cells causing the error will be highlighted, along with a corresponding list of error details</span></summary>
  <blockquote>
  
![dataCurator ValidateError](https://user-images.githubusercontent.com/15043209/66964059-4ea15780-f02a-11e9-96ad-cf7e236f0012.png)

* You can edit your file in a Google spreadsheet (click the link following the errors) and re-download it as a CSV or edit your CSV locally, as shown here on Excel.

![excel TemplateFixed](https://user-images.githubusercontent.com/15043209/66964181-bbb4ed00-f02a-11e9-95ef-2b8e8c3053fe.png)

* Upload your file and see your metadata updates reflected

![dataCurator UploadFixedFile](https://user-images.githubusercontent.com/15043209/66964212-d38c7100-f02a-11e9-9ce4-68bbac611bfc.png)

* Press the "Validate Metadata" button again

![dataCurator ValidateFixedFile](https://user-images.githubusercontent.com/15043209/66964227-e010c980-f02a-11e9-99f1-b7f06c42c3e5.png)

* If all errors have been resolved, you can submit your validated metadata

![dataCurator SubmitFixedFile](https://user-images.githubusercontent.com/15043209/66964257-f1f26c80-f02a-11e9-90d7-18f9459dab85.png)

* Please contact your DCC liaison if you cannot resolve a metadata error; or have questions regarding metadata updates and submission.

  </blockquote>
</details>
  
  </blockquote>
</details>
  
<a name="submission_confirmation"></a>
## Metadata and dataset submission confirmation

You can verify that both your dataset and metadata have been successfully submitted to the DCC by navigating to the Synapse project containing you dataset. The link to the project was provided by your DCC liaison in stage 1; the link is also generated by the DataCurator app above, in stage 2, if your metadata submission is successful. 

If your dataset has been successfully submitted, under the Table tab of your project, there would be a table named 'hta-x-dataset', containing the list of files in your dataset and their metadata. 
