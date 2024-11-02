Setting up your asset store
===========================


This document covers the minimal recommended elements needed in Synapse to interface with the Data Curator App (DCA) and provides options for Synapse project layout.

There are two options for setting up a DCC Synapse project:

1. Each team of DCC contributors has its own Synapse project that stores the team's datasets.
2. All DCC datasets are stored in the same Synapse project.

Option 1: Distributed Synapse Projects
--------------------------------------

Pick **option 1** if you answer "yes" to one or more of the following questions:

- Does the DCC have multiple contributing institutions/labs, each with different data governance and access controls?
- Does the DCC have multiple institutions with limited cross-institutional sharing?
- Will contributors submit more than 100 datasets per release or per month?
- Are you not willing to annotate each DCC dataset folder with the annotation `contentType:dataset`?

Access & Project Setup - Multiple Contributing Projects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a DCC Admin Team with admin permissions.
2. Create a Team for each data contributing institution. Begin with a "Test Team" if all teams are not yet identified.
3. Create a Synapse Project for each institution and grant the respective team **Edit** level access.
   - E.g., for institutions A, B, and C, create Projects A, B, and C with Teams A, B, and C. Team A has **Edit** access to Project A, etc.
4. Within each project, create top-level dataset folders in the **Files** tab for each dataset type.
5. Create another Synapse Project (e.g., MyDCC) containing the main **Fileview** that includes in the scope all the DCC projects.
   - Ensure all teams have **Download** level access to this file view.
   - Include both file and folder entities and add ALL default columns.


Option 2: Single Synapse Project
--------------------------------

Pick **option 2** if you don't select option 1 and you answer "yes" to any of these questions:

- Does the DCC have a project with pre-existing datasets in a complex folder hierarchy?
- Does the DCC envision collaboration on the same dataset collection across multiple teams with shared access controls?
- Are you willing to set up local access control for each dataset folder and annotate each with `contentType:dataset`?

If neither option fits, select option 1.


Access & Project Setup - Single Contributing Project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a Team for each data contributing institution.
2. Create a single Synapse Project (e.g., MyDCC).
3. Within this project, create dataset folders for each contributor. Organize them as needed.
   - Use `contentType:dataset` for each dataset folder, which should not nest inside other dataset folders and must have unique names.
4. In MyDCC, create the main **DCC Fileview** with `MyDCC` as scope. Add column `contentType` to the schema and grant teams **Download** level access.
   - Add both file and folder entities and add ALL default columns.


Synapse External Cloud Buckets Setup
------------------------------------

If DCC contributors require external cloud buckets, select one of the following configurations.  For more information on how to
set this up on Synapse, view this documentation: https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html

1. **Basic External Storage Bucket (Default)**:
   - Create an S3 bucket for Synapse uploads via web or CLI. Contributors will upload data without needing AWS credentials.
   - Provision an S3 bucket, attach it to the Synapse project, and create folders for specific assay types.

2. **Custom Storage Location**:

This is an advanced setup for users that do not want to upload files directly via the Synapse API, but rather
create pointers to the data.

   - For large datasets or if contributors prefer cloud storage, enable uploads via AWS CLI or GCP CLI.
   - Configure the custom storage location with an AWS Lambda or Google Cloud function for syncing.
   - If using AWS, provision a bucket, set up Lambda sync, and assign IAM write access.
   - For GCP, use Google Cloud function sync and obtain contributor emails for access.

Finally, set up a `synapse-service-lambda` account for syncing external cloud buckets with Synapse, granting "Edit & Delete" permissions on the contributor's project.
