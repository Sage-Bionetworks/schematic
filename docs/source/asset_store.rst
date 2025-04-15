Setting up your asset store
===========================

.. note::

   You can ignore this section if you are just trying to contribute manifests.

This document covers the minimal recommended elements needed in Synapse to interface with the Data Curator App (DCA) and provides options for Synapse project layout.

There are two options for setting up a DCC Synapse project:

1. **Distributed Projects**: Each team of DCC contributors has its own Synapse project that stores the team's datasets.
2. **Single Project**: All DCC datasets are stored in the same Synapse project.

In each of these project setups, there are two ways you can lay out your data:

1. **Flat Data Layout**: All top level folders structured under the project

   .. code-block:: shell

      my_flat_project
      ├── biospecimen
      └── clinical

2. **Hierarchical Data Layout**: Top level folders are stored within nested folders annotated with ``contentType: dataset``

   .. note::

      This requires you to add the column ``contentType`` to your fileview schema.

   .. code-block:: shell

      my_heirarchical_project
      ├── biospecimen
      │   ├── experiment_1 <- annotated
      │   └── experiment_2 <- annotated
      └── clinical
         ├── batch_1 <- annotated
         └── batch_2 <- annotated


Option 1: Distributed Synapse Projects
--------------------------------------

Pick **option 1** if you answer "yes" to one or more of the following questions:

- Does the DCC have multiple contributing institutions/labs, each with different data governance and access controls?
- Does the DCC have multiple institutions with limited cross-institutional sharing?
- Will contributors submit more than 100 datasets per release or per month?
- Are you not willing to annotate each DCC dataset folder with the annotation ``contentType:dataset``?

Access & Project Setup - Multiple Contributing Projects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a DCC Admin Team with admin permissions.
2. Create a Team for each data contributing institution. Begin with a "Test Team" if all teams are not yet identified.
3. Create a Synapse Project for each institution and grant the respective team **Edit** level access.

   - E.g., for institutions A, B, and C, create Projects A, B, and C with Teams A, B, and C. Team A has **Edit** access to Project A, etc.

4. Within each project, create "top level folders" in the **Files** tab for each dataset type.
5. Create another Synapse Project (e.g., MyDCC) containing the main **Fileview** that includes in the scope all the DCC projects.

   - Ensure all teams have **Download** level access to this file view.
   - Include both file and folder entities and add **ALL default columns**.

.. note::

   Note: If you want to upload data according to hierachical data layout, you can still use
   distributed projects, just the ``contentType`` column to your fileview, and you will have
   to annotate your top level folders with ``contentType:dataset``.


Option 2: Single Synapse Project
--------------------------------

Pick **option 2** if you don't select option 1 and you answer "yes" to any of these questions:

- Does the DCC have a project with pre-existing datasets in a complex folder hierarchy?
- Does the DCC envision collaboration on the same dataset collection across multiple teams with shared access controls?
- Are you willing to set up local access control for each dataset folder and annotate each with ``contentType: dataset``?

If neither option fits, select option 1.


Access & Project Setup - Single Contributing Project
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Create a Team for each data contributing institution.
2. Create a single Synapse Project (e.g., MyDCC).
3. Within this project, create dataset folders for each contributor. Organize them as needed.

   - Annotate ``contentType: dataset`` for each top level folder, which should not nest inside other dataset folders and must have unique names.
     Taking the above example, you cannot have something like this:

      .. code-block:: shell

         my_heirarchical_project
         ├── biospecimen
         │   ├── experiment_1 <- annotated
         │   └── experiment_2 <- annotated
         └── clinical
            ├── experiment_1 <- this is not allowed, because experiment_1 is duplicated
            └── batch_2 <- annotated

4. In MyDCC, create the main **DCC Fileview** with `MyDCC` as the scope. Add column ``contentType`` to the schema and grant teams **Download** level access.

   - Ensure all teams have **Download** level access to this file view.
   - Add both file and folder entities and add **ALL default columns**.

.. note::

   You can technically use the flat data layout with a single project setup, but it is not recommended
   as if you have different data contributors contributing similar datatypes, it would lead to a
   proliferation of folders per contributor and data type.

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
