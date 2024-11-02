.. Schematic documentation master file, created by
   sphinx-quickstart on Thu Feb 18 11:35:49 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Schematic's documentation!
=====================================

**SCHEMATIC** is an acronym for *Schema Engine for Manifest Ingress and Curation*. The Python-based infrastructure provides a *novel* schema-based, metadata ingress ecosystem, which is meant to streamline the process of biomedical dataset annotation, metadata validation, and submission to a data repository for various data contributors.

Schematic tackles these goals:

- Ensure the highest quality structured data or metadata be contributed to Synapse BEFORE it lands in Synapse
- Add accountability to data contributors for the data they upload
- Visualize data models and their relationships with each other

Important Concepts
------------------

.. important::
   Before moving reading more about schematic, this section covers essential Synapse concepts relevant for using the Schematic tool effectively.

Synapse FileViews
~~~~~~~~~~~~~~~~~
Data managers and DCC owners are responsible for setting up a **FileView** that integrates with Schematic. Note that FileViews appear under the "Tables" tab in Synapse and can be named according to the project’s needs. For instance, a FileView for the **NF project** could have a different name than a FileView for the **AD project**.

For more information on Synapse projects, visit:
- `Synapse projects <https://help.synapse.org/docs/Uploading-and-Organizing-Data-Into-Projects,-Files,-and-Folders.2048327716.html>`_
- `Synapse annotations <https://help.synapse.org/docs/Annotating-Data-With-Metadata.2667708522.html>`_

Synapse Folders
~~~~~~~~~~~~~~~

Folders in Synapse allow users to organize data within projects. More details on uploading and organizing data can be found at `Synapse folders <https://help.synapse.org/docs/Uploading-and-Organizing-Data-Into-Projects,-Files,-and-Folders.2048327716.html>`_

Datasets
~~~~~~~~
You will hear the term **dataset** used frequently at Sage. The term dataset refers to three different concepts:

1. Dataset: This is the concept of a dataset which is a collection of files.
2. Schematic Dataset: This refers to a folder containing files. These folders are annotated with `contentType:dataset`.
3. Synapse Dataset Entity: This is an object in Synapse which appears under the "Dataset" tab and represents a user-defined collection of Synapse files and versions.


The usage of JSON-LD
--------------------

The usage of JSON-LD to capture our data models extends beyond the creation, validation, and submission of annotations/manifests into Synapse. It can create relationships between different data models and, in the future, drive transformation of data from one data model to another. Visualization of these data models and their relationships is also possible (see *Schema Visualization - Design & Platform*), which allows the community to see the depth of connections between all the data uploaded into Synapse. As with all products, we must start somewhere.


The following are the three main endpoints that assist with the high-level goals outlined above, with additional goals to come.

1. Manifest Generation
----------------------

Provides a manifest template for users for a particular project or data type. If a project with annotations already exists, a semi-filled-out template is provided to the user so that they do not start from scratch. If there are no existing annotations, an empty manifest template is provided.

2. Validate Manifest
--------------------

Given a filled-out manifest:

- The manifest is validated against the JSON-LD schema as it maps to GX rules.
- A ``jsonschema`` is generated from the data model. The data model can be in CSV, JSON-LD format, as input formats are decoupled from the internal data model representation within Schematic.
- A set of validation rules is defined in the data model. Some validation rules are implemented via GX; others are custom Python code. All validation rules have the same interface.
- Certain GX rules require looping through all projects a user has access to, or a specified scope of projects, to find other projects with manifests.
- Validation results are provided before the manifest file is uploaded into Synapse.

3. Submit Manifest
------------------

- Validates the manifest. If errors are present, the manifest is not stored.
- If valid:
  - Stores the manifest in Synapse.
  - Uploads the manifest to a view, updating file views with annotations as follows:

      - **Store manifest only**
      - **Store manifest and annotations** (to update a file view)
      - **Store manifest and update a corresponding Synapse table**

More validation documentation can be found here: https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/3302785036/Schematic+Validation

4. Visualize Data Models
-------------------------

This endpoint allows you to visulize your data models and their relationships with each other.


.. toctree::
   :maxdepth: 1
   :hidden:

   installation
   asset_store
   configuration
   cli_reference
