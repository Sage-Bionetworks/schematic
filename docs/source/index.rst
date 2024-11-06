.. Schematic documentation master file, created by
   sphinx-quickstart on Thu Feb 18 11:35:49 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Schematic's documentation!
=====================================

.. warning::
   This documentation site is a work in progress, and the sublinks may change.  Apologies for the inconvenience.


**SCHEMATIC** is an acronym for *Schema Engine for Manifest Ingress and Curation*. The Python-based infrastructure provides a *novel* schema-based, metadata ingress ecosystem, which is meant to streamline the process of biomedical dataset annotation, metadata validation, and submission to a data repository for various data contributors.

Schematic tackles these goals:

- Ensure the highest quality structured data or metadata be contributed to Synapse before it lands in Synapse
- Provide excel templates that correspond to a data model that can be filled out by data contributors
- Visualize data models and their relationships with each other

.. contents::
   :depth: 2
   :local:

Important Concepts
------------------

.. important::
   Before moving reading more about schematic, this section covers essential concepts relevant for using the Schematic tool effectively.

Synapse FileViews
~~~~~~~~~~~~~~~~~
Users are responsible for setting up a **FileView** that integrates with Schematic. Note that FileViews appear under the "Tables" tab in Synapse and can be named according to the project's needs. For instance, a FileView for the **Project A** could have a different name than a FileView for the **Project B**.

For more information on Synapse projects, visit:
- `Synapse projects <https://help.synapse.org/docs/Uploading-and-Organizing-Data-Into-Projects,-Files,-and-Folders.2048327716.html>`_
- `Synapse annotations <https://help.synapse.org/docs/Annotating-Data-With-Metadata.2667708522.html>`_

Synapse Folders
~~~~~~~~~~~~~~~

Folders in Synapse allow users to organize data within projects. More details on uploading and organizing data can be found at `Synapse folders <https://help.synapse.org/docs/Uploading-and-Organizing-Data-Into-Projects,-Files,-and-Folders.2048327716.html>`_

Synapse Datasets
~~~~~~~~~~~~~~~~

This is an object in Synapse which appears under the "Dataset" tab and represents a user-defined collection of Synapse files and versions. https://help.synapse.org/docs/Datasets.2611281979.html

JSON-LD
~~~~~~~
JSON-LD is a lightweight Linked Data format. The usage of JSON-LD to capture our data models extends beyond the creation, validation, and submission of annotations/manifests into Synapse. It can create relationships between different data models and, in the future, drive transformation of data from one data model to another. Visualization of these data models and their relationships is also possible (see *Schema Visualization - Design & Platform*), which allows the community to see the depth of connections between all the data uploaded into Synapse. As with all products, we must start somewhere.

Manifest
~~~~~~~~
A manifest is a structured file that contains metadata about a files under a "top level folder".
It is a list of files and their associated metadata. The metadata includes information such as the file name,
file type, and file size. The manifest is used to validate the metadata before it is uploaded to Synapse.
The manifest can also used to create a view in Synapse that displays the metadata for each file in the "top level folder".

Component/Data type
~~~~~~~~~~~~~~~~~~~
"component" and "data type" are used interchangeably. The component/data type is determined from the specified JSON-LD data model.
If the string "component" exists in the depends on column, the "Attribute" value in that row is a data type.
Examples of a data type is "Biospecimen", "Patient": https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.csv#L3.
Each data type/component should a manifest template that has different columns.

Project Data Layout
~~~~~~~~~~~~~~~~~~~

Regardless of data layout, the data in your Synapse Project(s) are uploaded into Synapse Folders to be curated and annotated by schematic.
In both layouts listed below, the project administrators along with the data contributors may have preferences on how the
data is organized. The organization of your data is specified with the "Component / data type" attribute of your data model and
act as logical groupings for your data. Schematic has a concept of a ``dataset`` (parameters for the API/library/CLI), but this means
different things under these two layouts.

* **Hierarchical**: The "dataset" parameter under this data layout is associated with any folder that has ``contentType: dataset`` annotated
  and is often associated with a "dataset".
* **Flat**: The "dataset" parameter under this data layout is often referred to as "top level folders".

In both of these layouts, these are really just groupings of resources.


Schematic services
------------------

The following are the four main endpoints that assist with the high-level goals outlined above, with additional goals to come.

Manifest Generation
~~~~~~~~~~~~~~~~~~~

Provides a manifest template for users for a particular project or data type. If a project with annotations already exists, a semi-filled-out template is provided to the user so that they do not start from scratch. If there are no existing annotations, an empty manifest template is provided.

Manifest Validation
~~~~~~~~~~~~~~~~~~~

Given a filled-out manifest:

- The manifest is validated against the JSON-LD schema as it maps to GX rules.
- A ``jsonschema`` is generated from the data model. The data model can be in CSV, JSON-LD format, as input formats are decoupled from the internal data model representation within Schematic.
- A set of validation rules is defined in the data model. Some validation rules are implemented via GX; others are custom Python code. All validation rules have the same interface.
- Certain GX rules require looping through all projects a user has access to, or a specified scope of projects, to find other projects with manifests.
- Validation results are provided before the manifest file is uploaded into Synapse.

Manifest Submission
~~~~~~~~~~~~~~~~~~~

- Validates the manifest. If errors are present, the manifest is not stored.
- If valid:
  - Stores the manifest in Synapse.
  - Uploads the manifest to a view, updating file views with annotations as follows:

      - **Store manifest only**
      - **Store manifest and annotations** (to update a file view)
      - **Store manifest and update a corresponding Synapse table**

More validation documentation can be found here: https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/3302785036/Schematic+Validation

Data Model Visualization
~~~~~~~~~~~~~~~~~~~~~~~~

These endpoints allows you to visulize your data models and their relationships with each other.


API reference
-------------

For the entire Python API reference documentation, you can visit the docs here: https://sage-bionetworks.github.io/schematic/

.. toctree::
   :maxdepth: 1
   :hidden:

   installation
   asset_store
   configuration
   tutorials
   troubleshooting
   cli_reference
