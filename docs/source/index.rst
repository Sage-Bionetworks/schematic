.. Schematic documentation master file, created by
   sphinx-quickstart on Thu Feb 18 11:35:49 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Schematic's documentation!
=====================================

**SCHEMATIC** is an acronym for *Schema Engine for Manifest Ingress and Curation*. The Python-based infrastructure provides a *novel* schema-based, metadata ingress ecosystem, which is meant to streamline the process of biomedical dataset annotation, metadata validation, and submission to a data repository for various data contributors.

Schematic tackles these goals:

- “Ensure the highest quality structured data or metadata be contributed to Synapse BEFORE it lands in Synapse”
- “Add accountability to data contributors for the data they upload”
- “Visualize data models and their relationships with each other”

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

4. Visualize Data Models
-------------------------

This endpoint allows you to visulize your data models and their relationships with each other.


.. toctree::
   :maxdepth: 1
   :hidden:

   installation
   asset_store
   generate
   validate
   submit
   cli_reference
