.. _Validating a Metadata Manifest:

##############################
Validating a Metadata Manifest
##############################

*************
Prerequisites
*************

**Obtain Synapse Credentials**:
================================
Ensure you have a Synapse account and set up Synapse configuration file correctly. See the :ref:`setting up configuration files <installation:6. Set up configuration files>` section for more details.


**Using the Schematic API in Production**
=========================================

Visit the **Schematic API (Production Environment)**:
`<https://schematic.api.sagebionetworks.org/v1/ui/#/>`_

This will open the **Swagger UI**, where you can explore all available API endpoints.


**Before Using the Schematic CLI**
==================================

- **Install and Configure Schematic**:
  Ensure you have installed ``schematic`` and set up its dependencies.
  See the :ref:`installation:installation` section for more details.

- **Understand Important Concepts**:
  Familiarize yourself with key concepts outlined on the :ref:`index` of the documentation.

- **Configuration File**:
  For more details on configuring Schematic, refer to the :ref:`configuration:Configure Schematic` section.

- **Obtain a manifest**:
  Please obtain a manifest by following the documentation of :ref:`generating a manifest <manifest_generation>`.


************
Requirements
************

Authentication
==============

Authentication with Synapse is required for metadata validation that includes Cross Manifest Validation rules or the ``filenameExists`` rule.

File Format
===========

In general, metadata manifests must be stored as ``.CSV`` files. When validating through the api, manifests may alternatively be sent as a JSON string.

Required Column Headers
=======================

A ``Component`` column that specifies the data type of the metadata must be present in the manifest. Additionally, columns must be present for each attribute in the component that you wish to validate.

Restricted Column Headers
=========================
The columns ``Filename``, ``entityId``, and ``Component`` are reserved for use by schematic and should not be used as other attributes in a data model.

*******************
Manifest Validation
*******************

Overview
========

Invalidities within a manifest’s metadata are classified as either errors or warnings depending on the rule itself, whether the attribute is required, and what the data modeler has specified.
Errors are considered serious invalidities that must be corrected before submission. Warnings are considered less serious invalidities that are acceptable.
A manifest with errors should not be submitted and the presence of errors found during submission will block submission. The presence of warnings will not block submission.

.. note::
    Validation Can be performed as its own, separate step or during submission, by including the ``-vc`` parameter and the data type of the metadata to validate


Separately:

.. code-block:: bash

    schematic model -c /path/to/config.yml validate -dt <your data type> -mp <your csv manifest path>


or with the `/model/validate <https://schematic.api.sagebionetworks.org/v1/ui/#/Model%20Operations/schematic_api.api.routes.validate_manifest_route>`_ endpoint.

During submission:

.. code-block:: bash

    schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse top level folder id> -vc <your data type> -mrt file_only


or by specifying a value for the ``data_type`` parameter in the `/model/submit <https://schematic.api.sagebionetworks.org/v1/ui/#/Model%20Operations/schematic_api.api.routes.submit_manifest_route>`_ endpoint.

If you need further assistance, help is available by running the following command:

.. code-block:: bash

    schematic model -c /path/to/config.yml validate -h

or by viewing the parameter descriptions under the endpoints linked above.


With the CLI
=============

Authentication
--------------

To authenticate for use with the CLI, follow the installation guide instructions on how to :ref:`set up configuration files <set up configuration files>`

Parameters
----------
--manifest_path/-mp
    string

    Specify the path to the metadata manifest file that you want to submit to a dataset on Synapse. This is a required argument.

--data_type/-dt
    optinal string

    Data type of the metadata to be vaidated

    Specify the component (data type) from the data model that is to be used for validating the metadata manifest file. You can either explicitly pass the data type here or provide it in the ``config.yml`` file as a value for the ``(manifest > data_type)`` key.

--json_schema/-js
    optional string

    Specify the path to the JSON Validation Schema for this argument. You can either explicitly pass the ``.json`` file here or provide it in the ``config.yml`` file as a value for the ``(model > input > validation_schema)`` key.

--restrict_rules/-rr
    boolean flag

    If flag is provided when command line utility is executed, validation suite will only run with in-house validation rules, and Great Expectations rules and suite will not be utilized. If not, the Great Expectations suite will be utilized and all rules will be available.

--project_scope/-ps
    optional string

    Specify a comma-separated list of projects to search through for cross manifest validation. Used to speed up some interactions with synapse.

--dataset_scope/-ds
    string

    Specify a dataset to validate against for filename validation.

--data_model_labels/-dml
    string

    one of:

    * class_label - use standard class or property label
    * display_label - use display names (values given in the CSV data model, or the names designated as the display name field of the JSONLD data model) as label. Requires there to be no blacklisted characters in the label

    default: class_label

    .. warning::
        Do not change from default unless there is a real need, using 'display_label' can have consequences if not used properly.

The SynId of the fileview containing all relevant project assets should also be specifed in the ``config.yml`` file under ``(asset_store > synapse > master_fileview_id)``


With the API
============

Authentication
--------------

Your Synapse token should be included the in the request headers under the ``access_token`` key. In the SwaggerUI this can be added by clicking the padlock icon at the top right or next to the endoints that accept it.

Parameters
----------

schema_url
    string
    url to the raw version of the data model in either ``.CSV`` or ``.JSONLD`` formats

data_type
    string
    Data type of the metadata to be vaidated

data_model_labels
    string
    one of:

    * class_label - use standard class or property label
    * display_label - use display names (values given in the CSV data model, or the names designated as the display name field of the JSONLD data model) as label. Requires there to be no blacklisted characters in the label

    default: class_label

    .. warning::
        Do not change from default unless there is a real need, using 'display_label' can have consequences if not used properly.

restrict_rules
    boolean
    If True, validation suite will only run with in-house validation rule. If False, the Great Expectations suite will be utilized and all rules will be available.

json_str
    string
    optional
    The metadata manifest in the form of a JSON string.

asset_view
    string
    SynId of the fileview containing all project assets

project_scope
    optional array[string]
    list of SynIds of projects that are relevant for the current operation. Used to speed up some interactions with Synapse.

dataset_scope
    string
    Specify a dataset to validate against for filename validation.

Request Body
------------

file_name
    string($binary)

    ``.CSV`` or ``.JSON`` file of the metadata manifest


Response
--------
If valiation completes successfully, regardless of the presence of validation errors or warnings, you'll recieve a ``200`` response code.
The body will be a JSON string containing a list of valiation errors and warnings in the format of ``{"errors": [list of errors], "warnings": [warnings]}``

Validating though the CLI will display all the errors and warnings found during validation or a message that no errors or warnings were found and the manifest is considered valid.

*****************
With the Library
*****************
TODO
