.. _jsonschema_generation:

**************************************
Generate JSON Schema from Data Models
**************************************


JSONSchema Components and How to Set from Data Model
====================================================



Property Keys
-------------
Property keys are taken from the Attribute names in the data model. Class labels can be used as well as display names, provided they do not contain any characters blacklisted by synapse: ``"(", ")", ".", " ", "-"``.

Description
-----------
The JSONSchema description is taken from the data model's description field. If the data model does not have a description, the description will be set to ``TBD``.

Type
-------
Multiple types can be enforced in JSONSchema. Currently allowed types are: ``string``, ``numnber``, ``integer``, and ``boolean``.
Types can be set explicitly in the data model or inferred from an attribute's specified validation rules.
In the cases where no type is provided and no type can be inferred, no ``type`` key will be added to the JSONSchema.

Explicit Type Setting
^^^^^^^^^^^^^^^^^^^^^^
To explicitly set the type of a property in the JSONSchema, add the ``ColumnType`` column to the data model and specify one of the allowed types in that column for the appropriate attribute's row. It is acceptable to leave rows blank for attributes that you do not wish to specify type for.


Implicit Type Inference
^^^^^^^^^^^^^^^^^^^^^^^^
Types are also inferred from the validation rules set for an attribute. The following validation rules map to the indicated JSONSchema types:

===================  ================
Validation Rule      JSONSchema Type
===================  ================
list                 array
regex module         string
float                number
int                  integer
num                  number
string               string
inRange              integer or number
date                 string
datetime             format: date
URL                  format: uri
===================  ================


Validation
----------

Certain validation checks from schematic are also present in JSONSchema validation.

Type Checks
^^^^^^^^^^^^^^

Types discussed above are enforced in JSONSchema validation.

Valid Values
^^^^^^^^^^^^^^^
If an attribute has valid values specified, the JSONSchema validation will enforce that provided values are one of the valid values specified.
This will show up in the JSONSchema as an ``enum`` key with a list of valid values.

Validation Rules
^^^^^^^^^^^^^^^^^^

``inRange``
""""""""""""""
Aside from the type validation checks, the ``inRange`` rule will also be translated to the JSONSchema if provided for an attribute. The attribute must be a numberical type, and the ``maximum`` and ``minimum`` keys will be added to the JSONSchema for the property, witht the values taken from the range specified in the data model.

``regex`` module
"""""""""""""""""""""
If the ``regex`` module is specified for an attribute, the JSONSchema will include a ``pattern`` keyword with the value being the regex string provided in the data model. Note that in cases where ``regex match`` is the specified rule, the character ``^`` will be pre-prended to the regex string, which enables the ``match`` functionality on the backend.

Required
^^^^^^^^^^^^^^
For required attributes, the JSONSchema will have an additional ``not: {"type": "null"}`` key value pair added to the property.

Conditionals
-------------

Conditional properties will be added to the JSONSchema if present in the data model. The conditional formatting will look like a series of ``"if": {}, "then": {}`` key dictionary pairs.
