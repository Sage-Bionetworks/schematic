.. _jsonschema_generation:

**************************************
Generate JSON Schema from Data Models
**************************************


JSONSchema Components and How to Set from Data Model
====================================================

This document serves as a guide on what features in a CSV data model map to which components in a JSONSchema file. All examples of JSONSchema files were taken from this `example data model <https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.column_type_component.csv>`_.
For documentation on how to generate a JSONSchema file, see the :ref:`cli documentation<generate-jsonschema>`.

Property Keys
-------------
Property keys are taken from the Attribute names in the data model. Class labels can be used as well as display names, provided they do not contain any characters blacklisted by synapse: ``"(", ")", ".", " ", "-"``.

Description
-----------
The JSONSchema description is taken from the data model's description field. If the data model does not have a description, the description will be set to ``TBD``.

An attribute with a description::

  {"$id": "http://example.com/MockComponent_validation",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "description": "Component to hold mock attributes for testing all validation rules",
  "properties": {...}
  }

An attribute without a description::

    "CheckAges": {
    "description": "TBD",
    "not": {
    "type": "null"
    },
    "title": "Check Ages"
    },

Type
-------
Multiple types can be enforced in JSONSchema. Currently allowed types are in the table below. Types can be set explicitly in the data model or inferred from an attribute's specified validation rules.

.. list-table:: Currently Allowed Types
    :widths: 60
    :header-rows: 1

    * - Type
    * - ``string``
    * - ``number``
    * - ``integer``
    * - ``boolean``


Explicit Type Setting
^^^^^^^^^^^^^^^^^^^^^^
To explicitly set the type of a property in the JSONSchema, add the ``columnType`` column to the data model and specify one of the allowed types in that column for the appropriate attribute's row. It is acceptable to leave rows blank for attributes that you do not wish to specify type for.


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
datetime             string
URL                  string
===================  ================

An attribute with a specified type::

    "CheckRange": {
      "description": "TBD",
      "maximum": 100.0,
      "minimum": 50.0,
      "title": "Check Range",
      "type": "number"
    }

An attribute without a specified type::

    "YearofBirth": {
      "description": "TBD",
      "title": "Year of Birth"
    }


Implicit Format Inference
^^^^^^^^^^^^^^^^^^^^^^^^^^
Formats are also inferred from the validation rules set for an attribute. The following validation rules map to the indicated JSONSchema formats:

===================  =================
Validation Rule      JSONSchema Format
===================  =================
datetime             format: date
URL                  format: uri
===================  =================

Validation Checks
------------------

Certain validation checks from schematic are also present in JSONSchema validation.

Type Checks
^^^^^^^^^^^^^^

Types discussed above are enforced in JSONSchema validation.
For more information about these rules see :ref:`the documentation for type rules<Type Validation Type>`.

Valid Values
^^^^^^^^^^^^^^^
If an attribute has valid values specified, the JSONSchema validation will enforce that provided values are one of the valid values specified.
This will show up in the JSONSchema as an ``enum`` key with a list of valid values.

An attribute with valid values specified::

    "FileFormat": {
      "description": "TBD",
      "oneOf": [
        {
          "enum": [
            "BAM",
            "CRAM",
            "CSV/TSV",
            "FASTQ"
          ],
          "title": "enum"
        }
      ],
      "title": "File Format"
    },

An attribute with valid values specified along with the ``list`` rule::

    "CheckListEnum": {
      "description": "TBD",
      "oneOf": [
        {
          "items": {
            "enum": [
              "ab",
              "cd",
              "ef",
              "gh"
            ]
          },
          "title": "array",
          "type": "array"
        }
      ],
      "title": "Check List Enum"
    }

Required Attributes
^^^^^^^^^^^^^^^^^^^^^
For required attributes, the JSONSchema will have an additional ``not: {"type": "null"}`` key value pair added to the property.

A required attribute::

    "CheckDate": {
      "description": "TBD",
      "not": {
        "type": "null"
      },
      "title": "Check Date"
    }

Validation Rules
^^^^^^^^^^^^^^^^^^

``inRange``
""""""""""""""
Aside from the type validation checks, the ``inRange`` rule will also be translated to the JSONSchema if provided for an attribute. The attribute must be a ``number`` type, and the ``maximum`` and ``minimum`` keys will be added to the JSONSchema for the property, with the values taken from the range specified in the data model.

An attribute with an ``inRange`` validation rule::

    "CheckRange": {
      "description": "TBD",
      "maximum": 100.0,
      "minimum": 50.0,
      "title": "Check Range",
      "type": "number"
    }

For more information about the ``inRange`` rule see :ref:`the rule documentation<inRange>`.

``regex`` module
"""""""""""""""""""""
If the ``regex`` module is specified for an attribute, the JSONSchema will include a ``pattern`` keyword with the value being the regex string provided in the data model. Note that in cases where ``regex match`` is the specified rule, the character ``^`` will be automatically pre-prended to the regex string, which enables the ``match`` functionality on the backend. This caret does not need to be added within the data model to enable this functionality.

For example, an attribute with a ``regex`` rule ``regex search [a-f]`` specified will yield a property like::

    "CheckRegexSingle": {
      "description": "TBD",
      "pattern": "[a-f]",
      "type": "string",
      "title": "Check Regex Single"
    },


While an attribute with a ``regex`` rule ``regex match [a-f]`` specified will yield a property like::

    "CheckRegexFormat": {
      "description": "TBD",
      "pattern": "^[a-f]",
      "type": "string",
      "title": "Check Regex Format"
    }



For more information about the ``regex`` module rule see :ref:`the rule documentation<Regex Validation Type>`.


``date``
"""""""""""""

If the ``date`` validation rule is specified for an attribute, the JSONSchema will include a ``format: date`` key value pair.

An attribute with a ``date`` validation rule specified::

    "CheckDate": {
      "description": "TBD",
      "type": "string",
      "format": "date",
      "title": "Check Date"
    }

For more information about the ``date`` rule see :ref:`the rule documentation<date>`.


``URL``
"""""""""""""
If the ``URL`` validation rule is specified for an attribute, the JSONSchema will include a ``format: uri`` key value pair.

An attribute with a ``URL`` validation rule specified::

    "CheckURL": {
      "description": "TBD",
      "type": "string",
      "format": "uri",
      "title": "Check URL"
    }

For more information about the ``URL`` rule see :ref:`the rule documentation<URL Validation Type>`.


Conditional Dependencies
-------------------------

Conditional properties will be added to the JSONSchema if present in the data model. The conditional formatting will look like a series of ``"if": {}, "then": {}`` key dictionary pairs, in addition to the regular attribute dictionaries.

An example of a data type with conditional dependencies::

    {
    "$id": "http://example.com/BulkRNA-seqAssay_validation",
    "$schema": "http://json-schema.org/draft-07/schema#",
    "allOf": [
        {
        "if": {
            "properties": {
            "FileFormat": {
                "enum": [
                "BAM"
                ]
            }
            }
        },
        "then": {
            "properties": {
            "GenomeBuild": {
                "not": {
                "type": "null"
                }
            }
            },
            "required": [
            "GenomeBuild"
            ]
        }
        },
        {
        "if": {
            "properties": {
            "FileFormat": {
                "enum": [
                "CRAM"
                ]
            }
            }
        },
        "then": {
            "properties": {
            "GenomeBuild": {
                "not": {
                "type": "null"
                }
            }
            },
            "required": [
            "GenomeBuild"
            ]
        }
        },
        {
        "if": {
            "properties": {
            "FileFormat": {
                "enum": [
                "CSV/TSV"
                ]
            }
            }
        },
        "then": {
            "properties": {
            "GenomeBuild": {
                "not": {
                "type": "null"
                }
            }
            },
            "required": [
            "GenomeBuild"
            ]
        }
        },
        {
        "if": {
            "properties": {
            "FileFormat": {
                "enum": [
                "CRAM"
                ]
            }
            }
        },
        "then": {
            "properties": {
            "GenomeFASTA": {
                "not": {
                "type": "null"
                }
            }
            },
            "required": [
            "GenomeFASTA"
            ]
        }
        }
    ],
    "description": "TBD",
    "properties": {
        "Component": {
        "description": "TBD",
        "not": {
            "type": "null"
        },
        "title": "Component"
        },
        "FileFormat": {
        "description": "TBD",
        "oneOf": [
            {
            "enum": [
                "BAM",
                "CRAM",
                "CSV/TSV",
                "FASTQ"
            ],
            "title": "enum"
            }
        ],
        "title": "File Format"
        },
        "Filename": {
        "description": "TBD",
        "not": {
            "type": "null"
        },
        "title": "Filename"
        },
        "GenomeBuild": {
        "description": "TBD",
        "oneOf": [
            {
            "enum": [
                "GRCh37",
                "GRCh38",
                "GRCm38",
                "GRCm39"
            ],
            "title": "enum"
            },
            {
            "title": "null",
            "type": "null"
            }
        ],
        "title": "Genome Build"
        },
        "GenomeFASTA": {
        "description": "TBD",
        "title": "Genome FASTA"
        },
        "SampleID": {
        "description": "TBD",
        "not": {
            "type": "null"
        },
        "title": "Sample ID"
        }
    },
    "required": [
        "Component",
        "FileFormat",
        "Filename",
        "SampleID"
    ],
    "title": "BulkRNA-seqAssay_validation",
    "type": "object"
    }
