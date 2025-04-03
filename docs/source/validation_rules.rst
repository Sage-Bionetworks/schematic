================
Validation Rules
================

Overview
========

When Schematic validates a manifest, it uses a data model. The data model contains a list of validation rules for each component(data-type). This document describes all allowed validation rules currently implemented by Schematic.

Rules that change the validation behavior which must be taken from the following pre-specified list, formatted in the indicated ways, and added to the data model to apply. An `example data model <https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.csv>`_ using each rule is available for reference.

Rules can optionally be configured to raise  errors  and prevent manifest submission in the case of invalid entries, or warnings and allow submission when invalid entries are found within the attribute the rule is set for. Validators will be notified of the invalid values in both cases. Default message levels for each rule can be found below under each rule.

Attributes that are not required will raise warnings when invalid entries are identified. For attributes that are not required, if a user does not submit a response, a warning or error will no longer be logged. If you desire raising an error for non-entries, set Required to True in the data model.

Metadata validation is an explicit step in the submission pipeline and must be run either before or during manifest submission.

Validation Types
================

Validation rules are just one type of validation run by Schematic to ensure that submitted manifests conform to the expectations set by the data model.

This page details how to use Validation Rules, but please refer to `this documentation <https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/3302785036>`_ to learn about the other types of validation.

Rule Implementation
===================

================ ======== ======================= ======================
Rule             In-House Great Expectations (GX) JSON Schema Validation
================ ======== ======================= ======================
list             yes
regex module     yes
float            yes      yes
int              yes      yes
num              yes      yes
string           yes      yes
url              yes
matchAtLeastOne  yes
matchExactlyOne  yes
matchNone        yes
recommended               yes
protectAges               yes
unique                    yes
inRange                   yes
date                      yes
required                                          yes
================ ======== ======================= ======================

Rule Types and Details
======================

List Validation Type
--------------------

list
~~~~

- Use to parse the imported value to a list of values and (optionally) to verify that the user provided value was a comma separated list, depending on how strictly entries must conform to the list structure. Values can come from Valid Values.

- Format:

  - `list <conformity level> <raised message level>_`

    - `list strict`

      - Validates that entries are comma separated lists, and parses into list

      - Requires all attribute entries to be comma-delimited, even lists with only one element

    - `list like`

      - Assume entries are either lists or `list like` but do not verify that entries are comma separated lists, and attempt to parse into a list

      - Single values, or lists of length one, can be entered without a comma delimiter

- Can use `list` rule in conjunction with `regex` rule to validate that the items in a list follow a specific pattern.

  - All the values in the list need to follow the same pattern. This is ideal for when users need to provide a list of IDs.

- Default behavior: raises `error`

Regex Validation Type
---------------------

regex
~~~~~

- Use the `regex` validation rule when you want to require that a user input values in a specific format, i.e. an ID that follows a particular format.

- Format:

  - `regex <module> <regular_expression> <raised message level>_`

  - Module: is the Python `re` module that you want to use. A common one would be search. Refer to Python `re` source material to find the most appropriate module to use.

  - Single spaces separate the three strings.

- Example:

  - `regex search [0-9]{4}\/[0-9]*`

  - The regular expression defined above allows comparison to an expected format of a histological morphology code.

- Default behavior: raises `error`

- Notes/tips/warnings

  - `regex101.com <https://regex101.com/>_` is a tool that can be used to build and validate the behavior of your regular expression

  - If the module specified is match for a given attribute's validation rule, regex match validation will be preformed in Google Sheets (but not Excel) real-time during metadata entry.

  - The `strict_validation parameter` (in the config.yml file for CLI or in manifest generation REST API calls) sets whether to stop the user from entering incorrect information in a Google Sheets cell (`strict_validation = true`) or simply throws a warning (`strict_validation = false`). Default: `true`.

  - `regex` validation in Google Sheets is different than standard regex validation (for example, it does not support validation of digits). See `this documentation <https://github.com/google/re2/wiki/Syntax>_` for details on Google regex syntax. It is up to the user/modeler to validate that `regex match` is working in their manifests, as intended. This is especially important if the `strict_validation` parameter is set to `True` as users will be blocked from entering incorrect data. If you are using Google Sheets and do not want to use real-time validation use `regex search` instead of `regex match`.

Type Validation Type
--------------------

- There are two parameters

  - The first parameter is type and must be one of [ `float`, `int`, `num`, `str`]

  - The second optional parameter is the msg level and must be one of [ `error`, `warning` ], defaults to `error`.

- Examples: [ `str`, `str error`, `str warning`]

float
~~~~~

- Checks that the value is a float.

int
~~~

- Checks that the value is an integer.

num
~~~

- Checks that the value is either an integer or float.

str
~~~

- Checks that the value is a string (not a number).

URL Validation Type
-------------------

url
~~~

- Using the `url` rule implies the user should add a URL to a free text box as a string. This function will check that the user has provided a usable URL. It will check for any standard URL error and throw an error if one is found. Further additions to this rule can allow for checking that a specific type of URL is added. For example, if the user needs to add a <http://protocols.io> URL, <http://protocols.io> can be added after url to perform this check. If the provided url does not contain this specific string, an error will be raised.

- Format:

  - `url <optional strings> <raised message level>_`

    - `url` must be specified first then an arbitrary number of strings can be added after (separated by spaces) to add additional levels of specificity.

  - Alternatively, its valid to pass only `url`` to simply check if the input is a url.

- Examples:

  - `url http://protocols.io`_ Will check that any input is a valid URL, and will also check to see that the URL contains the string `http://protocols.io` If not, an error will be raised.

  - `url dx.doi http://protocols.io`_ Will check that any input is a valid URL, and will also check to see that the URL contains the strings `dx.doi` and `http://protocols.io`. If not, an error will be raised.

- Default behavior: raises `error`

Required Validation Type
------------------------

required
~~~~~~~~

ass validation.

An attribute's requirement is typically set using the required column (csv) or field (JSONLD) in the data model. A `True` value means a users must supply a value, `False` means they are allowed to skip providing a value.

Some users may want to use the same attribute across several manifests, but have different requirements based on the manifest/component. For example, say the data model contains an attribute called PatientID, and this attribute is used in manifests Biospecimen, Patient and Demographics. Say the modeler wants to require that PatientID be required in the Patient manifest but not Biospecimen or Demographics. In the standard Data Model format, there is only one requirement option per Attribute, so one would not be able to set requirements per component. But with the advent of component based rule settings, this can now be achieved.

Requirements can be specified per component by setting the required field in the data model to `False`, and using component based rule setting along with the required "rule".

Note: this new required validation rule is not a traditional validation rule, but rather impacts the JSON validation schema. This means requirements propagate automatically to manifests as well.

Notes:

- When using `required` in validation rules, the `Required` **column/field must be set to** `False` or this will cause the rule to not work as expected (i.e. components were the attribute is expected to not be required due to the validation rules, will still be required).

  - Note: a warning will be raised for discrepancies in requirements settings are found when running validation.

- `required` can be used in conjunction with other rules, without restriction.

- The messaging level, like all JSON validation checks, is always set at `error`, and not modifiable.

- `required` does not work with other rule modifiers, such as `warning`, `error` etc…

  - Though it will not throw an error if rule modifiers are added, it will not work as intended, and a warning will appear

    - For example, if the rule `^^#Biospecimen required warning`, is added to the data model a warning will be raised letting the user know that the rule modifier cannot be applied to required.

- Adding `required` sets `Required` to `True` for the specified component. There is no way to set `Required` to `False` using the validation rules column, that would come from the `Required` field in the data model.

- Controlling `required` through the validation rule will also impact Manifest formatting (in terms of required column highlighting).

  - To check that `required` rules are working as expected, one could generate all impacted manifests and check the formatting is as expected.

Examples:

- `#BiospecimenManifest unique required warning^^unique error`

  - For`BiospecimenManifest` manifests, the values supplied must be unique. If they aren't a warning will be raised. If values are missing, an error will be raised.

  - For all other manifests, the filling out values is optional. But, if the values supplied are not unique, an error will be raised.

- `#Demographics required^^#BiospecimenManifest required^^`

  - For `Demographics` and `BiospecimenManifest` manifests, values are required to be supplied, if they are not supplied an error will be raised.

  - For all other manifests this attribute is not required.

Cross-manifest Validation Type
------------------------------

Use cross-manifest validation rules when you want to check the values of an attribute in the manifest being validated against an attribute in the manifest(s) of a different component. For example, if a sample manifest has a patient id attribute and you want to check it against the id attribute of patient manifests.

The format for cross-validation is: `<rule> <targetComponent>.<targetAttribute> <scope> <raised message level>`

There are three rules that do cross-manifest validation: [`matchAtLeastOne`, `matchExactlyOne`, `matchNone`]

There are two scopes to choose from: [ `value`, `set`]

Value Scope
~~~~~~~~~~~

When the value scope is used all values from the target attribute in all target manifests are combined. The values from the manifest being validated are compared to this combined list. In other words, there is no distinction between what values came from what target manifest.

matchAtleastOne Value Scope
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The manifest is validated if each value in the target attribute exists at least once in the combined values of the target attribute of the target manifests.

matchExactlyOne Value Scope
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The manifest is validated if each value in the target attribute exists once, and only once, in the combined values of the target attribute of the target manifests.

matchNone Value Scope
^^^^^^^^^^^^^^^^^^^^^

The manifest is validated if each value in the target attribute does not exist in the combined values of the target attribute of the target manifests.

Example 1
^^^^^^^^^

Tested manifest: ["A"]

Target manifests: ["A", "B"]

- matchExactlyOne: passes

- matchAtleastOne: passes

- matchNone: fails

  - because "A" is in the target manifest

Example 2
^^^^^^^^^

Tested manifest: ["A", "C"]

Target manifests: ["A", "B"]

- matchExactlyOne: fails

  - because "C" is not in the target manifest

- matchAtleastOne: fails

  - because "C" is not in the target manifest

- matchNone: fails

  - because "A" is in the target manifest

Example 3
^^^^^^^^^

Tested manifest: ["C"]

Target manifests: ["A", "B"]

- matchExactlyOne: fails

  - because "C" is not in the target manifest

- matchAtleastOne: fails

  - because "C" is not in the target manifest

- matchNone: passes

Example 4
^^^^^^^^^

Tested manifest: ["A", "A"]

Target manifests: ["A", "B"]

- matchExactlyOne: passes

- matchAtleastOne: passes

- matchNone: fails

  - because "A" is in the target manifest

Example 5
^^^^^^^^^

Tested manifest: ["A"]

Target manifests: ["A", "A"]

- matchExactlyOne: fails

  - because "A" is in the target manifest twice

- matchAtleastOne: passes

- matchNone: fails

  - because "A" is in the target manifest

Example 6
^^^^^^^^^

Tested manifest: ["A"]

Target manifests: ["A"], ["A"]

matchExactlyOne: fails

because "A" is in both target manifests

matchAtleastOne: passes

matchNone: fails

because "A" is in the target manifest

Example 7
^^^^^^^^^

Tested manifest: ["A"]

Target manifests: ["A", "B"],  ["A", "B"]

- matchExactlyOne: fails

  - because "A" is in both target manifests

- matchAtleastOne: passes

- matchNone: fails

  - because "A" is in the target manifest

Set scope
~~~~~~~~~

When the set scope is used the values from the tested manifest are compared **one at a time** against each target manifest, and the number of matches are counted. The test to determine if the tested manifest matches the target manifest is to see if the tested manifest values are a subset of the target manifest values. Imagine a target manifest who's values are ["A", "B" "C"]:

- [ ], ["A"], ["A", "A"], ["A", "B", "C"] are all subsets of the example target manifest.

- [1], ["D"], ["D", "D"], ["D", "E"] are not subsets of the example target manifest.

matchAtleastOne Set scope
^^^^^^^^^^^^^^^^^^^^^^^^^

The manifest is validated if there is atleast one set match between the tested manifest and the target manifests

matchExactlyOne Set scope
^^^^^^^^^^^^^^^^^^^^^^^^^

The manifest is validated if there is one and only one set match between the tested manifest and the target manifests

matchNone Set scope
^^^^^^^^^^^^^^^^^^^

The manifest is validated if there are no set match between the tested manifest and the target manifests

Example 1
^^^^^^^^^

Tested manifest: ["A"]

Target manifests: ["A", "B"]

matchExactlyOne: passes

matchAtleastOne: passes

matchNone: fails

because "A" is in the target manifest

Example 2
^^^^^^^^^

Tested manifest: ["A"]

Target manifests: ["A", "B"], ["C", "D"]

- matchExactlyOne: passes

- matchAtleastOne: passes

- matchNone: fails

  - because "A" is in atleast one of the target manifest

Example 3
^^^^^^^^^

Tested manifest: ["A"]

Target manifests: ["A", "B"], ["A", "B"]

- matchExactlyOne: fails

  - because "A" is in more than one target manifest

- matchAtleastOne: passes

- matchNone: fails

  - because "A" is in atleast one of the target manifests

Example 4
^^^^^^^^^

Tested manifest: ["C"]

Target manifests: ["A", "B"]

- matchExactlyOne: fails

  - because "C" is not in the target manifest

- matchAtleastOne: fails

  - because "C" is not in the target manifest

- matchNone: passes

Content Validation Type
-----------------------

Rules can be used to validate the contents of entries for an attribute.

recommended
~~~~~~~~~~~

- Use to raise a warning when a manifest column is not required but empty. If an attribute is always necessary then `required`` should be set to `TRUE` instead of using the `recommended` validation rule.

- Format:

  - `recommended <raised message level>`

- Examples:

  - `recommended`

- Default behavior: raises `warning`

protectAges
~~~~~~~~~~~

- Use to ensure that patient ages under 18 and over 89 years of age are censored when uploading for sharing. If necessary, a censored version of the manifest will be created and uploaded along with the uncensored version. Uncensored versions will be uploaded as restricted and Terms of Use will need to be set.

- Format:

  - `protectAges <raised message level>`

- Examples:

  - `protectAges warning`

- Default behavior: raises `warning`

unique
~~~~~~

- Use to ensure that attribute values are not duplicated within a column.

- Format:

  - `unique <raised message level>`

- Examples:

  - `unique error`

- Default behavior: raises `error`

inRange
~~~~~~~

- Use to ensure that numerical data is within a specified range

- Format:

  - `inRange <lower range bound> <upper range bound> <raised message level>`

- Examples:

  - `inRange 50 100 error`

- Default behavior: raises `error`

date
~~~~

- Use to ensure the value parses as a date

- Uses `dateutils` to parse the value

  - Can parse many formats

  - YYYY-MM-DD format is recommended

  - Every value must be read as a string so no formats such as YYYYDDMM which would be read in as an int

- Default behavior: raises `error`

Filename Validation
-------------------

This requires paths to be enabled for the synapse master file view in use. Can be enabled by navigating to an existing view and selecting `show view schema` > `edit schema` > `add default view columns` > `save`. Paths are enabled on new views by default.

This should be used only with the Filename attribute in a data model and specified with `Component Based Rule Setting <https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/edit-v2/2645262364#Component-Based-Rule-Setting>`_

filenameExists
~~~~~~~~~~~~~~

- Used to validate that the filenames and paths as they exist in the metadata manifest match the paths that are in the Synapse master File View for the specified dataset

  - Conditions in which an error is raised:

    - `missing entityId`: The entityId field for a manifest row is null or an empty string

    - `entityId does not exist`: The entityId provided for a manifest row does not exist within the specified dataset's file view

    - `path does not exist`: The Filename in the manifest row does not exist within the specified dataset's file view

    - `mismatched entityId`: The entityId and Filename do not match the expected values from the specified dataset's file view

- Format

  - `filenameExists <dataset scope> <raised message level>`

- Example

  - This sets the rule for the MockFilename component ONLY with the specified dataset scope syn61682648

  - `#MockFilename filenameExists syn61682648^^`

- Default behavior: raises `error`

Given this File View:

```python
id,path
syn61682653,schematic - main/MockFilenameComponent/txt1.txt
syn61682659,schematic - main/MockFilenameComponent/txt4.txt
syn61682660,schematic - main/MockFilenameComponent/txt2.txt
syn61682662,schematic - main/MockFilenameComponent/txt3.txt
syn63141243,schematic - main/MockFilenameComponent/txt6.txt
```

We get the following results for this Manifest:

```python
Component,Filename,entityId
MockFilename,schematic - main/MockFilenameComponent/txt1.txt,syn61682653 # Pass
MockFilename,schematic - main/MockFilenameComponent/txt2.txt,syn61682660 # Pass
MockFilename,schematic - main/MockFilenameComponent/txt3.txt,syn61682653 # mismatched entityId
MockFilename,schematic - main/MockFilenameComponent/this_file_does_not_exist.txt,syn61682653 # path does not exist
MockFilename,schematic - main/MockFilenameComponent/txt4.txt,syn6168265 # entityId does not exist
MockFilename,schematic - main/MockFilenameComponent/txt6.txt,  # missing entityId
```

Rule Combinations
-----------------

Schematic allows certain combinations of existing validation rules to be used on a single attribute, where appropriate. Combinations currently allowed are enumerated in the table below, under 'Rule Combinations in Production'.

Note:  isNa and required can be combined with all rules and rule combos.

Rule combinations: [`list::regex`, `int::inRange`, `float::inRange`, `num::inRange`, `protectAges::inRange`]

- Format:

  - `<rule 1> <applicable rule 1 arguments>::<rule 2> <applicable rule 2 arguments>`

  - `::` delimiter used to separate each rule

- Example:

  - `list :: regex search [HTAN][0-9]{1}_[0-9]{4}_[0-9]*`

Component-Based Rule Setting
----------------------------

**Component-Based Rule Setting** is a powerful feature in data modeling that enables users to create rules tailored to specific subsets of components or manifests. This functionality was developed to address scenarios where a data modeler needs to enforce uniqueness for certain attribute values within one manifest while allowing non-uniqueness in another.

Here's how it works:

1. **Rule Definition at Attribute Level**: Rules are defined at the attribute level within the data model.

2. **Manifest-Level Referencing**: These rules can then be applied (or not) to specific manifests within the data model. This means that rules can be selectively enforced based on the manifest they're associated with.

This feature offers flexibility and applicability beyond its original use case. The new **Component-Based Rule Setting** feature provides users with the following options:

- **Apply a Rule to All Manifests Except Specified Ones**: Users can now define a rule that applies to all manifests within the data model except for those explicitly specified. In cases where exceptions are specified, users have the flexibility to define unique rules for these exceptions or opt not to apply any rule at all.

- **Specify a Rule for a Single Manifest**: Alternatively, users can specify a rule that applies to a single manifest exclusively. This allows for fine-grained control over rule enforcement at the manifest level.

- **Unique Rules for Each Manifest**: Users can also define unique rules for each manifest within the data model. This enables tailored rule enforcement based on the specific requirements and characteristics of each manifest.

By leveraging the enhanced Component-Based Rule Setting feature, data modelers can efficiently enforce rules across their data models with greater precision and flexibility, ensuring data integrity while accommodating diverse use cases and requirements.

Note: All restrictions to rule combos and implementation also apply to component based rules.

Note: As always try the rule combos with mock data to ensure they are working as intended before using in production.

- Format:

  - `^^`Double carrots indicate that Component-Based rules are being set

    - Use `^^` to separate component rule sets

  - `#` In the first position (prior to the rule) to define the component/manifest to apply the rule to

    - `#` character cannot be used without the `^^` to indicate component rule sets

- Use case:

  - Apply rule to all manifests *except* the specified set.

    - `validation_rule^^#ComponentA`

    - `validation_rule^^#ComponentA^^#ComponentB`

  - Apply a unique rule to each manifest.

    - `#ComponentA validation_rule_1^^#ComponentB validation_rule_2^^#ComponentC validation_rule_3`

  - For the specified manifest, apply the given validation rule, but for all others, run a different rule

    - `#ComponentA validation_rule_1^^validation_rule_2`

    - `validation_rule_2^^#ComponentA validation_rule_1`

  - Apply the validation rule to only one manifest

    - `#ComponentA validation_rule_1^^`

- Example Rules:

  - Test by adding these rules to the `Patient ID` attribute in the `example.model.csv` model, then run validation with new rules against the example manifests.

  - `Example Biospecimen Manifest <https://docs.google.com/spreadsheets/d/19_axG2Zj7URk4CT5qYjH0HfpMIOQ1dYEPvyaazSVNZE/edit#gid=0>`_

  - `Example Patient Manifest <https://docs.google.com/spreadsheets/d/1IO0TkzwBX-lsu3rJDjWfgWYR6VlepingN9zuhkrgVUE/edit#gid=0>`_

    - **Rule**: `#Patient int::inRange 100 900 error^^#Biospecimen int::inRange 100 900 warning`

      - For the `Patient` manifest, apply the combo `rule int::inRange 100 900` at the `error` level.

        - The value provided must be an integer in the range of 100-900; if it does not fall in the range, throw an error

      - For the `Biospecimen` manifest, apply the combo rule `int::inRange 100 900` at the `warning` level

        - The value provided must be an integer in the range of 100-900; if it does not fall in the range, throw a warning

    - **Rule**: `#Patient int::inRange 100 900 error^^int::inRange 100 900 warning`

      - For the `Patient` manifest, apply rule `int::inRange 100 900` at an `error` level

      - For all other manifests, apply the `rule int::inRange 100 900` at a warning level

    - **Rule**: `#Patient^^int::inRange 100 900 warning`

      - For all manifests except `Patient` apply the rule `int::inRange 100 900` at the `warning` level

    - **Rule**: `int::inRange 100 900 error^^#Biospecimen`

      - Apply the rule `int::inRange 100 900 error`, to all manifests except `Biospecimen`

    - **Rule**: `#Patient unique error^^`

      - To the `PatientManifest` only, apply the `unique` validation rule at the `error` level
