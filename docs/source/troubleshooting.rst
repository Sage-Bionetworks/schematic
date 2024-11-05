Troubleshooting
===============

These are some common issues you may encounter when using schematic


Manifest Generate: `KeyError: entityId`
---------------------------------------

If there is currently a manifest in your Schematic Dataset folder on Synapse with an incorrect Filename BUT entityId column.
You will be able to run manifest generate to create a new manifest with the new Filenames. However, If this manifest on Synapse does
NOT have the entityId column you will encounter that error. 

To fix: You will want to first check if your Schematic Dataset folder has a manifest without the entityId column.
If so, you can either submit your manifest using schematic OR (the less preferred solution) manually add the entityId column to the manifest on Synapse.

Manifest Generate: `ValueError: cannot insert eTag, already exists`
-------------------------------------------------------------------

If there is currently a manifest in your Schematic Dataset folder on Synapse with the 'eTag' column and you try to generate a manifest, it will fail.

To fix: You will want to first check if your Schematic Dataset folder has a manifest with the 'eTag' column and remove that column.


Manifest Submit: `RuntimeError: failed with SynapseHTTPError('400 Client Error: nan is not a valid Synapse ID.')`
-----------------------------------------------------------------------------------------------------------------

As for 24.10.2 version of Schematic, we require the `Filename` column to have the full paths to the file on Synapse including the project name.
If you try and submit a manifest with invalid Filenames (not containing full path), you will encounter the `nan`.  This is because we join the `Filename`
column together with what's in Synapse to append the `entityId` column if it's missing.

To fix: You will want to first check if your Schematic Dataset folder has a manifest with invalid Filename values in the column.
If so, please generate a manifest with schematic which should fix the Filenames OR (the less preferred solution) manually update the Filenames to include the full path to the file and manually upload.


Manifest Submit: `TypeError: boolean value of NA is ambiguous`
--------------------------------------------------------------

You may encounter this error if your manifest has a Component column but it is empty.  This may occur if the manifest in your Schematic Dataset folder
does not contain this column.  During manifest generate, it will create an empty column for you.  

To fix: Check if your manifest has an empty Component column.  Please fill out this column with the correct Component values and submit the manifest again.


Manifest validation: `The submitted metadata does not contain all required column(s)`
-------------------------------------------------------------------------------------

The required columns are determined by the data model, but `Component` should be a required column even if it's not set that way in the data model.
This is the validation error you may get if you don't have the `Component` column.

To fix: Check if your manifest has a Component column or missing other required columns. Please add the `Component` column (and fill it out) or any other required columns.


Manifest validation: `The submitted metadata contains << 'string' >> in the Component column, but requested validation for << expected string >>`
-------------------------------------------------------------------------------------------------------------------------------------------------

If the manifest has incorrect Component values, you might get the validation error message above. This is because the Component value is incorrect,
and the validation rule uses the "display" value of what's expected in the Component column.  For example, the display name could be "Imaging Assay"
but the actual Component name is "ImagingAssayTemplate".

To fix: Check if your manifest has invalid Component values and fill it out correctly.  Using the above example, fill out your Component column with "ImagingAssayTemplate"
