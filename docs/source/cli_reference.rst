#############
CLI Reference
#############

When you're using this tool ``-d`` flag is referring to the Synapse ID of a folder that would be found under the files tab
that contains a manifest and data. This would be referring to a "Top Level Folder". It is not required to provide a ``dataset_id``
but if you're trying to pull existing annotations by using the ``-a`` flag and the manifest is file-based then you would
need to provide a ``dataset_id``.

*****************************************
Generate a new manifest as a Google Sheet
*****************************************

.. code-block:: shell

   schematic manifest -c /path/to/config.yml get -dt <your data type> -s

******************************************
Generate an existing manifest from Synapse
******************************************

.. code-block:: shell

   schematic manifest -c /path/to/config.yml get -dt <your data type> -d <your synapse "Top Level Folder" folder id> -s

*****************************************
Validate a manifest
*****************************************

.. code-block:: shell

   schematic model -c /path/to/config.yml validate -dt <your data type> -mp <your csv manifest path>

*****************************************
Submit a manifest as a file
*****************************************

.. code-block:: shell

   schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse "Top Level Folder" id> -vc <your data type> -mrt file_only

*****************************************
In depth guide
*****************************************

.. click:: schematic.__main__:main
  :prog: schematic
  :nested: full
