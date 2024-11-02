=============
CLI Reference
=============


1. Generate a new manifest as a Google Sheet
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

   schematic manifest -c /path/to/config.yml get -dt <your data type> -s

2. Grab an existing manifest from Synapse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

   schematic manifest -c /path/to/config.yml get -dt <your data type> -d <your synapse dataset folder id> -s

3. Validate a manifest
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

   schematic model -c /path/to/config.yml validate -dt <your data type> -mp <your csv manifest path>

4. Submit a manifest as a file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: shell

   schematic model -c /path/to/config.yml submit -mp <your csv manifest path> -d <your synapse dataset folder id> -vc <your data type> -mrt file_only




.. click:: schematic.__main__:main
  :prog: schematic
  :nested: full
