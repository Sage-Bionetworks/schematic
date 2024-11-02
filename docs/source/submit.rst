Submit
======


- Validates the manifest. If errors are present, the manifest is not stored.
- If valid:
  - Stores the manifest in Synapse.
  - Uploads the manifest to a view, updating file views with annotations as follows:

      - **Store manifest only**
      - **Store manifest and annotations** (to update a file view)
      - **Store manifest and update a corresponding Synapse table**
