class BaseStorage(object):
    """Common interface for data store back-ends (like Synapse and Gen3)."""

    def __init__(self):
        """Base function for initializing object."""
        raise NotImplementedError()
