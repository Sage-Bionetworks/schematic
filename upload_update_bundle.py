def uploadBundle(bundle, storage_location) -> None:

    """Uploads bundle to specified storage location.
    Need to think more about what is necessary for this.
    Stated that most logic should be handled on storage-side
    once specific storage is instantiated.
    Where is specific storage defined previously?
    Args:
        bundle: bundle name
        storage_location: destination for bundle
    """
    pass


def updateBundle(old_bundle, new_bundle, storage_location) -> None:

    """Replaces bundle with new, updated version.
    Args:
        old_bundle: previous bundle to be replaced
        new_bundle: new bundle being uploaded
        storage_location: current location (GCS bucket, AWS location, Synapse, ETC of old bundle)
    """
    pass
