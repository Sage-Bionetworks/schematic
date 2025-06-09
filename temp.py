import uuid
from synapseclient.client import Synapse
from synapseclient.models import Folder

def create_synapse_folder(syn: Synapse, parent_id: str) -> str:
    folder_name = f"test_json_schemas_{str(uuid.uuid4())}"
    folder = Folder(name=folder_name, parent_id=parent_id)
    folder.store(synapse_client=syn)
    return folder.id

syn = Synapse()
syn.login()
create_synapse_folder(syn, "syn68175188")