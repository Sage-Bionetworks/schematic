"""This script is responsible for creating a 'pull through cache' class that can be
added through composition to any class where Synapse entities might be used. The idea
behind this class is to provide a mechanism such that if a Synapse entity is requested
multiple times, the entity is only downloaded once. This is useful for preventing
multiple downloads of the same entity, which can be time consuming."""
from dataclasses import dataclass, field
from typing import Dict, List, Union

import synapseclient
from synapseclient import Entity, File, Folder, Project, Schema


@dataclass
class SynapseEntiyTracker:
    """The SynapseEntiyTracker class handles tracking synapse entities throughout the
    lifecycle of a request to schematic. It is used to prevent multiple downloads of
    the same entity."""

    synapse_entities: Dict[str, Union[Entity, Project, File, Folder, Schema]] = field(
        default_factory=dict
    )
    project_headers: Dict[str, List[Dict[str, str]]] = field(default_factory=dict)
    """A dictionary of project headers for each user requested."""

    def get(
        self,
        synapse_id: str,
        syn: synapseclient.Synapse,
        download_file: bool = False,
        retrieve_if_not_present: bool = True,
        download_location: str = None,
        if_collision: str = None,
    ) -> Union[Entity, Project, File, Folder, Schema]:
        entity = self.synapse_entities.get(synapse_id, None)

        if entity is None or (download_file and not entity.path):
            if not retrieve_if_not_present:
                return None
            entity = syn.get(
                synapse_id,
                downloadFile=download_file,
                downloadLocation=download_location,
                ifcollision=if_collision,
            )
        self.synapse_entities.update({synapse_id: entity})
        return entity

    def add(
        self, synapse_id: str, entity: Union[Entity, Project, File, Folder, Schema]
    ) -> None:
        self.synapse_entities.update({synapse_id: entity})

    def remove(self, synapse_id: str) -> None:
        self.synapse_entities.pop(synapse_id, None)

    def search_local_by_parent_and_name(
        self, name: str, parent_id: str
    ) -> Union[Entity, Project, File, Folder, Schema, None]:
        for entity in self.synapse_entities.values():
            if entity.name == name and entity.parentId == parent_id:
                return entity
        return None

    def get_project_headers(
        self, syn: synapseclient.Synapse, current_user_id: str
    ) -> List[Dict[str, str]]:
        """Gets the paginated results of the REST call to Synapse to check what projects the current user has access to.

        Args:
            syn: A Synapse object
            current_user_id: profile id for the user whose projects we want to get.

        Returns:
            A list of dictionaries matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/ProjectHeader.html>
        """
        project_headers = self.project_headers.get(current_user_id, None)
        if project_headers:
            return project_headers

        all_results = syn.restGET(
            "/projects/user/{principalId}".format(principalId=current_user_id)
        )

        while (
            "nextPageToken" in all_results
        ):  # iterate over next page token in results while there is any
            results_token = syn.restGET(
                "/projects/user/{principalId}?nextPageToken={nextPageToken}".format(
                    principalId=current_user_id,
                    nextPageToken=all_results["nextPageToken"],
                )
            )
            all_results["results"].extend(results_token["results"])

            if "nextPageToken" in results_token:
                all_results["nextPageToken"] = results_token["nextPageToken"]
            else:
                del all_results["nextPageToken"]

        results = all_results["results"]
        self.project_headers.update({current_user_id: results})

        return results
