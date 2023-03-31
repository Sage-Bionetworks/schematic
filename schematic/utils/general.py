# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text
import os
import math
import logging

from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.table import EntityViewSchema
from synapseclient.entity import File, Folder, Project

logger = logging.getLogger(__name__)


def find_duplicates(_list):
    """Find duplicate items in a list"""
    return set([x for x in _list if _list.count(x) > 1])


def dict2list(dictionary):
    if type(dictionary) == list:
        return dictionary
    elif type(dictionary) == dict:
        return [dictionary]


def str2list(_str):
    if type(_str) == str:
        return [_str]
    elif type(_str) == list:
        return _str


def unlist(_list):
    if len(_list) == 1:
        return _list[0]
    else:
        return _list


def get_dir_size(path: str):
    """calculate total size of a directory
    Args:
        path: path to a folder
    return: total size of a directory
    """
    total = 0
    # Example usage of os.scandir could be found here: https://docs.python.org/3/library/os.html#os.scandir
    # Technically, scandir.close() is called automatically. But it is still advisable to call it explicitly or use the with statement.
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total


def convert_size(size_bytes: int):
    """convert bytes to a human readable format
    Args:
        size_bytes: total byte sizes
    return: a string that indicates bytes in a different format
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    # calculate the log of size (in bytes) to base 1024 and run it down to the nearest integer
    index_int = int(math.floor(math.log(size_bytes, 1024)))
    # return the value of 1024 raised to the power of index
    power_cal = math.pow(1024, index_int)
    # convert bytes to a different unit if applicable
    size_bytes_converted = round(size_bytes / power_cal, 2)
    return f"{size_bytes_converted} {size_name[index_int]})"


def convert_gb_to_bytes(gb: int):
    """convert gb to bytes
    Args:
        gb: number of gb
    return: total number of bytes
    """
    bytes_to_return = gb * 1024 * 1024 * 1024
    return bytes_to_return


def entity_type_mapping(syn, entity_id):
    """
    Return the entity type of manifest
    Args:
        entity: id of an entity
    Return:
        type_entity: type of the manifest being returned
    """
    # check the type of entity
    try:
        entity_name = syn.get(entity_id, downloadFile=False)
    except SynapseHTTPError as e:
        logger.error(
            f"cannot get {entity_id} from asset store. Please make sure that {entity_id} exists"
        )
        raise SynapseHTTPError(
            f"cannot get {entity_id} from asset store. Please make sure that {entity_id} exists"
        ) from e

    if isinstance(entity_name, EntityViewSchema):
        return "asset view"
    elif isinstance(entity_name, Folder):
        return "folder"
    elif isinstance(entity_name, File):
        return "file"
    elif isinstance(entity_name, Project):
        return "project"
    else:
        # if there's no matching type, return concreteType
        return entity_name.concreteType
