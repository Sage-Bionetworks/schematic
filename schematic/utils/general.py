# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text
import os
import math

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
    #convert bytes to a different unit if applicable
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


def entity_type_checking(syn, manifest_id):
    """
    check the entity type of the id that needs to be downloaded
    Args:
        manifest_id: id of a manifest
    Return: 
        type_entity: type of the manifest being returned
    """
    # check the type of entity
    try: 
        entity_name = syn.get(manifest_id, downloadFile=False)
    except: 
        raise(f'Here: {manifest_id}')
    type_entity = str(type(entity_name))

    if type_entity == "<class 'synapseclient.table.EntityViewSchema'>":
        return "asset view"
    elif type_entity == "<class 'synapseclient.entity.Folder'>":
        return "folder"
    elif type_entity == "<class 'synapseclient.entity.File'>":
        return "file"
    elif type_entity == "<class 'synapseclient.entity.Project'>":
        return "project"
    else: 
        return type_entity