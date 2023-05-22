# allows specifying explicit variable types
from typing import Any, Dict, Optional, Text
import os
import math
import logging
import pstats
from cProfile import Profile
from functools import wraps

import tempfile

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
    """Recursively descend the directory tree rooted at the top and call .st_size function to calculate size of files in bytes. 
    Args:
        path: path to a folder
    return: total size of all the files in a given directory in bytes. 
    """
    total = 0
    # Recursively scan directory to find entries
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
    return gb * 1024 * 1024 * 1024

def entity_type_mapping(syn, entity_id):
    """
    Return the entity type of manifest
    Args:
        entity_id: id of an entity
    Return:
        type_entity: type of the manifest being returned
    """
    # check the type of entity
    try:
        entity = syn.get(entity_id, downloadFile=False)
    except SynapseHTTPError as e:
        logger.error(
            f"cannot get {entity_id} from asset store. Please make sure that {entity_id} exists"
        )
        raise SynapseHTTPError(
            f"cannot get {entity_id} from asset store. Please make sure that {entity_id} exists"
        ) from e

    if isinstance(entity, EntityViewSchema):
        return "asset view"
    elif isinstance(entity, Folder):
        return "folder"
    elif isinstance(entity, File):
        return "file"
    elif isinstance(entity, Project):
        return "project"
    else:
        # if there's no matching type, return concreteType
        return entity.concreteType

def create_temp_folder(path: str) -> str:
    """This function creates a temporary directory in the specified directory 
    Args:
        path(str): a directory path where all the temporary files will live
    Returns: returns the absolute pathname of the new directory.
    """
    # Create a temporary directory in the specified directory
    path = tempfile.mkdtemp(dir=path)
    return path


def profile(output_file=None, sort_by='cumulative', lines_to_print=None, strip_dirs=False):
    """
    The function was initially taken from: https://towardsdatascience.com/how-to-profile-your-code-in-python-e70c834fad89
    A time profiler decorator.
    Inspired by and modified the profile decorator of Giampaolo Rodola:
    http://code.activestate.com/recipes/577817-profile-decorator/
    Args:
        output_file: str or None. Default is None
            Path of the output file. If only name of the file is given, it's
            saved in the current directory.
            If it's None, the name of the decorated function is used.
        sort_by: str or SortKey enum or tuple/list of str/SortKey enum
            Sorting criteria for the Stats object.
            For a list of valid string and SortKey refer to:
            https://docs.python.org/3/library/profile.html#pstats.Stats.sort_stats
        lines_to_print: int or None
            Number of lines to print. Default (None) is for all the lines.
            This is useful in reducing the size of the printout, especially
            that sorting by 'cumulative', the time consuming operations
            are printed toward the top of the file.
        strip_dirs: bool
            Whether to remove the leading path info from file names.
            This is also useful in reducing the size of the printout
    Returns:
        Profile of the decorated function
    """

    def inner(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _output_file = output_file or func.__name__ + '.prof'
            pr = Profile()
            pr.enable()
            retval = func(*args, **kwargs)
            pr.disable()
            pr.dump_stats(_output_file)

            #if we are running the functions on AWS: 
            if "SECRETS_MANAGER_SECRETS" in os.environ:
                ps = pstats.Stats(pr)
                # limit this to 30 line for now otherwise it will be too long for AWS log
                ps.sort_stats('cumulative').print_stats(30)
            else: 
                with open(_output_file, 'w') as f:
                    ps = pstats.Stats(pr, stream=f)
                    if strip_dirs:
                        ps.strip_dirs()
                    if isinstance(sort_by, (tuple, list)):
                        ps.sort_stats(*sort_by)
                    else:
                        ps.sort_stats(sort_by)
                    ps.print_stats(lines_to_print)
            return retval

        return wrapper

    return inner
