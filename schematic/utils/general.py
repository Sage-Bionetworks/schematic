# allows specifying explicit variable types
import logging
import math
import os
import pstats
import subprocess
import tempfile
from cProfile import Profile
from datetime import datetime, timedelta
from functools import wraps
from typing import Union

from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.entity import File, Folder, Project
from synapseclient.table import EntityViewSchema

import synapseclient.core.cache as cache

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

def calculate_datetime(minutes: int, input_date: datetime, before_or_after: str = "before") -> datetime:
    """calculate date time 

    Args:
        input_date (datetime): date time object provided by users
        minutes (int): number of minutes
        before_or_after (str): default to "before". if "before", calculate x minutes before current date time. if "after", calculate x minutes after current date time. 

    Returns:
        datetime:  return result of date time calculation
    """
    if before_or_after=="before": 
        date_time_result = input_date - timedelta(minutes=minutes)
    elif before_or_after=="after":
        date_time_result = input_date + timedelta(minutes=minutes)
    else:
        raise ValueError("Invalid value. Use either 'before' or 'after'.")
    return date_time_result


def check_synapse_cache_size(directory='/root/.synapseCache')-> Union[float, int]:
    """use du --sh command to calculate size of .synapseCache.

    Args:
        directory (str, optional): .synapseCache directory. Defaults to '/root/.synapseCache'

    Returns:
        float or integer: returns size of .synapsecache directory in bytes
    """
    # Note: this command might fail on windows user. But since this command is primarily for running on AWS, it is fine. 
    command = ['du', '-sh', directory]
    output = subprocess.run(command, capture_output=True).stdout.decode('utf-8')
    
    # Parsing the output to extract the directory size
    size = output.split('\t')[0]
    if "K" in size:
        size_in_kb = float(size.rstrip('K'))
        byte_size = size_in_kb * 1000
    elif "M" in size:
        size_in_mb = float(size.rstrip('M'))
        byte_size = size_in_mb * 1000000
    elif "G" in size: 
        size_in_gb = float(size.rstrip('G'))
        byte_size = convert_gb_to_bytes(size_in_gb)
    elif "B" in size:
        byte_size = float(size.rstrip('B'))
    else:
        logger.error('Cannot recongize the file size unit')
    return byte_size

def clear_synapse_cache(cache: cache.Cache, minutes: int) -> int:
    """clear synapse cache before a certain time

    Args:
        cache: an object of synapseclient Cache.
        minutes (int): all files before this minute will be removed
    Returns:
        int: number of files that get deleted
    """
    current_date = datetime.utcnow()
    minutes_earlier = calculate_datetime(input_date=current_date, minutes=minutes, before_or_after="before")
    num_of_deleted_files = cache.purge(before_date = minutes_earlier)
    return num_of_deleted_files

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

def normalize_path(path: str, parent_folder: str) -> str:
    """
    Normalizes a path.
    If the path is relative, the parent_folder is added to make it an absolute path.

    Args:
        path (str): The path to the file to normalize.
        parent_folder (str): The folder the file is in.

    Returns:
        str: The normalized path.
    """
    if not os.path.isabs(path):
        path = os.path.join(parent_folder, path)
    return os.path.normpath(path)
