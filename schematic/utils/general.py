"""General utils"""

# pylint: disable=logging-fstring-interpolation

import logging
import os
import pstats
import subprocess
import tempfile
from cProfile import Profile
from datetime import datetime, timedelta
from functools import wraps
from typing import Union, TypeVar, Any, Optional, Sequence, Callable

from synapseclient.core.exceptions import SynapseHTTPError  # type: ignore
from synapseclient.entity import File, Folder, Project  # type: ignore
from synapseclient.table import EntityViewSchema  # type: ignore
from synapseclient.core import cache  # type: ignore
from synapseclient import Synapse  # type: ignore

logger = logging.getLogger(__name__)

T = TypeVar("T")


def find_duplicates(_list: list[T]) -> set[T]:
    """Find duplicate items in a list"""
    return {x for x in _list if _list.count(x) > 1}


def dict2list(item: Any) -> Optional[Union[dict, list]]:
    """Puts a dictionary into a list

    Args:
        item (Any): Any type of input

    Returns:
        Optional[Union[dict, list]]:
          If input is a list, return it
          If input is a dict, return it in a list
          Return None for anything else
    """
    if isinstance(item, list):
        return item
    if isinstance(item, dict):
        return [item]
    return None


def str2list(item: Any) -> Optional[list]:
    """Puts a string into a list

    Args:
        item (Any): Any type of input

    Returns:
        Optional[list]:
          If input is a list, return it
          If input is a string, return it in a list
          Return None for anything else
    """
    if isinstance(item, str):
        return [item]
    if isinstance(item, list):
        return item
    return None


X = TypeVar("X")


def unlist(seq: Sequence[X]) -> Union[Sequence[X], X]:
    """Returns the first item of a sequence

    Args:
        seq (Sequence[X]): A Sequence of any type

    Returns:
        Union[Sequence[X], X]:
          if sequence is length one, return the first item
          otherwise return the sequence
    """
    if len(seq) == 1:
        return seq[0]
    return seq


def get_dir_size(path: str) -> int:
    """
    Recursively descend the directory tree rooted at the top and call
      .st_size function to calculate size of files in bytes.
    Args:
        path: path to a folder
    return: total size of all the files in a given directory in bytes.
    """
    total = 0
    # Recursively scan directory to find entries
    with os.scandir(path) as itr:
        for entry in itr:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total


def calculate_datetime(
    minutes: int, input_date: datetime, before_or_after: str = "before"
) -> datetime:
    """calculate date time

    Args:
        input_date (datetime): date time object provided by users
        minutes (int): number of minutes
        before_or_after (str): default to "before". if "before", calculate x minutes before
         current date time. if "after", calculate x minutes after current date time.

    Returns:
        datetime:  return result of date time calculation
    """
    if before_or_after == "before":
        date_time_result = input_date - timedelta(minutes=minutes)
    elif before_or_after == "after":
        date_time_result = input_date + timedelta(minutes=minutes)
    else:
        raise ValueError("Invalid value. Use either 'before' or 'after'.")
    return date_time_result


def check_synapse_cache_size(
    directory: str = "/root/.synapseCache",
) -> float:
    """use du --sh command to calculate size of .synapseCache.

    Args:
        directory (str, optional): .synapseCache directory. Defaults to '/root/.synapseCache'

    Returns:
        float: returns size of .synapsecache directory in bytes
    """
    # Note: this command might fail on windows user.
    # But since this command is primarily for running on AWS, it is fine.
    command = ["du", "-sh", directory]
    output = subprocess.run(command, capture_output=True, check=False).stdout.decode(
        "utf-8"
    )

    # Parsing the output to extract the directory size
    size = output.split("\t")[0]
    if "K" in size:
        size_in_kb = float(size.rstrip("K"))
        byte_size = size_in_kb * 1000
    elif "M" in size:
        size_in_mb = float(size.rstrip("M"))
        byte_size = size_in_mb * 1000000
    elif "G" in size:
        size_in_gb = float(size.rstrip("G"))
        byte_size = size_in_gb * (1024**3)
    elif "B" in size:
        byte_size = float(size.rstrip("B"))
    else:
        logger.error("Cannot recognize the file size unit")
    return byte_size


def clear_synapse_cache(synapse_cache: cache.Cache, minutes: int) -> int:
    """clear synapse cache before a certain time

    Args:
        synapse_cache: an object of synapseclient Cache.
        minutes (int): all files before this minute will be removed
    Returns:
        int: number of files that get deleted
    """
    current_date = datetime.utcnow()
    minutes_earlier = calculate_datetime(
        input_date=current_date, minutes=minutes, before_or_after="before"
    )
    num_of_deleted_files = synapse_cache.purge(before_date=minutes_earlier)
    return num_of_deleted_files


def entity_type_mapping(syn: Synapse, entity_id: str) -> str:
    """Return the entity type of manifest

    Args:
        syn (Synapse): Synapse object
        entity_id (str): id of an entity

    Raises:
        SynapseHTTPError: Re-raised SynapseHTTPError

    Returns:
        str: type of the manifest being returned
    """
    # check the type of entity
    try:
        entity = syn.get(entity_id, downloadFile=False)
    except SynapseHTTPError as exc:
        logger.error(
            f"cannot get {entity_id} from asset store. Please make sure that {entity_id} exists"
        )
        raise SynapseHTTPError(
            f"cannot get {entity_id} from asset store. Please make sure that {entity_id} exists"
        ) from exc

    if isinstance(entity, EntityViewSchema):
        entity_type = "asset view"
    elif isinstance(entity, Folder):
        entity_type = "folder"
    elif isinstance(entity, File):
        entity_type = "file"
    elif isinstance(entity, Project):
        entity_type = "project"
    else:
        # if there's no matching type, return concreteType
        entity_type = entity.concreteType
    return entity_type


def create_temp_folder(path: str) -> str:
    """This function creates a temporary directory in the specified directory
    Args:
        path(str): a directory path where all the temporary files will live
    Returns: returns the absolute pathname of the new directory.
    """
    # Create a temporary directory in the specified directory
    path = tempfile.mkdtemp(dir=path)
    return path


def profile(
    output_file: Optional[str] = None,
    sort_by: Any = "cumulative",
    lines_to_print: Optional[int] = None,
    strip_dirs: bool = False,
) -> Callable:
    """
    The function was initially taken from:
    https://towardsdatascience.com/how-to-profile-your-code-in-python-e70c834fad89
    A time profiler decorator.
    Inspired by and modified the profile decorator of Giampaolo Rodola:
    http://code.activestate.com/recipes/577817-profile-decorator/

    Args:
        output_file (Optional[str], optional):
            Path of the output file. If only name of the file is given, it's
            saved in the current directory.
            If it's None, the name of the decorated function is used.
            Defaults to None.
        sort_by (str, optional):
            str or SortKey enum or tuple/list of str/SortKey enum
            Sorting criteria for the Stats object.
            For a list of valid string and SortKey refer to:
            https://docs.python.org/3/library/profile.html#pstats.Stats.sort_stats
            Defaults to "cumulative".
        lines_to_print (Optional[int], optional):
            Number of lines to print.
            This is useful in reducing the size of the printout, especially
            that sorting by 'cumulative', the time consuming operations
            are printed toward the top of the file.
            Default (None) is for all the lines.
        strip_dirs (bool, optional):
            Whether to remove the leading path info from file names.
            This is also useful in reducing the size of the printout
            Defaults to False.

    Returns:
        Callable: Profile of the decorated function
    """

    def inner(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Callable:
            _output_file = output_file or func.__name__ + ".prof"
            profiler = Profile()
            profiler.enable()
            retval = func(*args, **kwargs)
            profiler.disable()
            profiler.dump_stats(_output_file)

            # if we are running the functions on AWS:
            if "SECRETS_MANAGER_SECRETS" in os.environ:
                p_stats = pstats.Stats(profiler)
                # limit this to 30 line for now otherwise it will be too long for AWS log
                p_stats.sort_stats("cumulative").print_stats(30)
            else:
                with open(_output_file, "w", encoding="utf-8") as fle:
                    p_stats = pstats.Stats(profiler, stream=fle)
                    if strip_dirs:
                        p_stats.strip_dirs()
                    if isinstance(sort_by, (tuple, list)):
                        p_stats.sort_stats(*sort_by)
                    else:
                        p_stats.sort_stats(sort_by)
                    p_stats.print_stats(lines_to_print)  # type: ignore
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
