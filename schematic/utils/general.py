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

def get_dir_size(path='.'):
    '''
    calculate total size of a directory
    args: 
    path: path to a folder or directory
    '''
    total = 0
    # Example usage of os.scandir could be found here: https://docs.python.org/3/library/os.html#os.scandir
    # Technically, scandir.close() is called automatically. But it is still advisible to call it explicitly or use the with statement. 
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(entry.path)
    return total

def convert_size(size_bytes):
    '''
    convert bytes to a human readable format
    '''
    if size_bytes == 0:
       return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])