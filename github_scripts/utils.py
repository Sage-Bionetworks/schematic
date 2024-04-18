"""Util functions for github scripts"""

import os
from datetime import date
import re
from typing import Optional
import json


def get_date_strings(todays_date:Optional[date]=None) -> tuple[str, str]:
    """Gets todays year and month in form as strings

    Args:
        todays_date (Optional[date], optional): Defaults to None.

    Returns:
        tuple[str, str]: The year and month in two digit form ie. ("24", "12")
    """
    if todays_date is None:
        todays_date = date.today()
    todays_year = str(todays_date.year)[-2:]
    todays_month = str(todays_date.month)
    return todays_year, todays_month

def get_version_list_from_tags(tag_string:str) -> list[str]:
    """Gets the versions from a list fo tags in string form

    Args:
        tag_string (str): A json in string form that contians tag information such as:
            '[
                {
                    "ref": "refs/tags/v24.2.1",
                },
                    "ref": "refs/tags/v24.2.1-beta",
                }
            ]'

    Returns:
        list[str]: A list of versions from each tag
    """
    tag_dict_list = json.loads(tag_string)
    assert isinstance(tag_dict_list, list)
    for tag in tag_dict_list:
        assert isinstance(tag, dict)
        assert "ref" in tag
        assert isinstance(tag["ref"], str)
    ref_list: list[str] = [tag_dict["ref"] for tag_dict in tag_dict_list]
    version_list = [os.path.basename(ref) for ref in ref_list]
    return version_list


def get_number_of_versions_this_month(
    version_list: list[str],
    todays_year: str,
    todays_month: str
) -> int:
    """
    This takes a list of version and returns the number of tags from this month.

    Args:
        version_list (list[str]): A list of tag version such as "v24.2.1"
        todays_year(str): todays year as a two digit string ie. 2024: "24"
        todays_month(str): todays month as a one or two digit string ie. December: "12"

    Returns:
        int: Ther number of tags already released this month
    """
    tags_this_month:int = 0
    for version in version_list:
        if re.match("^v[0-9]+\.[0-9]+\.[0-9]+$", version):
            version = version[1:]
            version_year, version_month, version_number = version.split(".")
            if version_year == todays_year and version_month == todays_month:
                tags_this_month = max(int(version_number), tags_this_month)
    return tags_this_month
