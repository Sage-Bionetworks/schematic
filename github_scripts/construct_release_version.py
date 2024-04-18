"""
This is a command line script meant to be used by a github action.
It constructs the current release tag using todays date, and the number of releases 
  already this month.
For example the first release of January 2024 would be 24.1.1 and the second would be 24.1.2
It assumes there is an environmental variable set called TAGS that contains the tag 
 information from the repo.
"""
import os
from datetime import date
import json
import re
from typing import Any


def get_date_string() -> tuple[str, str]:
    """Gets todays year and month in two digit form as strings

    Returns:
        tuple[int, int]: The year and month in two digit form ie. ("24", "12")
    """
    todays_date = date.today()
    todays_year = str(todays_date.year)[-2:]
    todays_month = str(todays_date.month)
    return todays_year, todays_month


def get_num_tags_this_month(tags: list[dict[str, Any]], todays_year: str, todays_month: str) -> int:
    """
    This takes a list of tags from github, and returns the number of tags from this month.

    Args:
        tags (list[dict[str, Any]]): 
            [
                {
                    "ref": "refs/tags/v24.2.1",
                },
                    "ref": "refs/tags/v24.2.1-beta",
                }
            ]
        todays_year(str): todays year as a two digit string ie. 2024: "24"
        todays_month(str): todays month as a one or two digit string ie. December: "12"

    Returns:
        int: Ther number of tags already released this month
    """
    tags_this_month:int = 0
    # ref field is a string such as "refs/tags/v0.1.1"
    ref_list: list[str] = [tag_dict["ref"] for tag_dict in tags]
    for ref in ref_list:
        version = os.path.basename(ref)
        if re.match("^v[0-9]+\.[0-9]+\.[0-9]+$", version):
            version = version[1:]
            version_year, version_month, version_number = version.split(".")
            if version_year == todays_year and version_month == todays_month:
                tags_this_month = max(int(version_number), tags_this_month)
    return tags_this_month

tag_string = os.getenv("TAGS")
assert isinstance(tag_string, str)
tag_list = json.loads(tag_string)
assert isinstance(tag_list, list)

year, month = get_date_string()
num_tags_this_month = get_num_tags_this_month(tag_list, year, month)


print (f"{year}.{month}.{num_tags_this_month + 1}")
