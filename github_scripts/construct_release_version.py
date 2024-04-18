"""
This is a command line script meant to be used by a github action.
It constructs the current release tag using todays date, and the number of releases 
  already this month.
For example the first release of January 2024 would be 24.1.1 and the second would be 24.1.2
It assumes there is an environmental variable set called TAGS that contains the tag 
 information from the repo.
"""
import os

from github_scripts.utils import (
    get_date_strings,
    get_number_of_versions_this_month,
    get_version_list_from_tags,
)

tag_string = os.getenv("TAGS")
assert isinstance(tag_string, str)

year, month = get_date_strings()
version_list = get_version_list_from_tags(tag_string)
num_tags_this_month = get_number_of_versions_this_month(version_list, year, month)


#print (f"{year}.{month}.{num_tags_this_month + 1}")
print("1.1.1-test")
