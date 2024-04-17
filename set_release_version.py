"""
Constructs release version based on todays date and number of release already this month.
Changes pyproject.toml file to have thew release date
"""

import os
from datetime import date
import json
import toml
import re

TODAY = date.today()
TODAYS_YEAR = str(TODAY.year)[-2:]
TODAYS_MONTH = str(TODAY.month)

tags = os.getenv("TAGS")
assert isinstance(tags, str)
tag_list = json.loads(tags)
assert isinstance(tag_list, list)

ref_list: list[str] = [tag_dict["ref"] for tag_dict in tag_list]
for ref in ref_list:
    version = os.path.basename(ref)
    print(version)
    print(re.match("^v[0-9]+\.[0-9]+\.[0-9]+$", version))




'''
version_strings = [os.path.basename(tag_dict["ref"]) for tag_dict in tag_list]
NUM_VERSIONS = 0
for string in version_strings:
    year, month, number = string.split(".")
    if year == TODAYS_YEAR and month == TODAYS_MONTH:
        NUM_VERSIONS += 1

RELEASE_VERSION = f"{TODAYS_YEAR}.{TODAYS_MONTH}.{NUM_VERSIONS + 1}"

data = toml.load("pyproject.toml")
data['tool']['poetry']['version']=RELEASE_VERSION
print('the version number of this release is: ', RELEASE_VERSION)

f = open("pyproject.toml",'w')
toml.dump(data, f)
f.close()
'''
