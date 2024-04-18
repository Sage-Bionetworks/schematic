"""
Constructs release version based on todays date and number of release already this month.
Changes pyproject.toml file to have thew release date
"""

import os
from datetime import date
import json
import re

import toml


TODAY = date.today()
TODAYS_YEAR = str(TODAY.year)[-2:]
TODAYS_MONTH = str(TODAY.month)

tags = os.getenv("TAGS")
assert isinstance(tags, str)
tag_list = json.loads(tags)
assert isinstance(tag_list, list)

NUM_VERSIONS = 0
ref_list: list[str] = [tag_dict["ref"] for tag_dict in tag_list]
for ref in ref_list:
    version = os.path.basename(ref)
    if re.match("^v[0-9]+\.[0-9]+\.[0-9]+$", version):
        NUM_VERSIONS += 1

RELEASE_VERSION = f"{TODAYS_YEAR}.{TODAYS_MONTH}.{NUM_VERSIONS + 1}"

data = toml.load("pyproject.toml")
data['tool']['poetry']['version']=RELEASE_VERSION
print('the version number of this release is: ', RELEASE_VERSION)

f = open("pyproject.toml",'w')
toml.dump(data, f)
f.close()
