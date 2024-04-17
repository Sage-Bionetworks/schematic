"""
Constructs release version based on todays date and number of release already this month.
Changes pyproject.toml file to have thew release date
"""

import os
from datetime import date

import toml

TODAY = date.today()
TODAYS_YEAR = str(TODAY.year)[-2:]
TODAYS_MONTH = str(TODAY.month)

tags = os.getenv("TAGS")
print(tags)
print(type(tags))
assert isinstance(tags, list)
for tag in tags:
    assert isinstance(tag, dict)
    assert "ref" in tag
'''
tags = [
    {
        'ref': 'refs/tags/24.4.1',
        'other': "x"
    },
    {
        'ref': 'refs/tags/24.4.2',
        'other': "x"
    },
    {
        'ref': 'refs/tags/24.3.1',
        'other': "x"
    }
]
'''


version_strings = [os.path.basename(tag_dict["ref"]) for tag_dict in tags]
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
