"""
This is a command line script meant to be used by a github action.
It modifies the pyproject.toml file to have the current release version.
Poetry use the toml version as the version it publishes to pypi.
It assumes there is an environmental variable set called RELEASE_VERSION 
  that contains the release version to use.
"""

import os
import toml

RELEASE_VERSION = os.getenv("RELEASE_VERSION")
assert isinstance(RELEASE_VERSION, str)

data = toml.load("pyproject.toml")
data['tool']['poetry']['version']=RELEASE_VERSION
print('the version number of this release is: ', RELEASE_VERSION)

with open("pyproject.toml",'w', encoding="utf-8") as file:
    toml.dump(data, file)
