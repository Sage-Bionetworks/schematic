import toml
import os 

data = toml.load("pyproject.toml")
#get release version 
# RELEASE_VERSION = os.getenv('RELEASE_VERSION')
# temporarily hard coded release version 
RELEASE_VERSION = 'v23.11.dev2'
# Modify field
data['tool']['poetry']['version']=RELEASE_VERSION
print('the version number of this release is: ', RELEASE_VERSION)
#override and save changes
f = open("pyproject.toml",'w')
toml.dump(data, f)
f.close()