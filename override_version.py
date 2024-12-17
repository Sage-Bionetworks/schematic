import toml
import sys

if len(sys.argv) != 2:
    print("Usage: python override_version.py VERSION")
    sys.exit(1)

RELEASE_VERSION = sys.argv[1]

data = toml.load("pyproject.toml")
# Modify field
data["tool"]["poetry"]["version"] = RELEASE_VERSION
print("the version number of this release is: ", RELEASE_VERSION)
# override and save changes
with open("pyproject.toml", "w") as f:
    toml.dump(data, f)
