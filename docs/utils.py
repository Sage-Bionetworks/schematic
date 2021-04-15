import re

from typing import List, Dict
from pathlib import Path

import pkg_resources
import toml


def _extract_author_names(authors_list: List[str]) -> str:
    """
    Parse out only names from 'name <email>' list.

    Args:
        authors_list: List of authors in 'name <email>'
        format.

    Returns:
        author_names_csv: String with only the 'name' part
        extracted from 'name <email>'.
    """
    author_names = []

    for author in authors_list:

        # extract name of each author by removing
        # <email> portion from each list item
        name = re.sub(r" \<[^)]*\>", "", author)
        author_names.append(name)

    # create comma separated string from list of authors
    author_names_csv = ", ".join(map(str, author_names))
    return author_names_csv


def _parse_toml(pyproject_path: Path) -> Dict[str, str]:
    """
    Parse pyproject.toml file to extract attribute details.

    Args:
        pyproject_path: Path to pyproject.toml file being used
        by poetry.

    Returns:
        setup_metadata: Dictionary containing metadata like
        name, version and list of authors of the package.
    """
    pyproject_text = pyproject_path.read_text()
    pyproject_data = toml.loads(pyproject_text)

    # read in [tool.poetry] section from pyproject.toml file
    poetry_data = pyproject_data["tool"]["poetry"]

    setup_metadata = {
        "name": poetry_data["name"],
        "version": poetry_data["version"],
        "authors": _extract_author_names(poetry_data["authors"]),
    }

    return setup_metadata
