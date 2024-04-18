""" Tests for github scripts"""

from datetime import date

from github_scripts.utils import (
    get_date_strings,
    get_version_list_from_tags,
    get_number_of_versions_this_month,
)

def test_get_date_string() -> None:
    """Tests get_date_string"""
    year, month = get_date_strings()
    assert len(year) == 2
    assert 100 > int(year) >= 0
    assert 3 > len(month) > 0
    assert 13 > int(month) > 0

    year, month = get_date_strings(date(2020, 1, 1))
    year = "20"
    month = "1"

    year, month = get_date_strings(date(1999, 12, 10))
    year = "99"
    month = "12"

def test_get_version_list_from_tags() -> None:
    """Tests get_version_list_from_tags"""
    tags = (
        '['
            '{"ref": "x/x/v24.1.1"},'
            '{"ref": "x/x/v24.1.2"},'
            '{"ref": "x/x/v24.1.3"}'
        ']'
    )
    assert get_version_list_from_tags(tags) == ["v24.1.1", "v24.1.2", "v24.1.3"]

def test_get_number_of_versions_this_month() -> None:
    """Tests get_number_of_versions_this_month"""
    versions = ["v24.1.1", "v24.1.2", "v24.1.3"]
    assert get_number_of_versions_this_month(versions, "24", "1") == 3
    assert get_number_of_versions_this_month(versions, "23", "1") == 0
    assert get_number_of_versions_this_month(versions, "24", "0") == 0
    versions2 = ["24.1.1", "v24.1.1-dev"]
    assert get_number_of_versions_this_month(versions2, "24", "1") == 0
