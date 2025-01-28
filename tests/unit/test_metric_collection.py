from re import search
from click.testing import CliRunner
import pytest


@pytest.fixture
def command_line_user_agent_pattern():
    yield "schematiccommandline" + "/(\\S+)"


@pytest.fixture
def library_user_agent_pattern():
    yield "schematic" + "/(\\S+)"


class TestUserAgentString:
    def test_user_agent_string(
        self,
        library_user_agent_pattern,
        command_line_user_agent_pattern,
    ):
        # GIVEN the default USER_AGENT string from the synapse client
        from synapseclient import USER_AGENT

        # WHEN schematic is imported to be used as a library
        from schematic.__main__ import main

        # THEN the User-Agent string should be updated to include the schematic library client string
        assert search(library_user_agent_pattern, USER_AGENT["User-Agent"])

        # AND when the command line is used to execute commands
        runner = CliRunner()
        result = runner.invoke(main)

        # THEN the User-Agent string should be updated to include the schematic command line client string
        assert search(command_line_user_agent_pattern, USER_AGENT["User-Agent"])
