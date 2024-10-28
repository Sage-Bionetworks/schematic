import asyncio
import os
import tempfile

from schematic.utils.general import create_temp_folder
from schematic.utils.io_utils import cleanup_temporary_storage


class TestCleanup:
    async def test_cleanup_temporary_storage_nothing_to_cleanup(self) -> None:
        # GIVEN a temporary folder that has a file that is not older than the time delta
        temp_folder = create_temp_folder(path=tempfile.gettempdir())

        # AND A File that is not older than the time delta
        with open(os.path.join(temp_folder, "file.txt"), "w") as f:
            f.write("hello world")

        assert os.path.exists(temp_folder)
        assert os.path.exists(os.path.join(temp_folder, "file.txt"))

        time_delta_seconds = 3600

        # WHEN I call the cleanup function
        cleanup_temporary_storage(
            temporary_storage_directory=temp_folder,
            time_delta_seconds=time_delta_seconds,
        )

        # THEN the folder should still exist
        assert os.path.exists(temp_folder)

        # AND the file should still exist
        assert os.path.exists(os.path.join(temp_folder, "file.txt"))

    async def test_cleanup_temporary_storage_file_to_cleanup(self) -> None:
        # GIVEN a temporary folder that has a file that will be older than the time delta
        temp_folder = create_temp_folder(path=tempfile.gettempdir())

        # AND A File that is older than the time delta
        with open(os.path.join(temp_folder, "file.txt"), "w") as f:
            f.write("hello world")

        assert os.path.exists(temp_folder)
        assert os.path.exists(os.path.join(temp_folder, "file.txt"))

        time_delta_seconds = 1

        # AND I wait for the time delta
        await asyncio.sleep(time_delta_seconds)

        # WHEN I call the cleanup function
        cleanup_temporary_storage(
            temporary_storage_directory=temp_folder,
            time_delta_seconds=time_delta_seconds,
        )

        # THEN the folder should still exist
        assert os.path.exists(temp_folder)

        # AND the file should not exist
        assert not os.path.exists(os.path.join(temp_folder, "file.txt"))

    async def test_cleanup_temporary_storage_nested_file_to_cleanup(self) -> None:
        # GIVEN a temporary folder that has a file that will be older than the time delta
        temp_folder = create_temp_folder(path=tempfile.gettempdir())

        # AND a nested temporary folder
        temp_folder_2 = create_temp_folder(path=temp_folder)

        # AND A File that is older than the time delta
        with open(os.path.join(temp_folder_2, "file.txt"), "w") as f:
            f.write("hello world")

        assert os.path.exists(temp_folder)
        assert os.path.exists(temp_folder_2)
        assert os.path.exists(os.path.join(temp_folder_2, "file.txt"))

        time_delta_seconds = 1

        # AND I wait for the time delta
        await asyncio.sleep(time_delta_seconds)

        # WHEN I call the cleanup function
        cleanup_temporary_storage(
            temporary_storage_directory=temp_folder,
            time_delta_seconds=time_delta_seconds,
        )

        # THEN the folder should still exist
        assert os.path.exists(temp_folder)

        # AND the nested folder should not exist
        assert not os.path.exists(temp_folder_2)

        # AND the file should not exist
        assert not os.path.exists(os.path.join(temp_folder_2, "file.txt"))
