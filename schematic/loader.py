"""Loader"""
from typing import Any
from collections.abc import Iterable
from errno import ENOENT
from os import pathsep
from re import split
from pkg_resources import (
    resource_exists,
    resource_filename,
    resource_stream,
    resource_string,
    resource_listdir,
)


class InvalidResourceError(Exception):
    """
    Args:
        uri {String}: The URI which was requested within the given loader's
        that did not exist or was malformed.
    """

    def __init__(self, namespace: str, requested_uri: str) -> None:
        self.namespace = namespace
        self.requested_uri = requested_uri
        self.message = "Resource does not exist or is declared incorrectly"
        self.errno = ENOENT
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f'{self.message}({self.errno}), "{self.requested_uri}" of {self.namespace}'
        )

    def __repr__(self) -> str:
        return self.__str__()


class Loader:
    """
    Args:
    namespace {String}: The namespace within the package (relative to the package root)
    to load resources from. Using the magic variable __name__ is suggested as when the script
    is run as "__main__" it will load the most recent local resources instead of the cached
    egg resources.

    prefix {String}: Set a prefix for all URIs. Use a prefix if resources are centrally
    located in a single place the uri's will be prefixed automatically by the loader.
    """

    def __init__(self, namespace: str, **opts: Any) -> None:
        self.namespace = namespace
        self.prefix = opts.get("prefix", "")
        self.local = opts.get("local", False)

        if not self.local:
            self.namespace = split(r"\.|\\|\/", self.namespace)[0]

    def _resolve(self, uri: str) -> tuple[str, str]:
        resource_uri = "/".join([self.prefix] + uri.split(pathsep))
        namespace = self.namespace

        if not resource_exists(namespace, resource_uri):
            raise InvalidResourceError(namespace, resource_uri)

        return namespace, resource_uri

    def read(self, uri: str) -> Any:
        """
        Read entire contents of resource. Same as open('path...').read()

        Args:
            uri {String}: URI of the resource.
        """
        namespace, uri = self._resolve(uri)
        return resource_string(namespace, uri)

    def open(self, uri: str) -> Any:
        """
        Open a file object like handle to the resource. Same as open('path...')

        Args:
            uri {String}: URI of the resource.
        """
        namespace, uri = self._resolve(uri)
        return resource_stream(namespace, uri)

    def filename(self, uri: str) -> str:
        """
        Return the "most correct" filename for a resource. Same as os.path.normpath('path...')

        Args:
            uri {String}: URI of the resource.
        """
        namespace, uri = self._resolve(uri)
        return resource_filename(namespace, uri)

    def list(self, url: str) -> Iterable[str]:
        """
        Return a list of all resources within the given URL

        Args:
            url {String}: URL of the resources.
        """
        namespace, uri = self._resolve(url)
        return map(lambda x: url + "/" + x, resource_listdir(namespace, uri))


# call Loader() and pass `schematic`, which is the global package namespace
LOADER = Loader("schematic", prefix="etc")
