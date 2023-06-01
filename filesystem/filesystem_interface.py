import io
import os
from abc import ABC, abstractmethod

from .util import chunkedByteStreamPipe


class FileSystem(ABC):
    """A FileSystem abstraction for working against different backends.

    The FileSystem has the familiar tree structure of files and directories,
    but there is no concept of a working directory, and therefore no concept
    of a relative path (with respect to the working directory).

    The API is purposefully minimalistic to simplify adding new backends.

    From the point of view of the client of a FileSystem object, all paths
    are absolute (whether they start with the 'sep' or not). One of the
    things the FileSystem implementation must do behind the scenes is to
    translate between its absolute paths and paths of the underlying backend.

    Note: Currently, there's the limitation that os.sep == "/".
    """

    sep = "/"
    """ The path separator for this FileSystem. """

    CHUNK_SIZE = 10 * 1024**2
    """ The number of bytes to read at once from a stream. """

    def __init__(self):
        if os.sep != FileSystem.sep:
            raise Exception(
                f"FileSystem only works on systems where os.sep == '{FileSystem.sep}'"
            )

    @property
    def isReadOnly(self):
        return False

    @staticmethod
    def _stripSeps(path: str):
        return path.lstrip(FileSystem.sep).rstrip(FileSystem.sep)

    @staticmethod
    def joinPaths(root, *paths) -> str:
        """Join the given paths."""
        joined = os.path.join(root, *(FileSystem._stripSeps(path) for path in paths)).rstrip(
            FileSystem.sep
        )
        normalized = os.path.normpath(joined)
        if normalized == ".":
            return ""
        else:
            return normalized

    @staticmethod
    def _normalizePath(path):
        path = FileSystem._stripSeps(path)
        if path == "":
            return ""

        normalizedPath = FileSystem._stripSeps(os.path.relpath(path))
        if normalizedPath.startswith(".."):
            raise OSError(f"Invalid path: {path}")
        return normalizedPath

    @staticmethod
    def splitPath(path):
        """Return a vector of directory names or [] if path==""."""
        normPath = FileSystem._normalizePath(path)
        pathVec = normPath.split(FileSystem.sep)
        return [p for p in pathVec if p != ""]

    @staticmethod
    def basename(path):
        return os.path.basename(path)

    @staticmethod
    def dirname(path):
        return os.path.dirname(path)

    def iterateFiles(self, prefix="", subpathFilter=None, returnModtimesAndSizes=False):
        """Return an iterator over 'listFiles'.

        Directories are not returned.

        Note: Implementations of the FileSystem interface should
        override the implementation if it can be done more efficiently.

        If 'subpathFilter' is passed, it must accept any path for a file or a directory,
        and if it returns 'False', that path (or directory) will be suppressed.

        If 'returnModtimesAndSizes', then return pairs of (key, modTimestamp, fileSize).
        """
        for x in self.listFiles(prefix=prefix):
            if subpathFilter is None or subpathFilter(x):
                if returnModtimesAndSizes:
                    yield x, self.getmtime(x), self.getsize(x)
                else:
                    yield x

    def listFiles(self, prefix=""):
        """Returns a list of all the files that start with the given prefix.

        Directories are not returned.

        Note: Implementations of the FileSystem interface should
        override the implementation if it can be done more efficiently.
        """
        prefix = prefix.lstrip(FileSystem.sep)
        parts = prefix.split(FileSystem.sep)

        if self.isdir(prefix):
            path = prefix
        elif len(parts) > 1:
            path = FileSystem.sep.join(parts[:-1])
        else:
            path = ""

        return [
            x
            for x in self.listdir(path, recursive=True)
            if x.startswith(prefix) and self.isfile(x)
        ]

    def listSubdirs(self, path="", recursive=False):
        """Returns all the directories under path

        Raises an OSError if path is not a directory.
        Files are not returned

        Note: Implementations of the FileSystem interface should
        override the implementation if it can be done more efficiently.
        """
        return [x for x in self.listdir(path, recursive=recursive) if self.isdir(x)]

    def stat(self, path):
        """Returns a dictionary of info about the file

        Raises an OSError if the path does not exist

        Note: Implementations of the Filesystem interface should
        override the implementation if it can be done more efficiently.
        """
        return {"modtime": self.getmtime(path), "size": self.getsize(path)}

    @staticmethod
    def withTrailingSep(path: str):
        return path if not path or path[-1] == FileSystem.sep else path + FileSystem.sep

    @staticmethod
    def _checkByteStreamForGet(byteStream):
        """raise exceptions if byteStream is not seekable or not at position 0.

        implementations of getInto that go over the network and have retry-logic
        need to be able to reset the byteStream in case an attempt fails half
        way through.
        """
        if not hasattr(byteStream, "seekable") or not byteStream.seekable():
            raise TypeError(
                f"Cannot get into byte-stream that is not seekable: {type(byteStream)}"
            )
        if byteStream.tell() != 0:
            raise ValueError("Cannot get into byte-stream that is not at position 0")

    @staticmethod
    def _checkContentInputTypeForSet(content):
        if not isinstance(content, (bytes, io.IOBase)):
            raise TypeError(f"Invalid type for content argument: {type(content)}")

    @staticmethod
    def _checkByteStreamForSet(byteStream):
        """raise exceptions if byteStream is not seekable or not at position 0.

        implementations of set that go over the network and have retry-logic
        need to be able to reset the byteStream in case an attempt fails half
        way through.
        """
        if not hasattr(byteStream, "seekable") or not byteStream.seekable():
            raise TypeError(
                f"Cannot set from byte-stream that is not seekable: {type(byteStream)}"
            )
        if byteStream.tell() != 0:
            raise ValueError("Cannot set from byte-stream that is not at position 0")

    @staticmethod
    def chunkedByteStreamPipe(inStr, outStr, amount=-1, chunkSize=None):
        if chunkSize is None:
            chunkSize = FileSystem.CHUNK_SIZE
        return chunkedByteStreamPipe(inStr, outStr, amount=amount, chunkSize=chunkSize)

    @abstractmethod
    def exists(self, path) -> bool:
        """True if the path exists, False otherwise."""
        pass

    @abstractmethod
    def isdir(self, path) -> bool:
        """True if path exists and is a directory, False otherwise."""
        pass

    @abstractmethod
    def isfile(self, path) -> bool:
        """True if path exists and is a file, False otherwise."""
        pass

    @abstractmethod
    def getmtime(self, path) -> float:
        """Return the timestamp of the last modification for the path.

        Raises OSError if the file does not exist or is inaccessible.
        """
        pass

    @abstractmethod
    def getsize(self, path) -> int:
        """Return the size in bytes of path.

        Raise OSError if the file does not exist or is inaccessible.
        """
        pass

    @abstractmethod
    def listdir(self, path="", *, recursive=False, maxEntries=None):
        """Return a list of paths

        Raises OSError if the file is not a directory or is inaccessible.

        Note: unlike os.listdir(path) which returns the contents of the
        given directory, this will return "full paths", in other words
        all results will start with `path`.

        Args:
            path (str): list the contents of this path. If it's not a
                directory, an OSError will be raised

            recursive (bool): if false only list the contents of path;
                if true also list the contents of any subdirectories
                recursively providing relative paths

            maxEntries (None|int): the maximum number of entries to
                return, or None if you want all of them. Some implementations
                can take advantage of this field for performance reasons
        """
        pass

    @abstractmethod
    def get(self, path) -> bytes:
        """Get the contents of a file

        Raises OSError if the path is not a valid file or is inaccessible.
        """
        pass

    @abstractmethod
    def getInto(self, path, byteStream):
        """Get the contents of a file into given bytesStream

        Args:
            path (str): path to the file
            byteStream (BytesIO, or binary file descriptor): byteStream into
                which to write the result*.

        *ATTENTION: contents in byteStream will be overwritten.
        Many implementations have retry logic to mitigate network errors.
        If the byteStream is seekable (which BytesIO and files open with "wb" are),
        this method will rewind the stream to its beginning, i.e., seek(0) to
        enable the retry logic.

        Raises OSError if the path is not a valid file or is inaccessible.
        """
        pass

    @abstractmethod
    def set(self, path, content) -> None:
        """Create or overwrite a file at path with provided content

        Args:
            path (str): path to the file
            content (bytes or io.BufferedIO): the data to write, which must be
                at position==0 if it is a stream (content.tell() == 0)

        Raises OSError if the path cannot be created or overwritten
        """
        pass

    @abstractmethod
    def rm(self, path) -> None:
        """Remove the given file or directory.

        Raises OSError if the file is inaccessible, it does not exist,
        or it is a non-empty directory.
        """
        pass

    @abstractmethod
    def __eq__(self, other) -> bool:
        pass

    @abstractmethod
    def __str__(self):
        pass

    @abstractmethod
    def __hash__(self):
        pass

    def tearDown(self):
        pass


class ReadOnlyFileSystem(FileSystem):
    """A read-only version of the FileSystem interface (abstract).

    A FileSystem implementation can inherit from this class instead of FileSystem

      class DatasetProviderFileSystem(ReadOnlyFileSystem):
          ...

      In this case you must remember to implement __str__, and __eq__, because the
      implementation provided by ReadOnlyFileSystem will not be appropriate.
    """

    @property
    def isReadOnly(self):
        return True

    def set(self, path, content):
        raise OSError("Read-Only Filesystem does not allow 'set'")

    def rm(self, path):
        raise OSError("Read-Only Filesystem does not allow 'rm'")
