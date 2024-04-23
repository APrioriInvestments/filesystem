import random
import string
import time
from enum import Enum

from .filesystem_interface import FileSystem


class _PathKind(Enum):
    FILE = 1
    DIR = 2


class _Metadata:
    def __init__(self, kind: _PathKind, modtime: float, size: int, contents):
        self.kind = kind
        self.modtime = modtime
        self.size = size
        self.contents = contents

    @staticmethod
    def _newDir():
        return _Metadata(kind=_PathKind.DIR, modtime=time.time(), size=4096, contents={})

    @staticmethod
    def _newFile(data: bytes):
        return _Metadata(
            kind=_PathKind.FILE, modtime=time.time(), size=len(data), contents=data
        )


class InMemFileSystem(FileSystem):
    ROOT = None
    # should be ROOT = _Metadata._newDir() but would cause
    # test_codebase_hashes_stable_across_invocation and
    # test_codebase_hashes_stable_across_relocation to fail

    @staticmethod
    def getRandomTempDir():
        return "".join(random.choice(string.ascii_letters) for i in range(10))

    def __init__(self, rootPath=None):
        """An in-memory simulated FileSystem for unit-tests.

        Args:
            rootPath(str): the root directory (allows creating nested FileSystems);
                None creates a fresh 'temporary directory rootPath'.
            clock(Clock): an optional clock; if None, time is used.
            filesystem(InMemFileSystem): optionally instantiate in an existing InMemFileSystem.
        """
        super().__init__()

        if InMemFileSystem.ROOT is None:
            InMemFileSystem.ROOT = _Metadata._newDir()

        self._filesystem = InMemFileSystem.ROOT

        if rootPath is None:
            self._isFresh = True
            newDir = _Metadata._newDir()

            self._filesystem = self.ROOT
            self._filesystem = self._walkTo("/tmp", createAsWeGo=True)
            tmpdir = self.getRandomTempDir()
            while tmpdir in self._filesystem.contents:
                tmpdir = self.getRandomTempDir()
            self._filesystem.contents[tmpdir] = newDir
            self._filesystem = newDir
            self._rootPath = self.joinPaths("/tmp", tmpdir)

        else:
            self._isFresh = False
            self._filesystem = self._walkTo(rootPath, createAsWeGo=True)
            self._rootPath = rootPath

    @property
    def rootPath(self):
        return self._rootPath

    def __eq__(self, other):
        if not isinstance(other, InMemFileSystem):
            return False

        return self.rootPath == other.rootPath

    def __hash__(self):
        return hash(("InMemFileSystem", self.rootPath))

    def tearDown(self):
        if self._filesystem.contents and self._isFresh:
            self._filesystem.contents = {}

    def __del__(self):
        self.tearDown()

    def _walkTo(self, pathVec, createAsWeGo=False):
        """Return the metadata for the given path.

        Raises OSError if the path does not exist.

        Args:
            pathVec(str or ListOf(str)): the path we want to walk to.
        """
        if isinstance(pathVec, str):
            pathVec = self.splitPath(pathVec)

        segmentMetadata = self._filesystem
        for segment in pathVec:
            if segmentMetadata.kind != _PathKind.DIR:
                raise OSError(
                    f"Invalid path '{self.joinPaths(*pathVec)}' "
                    f"at '{segment}' (not a directory)"
                )
            fs = segmentMetadata.contents
            if segment not in fs:
                if createAsWeGo:
                    fs[segment] = _Metadata._newDir()
                else:
                    raise OSError(f"Path does not exist: '{self.joinPaths(*pathVec)}'")

            segmentMetadata = fs[segment]

        return segmentMetadata

    def exists(self, path) -> bool:
        try:
            self._walkTo(path)
            return True
        except OSError:
            return False

    def isdir(self, path) -> bool:
        try:
            meta = self._walkTo(path)
            return meta.kind == _PathKind.DIR
        except OSError:
            return False

    def isfile(self, path) -> bool:
        try:
            meta = self._walkTo(path)
            return meta.kind == _PathKind.FILE
        except OSError:
            return False

    def getmtime(self, path) -> float:
        return self._walkTo(path).modtime

    def setmtime(self, path, modtime: float):
        self._walkTo(path).modtime = modtime

    def getsize(self, path) -> int:
        return self._walkTo(path).size

    def _listdir(self, result, path, pathMeta, recursive, maxEntries=None):
        for file, meta in pathMeta.contents.items():
            if maxEntries is not None and len(result) >= maxEntries:
                return result

            filePath = self.joinPaths("", path, file)
            result.append(filePath)
            if meta.kind == _PathKind.DIR and recursive:
                self._listdir(result, filePath, meta, recursive, maxEntries=maxEntries)

        return result

    def listdir(self, path="", *, recursive=False, maxEntries=None):
        pathMeta = self._walkTo(path)
        if pathMeta.kind != _PathKind.DIR:
            raise OSError(f"Not a directory '{path}'")

        result = []
        self._listdir(result, path, pathMeta, recursive, maxEntries)
        return result

    def get(self, path) -> bytes:
        meta = self._walkTo(path)
        if meta.kind != _PathKind.FILE:
            raise OSError(f"Path is not a file: '{path}")

        return meta.contents

    def getInto(self, path, byteStream):
        meta = self._walkTo(path)
        if meta.kind != _PathKind.FILE:
            raise OSError(f"Path is not a file: '{path}")

        byteStream.write(meta.contents)

    def set(self, path, content) -> None:
        if not path:
            raise OSError(f"Cannot set '{path}'")

        pathVec = self.splitPath(path)
        parentDir = self._walkTo(pathVec[:-1], createAsWeGo=True)
        filename = pathVec[-1]

        if isinstance(content, bytes):
            pass

        elif hasattr(content, "read"):
            # content must be a byte-stream
            content = content.read()

        else:
            raise TypeError(f"Invalid type for content argument: {type(content)}")

        parentDir.contents[filename] = _Metadata._newFile(content)

    def rm(self, path) -> None:
        if not path:
            raise OSError(f"Cannot remove '{path}'")

        pathVec = self.splitPath(path)
        parentDirMeta = self._walkTo(pathVec[:-1])
        filename = pathVec[-1]

        if parentDirMeta.kind != _PathKind.DIR:
            raise OSError(f"Failed to remove '{path}': does not exist")
        if filename not in parentDirMeta.contents:
            raise OSError(f"Failed to remove '{path}': does not exist")

        fileMeta = parentDirMeta.contents[filename]

        if fileMeta.kind == _PathKind.FILE or fileMeta.contents == {}:
            del parentDirMeta.contents[filename]
        else:
            raise OSError(f"Failed to remove '{path}': Directory not empty")

    def __str__(self):
        return f"InMemFileSystem(isFresh={self._isFresh}, rootPath='{self.rootPath}')"
