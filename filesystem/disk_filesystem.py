import os
import tempfile

from .filesystem_interface import FileSystem


class DiskFileSystem(FileSystem):
    def __init__(self, rootPath):
        super().__init__()
        if not os.path.exists(rootPath):
            os.makedirs(rootPath, exist_ok=True)
        if not os.path.isdir(rootPath):
            raise Exception(f"rootPath='{rootPath}' is not a directory")

        self._rootPath = os.path.abspath(rootPath)

    @property
    def rootPath(self):
        return self._rootPath

    def __eq__(self, other):
        if not isinstance(other, DiskFileSystem):
            return False
        return self.rootPath == other.rootPath

    def __hash__(self):
        return hash(("DiskFileSystem", self.rootPath))

    def _rooted(self, path):
        rootedPath = self.joinPaths(self._rootPath, path)
        if not rootedPath.startswith(self._rootPath):
            raise Exception(f"Unsafe path detected: '{path}'")
        return rootedPath

    def exists(self, path) -> bool:
        return os.path.exists(self._rooted(path))

    def isdir(self, path) -> bool:
        return os.path.isdir(self._rooted(path))

    def isfile(self, path) -> bool:
        return os.path.isfile(self._rooted(path))

    def getmtime(self, path) -> float:
        return os.path.getmtime(self._rooted(path))

    def getsize(self, path) -> int:
        return os.path.getsize(self._rooted(path))

    def listdir(self, path="", *, recursive=False, maxEntries=None):
        result = []
        self._listdir(result, path, recursive, maxEntries)
        return sorted(result)

    def _listdir(self, result, path, recursive, maxEntries=None):
        for file in os.listdir(self._rooted(path)):
            if maxEntries is not None and len(result) >= maxEntries:
                return

            filePath = self.joinPaths("", path, file)
            result.append(filePath)
            if recursive and self.isdir(filePath):
                self._listdir(result, filePath, recursive=recursive)

    def get(self, path) -> bytes:
        if not self.exists(path):
            raise OSError(f"File not found: '{path}'")

        if not self.isfile(path):
            raise OSError(f"Not a file: '{path}'")

        with open(self._rooted(path), "rb") as f:
            return f.read()

    def getInto(self, path, byteStream):
        if not self.exists(path):
            raise OSError(f"File not found: '{path}'")

        if not self.isfile(path):
            raise OSError(f"Not a file: '{path}'")

        with open(self._rooted(path), "rb") as file:
            self.chunkedByteStreamPipe(file, byteStream)

    @staticmethod
    def safeOpen(path, mode="wb"):
        """ensure parent directory exists before writing to a file."""
        dirname = os.path.split(path)[0]
        try:
            os.makedirs(dirname)
        except OSError:
            pass

        return open(path, mode)

    def set(self, path, content) -> None:
        if isinstance(content, bytes):
            with self.safeOpen(self._rooted(path)) as file:
                file.write(content)
        else:
            with self.safeOpen(self._rooted(path)) as file:
                self.chunkedByteStreamPipe(content, file)

    def rm(self, path) -> None:
        path = self._stripSeps(path)
        if not path:
            raise OSError(f"Cannot remove '{path}'")

        if not self.exists(path):
            raise OSError(f"Failed to remove '{path}': does not exist")

        if self.isfile(path):
            os.remove(self._rooted(path))

        elif self.isdir(path):
            os.rmdir(self._rooted(path))

        else:
            raise OSError(f"Unexpected path is neither a file not a directory: {path}")

    def __str__(self):
        return f"DiskFileSystem(rootPath='{self.rootPath}')"


class TempDiskFileSystem(DiskFileSystem):
    def __init__(self):
        self._tmpDir = tempfile.TemporaryDirectory()
        super().__init__(self._tmpDir.name)

    def tearDown(self):
        if self._rootPath is not None:
            self._tmpDir.cleanup()
            self._rootPath = None

    def __del__(self):
        self.tearDown()
