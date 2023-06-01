from .filesystem_interface import FileSystem, ReadOnlyFileSystem


class WriteProtectedFileSystem(ReadOnlyFileSystem):
    """A wraps a FileSystem instance but disallows external rm and set calls.

    This is useful when we want to use a FileSystem implementation but disallow modifications
    """

    def __init__(self, filesystem):
        super().__init__()

        if not isinstance(filesystem, FileSystem):
            raise TypeError(f"Invalid filesystem argument: {filesystem}")

        self.filesystem = filesystem

    @staticmethod
    def make(filesystemFactory):
        return WriteProtectedFileSystem(filesystemFactory())

    def __eq__(self, other):
        if not isinstance(other, WriteProtectedFileSystem):
            return False
        if self.filesystem != other.filesystem:
            return False
        return True

    def __hash__(self):
        return hash(("WriteProtectedFileSystem", hash(self.filesystem)))

    def exists(self, path) -> bool:
        return self.filesystem.exists(path)

    def isdir(self, path) -> bool:
        return self.filesystem.isdir(path)

    def isfile(self, path) -> bool:
        return self.filesystem.isfile(path)

    def getmtime(self, path) -> float:
        return self.filesystem.getmtime(path)

    def getsize(self, path) -> int:
        return self.filesystem.getsize(path)

    def listdir(self, path="", *, recursive=False, maxEntries=None):
        return self.filesystem.listdir(path, recursive=recursive, maxEntries=maxEntries)

    def get(self, path) -> bytes:
        return self.filesystem.get(path)

    def getInto(self, path, byteStream):
        self.filesystem.getInto(path, byteStream)

    def __str__(self):
        return f"WriteProtectedFileSystem({str(self.filesystem)})"

    def iterateFiles(self, prefix="", subpathFilter=None, returnModtimesAndSizes=False):
        return self.filesystem.iterateFiles(prefix, subpathFilter, returnModtimesAndSizes)

    def listFiles(self, prefix=""):
        return self.filesystem.listFiles(prefix)

    def listSubdirs(self, path="", recursive=False):
        return self.filesystem.listSubdirs(path, recursive)

    def tearDown(self):
        return self.filesystem.tearDown()

    def stat(self, path):
        return self.filesystem.stat(path)
