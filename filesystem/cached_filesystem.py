from .filesystem_interface import ReadOnlyFileSystem
from .s3_filesystem import S3FileSystem


class CachedFileSystem(ReadOnlyFileSystem):
    """A read-only FileSystem that wraps a (remote) FileSystem with a cache.

    We assume the 'frontFileSystem' is fast, and that we can write to it with
    impunity, and that the backFileSystem is slow and doesn't change.
    """

    def __init__(self, frontFileSystem, backFileSystem):
        super().__init__()
        assert not isinstance(frontFileSystem, S3FileSystem)

        self.frontFileSystem = frontFileSystem
        self.backFileSystem = backFileSystem

    def __eq__(self, other):
        if not isinstance(other, CachedFileSystem):
            return False
        if self.frontFileSystem != other.frontFileSystem:
            return False
        if self.backFileSystem != other.backFileSystem:
            return False
        return True

    def __hash__(self):
        return hash(
            ("CachedFileSystem", hash(self.frontFileSystem), hash(self.backFileSystem))
        )

    def exists(self, path) -> bool:
        return self.frontFileSystem.exists(path) or self.backFileSystem.exists(path)

    def isdir(self, path) -> bool:
        if self.frontFileSystem.exists(path):
            return self.frontFileSystem.isdir(path)
        else:
            return self.backFileSystem.isdir(path)

    def isfile(self, path) -> bool:
        if self.frontFileSystem.exists(path):
            return self.frontFileSystem.isfile(path)
        else:
            return self.backFileSystem.isfile(path)

    def getmtime(self, path) -> float:
        return self.backFileSystem.getmtime(path)

    def getsize(self, path) -> int:
        return self.backFileSystem.getsize(path)

    def listdir(self, path="", *, recursive=False, maxEntries=None):
        return self.backFileSystem.listdir(path, recursive=recursive, maxEntries=maxEntries)

    def get(self, path) -> bytes:
        if self.frontFileSystem.exists(path):
            return self.frontFileSystem.get(path)

        else:
            data = self.backFileSystem.get(path)
            self.frontFileSystem.set(path, data)
            return data

    def getInto(self, path, byteStream):
        self._checkByteStreamForGet(byteStream)

        if self.frontFileSystem.exists(path):
            self.frontFileSystem.getInto(path, byteStream)

        else:
            pos = byteStream.tell()
            self.backFileSystem.getInto(path, byteStream)
            byteStream.seek(pos)
            self.frontFileSystem.set(path, byteStream)

    def __str__(self):
        return (
            f"CachedFileSystem(front={str(self.frontFileSystem)}, "
            f"back={str(self.backFileSystem)})"
        )
