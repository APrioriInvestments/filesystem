import logging
import tempfile

from .filesystem_interface import FileSystem


class CloningFileSystem(FileSystem):
    """A FileSystem that lazily replicates a (remote) FileSystem and allows modifications.

    The 'front' FileSystem is updated with the value from the 'back' FileSystem on 'get'
    operations if a value is missing from it, and both are updated on 'set' operations.
    """

    def __init__(self, frontFileSystem, backFileSystem):
        super().__init__()
        self.frontFileSystem = frontFileSystem
        self.backFileSystem = backFileSystem

    @staticmethod
    def make(frontFileSystemFactory, backFileSystemFactory):
        return CloningFileSystem(frontFileSystemFactory(), backFileSystemFactory())

    def __eq__(self, other):
        if not isinstance(other, CloningFileSystem):
            return False
        if self.frontFileSystem != other.frontFileSystem:
            return False
        if self.backFileSystem != other.backFileSystem:
            return False
        return True

    def __hash__(self):
        return hash(
            ("CloningFileSystem", hash(self.frontFileSystem), hash(self.backFileSystem))
        )

    def _clone(self, path):
        if not self.frontFileSystem.isfile(path) and self.backFileSystem.isfile(path):
            with tempfile.SpooledTemporaryFile() as fd:
                self.backFileSystem.getInto(path, fd)
                fd.seek(0)
                self.frontFileSystem.set(path, fd)

    def exists(self, path) -> bool:
        return self.frontFileSystem.exists(path) or self.backFileSystem.exists(path)

    def isdir(self, path) -> bool:
        return self.frontFileSystem.isdir(path) or self.backFileSystem.isdir(path)

    def isfile(self, path) -> bool:
        return self.frontFileSystem.isfile(path) or self.backFileSystem.isfile(path)

    def getmtime(self, path) -> float:
        try:
            return self.frontFileSystem.getmtime(path)

        except OSError:
            if self.backFileSystem.isfile(path) and not self.frontFileSystem.isfile(path):
                self._clone(path)

                return self.frontFileSystem.getmtime(path)

            else:
                raise

    def getsize(self, path) -> int:
        try:
            return self.frontFileSystem.getsize(path)

        except OSError:
            return self.backFileSystem.getsize(path)

    def listdir(self, path="", *, recursive=False, maxEntries=None):
        try:
            backPaths = set(
                self.backFileSystem.listdir(path, recursive=recursive, maxEntries=maxEntries)
            )
        except OSError:
            raised = True
            backPaths = set()

        else:
            raised = False

        try:
            frontPaths = set(
                self.frontFileSystem.listdir(path, recursive=recursive, maxEntries=maxEntries)
            )
        except OSError:
            if raised:
                raise
            else:
                frontPaths = set()

        return sorted(backPaths.union(frontPaths))

    def get(self, path) -> bytes:
        if self.frontFileSystem.exists(path):
            return self.frontFileSystem.get(path)

        else:
            data = self.backFileSystem.get(path)
            try:
                self.frontFileSystem.set(path, data)
            except OSError as e:
                logging.getLogger(__name__).warning(
                    f"Failed to set {path} in 'front' file-system with error: {str(e)}"
                )
            return data

    def getInto(self, path, byteStream):
        if self.frontFileSystem.exists(path):
            self.frontFileSystem.getInto(path, byteStream)

        else:
            pos = byteStream.tell()
            self.backFileSystem.getInto(path, byteStream)
            byteStream.seek(pos)
            self.frontFileSystem.set(path, byteStream)

    def __str__(self):
        return (
            f"CloningFileSystem(front={str(self.frontFileSystem)}, "
            f"back={str(self.backFileSystem)})"
        )

    def set(self, path, content) -> None:
        if isinstance(content, bytes):
            self.backFileSystem.set(path, content)
            self.frontFileSystem.set(path, content)

        else:
            try:
                position = content.tell()
                seekable = True
            except Exception:
                position = None
                seekable = False

            if seekable:
                assert position is not None
                self.backFileSystem.set(path, content)
                content.seek(position)
                self.frontFileSystem.set(path, content)

            else:
                with tempfile.SpooledTemporaryFile() as fd:
                    self.chunkedByteStreamPipe(content, fd)
                    fd.seek(0)
                    self.backFileSystem.set(path, fd)
                    fd.seek(0)
                    self.frontFileSystem.set(path, fd)

    def rm(self, path) -> None:
        try:
            self.frontFileSystem.rm(path)

        finally:
            self.backFileSystem.rm(path)
