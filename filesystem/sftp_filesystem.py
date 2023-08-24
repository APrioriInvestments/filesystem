import io
import logging
import socket
import stat

from .util import retry
from .filesystem_interface import FileSystem

reconnectOnException = retry(
    caughtExceptions=(OSError, socket.timeout), onExceptionMember="handleException"
)


class SftpFileSystem(FileSystem):
    def __init__(
        self,
        username=None,
        password=None,
        host="localhost",
        port=22,
        rootPath="",
        socketTimeout=10,
    ):
        if username is None:
            username = "anonymous"
        if password is None:
            password = ""

        self._username = username
        self._password = password
        self._host = host
        self._port = port
        self._rootPath = rootPath
        self._socketTimeout = socketTimeout

        self._transport = None
        self._client = None

        self._logger = logging.getLogger(__name__)

        import paramiko

        self._paramiko = paramiko

    @property
    def rootPath(self):
        return self._rootPath

    def handleException(self, exc):
        self._logger.info(f"Reconnecting because encountered {exc}")
        self._connect

    def __eq__(self, other):
        if not isinstance(other, SftpFileSystem):
            return False
        if self.rootPath != other.rootPath:
            return False
        if self._username != other._username:
            return False
        if self._password != other._password:
            return False
        if self._host != other._host:
            return False
        if self._port != other._port:
            return False
        return True

    def __hash__(self):
        return hash(
            (
                "SftpFileSystem",
                self.rootPath,
                self._username,
                self._password,
                self._host,
                self._port,
            )
        )

    def _rooted(self, path):
        rootedPath = self.joinPaths(self._rootPath, path)
        if not rootedPath.startswith(self._rootPath):
            raise Exception(f"Unsafe path detected: '{path}'")
        return rootedPath

    def _ensureConnected(self):
        if self._client is None:
            self._connect()

    def _connect(self):
        self._disconnect()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self._socketTimeout)
        sock.connect((self._host, self._port))

        self._transport = self._paramiko.Transport(sock)
        self._transport.connect(username=self._username, password=self._password)
        assert self._transport.active
        self._client = self._paramiko.SFTPClient.from_transport(self._transport)

    def _disconnect(self):
        if self._client is not None:
            self._client.close()
            self._client = None

        if self._transport is not None:
            self._transport.close()
            self._transport = None

    def tearDown(self):
        self._disconnect()

    def __del__(self):
        self.tearDown()

    def exists(self, path):
        return self._existsRooted(self._rooted(path))

    def getClient(self):
        self._ensureConnected()
        return self._client

    @reconnectOnException
    def _existsRooted(self, rootedPath):
        try:
            self.getClient().lstat(rootedPath)
        except FileNotFoundError:
            return False
        else:
            return True

    @reconnectOnException
    def isdir(self, path):
        try:
            mode = self.getClient().lstat(self._rooted(path)).st_mode
        except FileNotFoundError:
            return False
        else:
            return True if stat.S_ISDIR(mode) else False

    @reconnectOnException
    def isfile(self, path):
        try:
            mode = self.getClient().lstat(self._rooted(path)).st_mode
        except FileNotFoundError:
            return False
        else:
            return False if stat.S_ISDIR(mode) else True

    def get(self, path):
        if not self.isfile(path):
            raise OSError(f"Path is not a file: '{path}")

        buffer = io.BytesIO()
        self._getInto(path, buffer)
        buffer.seek(0)
        return buffer.read()

    def getInto(self, path, byteStream):
        if not self.isfile(path):
            raise OSError(f"Path is not a file: '{path}")

        self._checkByteStreamForGet(byteStream)

        self._getInto(path, byteStream)

    @reconnectOnException
    def _getInto(self, path, byteStream):
        byteStream.seek(0)
        self.getClient().getfo(self._rooted(path), byteStream)

    def getmtime(self, path):
        if not self.exists(path):
            raise OSError(f"Path not found: '{path}'")
        return self._getmtime(path)

    @reconnectOnException
    def _getmtime(self, path):
        return self.getClient().lstat(self._rooted(path)).st_mtime

    def getsize(self, path):
        if not self.exists(path):
            raise OSError(f"Path not found: '{path}'")
        return self._getsize(path)

    @reconnectOnException
    def _getsize(self, path):
        return self.getClient().lstat(self._rooted(path)).st_size

    @reconnectOnException
    def _makeParentDirsIfNeeded(self, rootedPath):
        directory = self.dirname(rootedPath)
        if not directory:
            return

        if not self._existsRooted(directory):
            self._makeParentDirsIfNeeded(directory)
            self.getClient().mkdir(directory)
        elif self.isfile(directory):
            raise OSError(f"unable to make parent dirs because {directory} is a file")

    def set(self, path, content):
        self._checkContentInputTypeForSet(content)

        rootedPath = self._rooted(path)
        self._makeParentDirsIfNeeded(rootedPath)

        if isinstance(content, bytes):
            byteStream = io.BytesIO(content)

        else:  # it should be a byte-stream
            assert isinstance(content, io.BufferedIOBase), type(content)
            byteStream = content

        self._checkByteStreamForSet(byteStream)
        self._setFromByteStream(rootedPath, byteStream)

    @reconnectOnException
    def _setFromByteStream(self, rootedPath, byteStream):
        byteStream.seek(0)
        self.getClient().putfo(byteStream, rootedPath)

    def rm(self, path):
        if not path:
            raise OSError(f"Cannot remove '{path}'")

        if not self.exists(path):
            raise OSError(f"Failed to remove '{path}': does not exist")

        return self._rm(path)

    @reconnectOnException
    def _rm(self, path):
        rootPath = self._rooted(path)
        if self.isdir(path):
            self.getClient().rmdir(rootPath)
        else:
            self.getClient().remove(rootPath)

    def listdir(self, path="", *, recursive=False, maxEntries=None):
        if not self.isdir(path):
            raise OSError(f"Not a directory '{path}'")
        result = []
        self._listdir(result, path, recursive=recursive, skipDirs=False, maxEntries=maxEntries)
        return result

    def listFiles(self, prefix=""):
        prefix = prefix.lstrip(FileSystem.sep)

        result = []
        self._listdir(result, "", recursive=True, skipDirs=True)
        return [file for file in result if file.startswith(prefix)]

    @reconnectOnException
    def _listdir(
        self, result: list, path: str, recursive: bool, skipDirs: bool, maxEntries=None
    ):
        rootedPath = self._rooted(path)
        lst = self.getClient().listdir_attr(rootedPath)

        for attributes in lst:
            if maxEntries is not None and len(result) >= maxEntries:
                return

            filePath = self.joinPaths("", path, attributes.filename)
            if recursive and stat.S_ISDIR(attributes.st_mode):
                if not skipDirs:
                    result.append(filePath)

                self._listdir(
                    result,
                    filePath,
                    recursive=recursive,
                    skipDirs=skipDirs,
                    maxEntries=maxEntries,
                )
            else:
                result.append(filePath)

    def __str__(self):
        return (
            f"SftpFileSystem(username={self._username}, "
            f"host={self._host}, port={self._port}, rootPath='{self.rootPath}')"
        )
