import datetime
import ftplib
import io
import logging
import socket
import time

from .util import retry, toUtcDatetime
from .filesystem_interface import FileSystem


class FtpFileSystemCaughtException(Exception):
    pass


# Define retry decorator
caughtExceptions = (
    FtpFileSystemCaughtException,
    ConnectionError,
    TimeoutError,
    ftplib.error_temp,
    socket.timeout,
)

retryOnDisconnect = retry(
    retries=10, caughtExceptions=caughtExceptions, onExceptionMember="handleException"
)


class FtpFileSystem(FileSystem):
    VALID_PERMISSIONS_CHARS = ("r", "w", "x", "-")
    VALID_DIR_CHARS = ("d", "-")

    def __init__(
        self,
        username=None,
        password=None,
        host="localhost",
        port=21,
        rootPath="",
        socketTimeout=10,
        connectionRefreshSeconds=60,
    ):
        super().__init__()
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
        self._connectionRefreshSeconds = connectionRefreshSeconds

        self._client = None
        self._clientCreationTime = None

        self._canUseMlsdCache = None
        self._canParseDirCache = None
        self._nlstReturnsPathPrefix = None  # None means unknown

        self._logger = logging.getLogger(__name__)

    def __getstate__(self):
        """Control how pickle serializes instances.

        _client is not serializable and will be instantiated upon request
        _clientCreationTime must be reset to None
        """
        state = self.__dict__.copy()

        state["_client"] = None
        state["_clientCreationTime"] = None

        return state

    @property
    def _canUseMlsd(self):
        if self._canUseMlsdCache is None:
            self._canUseMlsdCache = self._checkCanUseMlsd()

        return self._canUseMlsdCache

    @property
    def _canParseDir(self):
        if self._canParseDirCache is None:
            self._canParseDirCache = self._checkCanParseDir()
        return self._canParseDirCache

    @property
    def rootPath(self):
        return self._rootPath

    def handleException(self, exc):
        self._logger.info(f"Reconnecting because encountered {exc}")
        self._disconnect()

    def __eq__(self, other):
        if not isinstance(other, FtpFileSystem):
            return False
        if self.rootPath != other.rootPath:
            return False
        if self._username != other._username:
            return False
        if self._password != other._password:
            return False
        if self._port != other._port:
            return False
        if self._host != other._host:
            return False
        return True

    def __hash__(self):
        return hash(
            (
                "FtpFileSystem",
                self.rootPath,
                self._username,
                self._password,
                self._port,
                self._host,
            )
        )

    def _rooted(self, path):
        rootedPath = self.joinPaths(self._rootPath, path)
        if not rootedPath.startswith(self._rootPath) and self._rootPath:
            raise Exception(f"Unsafe path detected: '{path}'")
        return rootedPath

    @retryOnDisconnect
    def _checkCanUseMlsd(self):
        """Return True if we can use mlsd and False otherwise.

        Apparently the MLSD command is not supported by all FTP servers.
        MLSD is a great command because it lists the contents of a directory
        and provides metadata about each file, such as type (file or dir),
        modtime, size, and some others. We keep the MLSD implementation
        because it is a bit simpler, and because I suspect it performs
        a lot better (has much fewer round-trips to the FTP server)
        """
        try:
            dict(self.getClient().mlsd())
            return True
        except Exception:
            return False

    @retryOnDisconnect
    def _checkCanParseDir(self):
        """See if we know how to parse the results from ftp.dir().

        If we can, we have a much more efficient implementation of
        isdir we can use in _nlstListDir.
        """
        needsCleanUp = False
        client = self.getClient()

        if len(client.nlst("")) == 0:
            # if the FTP site is empty, try to write a tmp file to see if
            # we can parse the results of `dir`.
            filename = "__testCanParseDir__.txt"
            try:
                self.set(filename, b"asdf;lkj")
                needsCleanUp = True
            except Exception:
                return False

        try:
            lst = []
            self.getClient().dir("", lst.append)
            assert len(lst) > 0

            for line in lst:
                permissions = line.split()[0]
                if len(permissions) != 10:
                    return False
                if permissions[0] not in self.VALID_DIR_CHARS:
                    return False
                for char in permissions[1:]:
                    if char not in self.VALID_PERMISSIONS_CHARS:
                        return False
            return True
        finally:
            if needsCleanUp:
                self.rm(filename)

    def tearDown(self):
        self._disconnect()

    def __del__(self):
        self.tearDown()

    def _connect(self):
        self._disconnect()
        self._clientCreationTime = time.time()
        self._client = ftplib.FTP(timeout=self._socketTimeout)

        self._client.connect(host=self._host, port=self._port)

        if self._username:
            self._client.login(user=self._username, passwd=self._password)

    @retryOnDisconnect
    def getClient(self):
        if (
            self._client is None
            or time.time() - self._clientCreationTime > self._connectionRefreshSeconds
        ):
            self._connect()
        return self._client

    def _disconnect(self):
        if self._client is not None:
            try:
                self._client.quit()
            except ftplib.all_errors:
                pass
            finally:
                self._client = None

    def exists(self, path):
        return self._existsRooted(self._rooted(path))

    @retryOnDisconnect
    def _existsRooted(self, rootedPath):
        try:
            lst = self.getClient().nlst(rootedPath)
            if len(lst) > 0:
                return True
            else:
                return self._isdirRooted(rootedPath)

        except ftplib.error_perm:
            return False

    def isdir(self, path):
        if path == "" or path == "/":
            return True

        return self._isdirRooted(self._rooted(path))

    @retryOnDisconnect
    def _isdirRooted(self, rootedPath):
        if rootedPath == "" or rootedPath == "/":
            return True

        client = self.getClient()

        if self._canUseMlsd:
            parentDir = self.dirname(rootedPath)
            basename = self.basename(rootedPath)
            try:
                lst = dict(client.mlsd(parentDir))
            except Exception:
                return False

            if basename in lst and lst[basename]["type"] == "dir":
                return True
            else:
                # we expect lst[basename]["type"] == "file"
                return False

        else:
            try:
                client.cwd(rootedPath)
                client.cwd("/")
                return True

            except ftplib.error_perm:
                return False

    @retryOnDisconnect
    def isfile(self, path):
        try:
            lst = self.getClient().nlst(self._rooted(path))
            if len(lst) != 1:
                return False
            else:
                return not self.isdir(path)

        except ftplib.error_perm:
            return False

    def _filesToPermissions(self, rootedPath):
        # FIXME: may break for filenames with spaces
        assert self._canParseDir
        assert rootedPath.startswith(self._rootPath)

        lst = []
        self.getClient().dir(rootedPath, lst.append)

        filesToPermissions = {}
        for line in lst:
            parts = line.split()
            filesToPermissions[parts[-1]] = parts[0]

        return filesToPermissions

    def _isDirFromPermissions(self, permissionsString):
        dirChar = permissionsString[0]
        if dirChar == "d":
            return True
        elif dirChar == "-":
            return False
        else:
            raise Exception(f"Invalid permissions string: {permissionsString}")

    @retryOnDisconnect
    def _makeParentDirsIfNeeded(self, rootedPath):
        directory = self.dirname(rootedPath)
        if not directory:
            return

        if not self._existsRooted(directory):
            self._makeParentDirsIfNeeded(directory)
            self.getClient().mkd(directory)
        elif self.isfile(directory):
            raise OSError(f"unable to make parent dirs because {directory} is a file")

    def set(self, path, content):
        self._checkContentInputTypeForSet(content)

        rootedPath = self._rooted(path)
        self._makeParentDirsIfNeeded(rootedPath)

        if isinstance(content, bytes):
            byteStream = io.BytesIO(content)

        else:
            assert isinstance(content, io.IOBase), type(content)
            byteStream = content
            self._checkByteStreamForSet(byteStream)

        self._setFromByteStream(rootedPath, byteStream)

    @retryOnDisconnect
    def _setFromByteStream(self, rootedPath, byteStream):
        byteStream.seek(0)
        self.getClient().storbinary("STOR " + rootedPath, byteStream)

    def get(self, path):
        if not self.isfile(path):
            raise OSError(f"Path is not a file: '{path}")

        res = io.BytesIO()
        self._getInto(path, res)
        return res.getvalue()

    def getInto(self, path, byteStream):
        if not self.isfile(path):
            raise OSError(f"Path is not a file: '{path}")

        self._checkByteStreamForGet(byteStream)

        self._getInto(path, byteStream)

    @retryOnDisconnect
    def _getInto(self, path, byteStream):
        byteStream.seek(0)
        self.getClient().retrbinary("RETR " + self._rooted(path), byteStream.write)

    @retryOnDisconnect
    def rm(self, path):
        if not path:
            raise OSError(f"Cannot remove '{path}'")

        if not self.exists(path):
            raise OSError(f"Failed to remove '{path}': does not exist")

        rootedPath = self._rooted(path)
        if self.isdir(path):
            self.getClient().rmd(rootedPath)
        else:  # it is a file
            self.getClient().delete(rootedPath)

    @retryOnDisconnect
    def getmtime(self, path):
        if not self.exists(path):
            raise OSError(f"Path not found: '{path}'")

        rootedPath = self._rooted(path)
        if self._canUseMlsd:
            parentDir = self.dirname(rootedPath)
            basename = self.basename(rootedPath)
            lst = dict(self.getClient().mlsd(parentDir))

            if basename not in lst:
                raise Exception("Can't find " + basename + " in " + repr(lst.keys())[:1024])

            return self._modtimeFromMetadata(lst[basename])

        else:
            try:
                modtimeStr = self.getClient().voidcmd("MDTM " + rootedPath)[4:].strip()
                return self._modtimeStrToTimestamp(modtimeStr)
            except Exception:
                self._logger.exception(f"Failed to get mod-time for path '{path}'")
                raise OSError(f"Failed to get mod-time for path '{path}'")

    @staticmethod
    def _modtimeStrToTimestamp(modtimeStr):
        dt = datetime.datetime.strptime(modtimeStr, "%Y%m%d%H%M%S")
        return toUtcDatetime(dt).timestamp()

    @staticmethod
    def _modtimeFromMetadata(metadata):
        assert "modify" in metadata, metadata
        return FtpFileSystem._modtimeStrToTimestamp(metadata["modify"])

    @retryOnDisconnect
    def getsize(self, path):
        if not self.exists(path):
            raise OSError(f"Path not found: '{path}'")

        rootedPath = self._rooted(path)
        if self._canUseMlsd:
            parentDir = self.dirname(rootedPath)
            basename = self.basename(rootedPath)
            lst = dict(self.getClient().mlsd(parentDir))
            return self._keysizeFromMetadata(lst[basename])

        else:
            client = self.getClient()
            try:
                return client.size(rootedPath)

            except Exception:
                # maybe we need to switch to binary mode first
                client.voidcmd("TYPE I")
                return client.size(rootedPath)

    @staticmethod
    def _keysizeFromMetadata(metadata):
        assert "size" in metadata, metadata
        return int(metadata["size"])

    @retryOnDisconnect
    def listdir(self, path="", *, recursive=False, maxEntries=None):
        if not self.isdir(path):
            raise OSError(f"Not a directory '{path}'")

        result = []
        if self._canUseMlsd:
            self._mlsdListdir(result, path, recursive=recursive, maxEntries=maxEntries)
        else:
            self._nlstListdir(result, path, recursive=recursive, maxEntries=maxEntries)

        return result

    def listFiles(self, prefix="", maxEntries=None):
        prefix = prefix.lstrip(FileSystem.sep)

        if prefix == "":
            startDir = prefix
        elif self.isdir(prefix):
            startDir = prefix
        elif "/" in prefix and self.isdir(prefix.rsplit("/", 1)[0]):
            startDir = prefix.rsplit("/", 1)[0]
        else:
            startDir = ""

        result = []
        if self._canUseMlsd:
            self._mlsdListdir(
                result, startDir, recursive=True, skipDirs=True, maxEntries=maxEntries
            )
        else:
            self._nlstListdir(
                result, startDir, recursive=True, skipDirs=True, maxEntries=maxEntries
            )

        return [file for file in result if file.startswith(prefix)]

    @retryOnDisconnect
    def _mlsdListdir(
        self, result, path="", *, recursive=False, skipDirs=False, maxEntries=None
    ):
        rootedPath = self._rooted(path)
        mlsd = dict(self.getClient().mlsd(rootedPath))

        for file, metadata in mlsd.items():
            if file not in [".", ".."]:
                if maxEntries is not None and len(result) >= maxEntries:
                    return

                filePath = self.joinPaths("", path, file)

                if recursive and metadata["type"] == "dir":
                    if not skipDirs:
                        result.append(filePath)
                    self._mlsdListdir(
                        result,
                        filePath,
                        recursive=recursive,
                        skipDirs=skipDirs,
                        maxEntries=maxEntries,
                    )
                else:
                    result.append(filePath)

    def _adjustNlstResults(self, nlst, rootedPath):
        # nlst(rootPath) sometimes returns paths with the rootPath as a prefix,
        # and sometimes without. This seems to depend on the remote server.
        prefixLen = len(rootedPath)
        if prefixLen == 0 or self._nlstReturnsPathPrefix is False:
            return nlst

        elif self._nlstReturnsPathPrefix is True:
            return [x[prefixLen:] for x in nlst]

        else:  # self._nlstReturnsPathPrefix is None:
            if all(x.startswith(rootedPath) for x in nlst):
                self._nlstReturnsPathPrefix = True
                return [x[prefixLen:] for x in nlst]
            else:
                self._nlstReturnsPathPrefix = False
                return nlst

    @retryOnDisconnect
    def _nlstListdir(
        self, result, path="", *, recursive=False, skipDirs=False, maxEntries=None
    ):
        self._logger.debug("Listing %s", path)

        try:
            rootedPath = self._rooted(path)
            client = self.getClient()

            lst = client.nlst(rootedPath)
            lst = self._adjustNlstResults(lst, rootedPath)

            if recursive:
                # define local isdir() based on whether we can parse dir results
                if self._canParseDir and len(lst) > 5:
                    # for fewer than 5 files it's cheaper to use the default isdir
                    filesToPermissions = self._filesToPermissions(rootedPath)

                    @retry(caughtExceptions=caughtExceptions, onException=self.handleException)
                    def isdir(filepath):
                        fname = self.basename(filepath)
                        if fname in filesToPermissions:
                            return self._isDirFromPermissions(filesToPermissions[fname])
                        else:
                            return self.isdir(filepath)

                else:
                    isdir = self.isdir

                for file in lst:
                    filePath = self.joinPaths("", path, file)

                    if maxEntries is not None and len(result) >= maxEntries:
                        return

                    if isdir(filePath):
                        if not skipDirs:
                            result.append(filePath)
                        self._nlstListdir(
                            result,
                            filePath,
                            recursive=recursive,
                            skipDirs=skipDirs,
                            maxEntries=maxEntries,
                        )
                    else:
                        result.append(filePath)

            else:
                for file in lst:
                    if maxEntries is not None and len(result) >= maxEntries:
                        return

                    filePath = self.joinPaths("", path, file)
                    result.append(filePath)
        except Exception as e:
            if isinstance(e, caughtExceptions):
                raise FtpFileSystemCaughtException("Failed on %s", path) from e

            else:
                raise Exception("Failed on %s", path) from e

    def __str__(self):
        return (
            f"FtpFileSystem(username={self._username}, "
            f"host={self._host}, port={self._port}, rootPath='{self.rootPath}')"
        )
