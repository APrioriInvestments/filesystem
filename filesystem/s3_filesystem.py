import functools
import io
import os
import threading
import time
import logging

from .filesystem_interface import FileSystem
from .close_protected_stream import CloseProtectedStream


def retryIfIrrelevantFailure(f):
    @functools.wraps(f)
    def inner(*args, **kwargs):
        slowdownAmount = 0.5

        while True:
            try:
                return f(*args, **kwargs)
            except Exception as e:
                if "An error occurred (SlowDown)" in str(e):
                    pass
                elif "Could not connect to the endpoint URL" in str(e):
                    pass
                elif "InternalError" in str(e):
                    pass
                elif "Connection reset by peer" in str(e):
                    pass
                elif "Remote end closed connection without response" in str(e):
                    pass
                elif "reached max retries" in str(e):
                    pass
                elif "Service Unavailable" in str(e):
                    pass
                else:
                    raise

            logging.warning("Error connecting to s3. Trying again.")
            time.sleep(slowdownAmount)
            slowdownAmount = min(10, slowdownAmount * 1.5)

    return inner


class S3FileSystem(FileSystem):
    def __init__(self, bucketname: str, keyPrefix: str, accessKey=None, secretKey=None):
        super().__init__()
        self._accessKey = accessKey
        self._secretKey = secretKey

        self._keyPrefix = ""
        self._bucketname = bucketname

        self._boto_thread_local = threading.local()
        # check that bucket exists
        if not self.isdir(""):
            raise Exception(f"S3 Bucket {bucketname} does not exist or cannot be acessed.")

        # check that the keyPrefix is a valid directory in the given bucket
        self._keyPrefix = self.withTrailingSep(keyPrefix)
        if not self.isdir(""):
            raise Exception(f"S3 prefix {keyPrefix} is not a directory in bucket {bucketname}")

    @property
    def keyPrefix(self):
        return self._keyPrefix

    @property
    def bucketname(self):
        return self._bucketname

    def __eq__(self, other):
        if not isinstance(other, S3FileSystem):
            return False
        if self._bucketname != other._bucketname:
            return False
        if self._keyPrefix != other._keyPrefix:
            return False
        if self._accessKey != other._accessKey:
            return False
        if self._secretKey != other._secretKey:
            return False
        return True

    def __hash__(self):
        return hash(
            (
                "S3FileSystem",
                self._bucketname,
                self._keyPrefix,
                self._accessKey,
                self._secretKey,
            )
        )

    def _pathToKey(self, path):
        """Translate FileSystem path to S3 bucket key."""
        path = self._stripSeps(path)
        if path == "":
            key = self.withTrailingSep(self._keyPrefix)
        else:
            key = self.joinPaths(self._keyPrefix, path)

        if not key.startswith(self._keyPrefix):
            raise OSError(f"Unsafe path detected: '{path}' [not under '{self._keyPrefix}']")

        return key

    def _keyToPath(self, key):
        """Translate S3 key to FileSystem path."""
        if not key.startswith(self._keyPrefix):
            raise OSError(f"Invalid key: '{key}' [doesn't start with '{self._keyPrefix}'")
        return key[len(self._keyPrefix) :]

    def _makeSessionAndClient(self):
        if not hasattr(self._boto_thread_local, "client"):
            self._boto_thread_local.client = {}
            self._boto_thread_local.session = {}
            self._boto_thread_local.bucket = {}

        accessKey = self._accessKey
        secretKey = self._secretKey

        kwargs = {}
        if accessKey is not None and accessKey != "":
            kwargs["aws_access_key_id"] = accessKey
            kwargs["aws_secret_access_key"] = secretKey

        # I can't see how to pass this into boto3 direclty because it's so
        # poorly documented. We need this to ensure we can actually download everything.
        os.environ["AWS_METADATA_SERVICE_NUM_ATTEMPTS"] = "10"
        os.environ["AWS_METADATA_SERVICE_TIMEOUT"] = "2"

        import boto3

        session = boto3.session.Session(**kwargs)
        self._boto_thread_local.session[accessKey] = session
        self._boto_thread_local.client[accessKey] = session.client("s3")

    def _getClient(self):
        if (
            not hasattr(self._boto_thread_local, "client")
            or self._accessKey not in self._boto_thread_local.client
        ):
            self._makeSessionAndClient()

        return self._boto_thread_local.client[self._accessKey]

    def _getSession(self):
        if (
            not hasattr(self._boto_thread_local, "session")
            or self._accessKey not in self._boto_thread_local.session
        ):
            self._makeSessionAndClient()

        return self._boto_thread_local.session[self._accessKey]

    def _getBucket(self):
        key = (self._bucketname, self._accessKey)
        session = self._getSession()

        if key not in self._boto_thread_local.bucket:
            self._boto_thread_local.bucket[key] = session.resource("s3").Bucket(
                self._bucketname
            )

        return self._boto_thread_local.bucket[key]

    def exists(self, path) -> bool:
        return self.isfile(path) or self.isdir(path)

    def isdir(self, path) -> bool:
        # In S3 a path can simultaneously be a file and a dir
        path = self._stripSeps(path)

        key = self.withTrailingSep(self._pathToKey(path))
        keysWithPrefix = self._listprefix(key)
        # If path is a not a dir, keysWithPrefix will be empty.
        # Unfortunately we cannot take len() without wrapping keysWithPrefix
        # in a list(), which could cause a lot of data transfer, if it hasn't
        # all been fetched yet. But we can use .limit(1) to say we just want
        # one result.
        try:
            length = len(list(keysWithPrefix.limit(1)))
        except Exception:
            return False
        else:
            return True if length > 0 or path == "" else False

    def _loadIt(self, path):
        key = self._pathToKey(path)

        @retryIfIrrelevantFailure
        def loadIt():
            o = self._getBucket().Object(key)
            o.load()  # make sure the object exists
            return o

        return loadIt()

    def isfile(self, path) -> bool:
        # In S3 a path can simultaneously be a file and a dir
        try:
            self._loadIt(path)

        except Exception:
            return False

        return True

    def getmtime(self, path) -> float:
        try:
            o = self._loadIt(path)

        except Exception as e:
            logging.exception("Failed to load s3 object in getmtime")
            raise OSError(f"File not accessible: '{path}'") from e

        return o.last_modified.timestamp()

    def getsize(self, path) -> int:
        """Return the size in bytes of path.

        Raise OSError if the file does not exist or is inaccessible.
        """
        try:
            o = self._loadIt(path)

        except Exception as e:
            logging.exception("Failed to load s3 object in getsize")
            raise OSError(f"File not accessible: '{path}'") from e

        return o.content_length

    def stat(self, path):
        try:
            o = self._loadIt(path)

        except Exception as e:
            logging.exception("Failed to load s3 object in stat")
            raise OSError(f"File not accessible: '{path}'") from e

        return {"modtime": o.last_modified.timestamp(), "size": o.content_length}

    def _listprefix(self, prefix):
        """return a collection of ObjectSummary objects that start with prefix

        Some notes on how the optional args of  Bucket.objects.filter work,
        because couldn't find sufficient docs and did some manual trying:
          - Prefix: works as expected: drop any entry if not key.startswith(Prefix)
          - Delimiter: it appears to be dropping  entries if Delimiter in key
          - Marker: return an entry only if it is lexicographically after Marker
        """
        assert prefix.startswith(self._keyPrefix), (prefix, self._keyPrefix)

        return self._getBucket().objects.filter(Prefix=prefix)

    def iterateFiles(self, prefix="", subpathFilter=None, returnModtimesAndSizes=False):
        """Returns a list of all the files that start with the given prefix.

        Directories are not returned.

        Note: Implementations of the FileSystem interface should
        override the implementation if it can be done more efficiently.

        If 'subpathFilter' is passed, then at each subdirectory, we will call the
        function and if it returns False, we will skip that directory. subpathFilter
        must also accept paths that return files.

        If 'returnModtimesAndSizes', then return pairs of (key, modTimestamp, fileSize).
        """
        for path in self.iterateObjects(
            prefix=prefix,
            subpathFilter=subpathFilter,
            returnModtimesAndSizes=returnModtimesAndSizes,
        ):
            yield path

    def iterateObjects(
        self,
        prefix="",
        subpathFilter=None,
        returnModtimesAndSizes=False,
        recursive=True,
        includeDirs=False,
    ):
        if includeDirs and returnModtimesAndSizes:
            raise Exception("includeDirs and returnModtimesAndSizes cannot both be True")

        prefix = prefix.lstrip(FileSystem.sep)

        searchedPrefix = self._pathToKey(prefix)

        if subpathFilter is None and recursive and not includeDirs:
            objects = self._listprefix(searchedPrefix)

            for o in objects:
                if o.key.startswith(searchedPrefix):
                    if returnModtimesAndSizes:
                        yield (self._keyToPath(o.key), o.last_modified.timestamp(), o.size)
                    else:
                        yield self._keyToPath(o.key)
        else:
            client = self._getClient()
            paginator = client.get_paginator("list_objects")

            for result in paginator.paginate(
                Bucket=self._bucketname,
                Delimiter="/",
                Prefix=searchedPrefix + ("/" if prefix[-1:] == "/" else ""),
            ):
                for key in result.get("Contents") or []:
                    path = self._keyToPath(key["Key"])
                    if subpathFilter is None or subpathFilter(path):
                        if returnModtimesAndSizes:
                            yield path, key["LastModified"].timestamp(), key["Size"]
                        else:
                            yield path

                for commonPrefix in result.get("CommonPrefixes") or []:
                    subdir = commonPrefix.get("Prefix").rstrip(FileSystem.sep)
                    path = self._keyToPath(subdir)
                    if subpathFilter is None or subpathFilter(path):
                        if includeDirs:
                            yield path

                        if recursive:
                            if self.withTrailingSep(path) != prefix:
                                for path in self.iterateObjects(
                                    prefix=self.withTrailingSep(path),
                                    subpathFilter=subpathFilter,
                                    returnModtimesAndSizes=returnModtimesAndSizes,
                                    recursive=True,
                                    includeDirs=includeDirs,
                                ):
                                    yield path

    def listFiles(self, prefix=""):
        return list(self.iterateFiles(prefix=prefix))

    def listdir(self, path="", *, recursive=False, maxEntries=None):
        path = self.withTrailingSep(path)
        prefix = self.withTrailingSep(self._pathToKey(path))
        keysWithPrefix = self._listprefix(prefix)

        if path != "" and len(list(keysWithPrefix.limit(1))) == 0:
            # Look at isdir for an explanation of the 2nd part of the condition
            raise OSError(f"Not a directory '{path}'")

        if maxEntries is None:
            return [
                obj
                for obj in self.iterateObjects(
                    prefix=path, recursive=recursive, includeDirs=True
                )
            ]

        else:
            keys = []
            for obj in self.iterateObjects(prefix=path, recursive=recursive, includeDirs=True):
                keys.append(obj)
                if len(keys) >= maxEntries:
                    break

            return keys

    def get(self, path) -> bytes:
        try:
            self._loadIt(path)
        except Exception as e:
            raise OSError(f"File not accessible: '{path}'") from e

        data = io.BytesIO()
        key = self._pathToKey(path)
        self._getInto(key, data)
        return data.getvalue()

    def getInto(self, path, byteStream):
        try:
            self._loadIt(path)
        except Exception as e:
            raise OSError(f"File not accessible: '{path}'") from e

        self._checkByteStreamForGet(byteStream)

        key = self._pathToKey(path)
        self._getInto(key, byteStream)

    @retryIfIrrelevantFailure
    def _getInto(self, key, byteStream):
        byteStream.seek(0)
        self._getClient().download_fileobj(self._bucketname, key, byteStream)

    @retryIfIrrelevantFailure
    def set(self, path, content) -> None:
        self._checkContentInputTypeForSet(content)

        key = self._pathToKey(path)
        if isinstance(content, bytes):
            byteStream = CloseProtectedStream(io.BytesIO(content))

        else:
            assert isinstance(content, io.IOBase), type(content)

            byteStream = CloseProtectedStream(content)
            self._checkByteStreamForSet(byteStream)

        try:
            self._setByteStream(key, byteStream)
        except Exception as e:
            raise OSError(f"Failed to set {path} with error {str(e)}") from e

    def _setByteStream(self, key, byteStream):
        byteStream.seek(0, io.SEEK_END)
        byteStream.seek(0)
        self._getClient().upload_fileobj(byteStream, Bucket=self._bucketname, Key=key)

    def rm(self, path) -> None:
        try:
            o = self._loadIt(path)
        except Exception as e:
            raise OSError(f"Failed not accessible: '{path}'") from e

        @retryIfIrrelevantFailure
        def deleteIt():
            o.delete()

        try:
            deleteIt()

        except Exception as e:
            raise OSError(str(e)) from e

    def __str__(self):
        return f"S3FileSystem(bucket={self._bucketname}, keyPrefix={self._keyPrefix})"
