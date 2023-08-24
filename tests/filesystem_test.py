import boto3
import io
import os
import pickle
import pytest
import time
import unittest
import uuid

from flaky import flaky
from moto import mock_s3

from .ftp_server import FtpServer
from .sftp_server import SftpServer
from .util import timer

from filesystem import (
    CachedFileSystem,
    TempDiskFileSystem,
    DiskFileSystem,
    InMemFileSystem,
    S3FileSystem,
    FtpFileSystem,
    SftpFileSystem,
    CloningFileSystem,
    WriteOnceFileSystem,
    WriteProtectedFileSystem,
)


class FileSystemTestCases:
    MODTIME_DIFFERENCE_THRESHOLD = 0.01

    def test_can_pickle(self):
        fs = self.filesystem
        filename = "file.txt"
        data = b"asdf"
        fs.set(filename, data)

        sour_cucumber = pickle.dumps(fs)
        sour_fs = pickle.loads(sour_cucumber)
        sour_data = sour_fs.get(filename)

        assert sour_data == data

    @pytest.mark.perf
    def test_transfer_performance(self):
        TRANSFER_TIME = 1.0

        fs = self.filesystem
        data = b"a" * 10 * 1024**2  # 10MB
        filename = "a.txt"

        # 1. set from bytes
        with timer() as upload1:
            fs.set(filename, data)

        # 2. set from stream
        outStream = io.BytesIO(data)
        # import pdb; pdb.set_trace()
        with timer() as upload2:
            fs.set(filename, outStream)

        # 3. get into memory
        with timer() as download1:
            data = fs.get(filename)

        # 4. get into bytestream
        inStream = io.BytesIO()
        with timer() as download2:
            data = fs.getInto(filename, inStream)

        lines = (
            f"Upload from Memory took {upload1['elapsed']:.2f} seconds",
            f"Upload from Stream took {upload2['elapsed']:.2f} seconds",
            f"Download from Memory took {download1['elapsed']:.2f} seconds",
            f"Download from Stream took {download2['elapsed']:.2f} seconds",
        )

        for line in lines:
            print(line)

        assert upload1["elapsed"] < TRANSFER_TIME
        assert upload2["elapsed"] < TRANSFER_TIME
        assert download1["elapsed"] < TRANSFER_TIME
        assert download2["elapsed"] < TRANSFER_TIME

    def test_bytestreams(self):
        data = b"asdf"
        bs = io.BytesIO(data)
        fs = self.filesystem
        fname = "file.txt"

        assert not fs.exists(fname)
        fs.set(fname, bs)
        assert fs.exists(fname)
        assert fs.get(fname) == data

        with pytest.raises(ValueError):
            fs.set(fname, bs)

        assert fs.get(fname) == data

        bs.seek(0)
        fs.set(fname, bs)
        assert fs.get(fname) == data

        bsIn = io.BytesIO()
        fs.getInto(fname, bsIn)
        assert bsIn.getvalue() == data

        with pytest.raises(ValueError):
            fs.getInto(fname, bsIn)
        assert bsIn.getvalue() == data

        bsIn.seek(0)
        fs.getInto(fname, bsIn)
        assert bsIn.getvalue() == data

    @flaky(max_runs=3, min_passes=1)
    def test_simple_filesystem(self):
        fs = self.filesystem
        print(fs)

        def checkFor(key: str, data: bytes):
            def checkInvariants():
                assert not fs.exists(key)
                assert not fs.isfile(key)
                assert not fs.isdir(key)

                with pytest.raises(OSError):
                    fs.get(key)

                with pytest.raises(OSError):
                    fs.getmtime(key)

                with pytest.raises(OSError):
                    fs.getsize(key)

                len(fs.listdir()) == 0

            checkInvariants()
            fs.set(key, data)
            local_modtime = time.time()

            assert fs.get(key) == data
            assert fs.exists(key)
            assert fs.isfile(key)
            assert not fs.isdir(key)

            keysize = fs.getsize(key)
            assert keysize == len(data)

            modtime = fs.getmtime(key)
            assert abs(local_modtime - modtime) < self.MODTIME_DIFFERENCE_THRESHOLD

            stat = fs.stat(key)
            assert stat["modtime"] == modtime
            assert stat["size"] == keysize

            expectedSubdirs = 1 if len(fs.dirname(key)) > 0 else 0
            subdirs = fs.listSubdirs()
            assert len(subdirs) == expectedSubdirs

            files = fs.listFiles()
            assert len(files) == 1

            fname = files[0]
            assert fname == key
            assert fs.getsize(fname) == keysize
            assert fs.getmtime(fname) == modtime

            fs.rm(key)
            assert not fs.exists(key)

            with pytest.raises(OSError):
                fs.rm(key)  # delete should raise OSError if key is missing

            checkInvariants()

        values = [b"", "blah".encode("ASCII"), "blah".encode("utf-8")]
        paths = ["file.txt", "dir1/file.txt", "dir1/dir2/file.txt"]

        for path in paths:
            for val in values:
                try:
                    checkFor(path, val)
                except Exception:
                    print(f"FAILED: test failed for path={path} and val={val}")
                    raise

    def test_create_delete_flat(self):
        fname = "test.txt"
        contents = b"abc"

        assert not self.filesystem.exists(fname)
        assert not self.filesystem.isfile(fname)
        assert not self.filesystem.isdir(fname)
        assert len(self.filesystem.listdir()) == 0

        with pytest.raises(OSError):
            self.filesystem.get(fname)

        with pytest.raises(OSError):
            self.filesystem.rm(fname)

        with pytest.raises(OSError):
            self.filesystem.getmtime(fname)

        with pytest.raises(OSError):
            self.filesystem.getsize(fname)

        self.filesystem.set(fname, contents)
        assert self.filesystem.exists(fname)
        assert (
            abs(self.filesystem.getmtime(fname) - time.time())
            < self.MODTIME_DIFFERENCE_THRESHOLD
        )
        assert self.filesystem.getsize(fname) == len(contents)
        assert self.filesystem.isfile(fname)
        assert not self.filesystem.isdir(fname)

        assert self.filesystem.get(fname) == contents

        lst = self.filesystem.listdir()
        assert len(lst) == 1
        assert lst == [fname]

        self.filesystem.rm(fname)
        assert not self.filesystem.exists(fname)
        assert not self.filesystem.isfile(fname)
        assert not self.filesystem.isdir(fname)
        assert len(self.filesystem.listdir()) == 0

    def test_create_delete_nested(self):
        fname = "dir1/test.txt"
        contents = b"abc"

        self.filesystem.set(fname, contents)
        assert self.filesystem.exists("dir1")
        assert self.filesystem.isdir("dir1")
        assert not self.filesystem.isfile("dir1")
        assert self.filesystem.exists(fname)
        assert not self.filesystem.isdir(fname)
        assert self.filesystem.isfile(fname)

        assert (
            abs(self.filesystem.getmtime(fname) - time.time())
            < self.MODTIME_DIFFERENCE_THRESHOLD
        )
        assert self.filesystem.getsize(fname) == len(contents)
        assert self.filesystem.get(fname) == contents

        assert self.filesystem.listdir() == ["dir1"]
        assert sorted(self.filesystem.listdir(recursive=True)) == ["dir1", fname]
        assert self.filesystem.listdir("dir1") == [fname]

        self.filesystem.rm(fname)
        assert not self.filesystem.exists(fname)
        assert not self.filesystem.isfile(fname)
        assert not self.filesystem.isdir(fname)
        # Note: in S3FileSystem dir1 will automatically disappear whereas
        # in other FileSystems that is not the case, so we don't assert
        # facts about it.

    def test_iterate_subdir_paths(self):
        fs = self.filesystem

        fs.set("b/c/f", b"abc")
        fs.set("a/c/f", b"abc")

        def dropA(path):
            return not path.startswith("a/")

        def dropBCF(path):
            return path != "b/c/f"

        assert sorted(fs.iterateFiles()) == ["a/c/f", "b/c/f"]

        assert sorted(
            self.makeSubFileSystem("a").iterateFiles(subpathFilter=lambda p: True)
        ) == ["c/f"]

        assert sorted(fs.iterateFiles(subpathFilter=dropA)) == ["b/c/f"]
        assert sorted(fs.iterateFiles(subpathFilter=dropBCF)) == ["a/c/f"]

        fs.set("b/c2", b"abc")
        fs.set("a/c2", b"abc")

        assert sorted(fs.iterateFiles()) == ["a/c/f", "a/c2", "b/c/f", "b/c2"]

        assert sorted(fs.iterateFiles(subpathFilter=dropA)) == ["b/c/f", "b/c2"]

        for path, modtime, filesize in fs.iterateFiles(
            subpathFilter=dropA, returnModtimesAndSizes=True
        ):
            assert abs(time.time() - modtime) < 10
            assert filesize == fs.getsize(path)

        for path, modtime, filesize in fs.iterateFiles(returnModtimesAndSizes=True):
            assert abs(time.time() - modtime) < 10
            assert filesize == fs.getsize(path)

    def test_corner_cases(self):
        fs = self.filesystem

        assert fs.exists("")
        assert fs.isdir("")
        assert not fs.isfile("")
        assert fs.exists("/")
        assert fs.isdir("/")
        assert not fs.isfile("/")

        fs.set("dir1/dir2/file1.txt", b"abc")

        assert not fs.exists("dir")

        assert fs.exists("dir1")
        assert fs.isdir("dir1")
        assert not fs.isfile("dir1")

        assert fs.exists("dir1/")
        assert fs.isdir("dir1/")
        assert not fs.isfile("dir1/")

        assert not fs.exists("dir1/dir")

        assert fs.exists("dir1/dir2")
        assert fs.isdir("dir1/dir2")
        assert not fs.isfile("dir1/dir2")

        assert fs.exists("dir1/dir2/")
        assert fs.isdir("dir1/dir2/")
        assert not fs.isfile("dir1/dir2/")

        assert fs.exists("dir1/dir2/file1.txt")
        assert fs.isfile("dir1/dir2/file1.txt")
        assert not fs.isdir("dir1/dir2/file1.txt")

        assert fs.exists("dir1/dir2/file1.txt/")

        assert fs.listdir() == ["dir1"]
        assert sorted(fs.listdir(recursive=True)) == [
            "dir1",
            "dir1/dir2",
            "dir1/dir2/file1.txt",
        ]

        assert fs.listdir("dir1") == ["dir1/dir2"]
        assert fs.listdir("dir1/") == ["dir1/dir2"]
        assert sorted(fs.listdir("dir1", recursive=True)) == [
            "dir1/dir2",
            "dir1/dir2/file1.txt",
        ]
        assert sorted(fs.listdir("dir1/", recursive=True)) == [
            "dir1/dir2",
            "dir1/dir2/file1.txt",
        ]

        assert fs.listdir("dir1/dir2") == ["dir1/dir2/file1.txt"]
        assert fs.listdir("dir1/dir2/") == ["dir1/dir2/file1.txt"]
        assert sorted(fs.listdir("dir1/dir2", recursive=True)) == ["dir1/dir2/file1.txt"]
        assert sorted(fs.listdir("dir1/dir2/", recursive=True)) == ["dir1/dir2/file1.txt"]

        with pytest.raises(OSError):
            # listdir on a file raises
            fs.listdir("dir1/dir2/file1.txt")

        with pytest.raises(OSError):
            # listdir on a non-existing path raises
            fs.listdir("asdf", recursive=True)

        fs.set("dir1/dir1/file2.txt", b"hi")

        assert fs.listdir() == ["dir1"]
        assert sorted(fs.listdir(recursive=True)) == [
            "dir1",
            "dir1/dir1",
            "dir1/dir1/file2.txt",
            "dir1/dir2",
            "dir1/dir2/file1.txt",
        ]
        assert sorted(fs.listdir("dir1", recursive=True)) == [
            "dir1/dir1",
            "dir1/dir1/file2.txt",
            "dir1/dir2",
            "dir1/dir2/file1.txt",
        ]
        assert fs.listdir("dir1/dir1", recursive=True) == ["dir1/dir1/file2.txt"]
        assert fs.listdir("dir1/dir2", recursive=True) == ["dir1/dir2/file1.txt"]

        fs.set("dir1/dir1/file3.txt", b"hello")
        assert sorted(fs.listdir("dir1/dir1", recursive=True)) == [
            "dir1/dir1/file2.txt",
            "dir1/dir1/file3.txt",
        ]

    def initFileSystem(self, fs):
        """Helper for test_nested_filesystems."""
        dirPaths = [
            "",
            "test1",
            "test2",
            fs.joinPaths("test1", "test11"),
            fs.joinPaths("test1", "test12"),
            fs.joinPaths("test2", "test21"),
            fs.joinPaths("test2", "test22"),
        ]
        allVals = []
        fileNames = ["f1.txt", "f2.csv", "f3.py"]

        for path in dirPaths:
            for fileName in fileNames:
                fileKey = fs.joinPaths(path, fileName)
                filePath = fileKey

                fs.set(fileKey, filePath.encode("utf8"))
                allVals.append(filePath)

        self.assertEqual(len(allVals), len(set(allVals)))

        self.assertEqual(len(fs.listFiles()), len(fileNames) * len(dirPaths))

        self.assertEqual(len(allVals), len(fileNames) * len(dirPaths))

        subDirs = fs.listSubdirs()
        self.assertEqual(len(subDirs), 2)
        return allVals

    def clearFileSystem(self, filesystem):
        for key in filesystem.listFiles():
            filesystem.rm(key)

        assert len(filesystem.listFiles()) == 0

    @flaky(max_runs=3, min_passes=1)
    def test_nested_filesystems(self):
        sep = self.filesystem.sep

        fullFileSystem = self.filesystem
        test1FileSystem = self.makeSubFileSystem("test1")
        test2FileSystem = self.makeSubFileSystem("test2")
        test2aFileSystem = self.makeSubFileSystem("test2/")
        test11FileSystem = self.makeSubFileSystem("test1/test11")

        allKeys = self.initFileSystem(fullFileSystem)

        self.assertEqual(sorted(fullFileSystem.listSubdirs("")), ["test1", "test2"])
        self.assertEqual(sorted(fullFileSystem.listSubdirs(sep)), ["test1", "test2"])
        self.assertEqual(
            sorted(fullFileSystem.listSubdirs("test1")),
            ["test1" + sep + "test11", "test1" + sep + "test12"],
        )
        self.assertEqual(
            sorted(fullFileSystem.listSubdirs("test2")),
            ["test2" + sep + "test21", "test2" + sep + "test22"],
        )

        self.assertEqual(
            sorted(fullFileSystem.listSubdirs("test2")),
            sorted(fullFileSystem.listSubdirs("test2" + sep)),
        )
        self.assertEqual(
            sorted(fullFileSystem.listSubdirs("test2" + sep)),
            sorted(fullFileSystem.listSubdirs(sep + "test2" + sep)),
        )
        self.assertEqual(
            sorted(fullFileSystem.listSubdirs(sep + "test2" + sep)),
            sorted(fullFileSystem.listSubdirs(sep + "test2")),
        )

        self.assertEqual(
            sorted(fullFileSystem.listFiles("test2")),
            sorted(fullFileSystem.listFiles("test2" + sep)),
        )
        self.assertEqual(
            sorted(fullFileSystem.listFiles("test2" + sep)),
            sorted(fullFileSystem.listFiles(sep + "test2" + sep)),
        )
        self.assertEqual(
            sorted(fullFileSystem.listFiles(sep + "test2" + sep)),
            sorted(fullFileSystem.listFiles(sep + "test2")),
        )

        for key in ["test21" + sep, sep + "test21" + sep, sep + "test21", ""]:
            self.assertEqual(
                sorted(test2aFileSystem.listSubdirs(key)),
                sorted(test2FileSystem.listSubdirs(key)),
            )
            self.assertEqual(
                sorted(test2aFileSystem.listFiles(key)), sorted(test2FileSystem.listFiles(key))
            )

        self.assertEqual(sorted(test1FileSystem.listSubdirs("")), ["test11", "test12"])
        self.assertEqual(sorted(test2FileSystem.listSubdirs("")), ["test21", "test22"])
        self.assertEqual(
            sorted([x for x in test11FileSystem.listFiles("")]), ["f1.txt", "f2.csv", "f3.py"]
        )

        fullObjectKeys = fullFileSystem.listFiles()
        self.assertEqual(sorted(fullObjectKeys), sorted(allKeys))

        test1ObjectKeys = [
            test1FileSystem.joinPaths("test1", x) for x in test1FileSystem.listFiles()
        ]
        test2ObjectKeys = [
            test2FileSystem.joinPaths("test2", x) for x in test2FileSystem.listFiles()
        ]

        extraKeys = ["f1.txt", "f2.csv", "f3.py"]

        self.assertEqual(
            sorted(fullObjectKeys), sorted(test1ObjectKeys + test2ObjectKeys + extraKeys)
        )

        def checkObjects(filesystem, prefix=""):
            objList = filesystem.listFiles()

            fileVals = []
            fileContents = []
            for key in objList:
                fileVals.append(filesystem.get(key).decode("utf8"))
                fileContents.append(filesystem.joinPaths(prefix, key))
            self.assertEqual(sorted(fileContents), sorted(fileVals))
            return fileVals

        allFileVals = checkObjects(fullFileSystem)
        self.assertEqual(sorted(allFileVals), sorted(allKeys))

        checkObjects(test1FileSystem, "test1")
        checkObjects(test2FileSystem, "test2")

        self.clearFileSystem(test1FileSystem)
        self.clearFileSystem(test2FileSystem)
        remainingFileVals = checkObjects(fullFileSystem)
        self.assertEqual(sorted(remainingFileVals), sorted(extraKeys))

    def test_listFiles(self):
        fs = self.filesystem

        fs.set("f0.txt", b"asdf")
        fs.set("root/f1.txt", b"asdf")
        fs.set("root/lvl1/f2.txt", b"asdf")
        fs.set("root/lvl1/f3.txt", b"asdf")

        self.assertEqual(len(fs.listFiles()), 4)
        self.assertEqual(len(fs.listFiles("a")), 0)
        self.assertEqual(len(fs.listFiles("r")), 3)
        self.assertEqual(len(fs.listFiles("root")), 3)
        self.assertEqual(len(fs.listFiles("root/lvl0")), 0)
        self.assertEqual(len(fs.listFiles("root/lvl")), 2)
        self.assertEqual(len(fs.listFiles("root/lvl1")), 2)
        self.assertEqual(fs.listFiles("root/lvl1/f3.t"), ["root/lvl1/f3.txt"])

    def test_write_once(self):
        fs = WriteOnceFileSystem(self.filesystem)
        fs.set("f0.txt", b"asdf")

        with pytest.raises(OSError):
            fs.rm("f0.txt")

        with pytest.raises(OSError):
            fs.set("f0.txt", b"asdf")

        assert fs.get("f0.txt") == b"asdf"

    def test_WriteProtectedFileSystem(self):
        if not isinstance(self.filesystem, CachedFileSystem):
            assert not self.filesystem.isReadOnly

        roFs = WriteProtectedFileSystem(self.filesystem)
        assert roFs.isReadOnly
        path = "f0.txt"
        data = b"asdf"

        with pytest.raises(OSError):
            roFs.set(path, data)

        self.filesystem.set(path, data)
        assert roFs.get(path) == data

        with pytest.raises(OSError):
            roFs.rm(path)

        self.filesystem.rm(path)
        assert not roFs.exists(path)

        assert roFs != self.filesystem
        assert str(roFs) != str(self.filesystem)
        assert hash(roFs) != hash(self.filesystem)

        roFs2 = WriteProtectedFileSystem(self.filesystem)
        assert roFs2 == roFs
        assert str(roFs2) == str(roFs)
        assert hash(roFs2) == hash(roFs)


class CloningFileSystemTestCases:
    def test_cloning_filesystem(self):
        # Similar to test_bytestreams but peer into the front and back filesystems
        fs = self.filesystem
        bfs = self.backFileSystem
        ffs = self.frontFileSystem

        data = b"asdf"
        bs = io.BytesIO(data)
        fname = "file.txt"

        assert not fs.exists(fname)
        fs.set(fname, bs)
        assert fs.exists(fname)
        assert fs.get(fname) == data
        assert ffs.get(fname) == data
        assert bfs.get(fname) == data

        with pytest.raises(ValueError):
            fs.set(fname, bs)

        assert fs.get(fname) == data
        assert ffs.get(fname) == data
        assert bfs.get(fname) == data

        bs.seek(0)
        fs.set(fname, bs)
        assert fs.get(fname) == data
        assert ffs.get(fname) == data
        assert bfs.get(fname) == data

        bsIn = io.BytesIO()
        fs.getInto(fname, bsIn)
        assert bsIn.getvalue() == data

        with pytest.raises(ValueError):
            fs.getInto(fname, bsIn)
        assert bsIn.getvalue() == data

        bsIn.seek(0)
        fs.getInto(fname, bsIn)
        assert bsIn.getvalue() == data

        # Check that getInto copies file from back to front
        fs.rm(fname)
        assert not fs.exists(fname)
        assert not ffs.exists(fname)
        assert not bfs.exists(fname)

        bs.seek(0)
        bfs.set(fname, bs)

        bsIn.seek(0)
        fs.getInto(fname, bsIn)
        assert bsIn.getvalue() == data
        assert ffs.get(fname) == data

        # Check that get copies file from back to front
        fs.rm(fname)
        assert not fs.exists(fname)
        assert not ffs.exists(fname)
        assert not bfs.exists(fname)

        bs.seek(0)
        bfs.set(fname, bs)

        assert fs.get(fname)
        assert ffs.get(fname) == data


class DiskFilesystemTests(FileSystemTestCases, unittest.TestCase):
    def setUp(self):
        self.filesystem = TempDiskFileSystem()

    def tearDown(self):
        self.filesystem.tearDown()

    def makeSubFileSystem(self, prefix):
        rootPath = self.filesystem.joinPaths(self.filesystem.rootPath, prefix)
        return DiskFileSystem(rootPath)


class InMemFilesystemTests(FileSystemTestCases, unittest.TestCase):
    def setUp(self):
        self.filesystem = InMemFileSystem()

    def tearDown(self):
        self.filesystem.tearDown()

    def makeSubFileSystem(self, prefix):
        rootPath = self.filesystem.joinPaths(self.filesystem.rootPath, prefix)
        return InMemFileSystem(rootPath)

    def test_dont_teardown_subfilesystem(self):
        fs = self.filesystem
        fs.set("a/b/c", b"abc")
        subFs = self.makeSubFileSystem("a")
        assert subFs.listFiles() == ["b/c"]
        subFs.tearDown()
        assert fs.listFiles() == ["a/b/c"]


class _WriteableCachedFileSystem(CachedFileSystem):
    """A class to enable running our tests against CachedFileSystem."""

    def rm(self, path):
        if self.frontFileSystem.exists(path):
            self.frontFileSystem.rm(path)
        return self.backFileSystem.rm(path)

    def set(self, path, content):
        return self.backFileSystem.set(path, content)


class CachedFileSystemTests(FileSystemTestCases, unittest.TestCase):
    def setUp(self):
        self.backFileSystem = TempDiskFileSystem()
        self.frontFileSystem = InMemFileSystem()
        self.filesystem = _WriteableCachedFileSystem(self.frontFileSystem, self.backFileSystem)

    def tearDown(self):
        self.backFileSystem.tearDown()
        self.frontFileSystem.tearDown()

    def makeSubFileSystem(self, prefix):
        backRootPath = self.backFileSystem.joinPaths(self.backFileSystem.rootPath, prefix)
        subBack = DiskFileSystem(backRootPath)

        frontRootPath = self.frontFileSystem.joinPaths(self.frontFileSystem.rootPath, prefix)
        subFront = InMemFileSystem(frontRootPath)

        subCached = _WriteableCachedFileSystem(subFront, subBack)

        return subCached


class CloningFileSystemTests(
    FileSystemTestCases, CloningFileSystemTestCases, unittest.TestCase
):
    def setUp(self):
        self.backFileSystem = TempDiskFileSystem()
        self.frontFileSystem = InMemFileSystem()
        self.filesystem = CloningFileSystem(self.frontFileSystem, self.backFileSystem)

    def tearDown(self):
        self.backFileSystem.tearDown()
        self.frontFileSystem.tearDown()

    def makeSubFileSystem(self, prefix):
        backRootPath = self.backFileSystem.joinPaths(self.backFileSystem.rootPath, prefix)
        subBack = DiskFileSystem(backRootPath)

        frontRootPath = self.frontFileSystem.joinPaths(self.frontFileSystem.rootPath, prefix)
        subFront = InMemFileSystem(frontRootPath)

        subCloned = CloningFileSystem(subFront, subBack)

        return subCloned


class MockS3ToMockS3CloningFileSystemTests(
    FileSystemTestCases, CloningFileSystemTestCases, unittest.TestCase
):
    MODTIME_DIFFERENCE_THRESHOLD = 1.1

    @classmethod
    def setUpClass(cls):
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"

        cls.bucketname = "nomadresearch-test"
        cls.mock = mock_s3()
        cls.mock.start()
        conn = boto3.resource("s3")
        conn.create_bucket(Bucket=cls.bucketname)

    @classmethod
    def tearDownClass(cls):
        cls.mock.stop()

    def makeFilesystem(self, keyPrefix):
        return S3FileSystem(bucketname=self.bucketname, keyPrefix=keyPrefix)

    @staticmethod
    def generateUniqueKeyPrefix():
        return str(uuid.uuid4())

    def setUp(self):
        self.backFileSystem = S3FileSystem(
            bucketname=self.bucketname, keyPrefix=self.generateUniqueKeyPrefix()
        )
        self.frontFileSystem = S3FileSystem(
            bucketname=self.bucketname, keyPrefix=self.generateUniqueKeyPrefix()
        )
        self.filesystem = CloningFileSystem(self.frontFileSystem, self.backFileSystem)

    def tearDown(self):
        self.backFileSystem.tearDown()
        self.frontFileSystem.tearDown()

    def makeSubFileSystem(self, prefix):
        backPrefix = S3FileSystem.joinPaths(self.backFileSystem.keyPrefix, prefix)
        subBack = S3FileSystem(self.bucketname, backPrefix)

        frontPrefix = S3FileSystem.joinPaths(self.frontFileSystem.keyPrefix, prefix)
        subFront = S3FileSystem(self.bucketname, frontPrefix)

        subCloned = CloningFileSystem(subFront, subBack)

        return subCloned


class S3FileSystemTestCases(FileSystemTestCases):
    MODTIME_DIFFERENCE_THRESHOLD = 1.1

    @classmethod
    def setUpClass(cls):
        cls.bucketname = "nomadresearch-test"

    def makeFilesystem(self, keyPrefix):
        return S3FileSystem(bucketname=self.bucketname, keyPrefix=keyPrefix)

    @staticmethod
    def generateUniqueKeyPrefix():
        return str(uuid.uuid4())

    def setUp(self):
        self.keyPrefix = self.generateUniqueKeyPrefix()
        self.filesystem = S3FileSystem(bucketname=self.bucketname, keyPrefix=self.keyPrefix)

    def tearDown(self):
        for key in self.filesystem.listFiles():
            self.filesystem.rm(key)

        assert len(self.filesystem.listdir()) == 0

    def makeSubFileSystem(self, prefix):
        subFileSystemPrefix = S3FileSystem.joinPaths(self.keyPrefix, prefix)
        return S3FileSystem(bucketname=self.bucketname, keyPrefix=subFileSystemPrefix)


class MockS3FileSystemTests(S3FileSystemTestCases, unittest.TestCase):
    AWS_ENV_VARS = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SECURITY_TOKEN",
        "AWS_SESSION_TOKEN",
    )

    @classmethod
    def setUpClass(cls):
        # stash the AWS environment variables we will override
        cls.stashed_aws_environment = {var: os.environ.get(var) for var in cls.AWS_ENV_VARS}

        # override some AWS environment variables to ensure we don't hit live AWS
        for var in cls.AWS_ENV_VARS:
            os.environ[var] = "testing"

        S3FileSystemTestCases.setUpClass()
        cls.mock = mock_s3()
        cls.mock.start()
        conn = boto3.resource("s3")
        conn.create_bucket(Bucket=cls.bucketname)

    @classmethod
    def tearDownClass(cls):
        # restore AWS environment variables
        for var in cls.AWS_ENV_VARS:
            os.environ[var] = cls.stashed_aws_environment[var]

        cls.mock.stop()


class BareFtpFileSystemTests(FileSystemTestCases, unittest.TestCase):
    MODTIME_DIFFERENCE_THRESHOLD = 1.1

    def setUp(self):
        self.username = "admin"
        self.password = "admin"
        self.server = FtpServer(username=self.username, password=self.password)
        self.host = self.server.host
        self.port = self.server.port

        self.filesystem = FtpFileSystem(self.username, self.password, self.host, self.port)

    def tearDown(self):
        self.filesystem.tearDown()
        self.server.tearDown()

    def makeSubFileSystem(self, prefix):
        subRootPath = self.filesystem.joinPaths(self.filesystem.rootPath, prefix)
        return FtpFileSystem(self.username, self.password, self.host, self.port, subRootPath)


class RootedFtpFileSystemTests(BareFtpFileSystemTests):
    def setUp(self):
        BareFtpFileSystemTests.setUp(self)
        self.filesystem._rootPath = "root/path"
        self.filesystem._makeParentDirsIfNeeded(self.filesystem._rooted("fakefile.txt"))
        assert self.filesystem.exists("")


class FtpFileSystemTestsNoMlsd(BareFtpFileSystemTests):
    def setUp(self):
        BareFtpFileSystemTests.setUp(self)
        self.patchFileSystem(self.filesystem)

    @staticmethod
    def patchFileSystem(filesystem):
        assert filesystem._canUseMlsd is True, filesystem._canUseMlsd
        filesystem._canUseMlsdCache = False

    def makeSubFileSystem(self, prefix):
        subFileSystem = super().makeSubFileSystem(prefix)
        self.patchFileSystem(subFileSystem)
        return subFileSystem


class BareSftpFileSystemTests(FileSystemTestCases, unittest.TestCase):
    MODTIME_DIFFERENCE_THRESHOLD = 1.1

    def setUp(self):
        self.username = "admin"
        self.password = "admin"

        self.server = SftpServer()
        self.host = self.server.host
        self.port = self.server.port

        self.filesystem = SftpFileSystem(self.username, self.password, self.host, self.port)

    def tearDown(self):
        self.filesystem.tearDown()
        self.server.tearDown()

    def makeSubFileSystem(self, prefix):
        subRootPath = self.filesystem.joinPaths(self.filesystem.rootPath, prefix)
        return SftpFileSystem(self.username, self.password, self.host, self.port, subRootPath)


class RootedSftpFileSystemTests(BareSftpFileSystemTests):
    MODTIME_DIFFERENCE_THRESHOLD = 1.1

    def setUp(self):
        BareSftpFileSystemTests.setUp(self)
        self.filesystem._rootPath = "root/path"
        self.filesystem._makeParentDirsIfNeeded(self.filesystem._rooted("fakefile.txt"))
        assert self.filesystem.exists("")

    def tearDown(self):
        self.filesystem.tearDown()
        self.server.tearDown()

    def makeSubFileSystem(self, prefix):
        subRootPath = self.filesystem.joinPaths(self.filesystem.rootPath, prefix)
        return SftpFileSystem(self.username, self.password, self.host, self.port, subRootPath)
