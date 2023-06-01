import io


class CloseProtectedStream(io.IOBase):
    """Wraps an underlying stream but protects from code closing it.

    This is a work-around for the bug in boto3 where upload_fileobj will (sometimes)
    close the file-like object passed to it:

    https://github.com/boto/s3transfer/issues/80
    """

    def __init__(self, stream):
        if not isinstance(stream, io.IOBase):
            raise TypeError(f"stream argument must be an io.IOBase, but was {type(stream)}")

        self._stream = stream
        self._closed = stream.closed

    # Implement io.IOBase
    def close(self):
        self._closed = True

    @property
    def closed(self):
        return self._closed

    def fileno(self):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.fileno()

    def flush(self):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.flush()

    def isatty(self):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.isatty()

    def readable(self):
        return False if self._closed else self._stream.readable()

    def readline(self, size=-1, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.readline(size)

    def readlines(self, hint=-1, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.readlines(hint)

    def seek(self, offset, whence=io.SEEK_SET, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.seek(offset, whence)

    def seekable(self):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.seekable()

    def tell(self):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.tell()

    def truncate(self, size=None, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.truncate(size)

    def writable(self):
        return False if self._closed else self._stream.writable()

    def writelines(self, lines, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.writelines(lines)

    def __del__(self):
        self._closed = True

    # Implement io.RawIOBase
    def read(self, size=-1, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.read(size)

    def readall(self):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.readall()

    def readinto(self, b, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.readinto(b)

    def write(self, b, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.write(b)

    def read1(self, size=-1, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.read1(size)

    def readinto1(self, b, /):
        if self._closed:
            raise ValueError("I/O operation on closed Stream")
        return self._stream.readinto1(b)
