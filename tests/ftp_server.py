import ftplib
import logging
import subprocess

from .subprocess_service_base import SubprocessServiceBase


class FtpServer(SubprocessServiceBase):
    def __init__(
        self,
        username=None,
        password=None,
        readOnly=False,
        host="localhost",
        port=0,
        rootDir=None,
        maxTries=10,
    ):
        """Create an FTP server service in a process."""

        if username is not None and password is None:
            raise Exception("FtpServer needs both a username and a password or neither")

        self._logger = logging.getLogger(__name__)
        self._username = username
        self._password = password
        self._readOnly = readOnly
        self._pythonBinary = self._discoverPythonBinary()

        super().__init__("FTP", host, port, rootDir, maxTries)

    def connect(self):
        """Try to connect. Returns None if we can't."""
        try:
            client = ftplib.FTP()

            client.connect(self._host, self._port)

            if self._username and self._password:
                client.login(self._username, self._password)
            else:
                client.login("anonymous")

            client.nlst()

            return client

        except Exception:
            return None

    def canConnect(self):
        client = self.connect()
        if client is None:
            return False
        else:
            # first disconnect, then return True
            try:
                client.quit()
            except Exception:
                pass
            return True

    def _buildCmd(self):
        cmd = [self._pythonBinary, "-m", "pyftpdlib"]
        args = [
            f"--interface={self._host}",
            f"--port={self._port}",
            f"--directory={self._rootDir}",
        ]
        if self._username and self._password:
            args.extend([f"--username={self._username}", f"--password={self._password}"])
        if not self._readOnly:
            args.append("--write")

        return cmd + args

    def _discoverPythonBinary(self):
        # try python3
        try:
            subprocess.Popen(["python3", "--version"])
            return "python3"
        except Exception:
            self._logger.exception("Didn't find 'python3'")
            pass

        # try python
        try:
            subprocess.Popen(["python", "--version"])
            return "python"
        except Exception:
            self._logger.exception("Didn't find 'python'")
            raise Exception("Didn't find a python binary called 'python3' or 'python'")
