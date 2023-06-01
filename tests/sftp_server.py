"""
SftpServer

utilities for bringing up a temporary Sftp backend for testing purposes.
"""
import logging
import os
import paramiko
import subprocess
import tempfile


from .subprocess_service_base import SubprocessServiceBase

CONNECT_TIMEOUT = 1.0


class SftpServer(SubprocessServiceBase):
    def __init__(self, host="localhost", port=0, rootDir=None, maxTries=10):
        """Create an SFTP server service in a process."""
        self._logger = logging.getLogger(__name__)
        self._tempKeyDir = tempfile.TemporaryDirectory()
        self._tempKeyFilepath = os.path.join(self._tempKeyDir.name, "private.pem")
        self._writeKey()

        super().__init__("SFTP", host, port, rootDir, maxTries, connectTimeout=CONNECT_TIMEOUT)

    def _writeKey(self):
        subprocess.run(
            ["openssl", "genrsa", "-out", self._tempKeyFilepath, "4096"], check=True
        )

        # as of openssl3.0.0, genras produces 'PCKS#8' style keys, which
        # are not what paramiko wants - it wants 'PCKS#1' keys, which are more 'traditional'
        # this command converts the key type.
        # see https://bugs.launchpad.net/ubuntu/+source/openssl/+bug/1973344
        subprocess.run(
            [
                "openssl",
                "rsa",
                "-in",
                self._tempKeyFilepath,
                "-out",
                self._tempKeyFilepath,
                "-traditional",
            ],
            check=True,
        )

    def _buildCmd(self):
        return [
            "sftpserver",
            "--host",
            self._host,
            "--port",
            str(self._port),
            "--keyfile",
            self._tempKeyFilepath,
        ]

    def _popenKwargs(self):
        return dict(cwd=self._rootDir)

    def connect(self):
        transport = None
        client = None

        try:
            transport = paramiko.Transport((self._host, self._port))
            transport.connect(username="admin", password="admin")
            client = paramiko.SFTPClient.from_transport(transport)

        except Exception:
            self.disconnect(transport, client)
            transport = None
            client = None

        return transport, client

    @staticmethod
    def disconnect(transport, client):
        if client is not None:
            client.close()
        if transport is not None:
            transport.close()

    def canConnect(self):
        transport, client = self.connect()

        if transport is not None and client is not None:
            result = True
        else:
            result = False

        self.disconnect(transport, client)
        return result

    def tearDown(self):
        if self._tempKeyDir is not None:
            self._tempKeyDir.cleanup()
            self._tempKeyDir = None

        super().tearDown()
