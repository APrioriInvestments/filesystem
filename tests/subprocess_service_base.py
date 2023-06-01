import subprocess
import tempfile
import time

from abc import ABC, abstractmethod
from .util import findFreePort


class SubprocessServiceBase(ABC):
    def __init__(
        self,
        name,
        host="localhost",
        port=0,
        rootDir=None,
        maxTries=None,
        pollLag=0.3,
        connectTimeout=0.5,
    ):
        """Create a service in a subprocess

        Args:
            name (str): name of the service, e.g., Redis, Ftp
            localhost (str): the host on which to run the service
            port (int): if specified, the port on which the service
                will run. If zero, a free port is found.
            maxTries (int): the number of tries to find a free port,
                if no port was specified. If None, we will try
                indefinitely.
            rootDir (str): path to the root directory for the service
            pollLag (float): the number of seconds to poll when checking
                if the subprocess is still online after starting it. If
                None, no polling is performed
            connectTimeout (float): the number of seconds to try to
                connect to the service after starting it. This number
                should be larger if the servie boot time is longer.
                If None, the check is not performed.
        """
        self._name = name

        if not hasattr(self, "_logger"):
            raise Exception(
                f"Derived SubprocessService class for {self._name} service must "
                "define self._logger before calling super()."
            )
        self._host = host
        self._port = port
        self._pollLag = pollLag
        self._connectTimeout = connectTimeout
        self._serviceProcess = None

        if rootDir is None:
            self._tempDir = tempfile.TemporaryDirectory()
            self._rootDir = self._tempDir.name
        else:
            self._tempDir = None
            self._rootDir = rootDir

        if self._port != 0:
            if self._tryStartServer():
                self._logger.info(f"Started {self._name} service on port {self._port}")
                return
            else:
                raise Exception(f"Failed to create {self._name} service on port {self._port}")

        # else (self._port == 0)
        tries = 0
        failedPorts = []
        if maxTries is None:
            cond = lambda: True
        else:
            cond = lambda: tries < maxTries

        while cond():
            self._port = findFreePort()
            if self._tryStartServer():
                self._logger.info(f"Started {self._name} service on port {self._port}")
                return
            tries += 1
            failedPorts.append(self._port)

        raise Exception(
            f"Failed to create {self._name} service on ports {failedPorts} "
            f"after {tries} tries"
        )

    @property
    def port(self):
        return self._port

    @property
    def host(self):
        return self._host

    def _pollSubprocess(self):
        """Poll the subprocess for errors. Return None if all is ok.

        Poll may return None (meaning the process is ok) when a process is
        in the process of dying, but is not dead yet. So, keep checking for
        some time before returning.
        """

        if self._serviceProcess is None or self._pollLag is None:
            return

        res = None
        t0 = time.time()
        while time.time() - t0 < self._pollLag:
            res = self._serviceProcess.poll()
            if res is not None:
                return res
            time.sleep(self._pollLag / 10)

    def _pollCanConnect(self):
        """Return True if we were able to connect or if we shouldn't check."""
        if self._connectTimeout is None:
            return True

        couldConnect = False
        t0 = time.time()
        while not couldConnect and time.time() - t0 < self._connectTimeout:
            couldConnect = self.canConnect()
            if not couldConnect:
                time.sleep(self._connectTimeout / 10)

        return couldConnect

    @abstractmethod
    def canConnect(self):
        """Returns True if we can connect, False if not."""
        pass

    @abstractmethod
    def _buildCmd(self):
        """Returns a list of strings to use as the command argument in subprocess.Popen."""
        pass

    def _popenKwargs(self):
        return {}

    def _tryStartServer(self) -> bool:
        """Tries to start the server in a subprocess.

        Returns True upon success or False on Failure
        """
        try:
            if self._connectTimeout is not None and self.canConnect():
                self._logger.error(
                    f"Connected to {self._name} service on port {self.port} "
                    "before starting it."
                )
                return False

            cmd = self._buildCmd()
            kwargs = self._popenKwargs()
            self._logger.info(f"Starting {self._name} service with command: {cmd}")
            self._serviceProcess = subprocess.Popen(cmd, **kwargs)

            if not self._pollCanConnect():
                self._logger.error(
                    f"Failed to connect to {self._name} service after starting it."
                )
                self.tearDownSubProcess()
                return False

            # poll is None when the subprocess is healthy
            if self._pollSubprocess() is not None:
                self.tearDownSubProcess()
                self._logger.error(f"{self._name} service died unexpectedly.")
                return False

            return True

        except Exception:
            self._logger.exception(f"{self._name} service initialization failed.")
            self.tearDownSubProcess()
            return False

        except BaseException:
            self.tearDownSubProcess()
            raise

    def tearDownSubProcess(self):
        if self._serviceProcess:
            self._serviceProcess.terminate()
            self._serviceProcess.wait()
        self._serviceProcess = None

    def tearDown(self):
        self.tearDownSubProcess()

        if self._tempDir is not None:
            self._tempDir.cleanup()
            self._tempDir = None

    def __del__(self):
        self.tearDown()
