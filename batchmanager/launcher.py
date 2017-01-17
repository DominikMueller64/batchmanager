import Pyro4
import subprocess
import sys
import logging
import os
import datetime
import psutil
import multiprocessing
import warnings
import socket

@Pyro4.expose
class launcher:

    def __init__(self, daemon):
        self.daemon = daemon
        self._pid = os.getpid()

    @property
    def processes(self):
        return self._processes

    @property
    def pid(self):
        return self._pid

def main(name):
    with Pyro4.Daemon() as daemon:
        mylauncher = launcher(daemon)
        mylauncher_uri = daemon.register(mylauncher)
        with Pyro4.locateNS() as ns:
            ns.register(name, mylauncher_uri)
        daemon.requestLoop()

if __name__ == '__main__':
    main(name=sys.argv[1])

