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
import shortuuid
import inspect
import time
import signal
import threading
import click
import io
import collections

from .functions import *
from .globvar import *

# Logging.
module_logger = logging.getLogger(__name__)
module_logger.info('Start logging.')


class async_reader(threading.Thread):

    def __init__(self, stream):
        super().__init__(group=None, target=None, name='async_reader', daemon=True)
        self.stream = stream
        self.buffer = io.StringIO()

    def run(self):
        for line in iter(self.stream.readline, ''):
            self.buffer.write(line)

    @classmethod
    def new(cls, stream):
        reader = cls(stream)
        reader.start()
        return reader


class Process:

    states = ('running', 'finished', 'terminated', 'killed', 'aborted')

    table = {None: 'running',  # Process is still running.
             0: 'finished',  # Process finished. Does not guarantee correctness!
             signal.SIGTERM.value: 'terminated',
             signal.SIGKILL.value: 'killed'}

    update_interval = 1  # Interval of updating the process (seconds).

    @classmethod
    # constructor
    def from_job(cls, job):
        prefix = job['prefix']
        script = job['script']
        args = job['args']
        cmdlist = [*prefix, script, *args]

        try:
            proc = subprocess.Popen(args=cmdlist,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

        except (FileNotFoundError, NotADirectoryError) as err:
            pass
        else:
            process = cls(proc=proc, job=job)
            return process

    def __init__(self, proc, job):
        self.pid = os.getpid()
        self.start = get_date()
        self.start_fmt = format_date(self.start)
        self.host_name = socket.gethostname()
        self.ip = Pyro4.socketutil.getIpAddress(None, workaround127=True)
        self.id = job['id']
        self.proc = proc
        self.job = job
        self.status = None

        self.start_updater()  # Start the updater thread upon initialization.
        self.stdout_reader = async_reader.new(io.TextIOWrapper(proc.stdout, encoding='utf-8'))
        self.stderr_reader = async_reader.new(io.TextIOWrapper(proc.stderr, encoding='utf-8'))


    # The status can take on: 'running', 'finished', 'terminated', 'failed'
    def updater(self):
        while True:
            poll = self.proc.poll()
            self.status = self.table.get(poll, 'aborted')  # Aborted if not found.
            if poll is not None:
                break  # If not running, die!
            time.sleep(self.update_interval)

    def start_updater(self):
        thread = threading.Thread(name='thread-updater',
                                  target=self.updater,
                                  daemon=True)
        thread.start()

    def read_stdout(self):
        return self.stdout_reader.buffer.getvalue()

    def read_stderr(self):
        return self.stderr_reader.buffer.getvalue()


@Pyro4.expose
class server:

    @classmethod
    def virtual_memory(cls):
        return psutil.virtual_memory()

    @classmethod
    def avail_mem(cls):
        return psutil.virtual_memory()[1]

    @classmethod
    def load_average(cls):
        return os.getloadavg()

    @classmethod
    def load(cls):
        return os.getloadavg()[0]

    def __init__(self, name, host, max_proc, max_load, ns, daemon):

        self._name = name
        self._host_name = host,
        self._max_proc = max_proc
        self._max_load = max_load
        self._ns = ns
        self._daemon = daemon

        self._processes = dict()
        self._pid = os.getpid()
        self._start = get_date()
        self._start_fmt = format_date(self.start)
        self._cpu_count = multiprocessing.cpu_count()
        self._ip = socket.gethostbyname(daemon.uriFor('myserver').host)  # Not perfect solution.
        self._id = shortuuid.uuid()

        self._lock = threading.RLock()

        self.logger = logging.getLogger(__name__ + '.batchserver')

    def is_available(self, mem):
        if (self.load() < self.max_load and
            float(self.avail_mem()) > mem and
            self.get_num_proc(status='running') < self.max_proc):
            return True
        return False

    def get_start_date(self, format=False):
        date = self.start
        if format:
            return format_date(date)
        return date

    def clear_proc(self):
        ids = set()
        for id, process in self.processes.items():
            if process.status not in (None, 'running'):
                ids.add(id)
        for id in ids:
            del self.processes[id]

    def get_num_proc(self, status='running'):
        if status not in Process.states:
            raise ValueError("'status' must be one of {}.".format(
                              ', '.join(Process.states)))

        states = tuple(p.status for p in self.processes.values())
        return states.count(status)

    def start_proc(self, job):
        fun_name = inspect.currentframe().f_code.co_name
        logger = logging.getLogger('.'.join((self.logger.name, fun_name)))

        process = Process.from_job(job)
        self.processes[process.id] = process  # Register newly launched process.
        return process.id  # Return process id for the caller (batchmanager).

    def get_proc_ids(self):
        return tuple(p.id for p in self.processes.values())

    def get_proc(self, id):
        try:
            process = self.processes[id]
        except KeyError:
            warnings.warn('Process not found.', RuntimeWarning)
        else:
            return process

    def get_proc_stdout(self, id):
        return self.get_proc(id).read_stdout()

    def get_proc_stderr(self, id):
        return self.get_proc(id).read_stderr()

    def get_proc_job(self, id):
        return self.get_proc(id).job

    def get_proc_start_date(self, id, format=False):
        if format:
            return self.get_proc(id).start_fmt
        return self.get_proc(id).start

    def get_proc_pid(self, id):
        return self.get_proc(id).proc.pid

    def get_proc_status(self, id):
        return self.get_proc(id).status

    def terminate_proc(self, id):
        self.get_proc(id).proc.terminate()

    def kill_proc(self, id):
        self.get_proc(id).proc.kill()

    def delete_proc(self, id):
        if self.get_proc(id).proc.poll() is None:
            warnings.warn('Process is still running.', RuntimeWarning)
        else:
            del self.processes[id]

    @Pyro4.oneway   # in case call returns much later than daemon.shutdown
    def shutdown(self, terminate_processes=True):
        if terminate_processes:
            for proc in self.processes:
                proc.terminate()
        if self.ns:
            self.ns.remove(self.name)
        self.daemon.shutdown()

    # Properties are necessary for Pyro communication.
    @property
    def name(self):
        return self._name

    @property
    def host_name(self):
        return self._host_name

    @property
    def max_proc(self):
        return self._max_proc

    @property
    def max_load(self):
        return self._max_load

    @property
    def ns(self):
        return self._ns

    @property
    def daemon(self):
        return self._daemon


    @property
    def processes(self):
        return self._processes

    @property
    def pid(self):
        return self._pid

    @property
    def start(self):
        return self._start

    @property
    def start_fmt(self):
        return self._start_fmt

    @property
    def cpu_count(self):
        return self._cpu_count

    @property
    def ip(self):
        return self._ip

    @property
    def id(self):
        return self._id

    @property
    def lock(self):
        return self._lock

@click.command()
@click.option('-N', '--name', default=socket.gethostname(), type=str,
              help='name of the server in the network ' +
              '(default: host)')

@click.option('-H', '--host',
              default=Pyro4.socketutil.getIpAddress(None, workaround127=True),
              type=click.STRING,
              help='hostname or ip address to bind the batchserver on. '
              'default: hostname of this machine.')
@click.option('-P', '--port', default=0, type=click.INT,
              help='port to bind the batchserver on. '
                   'default: a random port.')

@click.option('-M', '--max_proc', default=multiprocessing.cpu_count(),
              type=click.IntRange(min=1, max=2 * multiprocessing.cpu_count()),
              help='maximum number of allowed processes '
                   '(default: number of detected cpu on this machine)')

@click.option('-L', '--max_load', default=multiprocessing.cpu_count(),
              type=click.FLOAT,
              help='maximum load allowed '
                   '(default: number of detected cpu on this machine.)')

@click.option('-K', '--key', default=None, type=click.STRING,
              help='the HMAC key to use.')

@click.option('--ns/--no-ns', default=False,
              help='Enable/disable contacting a name server. '
                   '(default: --ns. If --no-ns, all other options relating to the '
                   'name server are ignored.)')

@click.option('--nshost', default=None, type=click.STRING,
              help='hostname or ip address of the name server in the network '
                   '(default: None. A network broadcast lookup is used.)')

@click.option('--nsport', default=None, type=click.INT,
              help='the port number on which the name server is running. '
                   '(default: None. The exact meaning depends on whether the host parameter is given: '
                '- host parameter given: the port now means the actual name server port. '
                '- host parameter not given: the port now means the broadcast port.)')

def main(name, host, port, max_proc, max_load, ns, nshost, nsport, key):
    """Start a server process.\n
    A server process serves as a handle for the batch process manager and runs
    on an individual server in the network.

    Before any server process can be started, it is necessary to start a Pyro4
    nameserver. This nameserver can be started with

        pyro4-ns --host <ip address or hostname of hosting server> &

    The hostname can be obtain from the command line by typing 'hostname'.
    It is sufficient to start a single nameserver in the network.

    When a server is started, it automatically registers itself in
    the nameserver and can be retrieved and accessed by the manager from there.
    """
    # Pyro4.config.SERIALIZERS_ACCEPTED.add('dill')  # Accept pickle as incoming serialization format.
    with Pyro4.Daemon(host=host, port=port) as daemon:
        daemon._pyroHmacKey = key
        myserver = server(name=name, host=host,
                          max_proc=max_proc, max_load=max_load,
                          ns=None,
                          daemon=daemon)
        myserver_uri = daemon.register(myserver)
        uri = daemon.uriFor('myserver')

        if ns:
            # Try to register the daemon in a name server.
            try:
                with Pyro4.locateNS(host=nshost, port=nsport,
                                    broadcast=True, hmac_key=key) as ns:
                    try:
                        ns.register(name, uri=myserver_uri, safe=False)
                    except Pyro4.errors.NamingError as err:
                        print('{} is already registered in the name server, '.format(name) +
                              'use a different one!')
                        raise
                    else:
                        myserver.ns = ns

            except ConnectionRefusedError as err:
                print('Connection to a name server failed. '
                      'The batchserver will not be registered in a name server. '
                      'Directly use the printed uri for registring the batchserver in '
                      'the batchmanager.')

        print()
        print('host: {}'.format(uri.host))
        print('port: {}'.format(uri.port))
        # print('uri: {}'.format(uri.asString()))  # Not the same as myserver_uri!
        print('uri: {}'.format(myserver_uri))

        daemon.requestLoop()

if __name__ == '__main__':
    main()
