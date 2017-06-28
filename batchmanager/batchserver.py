import collections
import datetime
import getpass
import inspect
import io
import logging
import multiprocessing
import os
import pickle
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
import warnings
import base64

import click
import psutil
import Pyro4
import shortuuid

from .functions import *
from .globvar import *

# Logging.
module_logger = logging.getLogger(__name__)
module_logger.info('Start logging.')

# ## Code for testing async_reader
# proc = subprocess.Popen(['ls', '-ahl'],
#                         universal_newlines=True,
#                         bufsize=1,
#                         stdout=subprocess.PIPE
#                         )
# print(proc.stdout.closed)
# r = async_reader.new(proc.stdout)
# print(proc.stdout.closed)
# print(r.list())
# print(r.isAlive())


class async_reader(threading.Thread):

    def __init__(self, stream, queue):
        super().__init__(group=None, target=None, name='async_reader', daemon=True)
        self.stream = stream
        self.queue = queue

    def run(self):
        ## See for more discussion:
        ## http://stackoverflow.com/questions/38181494/what-is-the-difference-
        ## between-using-universal-newlines-true-with-bufsize-1-an
        with self.stream as stream:  # Closes the stream automatically.
            ## Maybe this is necessary, but I don't understand the difference:
            ## It does not stop the thread when universal_newlines=True and bufsize=1 !
            # for line in iter(self.stream.readline, b''):
            for line in stream:
                self.queue.put(line.strip())  # Strip whitespace.

    def list(self):
        with self.queue.mutex:
            return list(self.queue.queue)

        # # Potentially obsolete solution by Raymond Hettinger:
        # # http://stackoverflow.com/questions/8196254/iterating-queue-queue-items
        # mycopy = []
        # while True:
        #     try:
        #         elem = q.get(block=False)
        #     except queue.Empty:
        #         break
        #     else:
        #         mycopy.append(elem)
        # for elem in mycopy:
        #     q.put(elem)
        # return mycopy

    @classmethod
    def new(cls, stream, start=True):
        q = queue.Queue()
        r = cls(stream, q)
        if start:
            r.start()
        return r


class Process(threading.Thread):

    states = ('running', 'finished', 'terminated', 'killed', 'aborted')

    table = {None: 'running',  # Process is still running.
             0: 'finished',  # Process finished. Does not guarantee correctness!
             signal.SIGTERM.value: 'terminated',
             signal.SIGKILL.value: 'killed'}

    update_interval = 1  # Interval of updating the process (seconds).


    def __init__(self, proc, job):
        super().__init__(group=None, target=None, name='thread-updater', daemon=True)

        self.pid = os.getpid()
        self.start_date = get_date()
        self.host_name = socket.gethostname()
        self.ip = Pyro4.socketutil.getIpAddress(None, workaround127=True)
        self.id = job['id']
        self.proc = proc
        self.job = job
        self.status = None
        self.runtime = None

        # Start threads.
        self.start()
        self.stdout_reader = async_reader.new(proc.stdout)
        self.stderr_reader = async_reader.new(proc.stderr)
        ## If universal_newlines=False, wrapping with io.TextIOWrapper is required:
        # self.stdout_reader = async_reader.new(io.TextIOWrapper(proc.stdout, encoding='utf-8'))
        # self.stderr_reader = async_reader.new(io.TextIOWrapper(proc.stderr, encoding='utf-8'))

    def get_start_date(self):
        return format_date(self.start_date)

    def get_runtime(self):
        return strfdelta(get_date() - self.start_date, "%D:%H:%M:%S")

    def get_stdout(self):
        return self.stdout_reader.list()

    def get_stderr(self):
        return self.stderr_reader.list()

    # The status can take on: 'running', 'finished', 'terminated', 'failed'
    # def updater(self):
    def run(self):
        while True:
            self.runtime = strfdelta(get_date() - self.start_date, "%D:%H:%M:%S")
            poll = self.proc.poll()
            self.status = self.table.get(poll, 'aborted')  # Aborted if not found.
            if poll is not None:
                break  # If not running, die!
            time.sleep(self.update_interval)

    @classmethod
    def from_job(cls, job):
        prefix = job['prefix']
        script = job['script']
        args = job['args']
        cmdlist = [*prefix, script, *args]

        try:
            proc = subprocess.Popen(args=cmdlist,
                                    # line-buffered mode (flush on newline):
                                    bufsize=1,
                                    # Use locale.getpreferredencoding(False) character
                                    # encoding to decode bytes:
                                    universal_newlines=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

        except (FileNotFoundError, NotADirectoryError) as err:
            # TODO: Add proper error handling.
            pass
        else:
            process = cls(proc=proc, job=job)
            return process


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

    # def __init__(self, name, host, max_proc, max_load, ns, daemon):
    def __init__(self, host, max_proc, max_load, daemon, key):

        # self._name = name
        self._host_name = host
        self._max_proc = max_proc
        self._max_load = max_load
        # self._ns = ns
        self._daemon = daemon
        self.key = key

        self._processes = dict()
        self._pid = os.getpid()
        self._start_date = get_date()
        # self._start_fmt = format_date(self.start)
        self._cpu_count = multiprocessing.cpu_count()
        self._ip = socket.gethostbyname(daemon.uriFor('batchserver').host)  # Not perfect solution.
        self._id = shortuuid.uuid()

        self._lock = threading.RLock()

        self.logger = logging.getLogger(__name__ + '.batchserver')

    def is_available(self, mem):
        if (self.load() < self.max_load and
            float(self.avail_mem()) > mem and
            self.get_num_proc(status='running') < self.max_proc):
            return True
        return False

    def get_start_date(self):
        return format_date(self.start_date)

    def get_runtime(self):
        return strfdelta(get_date() - self.start_date, "%D:%H:%M:%S")

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
        job = base64.b64decode(job['data'])
        # logger.info('The encrypted job is of type {}'.format(type(job)))
        # logger.info('The job is {}'.format(job))
        decr_job = decrypt_msg(job, self.key)
        process = Process.from_job(decr_job)
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

    def get_proc_runtime(self, id):
        return self.get_proc(id).get_runtime()

    def get_proc_stdout(self, id):
        return self.get_proc(id).get_stdout()

    def get_proc_stderr(self, id):
        return self.get_proc(id).get_stderr()

    def get_proc_job(self, id):
        return self.get_proc(id).job

    def get_proc_start_date(self, id):
        return format_date(self.get_proc(id).start_date)

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
        # if self.ns:
        #     self.ns.remove(self.name)
        self.daemon.shutdown()

    # Properties are necessary for Pyro communication.
    # @property
    # def name(self):
    #     return self._name

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
    def start_date(self):
        return self._start_date

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

# @click.option('-N', '--name', default=socket.gethostname(), type=str,
#               help='name of the server in the network ' +
#               '(default: host)')

# Entering passwords via the command line and not on prompt is dangerous!
# @click.option('--pw', default=None, type=click.STRING,
#               help='password for secure communication')

# Using a nameserver is deprecated!
# @click.option('--ns/--no-ns', default=False,
#               help='Enable/disable contacting a name server. '
#                    '(default: --ns. If --no-ns, all other options relating to the '
#                    'name server are ignored.)')

# @click.option('--nshost', default=None, type=click.STRING,
#               help='hostname or ip address of the name server in the network '
#                    '(default: None. A network broadcast lookup is used.)')

# @click.option('--nsport', default=None, type=click.INT,
#               help='the port number on which the name server is running. '
#                    '(default: None. The exact meaning depends on whether the host parameter is given: '
#                 '- host parameter given: the port now means the actual name server port. '
#                 '- host parameter not given: the port now means the broadcast port.)')

# def main(name, host, port, max_proc, max_load, pw, ns, nshost, nsport):
def main(host, port, max_proc, max_load):
    """Start a new batchserver.\n
    A batchserver is a server process that "serves" as a handle for the batchmanager
    (which manages all processes) and runs on an individual server in the network.

    The password will be used to secure the communication between the batchserver and
    the batchmanager. It is necessary that the passwords of all batchservers match
    with the password of the batchmanager to which they will be connected.

    After starting the batchserver and entering the password, the URI
    (uniform resource identifier) of the batchmanager instance will be printed to the
    console. This is needed when connecting the batchmanager to it.
    """

    # Pyro4.config.SERIALIZERS_ACCEPTED.add('dill')  # Accept pickle as incoming serialization format.

    try:
        pw = getpass.getpass()
    except Exception as err:
        print('ERROR:', err)
    else:
        key = get_hmac_key(pw, salt)
        del pw

    # module_logger.info('key : {}'.format(key))

    with Pyro4.Daemon(host=host, port=port) as daemon:
        daemon._pyroHmacKey = key
        # batchserver = server(name=name, host=host,
        #                      max_proc=max_proc, max_load=max_load,
        #                      ns=None,
        #                      daemon=daemon)
        batchserver = server(host=host, max_proc=max_proc, max_load=max_load, daemon=daemon, key=key)
        batchserver_uri = daemon.register(batchserver)
        uri = daemon.uriFor('batchserver')

        ## Using a nameserver is deprecated!
        # if ns:
        #     # Try to register the daemon in a name server.
        #     try:
        #         with Pyro4.locateNS(host=nshost, port=nsport,
        #                             broadcast=True, hmac_key=key) as ns:
        #             try:
        #                 ns.register(name, uri=batchserver_uri, safe=False)
        #             except Pyro4.errors.NamingError as err:
        #                 print('{} is already registered in the name server, '.format(name) +
        #                       'use a different one!')
        #                 raise
        #             else:
        #                 batchserver.ns = ns

        #     except ConnectionRefusedError as err:
        #         print('Connection to a name server failed. '
        #               'The batchserver will not be registered in a name server. '
        #               'Directly use the printed uri for registring the batchserver in '
        #               'the batchmanager.')

        print()
        print('host: {}'.format(uri.host))
        print('port: {}'.format(uri.port))
        # print('uri: {}'.format(uri.asString()))  # Not the same as batchserver_uri!
        print('uri: {}'.format(batchserver_uri))

        daemon.requestLoop()

if __name__ == '__main__':
    main()
