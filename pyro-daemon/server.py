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
import uuid
import inspect
import time
import signal

def get_date():
    return datetime.datetime.now()

def format_date(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

class process:

    states = ('running', 'finished', 'terminated', 'killed', 'aborted')

    table = {None: 'running',  # Process is still running.
             0: 'finished',  # Process finished. Does not guarantee correctness!
             signal.SIGTERM.value: 'terminated',
             signal.SIGKILL.value: 'killed'}

    update_interval = 1  # Interval of updating the process.

    def __init__(self, proc, job):
        self._pid = os.getpid()
        self._start = get_date()
        self._host_name = socket.gethostname()
        self._ip = Pyro4.socketutil.getIpAddress(None, workaround127=True)
        self._id = uuid.uuid4().int
        self._proc = proc
        self._job = job
        self._status = None

        self.start_updater()  # Start the updater thread upon initialization.

    @property
    def pid(self):
        return self._pid

    @property
    def start(self):
        return self._start

    @property
    def host_name(self):
        return self._host_name

    @property
    def ip(self):
        return self._ip

    @property
    def id(self):
        return self._id

    @property
    def proc(self):
        return self._proc

    @property
    def job(self):
        return self._job

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

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

    def __init__(self, name, daemon, max_num_proc=None, max_load=None):
        self._name = name
        self.daemon = daemon

        self._processes = dict()
        self._pid = os.getpid()
        self._start = get_date()
        self._cpu_count = multiprocessing.cpu_count()
        self._host_name = socket.gethostname()
        self._ip = Pyro4.socketutil.getIpAddress(None, workaround127=True)
        self._id = uuid.uuid4().int

        if max_num_proc is None or max_num_proc == '':
            self._max_num_proc = self.cpu_count
        else:
            self._max_num_proc = max_num_proc

        if max_load is None or max_load == '':
            self._max_load = self.cpu_count
        else:
            self._max_load = max_load

        self.logger = logging.getLogger('.'.join(('server', name)))
        self.logger.setLevel(logging.DEBUG)
        # Create handlers.
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler(filename='log', mode='w')
        level = logging.DEBUG
        stream_handler.setLevel(level)
        file_handler.setLevel(level)
        # Create formatter.
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # Add formatter to handlers.
        stream_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        # Add handler to logger.
        self.logger.addHandler(stream_handler)
        self.logger.addHandler(file_handler)

    @property
    def name(self):
        return self._name

    @property
    def max_num_proc(self):
        return self._max_num_proc

    @property
    def max_load(self):
        return self._max_load

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
    def cpu_count(self):
        return self._cpu_count

    @property
    def host_name(self):
        return self._host_name

    @property
    def ip(self):
        return self._ip

    @property
    def id(self):
        return self._id

    def available(self, mem):
        if (self.load() < self.max_load and
            self.avail_mem() > mem and
            self.num_proc(status='running') < self.max_num_proc):
            return True
        return False

    def start_date(self, format=False):
        date = self.start
        if format:
            return format_date(date)
        return date

    def num_proc(self, status='running'):
        if status not in process.states:
            raise ValueError("'status' must be one of {}.".format(
                              ', '.join(process.states)))

        states = tuple(p.status for p in self.processes.values())
        return states.count(status)

    # def update(self):
    #     for proc in self.processes.values():
    #         proc.poll()

    def proc_start(self, job):
        fun_name = inspect.currentframe().f_code.co_name
        logger = logging.getLogger('.'.join((self.logger.name, fun_name)))

        prefix = job['prefix']
        script = job['script']
        args = job['args']
        cmdlist = [*prefix, script, *args]

        # cmdlist = ['Rscript', '--vanilla', './R_test.R', 'adf']
        # proc = subprocess.Popen(args=cmdlist,
        #                         stdout=subprocess.PIPE,
        #                         stderr=subprocess.PIPE)
        # print(proc.poll())
        # stdout, stderr = proc.communicate()
        # print(stdout.decode('utf-8'))
        # print(stderr.decode('utf-8'))

        try:
            proc = subprocess.Popen(args=cmdlist,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)

        except (FileNotFoundError, NotADirectoryError) as err:
            logger.exception('File not found.')
        else:
            p = process(proc=proc, job=job)
            self.processes[p.id] = p
            return p.id

    def proc_ids(self):
        return tuple(p.id for p in self.processes.values())

    def get_proc(self, id):
        try:
            p = self.processes[id]
        except KeyError:
            warnings.warn('Process not found.', RuntimeWarning)
        else:
            return p

    def proc_job(self, id):
        return self.get_proc(id).job

    # def proc_poll(self, id):
    #     return self.get_proc(id).proc.poll()

    def proc_start_date(self, id, format=False):
        date = self.get_proc(id).start
        if format:
            return format_date(date)
        return date

    def proc_pid(self, id):
        return self.get_proc(id).proc.pid

    def proc_status(self, id):
        return self.get_proc(id).status

    def proc_terminate(self, id):
        self.get_proc(id).proc.terminate()

    def proc_kill(self, id):
        self.get_proc(id).proc.kill()

    def proc_delete(self, id):
        if self.get_proc(id).proc.poll() is None:
            warnings.warn('Process is still running.', RuntimeWarning)
        else:
            del self.processes[id]

    @Pyro4.oneway   # in case call returns much later than daemon.shutdown
    def shutdown(self, terminate_processes=True):
        if terminate_processes:
            for proc in self.processes:
                proc.terminate()
        self.daemon.shutdown()

def main(name, max_num_proc, max_load):
    # use port 9999?
    with Pyro4.Daemon() as daemon:
        myserver = server(name, daemon, max_num_proc, max_load)
        myserver_uri = daemon.register(myserver)
        with Pyro4.locateNS() as ns:
            ns.register(name, myserver_uri)
        daemon.requestLoop()

if __name__ == '__main__':
    main(name=sys.argv[1],
         max_num_proc=int(sys.argv[2]),
         max_load=int(sys.argv[3]))
