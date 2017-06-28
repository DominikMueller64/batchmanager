import collections
import datetime
import getpass
import itertools
import logging
import math
import operator
import os
import pathlib
import pickle
import subprocess
import sys
import threading
import time
import warnings

import click
import pandas
import Pyro4
import shortuuid
from cmd2 import Cmd, make_option, options

from .functions import *
from .globvar import *

# module_logger = logging.getLogger('batchmanager')
# module_logger.setLevel(logging.DEBUG)
# # Create handlers.
# stream_handler = logging.StreamHandler()
# file_handler = logging.FileHandler(filename='batchmanager.log', mode='w')
# # level = logging.INFO
# level = logging.DEBUG
# stream_handler.setLevel(level)
# file_handler.setLevel(level)

# # Create formatter.
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# # Add formatter to handlers.
# stream_handler.setFormatter(formatter)
# file_handler.setFormatter(formatter)

# # Add handler to logger.
# module_logger.addHandler(stream_handler)
# module_logger.addHandler(file_handler)


# Logging.

LEVELS = {
    'nolog' : logging.CRITICAL + 1,
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL,
}

module_logger = logging.getLogger(__name__)
module_logger.info('Start logging.')

# class Job:
#     def __init__(self, **kwargs):
#         self.__dict__.update(kwargs)


## from http://stackoverflow.com/questions/3462566/python-elegant-way-to-deal-with-lock-on-a-variable
## This wraps a variable together with its lock into a class.
class LockedVariable(object):
    def __init__(self, value, lock=None):
        self._value = value
        self._lock = lock if lock else threading.RLock()
        self._locked = False

    @property
    def locked(self):
        return self._locked

    def assign(self, value):
        with self:
            self._value = value

    def release():
        self._locked = False
        return self._lock.release()

    def __enter__(self):
        self._lock.__enter__()
        self._locked = True
        return self._value

    def __exit__(self, *args, **kwargs):
        if self._locked:
            self._locked = False
            return self._lock.__exit__(*args, **kwargs)



class launcher(threading.Thread):

    def __init__(self, manager, max, sleep):
        super().__init__(group=None, target=None, name='launcher', daemon=True)

        self.manager = manager
        self.max = max
        self.sleep = sleep
        self.can_run = threading.Event()
        self.should_stop = threading.Event()
        self.logger = logging.getLogger(__name__ + '.launcher')

        self.cycle = 1
        self.can_run.set()  # On default, the launcher can run once started.

    def status(self):
        table = list()
        header = ('name', 'status', 'cycle', 'max', 'sleep')
        table.append(header)

        if not self.is_alive():
            stat = 'stopped'
        elif self.should_stop.is_set():
            stat = 'stopping'
        elif self.can_run.is_set():
            stat = 'running'
        else:
            stat = 'paused'

        table.append((self.name, stat, self.cycle, self.max, self.sleep))
        print_table(table)

    def pause(self):
        self.logger.info('Pausing.')
        print('Pausing launcher.')
        self.can_run.clear()

    def resume(self):
        self.logger.info('Resuming.')
        print('Resuming launcher.')
        self.can_run.set()

    def stop(self):
        self.logger.info('Stopping.')
        print('Stopping launcher.')
        self.should_stop.set()

    def get_avail_server(self, mem):
        self.logger.debug('Getting server.')
        with self.manager.server_lock:
            servers = self.manager.servers
            ids = {i for i, s in servers.items() if s.is_available(mem * 1024)}
            if not ids:
                self.logger.debug('No server available.')
                return  # No server satisfies the requirements.
            # Get server with minimum load.
            dload = {i: s.load_average()[0] for i, s in servers.items()}
            return servers[min({i: dload[i] for i in ids}, key=dload.get)]

    def run(self):
        self.logger.debug('Starting.')
        print('Starting launcher.')
        while self.cycle <= self.max:
            if self.should_stop.wait(self.sleep):
                break

            if self.can_run.is_set():
                self.logger.debug('Cycle {} entered.'.format(self.cycle))
                self.cycle += 1
                with self.manager.server_lock:
                    if not self.manager.servers or not self.manager.queue:
                        self.logger.debug('No servers registered or no jobs available.')
                        continue
                    with self.manager.queue_lock:
                        job = self.manager.queue.popleft()
                        server = self.get_avail_server(job['mem'])
                        if server is None:
                            self.manager.queue.appendleft(job)  # Add job back.
                            continue  # No server satisfies the requirements.
                        encr_job = encrypt_msg(job, self.manager.key)
                        # self.logger.info('The job is {}'.format(encr_job))
                        server.start_proc(encr_job)

class manager(Cmd):

    def __init__(self):
        super(manager, self).__init__()
        self.allow_cli_args = False  # This is vital for allowing to use argparse or click!

        self.servers = dict()
        self.queue = collections.deque()
        self.server_lock = threading.RLock()  # Lock for server dictionary.
        self.queue_lock = threading.RLock()  # Lock for the job queue.
        self.launcher = None  # Here comes the launcher once started.
        self.key = None

        self.logger = logging.getLogger(__name__ + '.manager')

        self.config_items = {'sleep': {
                                'type': float,
                                'min': 1.0
                            },'max': {
                                'type': int,
                                'min': 1
                            }}

    ## Controlling of launcher.
    @options([
        make_option('-s', '--sleep', type="int", default=5, help="Sleeping time between cycles in seconds (default=5)."),
        make_option('-m', '--max', type="int", default=int(1e6), help="Maximum number of cycles (default=1e6)."),
    ])
    def do_start(self, args, opts=None):
        """Start a new process launcher

        The process launcher is a central component of the batchmanager.
        It runs as a thread in the background an continuously checks if
        there are processes in the queue and if there are sufficient resources
        to start them. If there are not enough resources, no process is started.

        The choice on which server a process is started depends on two criteria:
        (i) the expected memory required by the process (given by the user) and
        (ii) the load average. A process can only be started on a server if
        there is sufficient memory available. Among
        the servers that satisfy this condition, the one with the smallest 1
        minute load average is selected.

        To save resources, the launcher only wakes up occasionally and checks if a
        process can be started. The launcher only wakes up for a limited number of
        cycles to prevent running indefinitely. The sleep length and maximum number of
        cycles can be specified when the launcher is started. After it is started, they
        can be altered via "set_sleep" and "set_max".
        """
        self.launcher = launcher(manager=self, max=opts.max, sleep=opts.sleep)
        self.launcher.start()

    @options([])
    def do_pause(self, args):
        """Pause the launcher

        The launcher pauses and no new processes are started (no new cycles are entered).
        The launcher can be resumed by the command "resume".
        """
        self.launcher.pause()

    @options([])
    def do_resume(self, args):
        """Resume the launcher

        This resumes the launcher after it has been paused.
        """
        self.launcher.resume()

    @options([])
    def do_stop(self, args):
        """Stop the launcher

        This stops the launcher.
        """
        self.launcher.stop()

    @options([])
    def do_launcher(self, args):
        """Launcher status

        Show the status of the launcher.
        """
        try:
            self.launcher.status()
        except AttributeError:
            print('Launcher has not yet been started.')

    ## Configuration of launcher.
    def config_parser(self, value):
        return value.split()[0]

    def config_launcher(self, value, what):
        items = self.config_items[what]
        value = self.config_parser(value)
        try:
            value = items['type'](value)
        except ValueError as err:
            msg = 'Could not set to {}.'.format(value)
            self.logger.warning(msg)
            warnings.warn(msg)
        else:
            if value < items['min']:
                msg = 'Must be at least {}!'.format(items['min'])
                self.logger.warning(msg)
                warnings.warn(msg)
                return

            setattr(self.launcher, what, value)
            msg = 'Set to {}.'.format(value)
            self.logger.info(msg)
            print(msg)

    def do_set_sleep(self, value):
        self.config_launcher(value, 'sleep')

    def do_set_max(self, value):
        self.config_launcher(value, 'max')

    ## Controlling of server.
    @options([])
        # [make_option('-P', '--pw', default=None, type='str',
        #                   help='password for secure communication'),

        #  make_option('--ns', action='store_true',
        #              help='Enable contacting a name server.'),

        #  make_option('--nshost', default=None, type='str',
        #              help='hostname or ip address of the name server in the network '
        #              '(default: None. A network broadcast lookup is used.)'),

        #  make_option('--nsport', default=None, type='int',
        #              help='the port number on which the name server is running. '
        #              '(default: None. The exact meaning depends on whether the host parameter is given: '
        #              '- host parameter given: the port now means the actual name server port. '
        #              '- host parameter not given: the port now means the broadcast port.)')]

    def do_add_server(self, args, opts=None):
        """Add server to the manager."""
        # args = args.strip().split()
        ct = itertools.count()
        for name in args:
            uri = name
            # self.logger.info('Uri : {}'.format(uri))
            # TODO: Find out how to integrate context manager here.
            # with Pyro4.locateNS(hmac_key=key) as ns:
            # server = Pyro4.Proxy(':'.join(('PYRONAME', str(name))))

            # if opts.ns:
            #     # Try to contact the name server.
            #     try:
            #         ns = Pyro4.locateNS(host=host, port=port,
            #                             broadcast=True, hmac_key=key)
            #     except Pyro4.errors.NamingError as err:
            #         msg = ('Failed to locate the nameserver. '
            #             'Did you set the correct HMAC key?')
            #         self.logger.exception(msg)
            #         continue

            #     # Try to lookup the uri on the name server.
            #     try:
            #         uri = ns.lookup(name)
            #     except Pyro4.errors.NamingError as err:
            #         msg = ('Name {} not found on the nameserver.'.format(name))
            #         self.logger.exception(msg)
            #         continue

            # Try to obtain a proxy.
            try:
                server = Pyro4.Proxy(uri)
            except Pyro4.errors.PyroError as err:
                msg = ('{} is an invalid uri.'.format(name))
                self.logger.exception(msg)
                continue
            else:
                server._pyroHmacKey = self.key

            # Try to bind to the proxy.
            try:
                server._pyroBind()
            except Pyro4.errors.CommunicationError:
                msg = ('HMAC keys do not match.')
                self.logger.exception(msg)
                continue
            else:
                with self.server_lock:
                    self.servers[server.id] = server
                next(ct)

        self.logger.info('Added {} server.'.format(ct))

    # def get_server_id_by_hostname(self, host_name):
    #     """Get the id of a server from its (unique!) name."""
    #     # with self.server_lock:
    #         # servers = list(self.servers.values())
    #         # names = tuple(server.name for server in servers)
    #         # if name not in names:
    #         #     msg = 'Server {} not found.'.format(name)
    #         #     self.logger.warning(msg)
    #         #     warnings.warn(msg, RuntimeWarning)
    #         # else:
    #         #     server = servers[names.index(name)]
    #         #     return server.id
    #     with self.server_lock:
    #         for server in self.servers.values():
    #             if server.host_name == host_name:
    #                 return server.id

    def remove_server(self, identifier, by_id=True):
        if by_id:
            return self.remove_server_by_id(identifier)
        else:
            return self.remove_server_by_ip(identifier)

    def remove_server_by_id(self, id):
        with self.server_lock:
            try:
                del self.servers[id]
            except KeyError:
                msg = 'Server {} not found.'.format(id)
                self.logger.warning(msg)
                warnings.warn(msg, RuntimeWarning)
                return False
            else:
                return True

    def remove_server_by_ip(self, ip):
        with self.server_lock:
            for key in self.servers.copy():
                if self.servers[key].ip == ip:
                    del self.servers[key]
                    return True

            msg = 'Server {} not found.'.format(ip)
            self.logger.warning(msg)
            warnings.warn(msg, RuntimeWarning)
            return False

    def get_server(self, identifier, by_id=True):
        if by_id:
            return self.get_server_by_id(identifier)
        else:
            return self.get_server_by_ip(identifier)

    def get_server_by_id(self, id):
        with self.server_lock:
            try:
                server = self.servers[id]
            except KeyError:
                msg = 'Server {} not found.'.format(id)
                self.logger.warning(msg)
                warnings.warn(msg, RuntimeWarning)
            else:
                return server

    def get_server_by_ip(self, ip):
        with self.server_lock:
            for server in self.servers.values():
                if server.ip == ip:
                    return server

        msg = 'Server {} not found.'.format(ip)
        self.logger.warning(msg)
        warnings.warn(msg, RuntimeWarning)

    # def get_server_id_by_ip(self, ip):
    #     """Get the id of a server from its ip."""
    #     with self.server_lock:
    #         for server in self.servers.values():
    #             if server.ip == ip:
    #                 return server.id

    #     msg = 'Server {} not found.'.format(ip)
    #     self.logger.warning(msg)
    #     warnings.warn(msg, RuntimeWarning)

    def get_server_by_process_id(self, id):
        with self.server_lock:
            for server in self.servers.values():
                if id in server.get_proc_ids():
                    return server

        msg = 'Process id: {} could not be found.'.format(id)
        self.logger.warning(msg)
        warnings.warn(msg, RuntimeWarning)

    def apply_to_server(self, what, args, opts):
        with self.server_lock:
            if opts.all:
                args = (server.id for server in self.servers.values())
                opts.id = True
            else:
                args = args.strip().split()

            ct = itertools.count()
            for identifier in args:
                if identifier is not None:
                    if what == 'shutdown':
                        server = self.get_server(identifier, by_id=opts.id)
                        if server:
                            server.shutdown(opts.terminate)
                            self.remove_server(identifier, by_id=opts.id)
                            next(ct)
                    elif what == 'remove':
                        if self.remove_server(identifier, by_id=opts.id):
                            next(ct)
                    elif what == 'clear':
                        server = self.get_server(identifier, by_id=opts.id)
                        if server:
                            server.clear_proc()
                            next(ct)
        return next(ct)

    @options([make_option('-A', '--all', action='store_true', help='shutdown all servers'),
              make_option('-T', '--terminate', action='store_true', help='terminate processes'),
              make_option('--id', action='store_true', help='identify server by its id')])
    def do_shutdown_server(self, args, opts=None):
        """Shutdown servers."""
        ct = self.apply_to_server(what='shutdown', args=args, opts=opts)
        self.logger.info('Shutdown {} server(s).'.format(ct))

    @options([make_option('-A', '--all', action='store_true', help='remove all servers'),
              make_option('--id', action='store_true', help='identify server by its id')])
    def do_remove_server(self, args, opts=None):
        """Remove servers from the manager."""
        ct = self.apply_to_server(what='remove', args=args, opts=opts)
        self.logger.info('Removed {} server(s).'.format(ct))

    @options([make_option('-A', '--all', action='store_true', help='clear all servers'),
              make_option('--id', action='store_true', help='identify server by its id')])
    def do_clear_server(self, args, opts=None):
        """Clear servers from process that are not running."""
        ct = self.apply_to_server(what='clear', args=args, opts=opts)
        self.logger.info('Cleared {} server(s).'.format(ct))


    def do_server(self, args):
        table = list()
        header = ('pid'
                  ,'id'
                  ,'host (ip)'
                  ,'start'
                  ,'time (d:h:m:s)'
                  ,'cpu'
                  ,'proc (r, f, c)'
                  ,'load (1, 5, 15)'
                  ,'mem (total, avail., used)'
        )
        table.append(header)
        with self.server_lock:
            for server in self.servers.values():
                # pid = server.pid
                # id = server.id
                # host = server.ip
                # start = server.get_start_date()
                # runtime = server.get_runtime()
                # cpu = server.cpu_count
                # proc = server.get_num_proc(status='running')
                nr = server.get_num_proc(status='running')
                nf = server.get_num_proc(status='finished')
                nc = (server.get_num_proc(status='killed') +
                      server.get_num_proc(status='terminated') +
                      server.get_num_proc(status='aborted'))
                proc = ', '.join(str(_) for _ in (nr, nf, nc))
                load = ', '.join(str(_) for _ in server.load_average())
                mem = server.virtual_memory()
                mem = ', '.join((convert_size(mem[0]), convert_size(mem[1]), str(mem[2]) + '%'))
                table.append(
                    (server.pid,
                     server.id,
                     server.ip,
                     server.get_start_date(),
                     server.get_runtime(),
                     server.cpu_count,
                     proc,
                     load,
                     mem,
                    )
                )

        print_table(table)


    # Controlling of processes.
    def get_server_from_proc_id(self, id):
        for server in self.servers.values():
            for i in server.get_proc_ids():
                if i == id:
                    return server
                    break
            else:
                continue
            break
        else:
            msg = 'Server could not be found from process id.'
            self.logger.warn(msg)
            warnings.warn(msg)

    def do_stdout(self, id):
        id = str(id)  # Conversion to a standard string is explicitly needed.
        self._print_out(id, 'stdout')

    def do_stderr(self, id):
        id = str(id)  # Conversion to a standard string is explicitly needed.
        self._print_out(id, 'stderr')

    def _print_out(self, id, which):
        if which not in ('stdout', 'stderr'):
            raise ValueError
        server = self.get_server_from_proc_id(id)
        if server:
            if which == 'stdout':
                out = server.get_proc_stdout(id)
            elif which == 'stderr':
                out = server.get_proc_stderr(id)
            for line in out:
                print(line)

    # TODO There is much room for improvement.
    # - Terminate all processes.
    # - Terminate all processes of a specific set of servers.
    # - Terminate all processes of a set of process ids.
    @options([make_option('-A', '--all', action='store_true', help='terminate all processes')])
    def do_terminate_proc(self, args, opts=None):
        with self.server_lock:
            if opts.all:
                for server in self.servers.values():
                    for id in server.get_proc_ids():
                        server.terminate_proc(id)
            else:
                ids = args.split()
                for id in ids:
                    server = self.get_server_by_process_id(id)
                    if server is not None:
                        server.terminate_proc(id)
                    # try:
                    #     server = self.get_server_by_process_id(id)
                    # except RuntimeWarning as err:
                    #     raise
                    # else:
                    #     server.proc_terminate(id)

    def do_clear_jobs(self, args):
        """Clear all jobs from the queue."""
        with self.queue_lock:
            self.queue.clear()

    @options([make_option('-T', '--type', type='str',
                          default='sqlite3', nargs=1,
                          help='type (sqlite3, txt, dumped)'),
              make_option('-C', '--check', action='store_true',
                          help='Should the existence of the script be checked?')])
    def do_add_jobs(self, args, opts=None):
        logger = logging.getLogger('batchmanager.manager.do_add_jobs')

        if not opts.type in ('sqlite3', 'txt', 'dumped'):
            logger.warning('The filetype must be one of "sqlite3", "txt" or "dumped".')
            return

        # Check existence of the file.
        db_path = os.path.expanduser(args.strip())
        if not os.path.exists(db_path):
             msg = 'The database {} could not be located'.format(db_path)
             logger.warning(msg)
             warnings.warn(msg, RuntimeWarning)
             return

        ## TODO: Program this here properly.
        if opts.type == 'dumped':
            with open(file=db_path, mode='rb') as f:
                try:
                    jobs = pickle.load(f)
                except EOFError:
                    pass  ## Logging + warning must be done.
                else:
                    self.queue.extend(jobs)
                    return

        if opts.type == 'sqlite3':
            import sqlite3
            try:
                # conn = sqlite3.connect(db_path, timeout=5, isolation_level='EXCLUSIVE')
                with sqlite3.connect(db_path, timeout=5, isolation_level='EXCLUSIVE') as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    query = "SELECT name FROM sqlite_master WHERE type='table';"
                    res = cursor.execute(query).fetchall()
                    if len(res) > 1:
                        warnings.warn('The database contains more than one table, ' +
                                      'only the first one is used', RuntimeWarning)

                    tn = res[0]['name']
                    # logging.debug('There are currently {} processes.'.format(len(processes)))
                    query = 'SELECT * FROM {tn} ORDER BY grp ASC, priority ASC'.format(tn=tn)
                    col_names = {_[0] for _ in cursor.description}
                    conn.row_factory = dict_factory
                    cursor = conn.cursor()
                    tbl = cursor.execute(query).fetchall()

                    ## TODO: Here is some bug, this is not working.
                    # if not col_names.issubset(required_col_names):
                    #     msg = ('Input from sqlite3 database does not contain ' +
                    #            'the appropriate columns in its (first) table.')
                    #     logger.warn(msg)
                    #     print(msg)
                    #     return


            except (sqlite3.OperationalError, sqlite3.DatabaseError) as err:
                logging.debug('Connecting to database failed')

        elif opts.type == 'txt':
            tbl = list()
            with open(db_path, mode='r') as f:
                for l in f:
                    job = dict()
                    l = l.strip().split(':')
                    for k, c in zip(required_col_names, l):
                        job[k] = c.strip()

                    job['grp'] = int(job['grp'])
                    job['priority'] = int(job['priority'])
                    job['mem'] = int(job['mem'])
                    tbl.append(job)

        ## Modify the jobs and place them in the queue.
        ct = itertools.count()
        with self.queue_lock:
            for job in tbl:

                # Check prefix.
                interpreter, *options = job['prefix'].split()
                interpreter = os.path.expanduser(interpreter)
                job['prefix'] = [interpreter, *options]

                # Check script.
                script_path = pathlib.Path(job['script'].strip()).expanduser()
                script = str(pathlib.PurePath(script_path))
                if opts.check:
                    if not script_path.is_file():
                        msg = ('The script {} is not an existing file. ' +
                               'The process is ignored.'.format(script))
                        logger.warn(msg)
                        warnings.warn(msg)

                job['script'] = script

                # Check args.
                job['args'] = job['args'].split()
                job['id'] = shortuuid.uuid()

                # job = Job(**job)  # Make Job instance.
                self.queue.append(job)
                next(ct)

        print('Read in {} jobs. '.format(next(ct)))
        # + 'The last one is:\n{}.'.format(str(job)))


    @options([make_option('-l', '--level', type='int',
                          default=2, nargs=1, help='set depth of paths'),
              make_option('-e', '--extended',  action='store_true',
                          help='show extended output')])
    def do_proc(self, args, opts=None):
        table = list()
        if opts.extended:
            header = ('pid', 'grp', 'priority', 'id', 'ip', 'prefix', 'script', 'args',
                      'mem (MB)', 'start', 'time (d:h:m:s)', 'status')
        else:
            header = ('pid', 'id', 'script', 'args', 'time (d:h:m:s)', 'status')
        table.append(header)

        with self.server_lock:
            for server in self.servers.values():
                ip = server.ip
                for id in server.get_proc_ids():
                    job = server.get_proc_job(id)
                    # status = server.get_proc_status(id)
                    # start = server.get_proc_start_date(id)
                    # runtime = server.get_proc_runtime(id)
                    # pid = server.get_proc_pid(id)
                    # script = trim_path(job['script'], opts.level)
                    # args = ' '.join(job['args'])

                    if opts.extended:
                        tmp = (str(_) for _ in (server.get_proc_pid(id),
                                                job['grp'],
                                                job['priority'],
                                                id,
                                                ip,
                                                ' '.join(job['prefix']),
                                                trim_path(job['script'], opts.level),
                                                ' '.join(job['args']),
                                                job['mem'],
                                                server.get_proc_start_date(id),
                                                server.get_proc_runtime(id),
                                                server.get_proc_status(id),
                                                ))
                    else:
                        tmp = tuple(map(str, (server.get_proc_pid(id),
                                              id,
                                              trim_path(job['script'], opts.level),
                                              ' '.join(job['args']),
                                              server.get_proc_runtime(id),
                                              server.get_proc_status(id),
                        )))
                    table.append(list(tmp))

        print_table(table)


    @options([make_option('-L', '--level', type='int',
                          default=2, nargs=1, help='depth of paths'),
              make_option('-N', '--n', type='int', default=10, nargs=1,
                          help='The number of jobs displayed.'),
              make_option('-T', '--tail', action='store_true',
                          help='Should the queue be printed from bottom?')])
    def do_jobs(self, args, opts=None):
        table = list()
        header = job_keys
        table.append(header)
        with self.queue_lock:
            for job in self.queue:
                tmp = (str(_) for _ in (job['id'],
                                        job['grp'],
                                        job['priority'],
                                        ' '.join(job['prefix']),
                                        trim_path(job['script'], opts.level),
                                        ' '.join(job['args']),
                                        job['mem']))
                table.append(list(tmp))

        n = opts.n
        if opts.tail:
            table = table[-n:]
        else:
            table = table[:n + 1]

        print_table(table)
        # print(pandas.DataFrame(table[:opts.head]))

    def do_quit(self, arg):
        print('Stopping.')
        if self.launcher:
            self.launcher.stop()  # Send stop signal to launcher.
            self.launcher.join()  # Wait for the launcher thread to stop.
            self.launcher.status()  # See if launcher really stopped.

        if self.queue:

            while True:
                ans = input('There are still {} jobs enqueued. '.format(len(self.queue)) +
                            'Store them on hard disk for later usage? [y/n/c]: ')
                if ans in ('y', 'n', 'c'):
                    break

            if ans == 'c':
                return

            elif ans == 'y':
                timestamp = time.strftime('%Y-%m-%d_%H:%M:%S')
                path = os.path.join('.', 'dumped_jobs_' + timestamp)
                with open(file=path, mode='wb') as f:
                    pickle.dump(self.queue, f)
                print('Extant jobs were dumped to: ' + path)

        return self._STOP_AND_EXIT  # Send stop signal to cmd2

    do_q = do_quit
    do_exit = do_quit

@click.command()
@click.option('--log/--no-log', default=False, help='enable/disable logging')
def main(log):
    """Start a new batchmanager.\n

    A batchmanager manages one to several instances of a batchserver and delegates
    processes to it.

    To run batch processes, proceed as follows:

    1) Start batchserver processes on all servers that should be employed

    2) Start a batchmanager instance somewhere in the network. The ports on which the
    batchserver instances run need to be accessible by the batchmanager.

    3) Add the batchservers using the command "add_servers" via their URI.

    4) Add the processes that should run by the command by "add_jobs".

    5) Start the process launcher by the command "launcher".

    The password will be used to secure the communication between the batchserver and
    the batchmanager. It is necessary that the passwords of all batchservers match
    with the password of the batchmanager to which they will be connected.
    """

    # import argparse
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--pw', action='store',
    #                     dest='pw',
    #                     help='password for secure communication')
    # results = parser.parse_args()

    level_name = 'debug' if log else 'nolog'
    level = LEVELS.get(level_name, logging.NOTSET)
    module_logger.setLevel(level)

    man = manager()
    man.prompt = '> '
    man.colors = True

    try:
        pw = getpass.getpass()
    except Exception as err:
        print('ERROR:', err)
    else:
        man.key = get_hmac_key(pw, salt)
        del pw

    man.cmdloop()

# ## This is never executed, as the entry point is main.
# if __name__ == '__main__':
#     main()

## This is problematic if no name server is used.
# @classmethod
# def do_objects(cls, args):
#     with Pyro4.naming.locateNS() as ns:
#         table = list()
#         header = ('name', 'uri (uniform resource identifier)')
#         table.append(header)
#         for name, uri in ns.list().items():
#             if name == 'Pyro.NameServer':
#                 continue
#             table.append((name, uri))
#     print_table(table)
