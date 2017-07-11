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
import json

import click
import pandas
import Pyro4
import shortuuid
from cmd2 import Cmd, make_option, options
import jsonschema

from .functions import *
from .globvar import *


job_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "title": "Job set",
    "description": "A job set to be loaded.",
    "type": "array",
    "items": {
        "title": "Job",
        "type": "object",
        "properties": {
            "id": {
                "description": "A uuid.",
                "type": "string"
            },
            "priority": {
                "description": "The priority of the job.",
                "type": "integer"
            },
            "grp": {
                "description": "The group of the job.",
                "type": "integer"
            },
            "script": {
                "description": "The (relative) path to the script for the job.",
                "type": "string"
            },
            "prefix": {
                "description": "The path to the executable for running the script including options",
                "type": "array",
                "items": {
                    "type": "string"
                }
            },
            "args": {
                "description": "Argument to be passed to the script.",
                "type": "array",
                "items": {
                    "type": "string"
                }
            },
            "mem": {
                "description": "Expected memory in MB required by the job.",
                "type": "number",
                "minimum": 0
            }
        },
        "required": ["script"]
    }
}

def complement_job(job):
    if 'id' not in job:
        job['id'] = shortuuid.uuid()
    if 'grp' not in job:
        job['grp'] = 0
    if 'priority' not in job:
        job['priority'] = 0
    if 'prefix' not in job:
        job['prefix'] = []
    if 'mem' not in job:
        job['mem'] = 0.0
    if 'args' not in job:
        job['args'] = []

    return job



class Error(Exception):
    """Base class for exceptions in this module."""
    pass

class ServerLookupError(Error):
    """Exception raised if a server is not found."""
    def __init__(self, identifier):
        self.identifier = identifier


# Logging.


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

    def release(self):
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
        if self.can_run.is_set():
            msg = 'Pausing launcher.'
            self.logger.info(msg)
            self.manager.pfeedback(msg)
            self.can_run.clear()
        else:
            msg = 'Launcher is already paused.'
            self.logger.info(msg)
            self.manager.pfeedback(msg)

    def resume(self):
        if self.can_run.is_set():
            msg = 'Launcher is already running.'
            self.logger.info(msg)
            self.manager.pfeedback(msg)
        else:
            msg = 'Resuming launcher.'
            self.logger.info(msg)
            self.manager.pfeedback(msg)
            self.can_run.set()

    def stop(self):
        msg = 'Stopping launcher.'
        self.logger.info(msg)
        self.manager.pfeedback(msg)
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
        self.logger.info('Trying to start launcher.')
        if self.launcher:
            msg = 'A launcher has already been started'
            self.logger.info(msg)
            self.pfeedback(msg)
        else:
            self.launcher = launcher(manager=self, max=opts.max, sleep=opts.sleep)
            msg = 'Starting launcher.'
            self.logger.info(msg)
            self.pfeedback(msg)
            self.launcher.start()

    @options([])
    def do_pause(self, args, opts=None):
        """Pause the launcher

        The launcher pauses and no new processes are started (no new cycles are entered).
        The launcher can be resumed by the command "resume".
        """
        self.logger.info('Trying to pause launcher.')
        try:
            self.launcher.pause()
        except AttributeError:
            msg = 'Launcher has not yet been started.'
            self.logger.info(msg)
            self.pfeedback(msg)

    @options([])
    def do_resume(self, args, opts=None):
        """Resume the launcher

        This resumes the launcher after it has been paused.
        """
        self.logger.info('Trying to resume launcher.')
        try:
            self.launcher.resume()
        except AttributeError:
            msg = 'Launcher has not yet been started.'
            self.logger.info(msg)
            self.pfeedback(msg)

    @options([])
    def do_stop(self, args, opts=None):
        """Stop the launcher

        This stops the launcher.
        """
        self.logger.info('Trying to stop launcher.')
        try:
            self.launcher.stop()
        except AttributeError:
            msg = 'Launcher has not yet been started.'
            self.logger.info(msg)
            self.pfeedback(msg)
        else:
            self.launcher.join()  # Wait until launcher thread is finished.
            self.launcher = None

    @options([])
    def do_launcher(self, args, opts=None):
        """Launcher status

        Show the current status of the launcher.
        """
        try:
            self.launcher.status()
        except AttributeError:
            msg = 'Launcher has not yet been started.'
            self.pfeedback(msg)
            self.logger.info(msg)

    ## Configuration of launcher.
    def config_parser(self, value):
        return value.split()[0]

    def config_launcher(self, value, what):
        items = self.config_items[what]
        value = self.config_parser(value)
        try:
            value = items['type'](value)
        except ValueError as err:
            msg = 'Could not set {} to {}.'.format(what, value)
            self.logger.warning(msg)
            self.pfeedback(msg)
        else:
            if value < items['min']:
                msg = '{} must be at least {}!'.format(what, items['min'])
                self.logger.warning(msg)
                self.pfeedback(msg)
                return

            if self.launcher:
                setattr(self.launcher, what, value)
                msg = '{} set to {}.'.format(what, value)
                self.logger.info(msg)
                self.pfeedback(msg)
            else:
                msg = 'Launcher has not yet been started.'
                self.logger.info(msg)
                self.pfeedback(msg)


    def do_set_sleep(self, value):
        """Set sleep time

        Set the sleep time (seconds) between cycles of the launcher.
        """
        self.config_launcher(value, 'sleep')

    def do_set_max(self, value):
        """Set maximum cycles

        Set the maximum number of cycles of the launcher.
        """
        self.config_launcher(value, 'max')

    @options([])
    def do_add_server(self, args, opts=None):
        """Add a batchserver to the manager

        Register one or serveral batchservers in the manager.
        """
        self.logger.info('Trying to add server(s).')
        ct = itertools.count()
        for name in args:
            uri = name
            # Try to obtain a proxy.
            try:
                server = Pyro4.Proxy(uri)
            except Pyro4.errors.PyroError as err:
                msg = ('{} is an invalid uri.'.format(name))
                self.logger.warning(msg)
                self.pfeedback(msg)
                continue
            else:
                server._pyroHmacKey = self.key

            # Try to bind to the proxy.
            try:
                server._pyroBind()
            except Pyro4.errors.CommunicationError:
                msg = ('HMAC keys do not match (uri: {}).'.format(name))
                self.logger.warning(msg)
                self.pfeedback(msg)
                continue
            else:
                with self.server_lock:
                    self.servers[server.id] = server
                next(ct)

        msg = 'Added {} server(s).'.format(next(ct))
        self.logger.info(msg)
        self.pfeedback(msg)

    def remove_server_by_id(self, id):
        with self.server_lock:
            try:
                del self.servers[id]
            except KeyError:
                msg = 'Server {} not found.'.format(id)
                self.logger.warning(msg)
                self.perror(msg)
                raise ServerLookupError(msg)

    def get_server_by_id(self, id):
        with self.server_lock:
            try:
                server = self.servers[id]
            except KeyError:
                msg = 'Server {} not found.'.format(id)
                self.logger.error(msg)
                self.perror(msg)
                raise ServerLookupError(msg)
            else:
                return server

    def get_server_by_process_id(self, id):
        for server in self.servers.values():
            if id in server.get_proc_ids():
                return server

        msg = 'Server could not be found from process id {}.'.format(id)
        self.logger.error(msg)
        self.perror(msg)
        raise ServerLookupError(msg)


    def apply_to_server(self, what, args, opts):
        with self.server_lock:
            if opts.all:
                args = (server.id for server in self.servers.values())

            if not args:
                try:
                    args = (self.select_server().id, )
                except ServerLookupError:
                    pass

            ct = itertools.count()
            for identifier in args:
                # if identifier is not None:
                if identifier:
                    try:
                        server = self.get_server_by_id(identifier)
                    except ServerLookupError:
                        pass
                    else:
                        if what == 'shutdown':
                            server.shutdown(opts.terminate)
                            self.remove_server_by_id(identifier)
                        elif what == 'remove':
                            self.remove_server_by_id(identifier)
                        elif what == 'clear':
                            server.clear_proc()
                        else:
                            raise ValueError()  # It should never get this far.
                        next(ct)

            return next(ct)


    @options([
        make_option('-A', '--all', action='store_true', help='Shutdown all servers.'),
        make_option('-T', '--terminate', action='store_true', help='Terminate processes when shutting down.'),
    ])
    def do_shutdown_server(self, args, opts=None):
        """Shutdown servers"""
        ct = self.apply_to_server(what='shutdown', args=args, opts=opts)
        self.logger.info('Shutdown {} server(s).'.format(ct))

    @options([
        make_option('-A', '--all', action='store_true', help='Remove all servers.'),
    ])
    def do_remove_server(self, args, opts=None):
        """Remove servers from the manager."""
        ct = self.apply_to_server(what='remove', args=args, opts=opts)
        self.logger.info('Removed {} server(s).'.format(ct))

    @options([
        make_option('-A', '--all', action='store_true', help='Clear all servers.'),
    ])
    def do_clear_server(self, args, opts=None):
        """Clear servers from process that are not running. """
        ct = self.apply_to_server(what='clear', args=args, opts=opts)
        self.logger.info('Cleared {} server(s).'.format(ct))

    def do_server(self, args):
        """Show servers"""

        self.logger.info('Inspecting server(s).')

        table = list()
        header = ('pid',
                  'id',
                  'host (ip)',
                  'start',
                  'time (d:h:m:s)',
                  'cpu',
                  'proc (r, f, c)',
                  'load (1, 5, 15)',
                  'mem (total, avail., used)',
        )
        table.append(header)
        with self.server_lock:
            for server in self.servers.values():
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

    def do_stdout(self, id):
        """Show stdout

        Print stdout of a process
        """
        id = str(id)  # Conversion to a standard string is explicitly needed.
        self._print_out(id, 'stdout')

    def do_stderr(self, id):
        """Show stderr

        Print stderr of a process
        """
        id = str(id)  # Conversion to a standard string is explicitly needed.
        self._print_out(id, 'stderr')

    def _print_out(self, id, which):
        if which not in ('stdout', 'stderr'):
            raise ValueError
        server = self.get_server_by_process_id(id)
        if server:
            if which == 'stdout':
                out = server.get_proc_stdout(id)
            elif which == 'stderr':
                out = server.get_proc_stderr(id)
            for line in out:
                print(line)

    @options([
        make_option('-A', '--all', action='store_true', help='Terminate all processes.'),
        make_option('-s', '--server', action='store_true', help='Terminate all processes of the specified servers.'),
             ])
    def do_terminate_proc(self, args, opts=None):
        """Terminate processes """
        with self.server_lock:
            ct = itertools.count()
            if opts.all:
                for server in self.servers.values():
                    for id in server.get_proc_ids():
                        if server.get_proc_status(id) == 'running':
                            server.terminate_proc(id)
                            next(ct)

            elif opts.server:
                if args:
                    for server_id in args:
                        try:
                            server = self.get_server_by_id(server_id)
                        except ServerLookupError:
                            pass
                        else:
                            for id in server.get_proc_ids():
                                if server.get_proc_status(id) == 'running':
                                    server.terminate_proc(id)
                                    next(ct)
                else:
                    try:
                        server = self.select_server()
                    except ServerLookupError:
                        pass
                    else:
                        for id in server.get_proc_ids():
                            if server.get_proc_status(id) == 'running':
                                server.terminate_proc(id)
                                next(ct)

            else:
                for process_id in args:
                    try:
                        server = self.get_server_by_process_id(process_id)
                    except ServerLookupError:
                        pass
                    else:
                        if server.get_proc_status(process_id) == 'running':
                            server.terminate_proc(process_id)
                            next(ct)

            self.pfeedback('Terminated {} processes. '.format(next(ct)))


    def do_clear_jobs(self, args):
        """Clear all jobs from the queue."""
        with self.queue_lock:
            self.queue.clear()

    # TODO: Checking the existence of the script may not be too meaningful for the manager, because it only needs to be
    #       be reached/interpreted by the batchserver to which it is dispatched.
    @options([
        # make_option('-T', '--type', type='str', default='txt', nargs=1, help='type (sqlite3, txt, dumped)'),
        make_option('-C', '--check', action='store_false', help='Should the existence of the script be checked?'),
    ])
    def do_add_jobs(self, args, opts=None):
        """Add jobs to the queue."""
        # if not opts.type in ('json',):
        #     self.logger.warning('The filetype must be one of "sqlite3", "txt", "dumped" of "json".')
        #     return

        self.logger.info('Trying to add jobs.')

        try:
            args = args[0]
        except IndexError:
            msg = 'Path to JSON job file missing.'
            self.logger.warning(msg)
            self.pfeedback(msg)
            return

        # Check existence of the file.
        db_path = os.path.expanduser(args.strip())
        if not os.path.exists(db_path):
             msg = 'The job file at {} could not be located.'.format(db_path)
             self.logger.warning(msg)
             self.pfeedback(msg)
             return

        # if opts.type == 'json':
        with open(file=db_path, mode='rb') as f:
            try:
                jobs = json.load(f)
            except json.JSONDecodeError:
                msg = 'The json file could not be decoded. Is it correct?'
                self.logger.warning(msg)
                self.pfeedback(msg)
            else:
                try:
                    jsonschema.validate(jobs, job_schema)
                except jsonschema.exceptions.ValidationError as err:
                    msg = 'The json file does not have the correct format:\n' + str(err)
                    self.logger.warning(msg)
                    self.pfeedback(msg)
                else:
                    for job in jobs:
                        complement_job(job)

        # Post-processing of jobs and placing them in the queue.
        # This should all be done the the batchserver!!!
        ct = itertools.count()
        with self.queue_lock:
            for job in jobs:

                # # Check prefix.
                # if job['prefix']:
                #     interpreter, *options = job['prefix'].split()
                #     interpreter = os.path.expanduser(interpreter)
                #     job['prefix'] = [interpreter, *options]

                # # Check script.
                # script_path = pathlib.Path(job['script'].strip()).expanduser()
                # script = str(pathlib.PurePath(script_path))
                # if opts.check:
                #     if not script_path.is_file():
                #         msg = ('The script {} is not an existing file. ' +
                #                'The process is ignored.'.format(script))
                #         self.logger.warn(msg)
                #         self.pfeedback(msg)
                #         continue
                # job['script'] = script

                # # Check args.
                # job['args'] = job['args'].split()

                self.queue.append(job)
                next(ct)

        msg = 'Read in {} jobs. '.format(next(ct))
        self.logger.info(msg)
        self.pfeedback(msg)

    def select_server(self):
        if not self.servers:
            msg = 'No servers registered'
            self.pfeedback(msg)
            raise ServerLookupError(msg)
        choices = ' '.join(server_id for server_id in self.servers)
        result = self.select(choices, 'Which batchserver? ')
        return self.get_server_by_id(result)

    @options([
        make_option('-s', '--server', action='store_true', help='Show processes of the specified servers'),
        make_option('-e', '--extended',  action='store_true', help='Show extended output.'),
        make_option('-l', '--level', type='int', default=2, nargs=1, help='Set depth of paths.'),
    ])
    def do_proc(self, args, opts=None):
        """List launched processes."""
        table = list()

        header = ['pid', 'id', 'script', 'args', 'time (d:h:m:s)', 'status']
        if opts.extended:
            header.extend(['grp', 'priority', 'ip', 'prefix', 'mem (MB)',
                           'start', 'stderr', 'stdout'])

        table.append(header)

        if opts.server:
            servers = list()
            if args:
                for server_id in args:
                    try:
                        server = self.get_server_by_id(server_id)
                    except ServerLookupError:
                        pass
                    else:
                        servers.append(server)
            else:
                try:
                    server = self.select_server()
                except ServerLookupError:
                    pass
                else:
                    servers.append(server)

        else:
            servers = self.servers.values()

        with self.server_lock:
            for server in servers:
                for id in server.get_proc_ids():
                    job = server.get_proc_job(id)

                    err = server.get_proc_stderr(id)
                    err = err[-1] if err else ''
                    out = server.get_proc_stdout(id)
                    out = out[-1] if out else ''

                    tmp = [str(_) for _ in (
                        server.get_proc_pid(id),
                        id,
                        trim_path(job['script'], opts.level),
                        ' '.join(job['args']),
                        server.get_proc_runtime(id),
                        server.get_proc_status(id),
                    )]

                    if opts.extended:
                        tmp.extend(str(_) for _ in (
                            job['grp'],
                            job['priority'],
                            server.ip,
                            ' '.join(job['prefix']),
                            job['mem'],
                            server.get_proc_start_date(id),
                            err,
                            out,
                        ))

                    table.append(list(tmp))

        print_table(table)


    @options([make_option('-L', '--level', type='int',
                          default=2, nargs=1, help='depth of paths'),
              make_option('-N', '--n', type='int', default=10, nargs=1,
                          help='The number of jobs displayed.'),
              make_option('-T', '--tail', action='store_true',
                          help='Should the queue be printed from bottom?')])
    def do_jobs(self, args, opts=None):
        """List in the queue."""

        self.logger.info('Inspecting jobs.')

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

    def do_quit(self, arg):
        """Quit the batchmanager."""

        msg = 'Quitting.'
        self.logger.info(msg)
        self.pfeedback(msg)

        if self.launcher:
            self.launcher.stop()  # Send stop signal to launcher.
            self.launcher.join()  # Wait for the launcher thread to stop.

        if self.queue:

            while True:
                ans = input('There are still {} jobs enqueued. '.format(len(self.queue)) +
                            'Store them on hard disk for later usage? [y(es)/n(no)/c(ancel)]: ')
                if ans in ('y', 'n', 'c'):
                    break

            if ans == 'c':
                return

            elif ans == 'y':
                timestamp = time.strftime('%Y-%m-%d_%H-%M-%S')
                path = os.path.join('./' + timestamp + '.json')
                # with open(file=path, mode='wb') as f:
                    # pickle.dump(self.queue, f)
                with open(file=path, mode='w', encoding='utf-8') as f:
                    json.dump(list(self.queue), f, indent=4)

                msg = 'Extant jobs were dumped to: ' + path
                self.logger.info(msg)
                self.pfeedback(msg)

        return self._STOP_AND_EXIT  # Send stop signal to cmd2

    do_q = do_quit
    do_exit = do_quit

@click.command()
@click.option('--quiet/--no-quiet', default=False, help='enable/disable informative messages')
@click.option('--logfile/--no-logfile', default=False, help='enable/disable logging to logfile')
@click.option('--logconsole/--no-logconsole', default=False, help='enable/disable logging to console')
def main(quiet, logfile, logconsole):
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

    """
    Configure logging
    """
    LEVELS = {
        'nolog' : logging.CRITICAL + 1,
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL,
    }

    # Select level
    level_file = LEVELS.get('info' if logfile else 'nolog', logging.NOTSET)
    level_console = LEVELS.get('info' if logconsole else 'nolog', logging.NOTSET)
    # Create module logger
    module_logger = logging.getLogger(__name__)
    module_logger.setLevel(logging.DEBUG)  # Passes all logging messages.
    # Create handlers.
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level_console)
    file_handler = logging.FileHandler(filename=__name__ + '.log', mode='w')
    file_handler.setLevel(level_file)
    # Create formatter.
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # Add formatter to handlers.
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    # Add handler to logger.
    module_logger.addHandler(stream_handler)
    module_logger.addHandler(file_handler)
    module_logger.info('Start logging.')

    """
    Create manager process
    """
    man = manager()
    man.quiet = quiet
    man.prompt = '> '
    man.colors = True

    # TODO: Fix exception handling here. When does an exception occur?
    try:
        pw = getpass.getpass()
    except Exception as err:
        print('ERROR:', err)
    else:
        man.key = get_hmac_key(pw, salt)
        del pw

    module_logger.info('Manager created. Starting program loop.')
    man.cmdloop()

# ## This is never executed, as the entry point is main.
# if __name__ == '__main__':
#     main()


# import argparse
# parser = argparse.ArgumentParser()
# parser.add_argument('--pw', action='store',
#                     dest='pw',
#                     help='password for secure communication')
# results = parser.parse_args()


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

# Probably overly complicated
# def get_server_from_proc_id(self, id):
#     for server in self.servers.values():
#         for i in server.get_proc_ids():
#             if i == id:
#                 return server
#                 break
#         else:
#             continue
#         break
#     else:
#         msg = 'Server could not be found from process id.'
#         self.logger.warn(msg)
#         warnings.warn(msg)

# def get_server_by_process_id(self, id):
#     with self.server_lock:
#         for server in self.servers.values():
#             if id in server.get_proc_ids():
#                 return server

#     msg = 'Process id: {} could not be found.'.format(id)
#     self.logger.warning(msg)
#     warnings.warn(msg, RuntimeWarning)

#             if what == 'shutdown':
#                 try:
#                     server = self.get_server_by_id(identifier)
#                 except ServerLookupError:
#                     pass
#                 else:
#                     server.shutdown(opts.terminate)
#                     self.remove_server_by_id(identifier)
#                     next(ct)
#             elif what == 'remove':
#                 try:
#                     self.remove_server_by_id(identifier)
#                 except ServerLookupError:
#                     pass
#                 else:
#                     next(ct)
#             elif what == 'clear':
#                 try:
#                     server = self.get_server_by_id(identifier)
#                 except ServerLookupError:
#                     pass
#                 else:
#                     server.clear_proc()
#                     next(ct)

# return next(ct)


# if opts.type == 'dumped':
#     with open(file=db_path, mode='rb') as f:
#         try:
#             jobs = pickle.load(f)
#         except EOFError:
#             pass  ## Logging + warning must be done.
#         else:
#             self.queue.extend(jobs)
#             return

# if opts.type == 'sqlite3':
#     import sqlite3
#     try:
#         # conn = sqlite3.connect(db_path, timeout=5, isolation_level='EXCLUSIVE')
#         with sqlite3.connect(db_path, timeout=5, isolation_level='EXCLUSIVE') as conn:
#             conn.row_factory = sqlite3.Row
#             cursor = conn.cursor()
#             query = "SELECT name FROM sqlite_master WHERE type='table';"
#             res = cursor.execute(query).fetchall()
#             if len(res) > 1:
#                 warnings.warn('The database contains more than one table, ' +
#                               'only the first one is used', RuntimeWarning)

#             tn = res[0]['name']
#             # logging.debug('There are currently {} processes.'.format(len(processes)))
#             query = 'SELECT * FROM {tn} ORDER BY grp ASC, priority ASC'.format(tn=tn)
#             col_names = {_[0] for _ in cursor.description}
#             conn.row_factory = dict_factory
#             cursor = conn.cursor()
#             tbl = cursor.execute(query).fetchall()

#             ## TODO: Here is some bug, this is not working.
#             # if not col_names.issubset(required_col_names):
#             #     msg = ('Input from sqlite3 database does not contain ' +
#             #            'the appropriate columns in its (first) table.')
#             #     logger.warn(msg)
#             #     print(msg)
#             #     return

#     except (sqlite3.OperationalError, sqlite3.DatabaseError) as err:
#         logging.debug('Connecting to database failed')

# elif opts.type == 'txt':
#     tbl = list()
#     with open(db_path, mode='r') as f:
#         for l in f:
#             job = dict()
#             l = l.strip().split(':')
#             for k, c in zip(required_col_names, l):
#                 job[k] = c.strip()

#             job['grp'] = int(job['grp'])
#             job['priority'] = int(job['priority'])
#             job['mem'] = int(job['mem'])
#             tbl.append(job)
