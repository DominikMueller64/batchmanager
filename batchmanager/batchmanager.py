import Pyro4
import os
import subprocess
# import configobj
import warnings
import sys
import datetime
# from cmd import Cmd
from cmd2 import Cmd, options, make_option
import math
# import uuid
import shortuuid
import threading
import logging
import time
import collections
import pathlib
import itertools

# Auxiliary function. Should be ported to separate module.

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def format_date(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

def convert_size(size, ndigits=2):
   if (size == 0):
       return '0B'
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size, 1024)))
   p = math.pow(1024, i)
   s = round(size/p, ndigits)
   return '%s%s' % (s, size_name[i])

def set_password():
    os.environ["SSHPASS"] = input('Password: ').strip()

def print_table(table):
    """Print a table"""
    col_width = [max(len(str(x)) for x in col) for col in zip(*table)]
    for line in table:
        print(" | ".join("{:{}}".format(x, col_width[i])
                         for i, x in enumerate(line)))

def convert_poll(poll):
    lkup = {0: 'finished'}
    if poll is None:
        return 'running'
    else:
        try:
            msg = lkup[poll]
        except KeyError:
            raise RuntimeError('Conversion of poll {} failed.'.format(poll))
        else:
            return msg


def trim_path(path, level):
    path = list(pathlib.Path(path).parts)
    # del path[0]  ## likely a bug
    return os.path.join(*path[-level:])
# def server_param(server):
#     param = collections.namedtuple('param', ['load', 'avail_mem',
#                                              'cpu_count', 'num_proc'])
#     return param(load=server.load_average()[0],
#                  avail_mem=server.virtual_memory()[1],
#                  cpu_count=server.cpu_count,
#                  num_proc=None)


module_logger = logging.getLogger('client')
module_logger.setLevel(logging.DEBUG)

# Create handlers.
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler(filename='log', mode='w')
level = logging.INFO
stream_handler.setLevel(level)
file_handler.setLevel(level)

# Create formatter.
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add formatter to handlers.
stream_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handler to logger.
module_logger.addHandler(stream_handler)
module_logger.addHandler(file_handler)

class manager(Cmd):

    sleep_cycles = 5
    max_cycles = int(1e6)

    settable = Cmd.settable + '''sleep_cycles Sleep length between cycles in seconds.
                                 max_cycles Maximum number of cycles to run.'''

    @classmethod
    def do_objects(cls, args):
        with Pyro4.naming.locateNS() as ns:
            table = list()
            header = ('name', 'uri (uniform resource identifier)')
            table.append(header)
            for name, uri in ns.list().items():
                if name == 'Pyro.NameServer':
                    continue
                table.append((name, uri))
        print_table(table)

    # @property
    # def sleep_between_cycles(cls):
    #     return cls._sleep_between_cycles

    # @property
    # def max_num_cycles(cls):
    #     return cls._max_num_cycles

    def __init__(self):
        super(manager, self).__init__()
        self._servers = dict()
        self._queue = collections.deque()
        self.logger = logging.getLogger('client.manager')

    @property
    def servers(self):
        return self._servers

    @property
    def queue(self):
        return self._queue

    def launcher(self):
        """Launcher"""
        logger = logging.getLogger('client.manager.launcher')
        logger.debug('Launcher thread started.')

        def get_avail_server(servers, mem):
            # 1. Get all server ids that have enough available memory and small enough load.
            # 2. Get the the server with the smallest load among these.
            # idload = {i for i, s in servers.items() if s.load_average()[0] < s.max_load_avg}
            # idmem = {i for i, s in servers.items() if mem * 1024 < s.virtual_memory()[1]}
            # idrun = {i for i, s in servers.items() if s.num_running() < s.max_proc}
            # ids = idload & idmem & idrun
            # dload = {i: server_param(s).load for i, s in servers.items()}
            # idload = {i for i, v in dload.items() if v < MAX_LOAD_AVERAGE}
            # dmem = {i: server_param(s).avail_mem for i, s in servers.items()}
            # idmem = {i for i, v in dmem.items() if mem * 1024 < v}

            ids = {i for i, s in servers.items() if s.available(mem * 1024)}

            if not ids:
                return None  # No server satisfies the requirements.
            # Get server with minimum load.
            dload = {i: s.load_average()[0] for i, s in servers.items()}
            return servers[min({i: dload[i] for i in ids}, key=dload.get)]

        for cycle in range(1, self.max_cycles + 1):
            time.sleep(self.sleep_cycles)
            logger.debug('Cycle {} entered.'.format(cycle))
            # Update (poll) all processes.
            # for server in self.servers.values():
            #     server.update()

            if not self.servers or not self.queue:
                logger.debug('No servers or jobs available.')
                continue  # No servers or jobs available.
            job = self.queue.popleft()
            server = get_avail_server(self.servers, job['mem'])

            if server is None:
                logger.debug('No server satisfies the job requirements.')
                self.queue.appendleft(job)  # Add job back.
                continue  # No server satisfies the requirements.
            server.proc_start(job)

    def do_start_launcher(self, args):
        logger = logging.getLogger('client.manager.do_start_launcher')
        logger.debug('Starting launcher')
        l = threading.Thread(target=self.launcher, daemon=True)
        l.start()

    @options([make_option('-K', '--key', default=None, type='str',
                          help='the HMAC key to use'),

              make_option('--ns', action='store_true',
                          help='Enable contacting a name server.'),

              make_option('--nshost', default=None, type='str',
                          help='hostname or ip address of the name server in the network '
                          '(default: None. A network broadcast lookup is used.)'),

              make_option('--nsport', default=None, type='int',
                          help='the port number on which the name server is running. '
                          '(default: None. The exact meaning depends on whether the host parameter is given: '
                          '- host parameter given: the port now means the actual name server port. '
                          '- host parameter not given: the port now means the broadcast port.)')])

    def do_add_server(self, args, opts=None):
        """Add server to the manager."""
        logger = logging.getLogger('client.manager.add_server')
        args = args.strip().split()
        ct = 0
        for name in args:
            uri = name
            # TODO: Find out how to integrate context manager here.
            # with Pyro4.locateNS(hmac_key=opts.key) as ns:
            # server = Pyro4.Proxy(':'.join(('PYRONAME', str(name))))
            if opts.ns:
                # Try to contact the name server.
                try:
                    ns = Pyro4.locateNS(host=host, port=port,
                                        broadcast=True, hmac_key=opts.key)
                except Pyro4.errors.NamingError as err:
                    msg = ('Failed to locate the nameserver. '
                        'Did you set the correct HMAC key?')
                    logger.exception(msg)
                    continue

                # Try to lookup the uri on the name server.
                try:
                    uri = ns.lookup(name)
                except Pyro4.errors.NamingError as err:
                    msg = ('Name {} not found on the nameserver.'.format(name))
                    logger.exception(msg)
                    continue

            # Try to obtain a proxy.
            try:
                server = Pyro4.Proxy(uri)
            except Pyro4.errors.PyroError as err:
                msg = ('{} is an invalid uri.'.format(name))
                logger.exception(msg)
                continue
            else:
                server._pyroHmacKey = opts.key

            # Try to bind to the proxy.
            try:
                server._pyroBind()
            except Pyro4.errors.CommunicationError:
                msg = ('HMAC keys do not match.')
                logger.exception(msg)
                continue
            else:
                self.servers[server.id] = server
                ct += 1

        logger.info('Added {} server.'.format(ct))

    def get_server_id(self, name):
        """Get the id of a server from its (unique!) name."""
        logger = logging.getLogger('client.manager.get_server_id')
        servers = list(self.servers.values())
        names = tuple(server.name for server in servers)
        if name not in names:
            msg = 'Server {} not found.'.format(name)
            logger.warning(msg)
            warnings.warn(msg, RuntimeWarning)
        else:
            server = servers[names.index(name)]
            return server.id

    @options([make_option('-A', '--all', action='store_true', help='shutdown all servers'),
              make_option('-T', '--terminate', action='store_true', help='terminate processes')])
    def do_shutdown_server(self, args, opts=None):
        logger = logging.getLogger('client.manager.shutdown_server')
        if opts.all:
            for server in self.servers.values():
                server.shutdown(opts.terminate)
            self.servers.clear()
            logger.info('Shutdown all servers.')
            return

        args = args.strip().split()
        ct = 0
        for name in args:
            id = self.get_server_id(name)
            if id is not None:
                try:
                    self.servers[id].shutdown(opts.terminate)
                    del self.servers[id]
                except KeyError as err:
                    # This indicates a programming error.
                    raise RuntimeError('Bug: Server {} could not be removed!'.format(name))
                else:
                    ct += 1

        logger.info('Shutdown {} server.'.format(ct))

    @options([make_option('-A', '--all', action='store_true', help='remove all servers')])
    def do_remove_server(self, args, opts=None):
        """Remove servers from the manager."""
        logger = logging.getLogger('client.manager.remove_server')
        if opts.all:
            self.servers.clear()
            logger.info('Removed all servers.')
            return

        args = args.strip().split()
        ct = 0
        for name in args:
            id = self.get_server_id(name)
            if id is not None:
                try:
                    del self.servers[id]
                except KeyError as err:
                    raise RuntimeError('Bug: Server {} could not be removed!'.format(name))
                else:
                    ct += 1

        logger.info('Removed {} server.'.format(ct))

    @options([make_option('-A', '--all', action='store_true', help='remove all servers')])
    def do_clear_server(self, args, opts=None):
        """Clear servers from process that are not running."""
        logger = logging.getLogger('client.manager.clear_server')
        if opts.all:
            for server in self.servers.values():
                server.clear()
            logger.info('Cleared all servers.')
            return

        args = args.strip().split()
        ct = 0
        for name in args:
            id = self.get_server_id(name)
            if id is not None:
                self.servers[id].clear()
                ct += 1

        logger.info('Removed {} server.'.format(ct))

    def server_by_process_id(self, id):
        logger = logging.getLogger('client.manager.server_by_process_id')
        for server in self.servers.values():
            if id in server.proc_ids():
                return server

        msg = 'Process id: {} could not be found.'.format(id)
        logger.warning(msg)
        warnings.warn(msg, RuntimeWarning)

    @options([make_option('-A', '--all', action='store_true', help='terminate all processes')])
    def do_terminate_proc(self, args, opts=None):
        logger = logging.getLogger('client.manager.do_terminate_processes')
        if opts.all:
            for server in self.servers.values():
                for id in server.proc_ids():
                    server.proc_terminate(id)
        else:
            ids = args.split()
            for id in ids:
                server = self.server_by_process_id(id)
                if server is not None:
                    server.proc_terminate(id)
                # try:
                #     server = self.server_by_process_id(id)
                # except RuntimeWarning as err:
                #     raise
                # else:
                #     server.proc_terminate(id)

    def do_server_status(self, args):
        table = list()
        header = ('pid', 'name', 'id', 'host', 'start', 'cpu', 'proc',
                  'load (1, 5, 15 min.)',
                  'mem (total, avail., used)')
        table.append(header)
        for server in self.servers.values():
            pid = str(server.pid)
            name = str(server.name)
            id = str(server.id)
            host = str(server.ip)
            start = str(server.start_fmt)
            cpu = str(server.cpu_count)
            proc = str(server.num_proc(status='running'))
            load = ', '.join(str(_) for _ in server.load_average())
            mem = server.virtual_memory()
            mem = ', '.join((convert_size(mem[0]), convert_size(mem[1]), str(mem[2]) + '%'))
            table.append((pid, name, id, host, start, cpu, proc, load, mem))

        print_table(table)

    @options([make_option('-T', '--ftype', type='str',
                          default='sqlite3', nargs=1,
                          help='filetype (sqlite3 or txt)')])
    def do_add_jobs(self, args, opts=None):
        logger = logging.getLogger('client.manager.do_add_jobs')
        db_path = os.path.expanduser(args.strip())
        if not os.path.exists(db_path):
            # warnings.warn('The database could not be located.', RuntimeWarning)
            # print('The database could not be located.')
            logger.warning('The database {} could not be located'.format(db_path))
            return None

        if not opts.ftype in ('sqlite3', 'txt'):
            logger.warning('The filetype must be one of sqlite3 or txt.')
            return

        if opts.ftype == 'sqlite3':
            import sqlite3
            try:
            # conn = sqlite3.connect(db_path, timeout=5, isolation_level='EXCLUSIVE')
                with sqlite3.connect(db_path, timeout=5, isolation_level='EXCLUSIVE') as conn:
                    conn.row_factory = dict_factory
                    cursor = conn.cursor()
                    query = "SELECT name FROM sqlite_master WHERE type='table';"
                    res = cursor.execute(query).fetchall()
                    if len(res) > 1:
                        warnings.warn('The database contains more than one table, ' +
                                    'only the first one is used', RuntimeWarning)

                    tn = res[0]['name']
                    # logging.debug('There are currently {} processes.'.format(len(processes)))
                    query = 'SELECT rowid, * FROM {tn} ORDER BY grp ASC, priority ASC'.format(tn=tn)
                    tbl = cursor.execute(query).fetchall()
                    # Start processes

            except (sqlite3.OperationalError, sqlite3.DatabaseError) as err:
                logging.debug('Connecting to database failed')


        elif opts.ftype == 'txt':
            keys = ('grp', 'priority', 'prefix', 'script', 'args', 'mem')
            tbl = list()
            with open(db_path, mode='r') as f:
                for l in f:
                    job = dict.fromkeys(keys)
                    l = l.strip()
                    for k, c in zip(keys, l.split(':')):
                        job[k] = c.strip()
                    tbl.append(job)

        ct = itertools.count()
        for job in tbl:
            # Check prefix.
            interpreter, *options = job['prefix'].split()
            interpreter = os.path.expanduser(interpreter)
            job['prefix'] = [interpreter, *options]

            # Check script.
            script_path = pathlib.Path(job['script'].strip()).expanduser()
            script = str(pathlib.PurePath(script_path))
            if not script_path.is_file():
                warnings.warn('The script {} is not an existing file. ' +
                                'The process is ignored.'.format(script))
                continue

            job['script'] = script
            # Check args.
            job['args'] = job['args'].split()
            job['id'] = shortuuid.uuid()

            self.queue.append(job)
            next(ct)

        print('Read {0} jobs.'.format(next(ct)))


    @options([make_option('-L', '--level', type='int',
                          default=2, nargs=1, help='depth of paths'),
              make_option('-S', '--short',  action='store_true',
                          help='abbreviate the exposition')])
    def do_proc_status(self, args, opts=None):
        table = list()
        if opts.short:
            header = ('pid', 'script', 'args', 'status')
        else:
            header = ('pid', 'grp', 'priority', 'id', 'ip', 'prefix', 'script', 'args',
                      'mem (MB)', 'start', 'status')
        table.append(header)

        for server in self.servers.values():
            ip = server.ip
            for id in server.proc_ids():
                job = server.proc_job(id)
                status = server.proc_status(id)
                start = server.proc_start_date(id, format=True)
                pid = server.proc_pid(id)
                script = trim_path(job['script'], opts.level)
                args = ' '.join(job['args'])

                if opts.short:
                    tmp = tuple(map(str, (pid, script, args, status)))
                else:
                    tmp = (str(_) for _ in (pid,
                                            job['grp'],
                                            job['priority'],
                                            id,
                                            ip,
                                            job['prefix'],
                                            script,
                                            args,
                                            job['mem'],
                                            start,
                                            status))
                table.append(list(tmp))

        print_table(table)

    @options([make_option('-L', '--level', type='int',
                          default=2, nargs=1, help='depth of paths')])
    def do_job_status(self, args, opts=None):
        table = list()
        header = ('grp', 'pr.', 'prefix', 'script', 'args', 'mem (MB)')
        table.append(header)
        for job in self.queue:
            tmp = (str(_) for _ in (job['grp'],
                                    job['priority'],
                                    job['prefix'],
                                    trim_path(job['script'], opts.level),
                                    ' '.join(job['args']),
                                    job['mem']))
            table.append(list(tmp))

        print_table(table)

    def postloop(self):
        print()


def main():
    prompt = manager()
    prompt.prompt = '> '
    prompt.colors = True
    prompt.do_start_launcher(args=None)
    prompt.cmdloop()

if __name__ == '__main__':
    main()

# # This is the ip of localhost.
# server_ip = Pyro4.socketutil.getIpAddress(None, workaround127=True)

# cmd = 'ls'

# # cmd = 'cd ~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon; python server.py blablacar &'
# cmdlist = ['sshpass', '-e', 'ssh', '-o StrictHostKeyChecking=no',
#            '@'.join((user, server_ip)), cmd]
# print(cmdlist)
# subprocess.Popen(cmdlist)

