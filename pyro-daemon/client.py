import Pyro4
import os
import subprocess
import configobj
import validate
import sqlite3
import warnings
import sys
import datetime
import cmd
import math


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

class manager():

    def __init__(self):
        self._servers = list()

    @property
    def servers(self):
        return self._servers

    def add_server(self, name):
        try:
            server = Pyro4.Proxy(':'.join(('PYRONAME', str(name))))
            server._pyroBind()
        except (Pyro4.errors.NamingError, Pyro4.errors.CommunicationError) as err:
            warnings.warn('Server could not be successfully added.', RuntimeWarning)
            print("Error: {0}".format(err))
        else:
            self.servers.append(server)

    def server_status(self):
        table = list()
        header = ('pid', 'ip', 'started', 'cpu', 'proc',
                  'load (1, 5, 15 min.)',
                  'mem (total, avail., used)')
        table.append(header)
        for server in self.servers:
            pid = str(server.pid)
            ip = str(server.get_host_ip())
            started = server.start_date
            cpu = str(server.cpu_count())
            proc = str(server.num_proc())
            load = ', '.join(str(_) for _ in server.load_average())
            mem = server.virtual_memory()
            mem = ', '.join((convert_size(mem[0]), convert_size(mem[1]), str(mem[2]) + '%'))
            table.append((pid, ip, started, cpu, proc, load, mem))

        print_table(table)

    def read_jobs(self, db_path):
        db_path = os.path.expanduser(db_path)
        if not os.path.exists(db_path):
            raise FileNotFoundError('The database could not be located.')

        try:
            with sqlite3.connect(db_path, timeout=5, isolation_level='EXCLUSIVE') as conn:
                # if LOG:
                #     logging.debug('Connected to database.')
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                # logging.debug('There are currently {} processes.'.format(len(processes)))
                tbl = cursor.execute('SELECT rowid, * FROM {tn} ORDER BY grp ASC, priority ASC'.format(tn=tn)).fetchall()
                # Start processes
                for row in tbl:
                    if (row['status'] == 'WAITING' and 
                        row['mem'] < psutil.virtual_memory().available and 
                        os.getloadavg()[0] < MAX_LOAD_AVG and 
                        len(processes) < MAX_NUM_PROCESSES):

                        rowid = row['rowid']
                        args = row['args'].split(sep=' ')
                        script = os.path.expanduser(row['name'])

                        try:
                            proc = subprocess.Popen([SCRIPT_NAME_PREFIX, script, *args],
                                                    stdout=subprocess.PIPE)

                        except (FileNotFoundError, OSError):
                            pass
                        else:
                            start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            proc.rowid = rowid
                            proc.args = args
                            proc.start_date = start_date
                            cursor.execute("UPDATE {tn} SET status='RUNNING' WHERE rowid={rid}".format(tn=tn, rid=row['rowid']))
                            cursor.execute("UPDATE {tn} SET pid='{pid}' WHERE rowid={rid}".format(tn=tn, pid=proc.pid, rid=row['rowid']))
                            cursor.execute("UPDATE {tn} SET ppid='{ppid}' WHERE rowid={rid}".format(tn=tn, ppid=daemon_pid, rid=row['rowid']))
                            cursor.execute("UPDATE {tn} SET startdate='{dt}' WHERE rowid={rid}".format(tn=tn, dt=start_date, rid=row['rowid']))

                            # conn.commit()
                            processes.append(proc)

                            if LOG:
                                logging.debug('Process {pid} started.'.format(pid=proc.pid))

                        # proc.terminate()
                        time.sleep(SLEEP_LENGTH_PROCESS_DELAY)

        except (sqlite3.OperationalError, sqlite3.DatabaseError) as err:
            if LOG:
                logging.debug('Connecting to database failed')



class MyPrompt(cmd.Cmd):

    def do_add_server(self, args):
        """Add server to the manager"""
        args = args.strip().split()
        for name in args:
            man.add_server(name)

    def do_server_status(self, args):
        """List server"""
        man.server_status()

    def do_exit(self, args):
        """Exit the program."""
        print('Exiting.')
        raise SystemExit

    def do_read_jobs(self, args):
        """Parse jobs from a sqlite3 database"""
        man.read_jobs(args)

    def postloop(self):
        print()

if __name__ == '__main__':
    man = manager()
    prompt = MyPrompt()
    prompt.prompt = '> '
    prompt.cmdloop('Starting batch process manager.')

# def main():
#     # main loop
#     man = manager()
#     while(True):

#         print('add_server: add a server')
#         print('exit: showdown')
#         cmd = input('Gimme job: ')

#         if cmd == 'add_server':
#             names = input('server names: ')
#             names = names.strip().split()
#             for name in names:
#                 man.add_server(name)

#         if cmd == 'exit':
#             sys.exit()

# if __name__ == '__main__':
#     main()

# man = manager()
# man.add_server(name='sdsf1')
# man.add_server(name='s1')
# man.add_server(name='s1')
# server = man.servers[0]
# script = os.path.expanduser('~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon/R_test.R')
# cmdlist = ['/usr/bin/Rscript', script, '100']
# server.start_process(cmdlist)
# server.shutdown(terminate_processes=False)

# server = Pyro4.Proxy("PYRONAME:s1")
# print(server.pid)




# # ns = Pyro4.locateNS()

# script = os.path.expanduser('~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon/R_test.R')
# cmdlist = ['/usr/bin/Rscript', script, '100']
# server.start_process(cmdlist)
# server.start_process(cmdlist)

# print(server.load_average())
# print(server.status())
# print(server.get_pid())
# print(server.get_pids())
# print(server.remove(pid=15858))
# server.terminate(server.get_pid())
# server.termin


# user = 'domi89'
# server_ip = '144.41.140.191'

# # This is the ip of localhost.
# server_ip = Pyro4.socketutil.getIpAddress(None, workaround127=True)

# cmd = 'ls'

# # cmd = 'cd ~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon; python server.py blablacar &'
# cmdlist = ['sshpass', '-e', 'ssh', '-o StrictHostKeyChecking=no',
#            '@'.join((user, server_ip)), cmd]
# print(cmdlist)
# subprocess.Popen(cmdlist)

