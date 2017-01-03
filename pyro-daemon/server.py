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

def get_date():
    return datetime.datetime.now()

def format_date(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

@Pyro4.expose
class server:

    def __init__(self, daemon):
        self.daemon = daemon
        self._processes = []
        self._pid = os.getpid()
        self._start_date = get_date()

    def num_proc(self):
        return len(self.processes)

    def start_process(self, cmdlist):

        try:
            proc = subprocess.Popen(cmdlist, stdout=subprocess.PIPE)

        except (FileNotFoundError, OSError):
            pass
        else:
            proc.start_date = get_date()
            self.processes.append(proc)

    def get_pids(self):
        return tuple(proc.pid for proc in self.processes)

    @Pyro4.oneway   # in case call returns much later than daemon.shutdown
    def shutdown(self, terminate_processes=True):
        if terminate_processes:
            for proc in self.processes:
                proc.terminate()
        self.daemon.shutdown()

    def terminate(self, pid, remove=False):
        for proc in self.processes:
            if proc.pid == pid:
                proc.terminate()
                if remove:
                    self.processes.remove(proc)
                break
        else:
            warnings.warn('Process not found.', RuntimeWarning)


    def remove(self, pid):
        for proc in self.processes:
            if proc.pid == pid:
                if proc.poll() is None:
                    warnings.warn('Process is still running.', RuntimeWarning)
                else:
                    self.processes.remove(proc)
                break
        else:
            warnings.warn('Process not found.', RuntimeWarning)


    # def status(self):
    #     stringlist = list()
    #     fmt_active = '{pid} {dt} {rt}'
    #     header_active = 'pid started runtime'
    #     header = 'Server (pid: {pid}, started: {dt}'.format(pid=self.pid,
    #                                                  dt=format_date(self.start_date))
    #     stringlist.extend((header, header_active))

    #     for proc in self.processes:
    #         if proc.poll() is None or proc.poll() == 0:  # Process is still running.
    #             tmp = fmt_active.format(pid=proc.pid,
    #                                     dt=format_date(proc.start_date),
    #                                     # args=' '.join(proc.args),
    #                                     rt=str(get_date() - proc.start_date))
    #             stringlist.append(tmp)

    #     return '\n'.join(stringlist)

    @property
    def processes(self):
        return self._processes

    @property
    def pid(self):
        return self._pid

    @property
    def start_date(self):
        return format_date(self._start_date)

    @classmethod
    def virtual_memory(cls):
        return psutil.virtual_memory()

    @classmethod
    def load_average(cls):
        return os.getloadavg()

    @classmethod
    def cpu_count(cls):
        return multiprocessing.cpu_count()

    @classmethod
    def get_host_name(cls):
        return socket.gethostname()

    @classmethod
    def get_host_ip(cls):
        return Pyro4.socketutil.getIpAddress(None, workaround127=True)


def main(name):
    # use port 9999?
    with Pyro4.Daemon() as daemon:
        myserver = server(daemon)
        myserver_uri = daemon.register(myserver)
        with Pyro4.locateNS() as ns:
            ns.register(name, myserver_uri)
        daemon.requestLoop()

if __name__ == '__main__':
    main(name=sys.argv[1])



