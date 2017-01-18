import logging
import threading
import time


def worker():
    logging.debug('Starting')
    time.sleep(0.2)
    logging.debug('Exiting')


def my_service():
    logging.debug('Starting')
    time.sleep(0.3)
    logging.debug('Exiting')

def servant():
    logging.debug('Starting')
    for _ in range(5):
        logging.debug('{}'.format(_))
        time.sleep(1)
    logging.debug('Exiting')

logging.basicConfig(
    level=logging.DEBUG,
    format='[%(levelname)s] (%(threadName)-10s) %(message)s',
)

t = threading.Thread(name='my_service', target=my_service)
w = threading.Thread(name='worker', target=worker)
w2 = threading.Thread(target=worker)  # use default name
s = threading.Thread(target=servant)  # use default name

w.start()
w2.start()
t.start()
s.start()


import time
from threading import Thread

def timer(name):
    count = 0
    for _ in range(5):
        time.sleep(3)
        count += 1            
        print("Hi " + name + "This program has now been running for " + str(count) + " minutes.")

name = 'asdf'
background_thread = Thread(target=timer, args=(name,))
background_thread.start()

background_thread = Thread(target=timer, args=(name,))
background_thread.start()


import logging
import threading
import time

class launcher(threading.Thread):
    logger = logging.getLogger('client.manager.launcher')
    logger.debug('Launcher thread started.')
    def __init__(self, servers, queue, max_cycles=int(1e6), sleep_cycles=5):
        super().__init__(group=None, target=None, name='launcher', daemon=False)
        self.servers = servers
        self.queue = queue
        self.max_cycles = max_cycles
        self.sleep_cycles = sleep_cycles
        self.can_run = threading.Event()
        self.should_stop = threading.Event()

    def pause(self):
        self.can_run.clear()

    def resume(self):
        self.cand_run.set()

    def stop(self):
        self.should_stop.set()

    def get_avail_server(self, mem):
        ids = {i for i, s in self.servers.items() if s.available(mem * 1024)}
        if not ids:
            return  # No server satisfies the requirements.
        # Get server with minimum load.
        dload = {i: s.load_average()[0] for i, s in self.servers.items()}
        return self.servers[min({i: dload[i] for i in ids}, key=dload.get)]

    def run(self):
        for cycle in range(1, self.max_cycles + 1):
            # time.sleep(self.sleep_cycles)
            logger.debug('Cycle {} entered.'.format(cycle))
            if self.should_stop.wait(self.sleep_cycles):
                break

            if not self.can_run.is_set():
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


def wait_for_event(e):
    """Wait for the event to be set before doing anything"""
    logging.debug('wait_for_event starting')
    for _ in range(20):
        time.sleep(1)
        event_is_set = e.wait(0.1)
        logging.debug('event set: %s   is_set: %s', event_is_set, e.is_set())

logging.basicConfig(
    level=logging.DEBUG,
    format='(%(threadName)-10s) %(message)s',
)

e = threading.Event()
t1 = threading.Thread(
    name='block',
    target=wait_for_event,
    args=(e,),
)
t1.start()
e.set()
e.clear()



