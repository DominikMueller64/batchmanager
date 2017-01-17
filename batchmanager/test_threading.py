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

