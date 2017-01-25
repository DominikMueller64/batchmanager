import threading
import subprocess
import io
import os
import time

class async_reader(threading.Thread):

    def __init__(self, stream):
        super().__init__(group=None, target=None, name='async_reader', daemon=True)
        self.stream = stream
        self.buffer = io.StringIO()

    def run(self):
        for line in iter(self.stream.readline, ''):
            print(line)
            self.buffer.write(line)

    @classmethod
    def new(cls, stream):
        reader = cls(stream)
        reader.start()
        return reader


# script = os.path.expanduser('~/Dropbox/Python_Projects/batchmanager/batchmanager/R_test.R')
# subprocess.Popen(['/usr/bin/Rscript', script, '5'])

proc = subprocess.Popen(['/usr/bin/Rscript', script, '5'],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

stdout_reader = async_reader.new(io.TextIOWrapper(proc.stdout, encoding='utf-8'))
stderr_reader = async_reader.new(io.TextIOWrapper(proc.stderr, encoding='utf-8'))
print('hi')

print(stdout_reader.buffer.getvalue())
print(stderr_reader.buffer.getvalue())

print(stdout_reader.is_alive())


