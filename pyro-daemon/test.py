import threading
import time


class myclass:

    def __init__(self):
        self.run_func()

    def func(self):
        for _ in range(5):
            time.sleep(1)
            print('Hello')

    def run_func(self):
        thread = threading.Thread(target=self.func,
                                  name='my_thread')
        thread.start()
        # thread.join()

m = myclass()
