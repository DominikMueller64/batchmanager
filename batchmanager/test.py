import click

@click.command()
@click.option('--count', default=1, help='number of greetings')
@click.argument('name')
def hello(count, name):
    for x in range(count):
        click.echo('Hello %s!' % name)
if __name__ == '__main__':
    hello()



# import threading
# import time


# class myclass:

#     def __init__(self):
#         self.run_func()

#     def func(self):
#         for _ in range(5):
#             time.sleep(1)
#             print('Hello')

#     def run_func(self):
#         thread = threading.Thread(target=self.func,
#                                   name='my_thread')
#         thread.start()
#         # thread.join()

# m = myclass()
