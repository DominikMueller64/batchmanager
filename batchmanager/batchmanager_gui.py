# from tkinter import *
import tkinter
from tkinter import ttk
from tkinter import simpledialog, filedialog

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

import Pyro4
import shortuuid
import jsonschema

# from .functions import *
# from .globvar import *



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


module_logger = logging.getLogger(__name__)

class Job:

    def __init__(self, script,
                 prefix=None, args=None,
                 id=None,
                 priority=0, grp=0, mem=0.0):
        self.script = script
        self.prefix = prefix if prefix else []
        self.args = args if args else []
        self.id = id if id else shortuuid.uuid()
        self.priority = priority
        self.grp = grp
        self.mem = mem

    def __eq__(self, other):
        return self.id == other.id


class LockedDeque(collections.deque):
    def __init__(self):
        super().__init__()
        self._lock = threading.RLock()

    @property
    def locked(self):
        return self._locked

    def release(self):
        self._locked = False
        return self._lock.release()

    def __enter__(self):
        self._lock.__enter__()
        self._locked = True
        return self

    def __exit__(self, *args, **kwargs):
        if self._locked:
            self._locked = False
            return self._lock.__exit__(*args, **kwargs)

    def index_job(self, id):
        with self:
            for i, job in enumerate(self):
                # module_logger.info('{}'.format(job.id))
                if job.id == id:
                    return i
            raise ValueError('Job not found.')

    def delete_job(self, id):
        try:
            index = self.index_job(id)
        except ValueError:
            raise
        else:
            stack = list()
            with self:
                for i in range(index - 1):
                    stack.append(self.popleft())
                self.popleft()
                while stack:
                    self.appendleft(stack.pop())

class myNotebook(ttk.Notebook):
    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)


class myJobTree(ttk.Treeview):
    def __init__(self, master=None):
        super().__init__(master, columns=('script', 'prefix', 'args', 'priority', 'group', 'memory'))
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        # self.column('id', width=50, anchor='center')
        # self.column('script', width=50, anchor='center')

        self.heading('#0', text='ID')
        self.column('#0', stretch=False)
        for column in self['columns']:
            self.heading(column, text=column.upper())
            self.column(column, stretch=False)


        self.bind("<Button-3>", self.OnRightClick)

    def insert_job(self, job):
        self.insert('', 'end',
                    iid=job.id,
                    text=job.id,
                    values=(job.script, job.prefix, job.args,
                            job.priority, job.grp, job.mem))


    def OnRightClick(self, event):
        item = self.identify('item', event.x, event.y)
        print("you clicked on", self.item(item, 'text'))

        menu = tkinter.Menu(self)
        for i in ('delete',):
            menu.add_command(label=i)

        menu.post(event.x_root, event.y_root)

        app.queue.delete_job(self.item(item, 'text'))
        self.delete(item)


class Application(ttk.Frame):

    def __init__(self, master=None, **kw):
        super().__init__(master, **kw)

        self.servers = dict()
        self.queue = LockedDeque()
        self.server_lock = threading.RLock()  # Lock for server dictionary.
        self.launcher = None  # Here comes the launcher once started.
        self.key = None

        self.logger = logging.getLogger(__name__ + '.manager')

        # Gui
        self.grid(column=0, row=0, sticky='nwes')
        self.master.columnconfigure(0, weight=1)
        self.master.rowconfigure(0, weight=1)
        self.master.option_add('*tearOff', False)

        password = simpledialog.askstring("Password", "Enter password:", show='*')

        menubar = tkinter.Menu(self.master)
        self.master['menu'] = menubar

        menu_file = tkinter.Menu(menubar)
        menu_settings = tkinter.Menu(menubar)
        menu_help = tkinter.Menu(menubar)
        menubar.add_cascade(menu=menu_file, label='File')
        menubar.add_cascade(menu=menu_settings, label='Settings')
        menubar.add_cascade(menu=menu_help, label='Help')

        menu_file.add_command(label='Open', underline=0, command=self.add_jobs)
        menu_file.add_separator()
        menu_file.add_command(label='Quit', underline=0, command=self.master.destroy)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.rowconfigure(1, weight=0)
        self.rowconfigure(2, weight=0)

        self.create_widgets()

    def create_widgets(self):

        # Create Notebook
        notebook = ttk.Notebook(self)
        overview_frame = ttk.Frame(notebook)
        servers_frame = ttk.Frame(notebook)
        jobs_frame = ttk.Frame(notebook)

        jobs_frame.rowconfigure(0, weight=1)
        jobs_frame.columnconfigure(0, weight=1)

        notebook.add(overview_frame, text='Overview')
        notebook.add(servers_frame, text='Servers')
        notebook.add(jobs_frame, text='Jobs')
        notebook.grid(column=0, row=0, columnspan=2, sticky='nsew')

        # Create tree
        self.job_tree = myJobTree(jobs_frame)
        self.job_tree.grid(column=0, row=0, sticky='nsew')
        vsc_tree = ttk.Scrollbar(jobs_frame, orient='vertical', command=self.job_tree.yview)
        hsc_tree = ttk.Scrollbar(jobs_frame, orient='horizontal', command=self.job_tree.xview)
        self.job_tree.configure(yscrollcommand=vsc_tree.set)
        self.job_tree.configure(xscrollcommand=hsc_tree.set)
        vsc_tree.grid(column=1, row=0, sticky='nes')
        hsc_tree.grid(column=0, row=1, sticky='wes')

        # Create Text (Logging)
        self.logtext = tkinter.Text(self, state='disabled', width=80, height=8, wrap='none')
        vscrollbar = ttk.Scrollbar(self, orient='vertical', command=self.logtext.yview)
        hscrollbar = ttk.Scrollbar(self, orient='horizontal', command=self.logtext.xview)
        self.logtext.configure(yscrollcommand=vscrollbar.set)
        self.logtext.configure(xscrollcommand=hscrollbar.set)

        self.logtext.grid(column=0, row=1, sticky='nwes')
        vscrollbar.grid(column=1, row=1, sticky='nes')
        hscrollbar.grid(column=0, row=2, sticky='wes')

        ttk.Sizegrip(self).grid(column=999, row=999, sticky='se')



        # # Inserted at the root, program chooses id:
        # job_tree.insert('', 'end', 'widgets', text='Widget Tour')


        ## not working
        # separator = ttk.Separator(self, orient='horizontal')
        # separator.grid(column=0, row=3, columnspan=2)

    def add_jobs(self):
        """Add jobs to the queue."""

        filename = filedialog.askopenfilename()

        self.logger.info('Trying to add jobs.')
        db_path = os.path.expanduser(filename.strip())

        with open(file=db_path, mode='rb') as f:
            try:
                jobs = json.load(f)
            except json.JSONDecodeError:
                msg = 'The json file could not be decoded. Is it correct?'
                self.logger.warning(msg)
            else:
                try:
                    jsonschema.validate(jobs, job_schema)
                except jsonschema.exceptions.ValidationError as err:
                    msg = 'The json file does not have the correct format:\n' + str(err)
                    self.logger.warning(msg)
                else:
                    for i, job in enumerate(jobs):
                        # complement_job(job)
                        jobs[i] = Job(**job)

        ct = itertools.count()
        with self.queue:
            for job in jobs:
                self.queue.append(job)
                self.job_tree.insert_job(job)
                next(ct)

        msg = 'Read in {} jobs. '.format(next(ct))
        self.logger.info(msg)



    def callback_pw(*args):
        print('hi')

    def start(self):
        self.master.mainloop()


class MyHandlerText(logging.StreamHandler):
    def __init__(self, textctrl):
        super().__init__()
        self.textctrl = textctrl

    def emit(self, record):
        msg = self.format(record)
        self.textctrl.config(state="normal")
        self.textctrl.insert("end", msg + "\n")
        self.flush()
        self.textctrl.config(state="disabled")

if __name__ == "__main__":
    root = tkinter.Tk()
    # root.geometry("500x500")
    root.title('batchmanager')

    app = Application(root, padding="3 3 12 12", borderwidth=5, relief="ridge")

    module_logger.setLevel(logging.DEBUG)  # Passes all logging messages.
    console_handler = logging.StreamHandler()
    gui_handler = MyHandlerText(app.logtext)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    gui_handler.setFormatter(formatter)
    module_logger.addHandler(console_handler)
    module_logger.addHandler(gui_handler)

    module_logger.info(root.tk.call('tk', 'windowingsystem'))

    app.start()

# Application(root).start()







# root.mainloop()

