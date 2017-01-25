import math
import datetime
import os
import pathlib
import logging

# Logging.
module_logger = logging.getLogger(__name__)

# Auxiliary functions.

def get_date():
    return datetime.datetime.now()

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
    logger = logging.getLogger(__name__ + '.print_table')
    col_width = [max(len(str(x)) for x in col) for col in zip(*table)]
    for line in table:
        print(" | ".join("{:{}}".format(x, col_width[i])
                         for i, x in enumerate(line)))

def trim_path(path, level):
    path = list(pathlib.Path(path).parts)
    # del path[0]  ## likely a bug
    return os.path.join(*path[-level:])
