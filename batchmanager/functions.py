import base64
import datetime
import hashlib
import logging
import math
import os
import pathlib
import pickle
import string

import cryptography.fernet

# Logging.
module_logger = logging.getLogger(__name__)

def get_date():
    return datetime.datetime.now()

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def format_date(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

def strfdelta(tdelta, fmt):

    class DeltaTemplate(string.Template):
        delimiter = "%"

    d = dict()
    sec = abs(int(tdelta.total_seconds()))
    d['D'], sec = divmod(sec, 86400)
    d['H'], sec = divmod(sec, 3600)
    d['M'], d['S'] = divmod(sec, 60)
    template = DeltaTemplate(fmt)
    return template.substitute(**d)

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

def get_hmac_key(pw, salt):
    if pw is None:
        return pw
    bpw = pw.encode('utf-8')
    key = hashlib.pbkdf2_hmac(hash_name='sha256', password=bpw,
                              salt=salt, iterations=int(1e6))
    return base64.urlsafe_b64encode(key)

def encrypt_msg(msg, key):
    cipher_suite = cryptography.fernet.Fernet(key)
    # Serialize the python object using pickle.
    msg = pickle.dumps(msg)
    # Encrypt the message based on the generated key.
    encr_msg = cipher_suite.encrypt(msg)
    return base64.b64encode(encr_msg)

def decrypt_msg(msg, key):
    cipher_suite = cryptography.fernet.Fernet(key)
    # Decipher the encrypted message and deserialize it.
    msg = base64.b64decode(msg)
    msg = cipher_suite.decrypt(msg)
    return pickle.loads(msg)
