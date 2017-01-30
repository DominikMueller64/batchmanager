import os

# Global variables.
required_col_names = 'grp priority prefix script args mem'.split()
job_keys = required_col_names.copy()
job_keys.insert(0, 'id')
# Salt for generating the crytographic (HMAC-) key from the password.
salt = b'\xf7\x1bm\xb9j\xe3\x94\xa6\xc1Lq\x94\xc4\xb5\x07y'
