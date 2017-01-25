# import collections

# Global variables.
required_col_names = 'grp priority prefix script args mem'.split()
job_keys = required_col_names.copy()
job_keys.insert(0, 'id')
# Job = collections.namedtuple('Job', job_keys)
