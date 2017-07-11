import click

# Insert a row of data
# Larger example that inserts many records at a time
# jobs = [(2, 2, '/usr/bin/Rscript --vanilla', '~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon/R_test.R', '20 wurst 6.5', 500),
#         (1, 2, '/usr/bin/Rscript --vanilla', '~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon/R_test.R', '30 brot 8.9', 550),
#         (1, 1, '/usr/bin/Rscript --vanilla', '~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon/R_test.R', '30 bier 3.7', 590),
#         (2, 1, '/usr/bin/Rscript --vanilla', '~/Dropbox/Python_Projects/pyro-daemon/pyro-daemon/R_test.R', '20 wein 5.2', 523)] * 3
jobs = [(2, 2, '/usr/bin/Rscript --vanilla', './R_test.R', '20 wurst 6.5', 500),
        (1, 2, '/usr/bin/Rscript --vanilla', './R_test.R', '30 brot 8.9', 550),
        (1, 1, '/usr/bin/Rscript --vanilla', './R_test.R', '30 bier 3.7', 590),
        (2, 1, '/usr/bin/Rscript --vanilla', './R_test.R', '20 wein 5.2', 523)] * 3

jobs_dict = [
    {
        'grp': 2, 'priority': 2, 'prefix': ['/usr/bin/Rscript', '--vanilla'],
        'script': './R_test.R', 'args': '20 wein 5.2', 'mem': 500
    },
    {
        'grp': 1, 'priority': 1, 'prefix': '/usr/bin/Rscript --vanilla',
        'script': './R_test.R', 'args': '30 brot 8.4', 'mem': 531
    }
]

@click.command()
@click.option('--type', default='sqlite3', type=click.Choice(['sqlite3', 'txt', 'json']),
              help='File type, either "sqlite3" or "txt" or "json".')
def main(type):
    if type == 'sqlite3':
        import sqlite3
        conn = sqlite3.connect('jobs.db')
        c = conn.cursor()

        # Create table
        c.execute('''CREATE TABLE database
        (grp integer, priority integer, prefix text, script text, args text, mem real)''')

        c.executemany('INSERT INTO database VALUES (?,?,?,?,?,?)', jobs)


        # Save (commit) the changes
        conn.commit()

        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        conn.close()
    elif type == 'txt':
        with open('jobs.txt', 'w') as f:
            for line in jobs:
                f.write(':'.join(tuple(map(str, line))) + '\n')


    elif type == 'json':
        import json
        with open('jobs.json', 'w') as f:
            json.dump(jobs_dict, f, indent=2)

if __name__ == '__main__':
    main()


import jsonschema
from jsonschema import validate

with open('jobs.json', 'r') as f:
    data = json.load(f)


with open('jobs.txt', 'r') as f:
    data = json.load(f)

data = [
    {
        'grp': 2, 'priority': 2, 'prefix': '/usr/bin/Rscript --vanilla',
        'script': './R_test.R', 'args': '20 wein 5.2', 'mem': 500
    },
    {
        'grp': 1, 'priority': 1, 'prefix': '/usr/bin/Rscript --vanilla',
        'script': './R_test.R', 'args': '30 brot 8.4', 'mem': 'df'
    }
]


jsonschema.validate(data, schema)

d = dict(a=5, b=8)

try:
    d['g']
except KeyError as err:
    print(str(err))
    raise ValueError('shit happend' +
                     str(err))

