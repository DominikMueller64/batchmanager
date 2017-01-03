import sqlite3
conn = sqlite3.connect('database.db')
c = conn.cursor()

# Create table
c.execute('''CREATE TABLE database
(grp integer, priority integer, name text, args text,
mem real)''')

# Insert a row of data
# Larger example that inserts many records at a time
jobs = [(2, 2, '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '5 wurst 6.5', 500),
        (1, 2, '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '8 brot 8.9', 550),
        (1, 1, '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '2 bier 3.7', 590),
        (2, 1, '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '10 wein 5.2', 523)]

c.executemany('INSERT INTO database VALUES (?,?,?,?,?)', jobs)

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()


