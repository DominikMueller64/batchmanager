import sqlite3
conn = sqlite3.connect('database.db')
c = conn.cursor()

# Create table
c.execute('''CREATE TABLE database
(grp integer, priority integer, prefix text, script text, args text, mem real)''')

# Insert a row of data
# Larger example that inserts many records at a time
jobs = [(2, 2, '/usr/bin/Rscript', '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '20 wurst 6.5', 500),
        (1, 2, '/usr/bin/Rscript', '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '30 brot 8.9', 550),
        (1, 1, '/usr/bin/Rscript', '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '30 bier 3.7', 590),
        (2, 1, '/usr/bin/Rscript', '~/Dropbox/Python_Projects/daemon/daemon/R_test.R', '20 wein 5.2', 523)]

c.executemany('INSERT INTO database VALUES (?,?,?,?,?,?)', jobs)

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()


