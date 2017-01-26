# some random infinite R process requiring minimal system resources.
args = commandArgs(trailingOnly=TRUE)
dput(args)

print('This is stdout start.\n')

time1 = Sys.time()
ct <- 1L
while(Sys.time() - time1 < as.integer(args[1L])) {
  print(sprintf('This is line number: %d', ct))
  Sys.sleep(0.05)
  ct <- ct + 1L
}

print('This is stdout end.\n')

Sys.sleep(5)
print('Last output.')

stop('Here, an Error occurred.')




