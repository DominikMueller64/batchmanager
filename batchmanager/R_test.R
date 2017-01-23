# some random infinite R process requiring minimal system resources.
args = commandArgs(trailingOnly=TRUE)
dput(args)

print('This is some stdout.')

time1 = Sys.time()
while(Sys.time() - time1 < as.integer(args[1L])) {
  ## Sys.sleep(1)
}

print('This is more stdout.')

stop('Here, an Error occurred.')

print('This will not be received in stdout.')




