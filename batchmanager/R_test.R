# some random infinite R process requiring minimal system resources.
args = commandArgs(trailingOnly=TRUE)
dput(args)

time1 = Sys.time()
while(Sys.time() - time1 < as.integer(args[1L])) {
  ## Sys.sleep(1)
}



