time1 <- Sys.time()
runtime <- 3 # in seconds
while(Sys.time() - time1 < runtime) {
  Sys.sleep(0.01)
}
