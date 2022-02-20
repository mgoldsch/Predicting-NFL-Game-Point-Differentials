library(nflfastR) #nfl data
library(tictoc) #used for timing code execution


wd <- dirname(sys.frame(1)$ofile) #gets the directory where this script is located
setwd(wd) #sets the working direcotry to the directory of this script, this is so relative paths can be used below

tic() #start clock

#builds the SQLite database called pdp_db in the data directory. The table name will be nflfastR_pdp
#force_rebuild = TRUE forces the database to be rebuilt by scratch

update_db(dbdir = '../data', force_rebuild = TRUE)

toc() #stop clock