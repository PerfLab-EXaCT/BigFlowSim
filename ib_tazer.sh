#!/bin/bash

#----clean up-----
rm /dev/shm/*tazer*
rm -r /tmp/${USER}/*tazer*


export TAZER_NETWORK_CACHE=1 
export TAZER_SERVER_CACHE_SIZE=$(( 16*1024*1024*1024 )) 
export TAZER_SHARED_MEM_CACHE=0
export TAZER_SHARED_MEM_CACHE_SIZE=$(( 16*1024*1024*1024 )) 
export TAZER_BOUNDED_FILELOCK_CACHE=1 
export TAZER_BOUNDED_FILELOCK_CACHE_SIZE=$(( 500*1024*1024*1024 )) 
#------------ you can change this (probably should keep on /files0 for now tho...)
export TAZER_BOUNDED_FILELOCK_CACHE_PATH=/files0/${USER}/tazer_server_cache
#---------------------

export TAZER_SERVER_CONNECTIONS=conns.meta 
#------------launch a server on ib net and on ethernet--------------
gdb --batch --command=tazer.gdb --arg ${TAZER_ROOT}/bin/server 5001 "$(hostname -a)".ibnet &> "$(hostname -a)".ibnet.log &
gdb --batch --command=tazer.gdb --arg ${TAZER_ROOT}/bin/server 5001 "$(hostname -a)" &> "$(hostname -a)".log

wait