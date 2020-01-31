#!/bin/bash

#----clean up-----
rm /dev/shm/*tazer*
rm -r /tmp/${USER}/*tazer*


mode=$1
data_dir=$2
cd $data_dir

export TAZER_SERVER_CACHE_SIZE=$(( 16*1024*1024*1024 )) 

if [ "${mode}" == "3" ] || [ "${mode}" == "4" ] ; then #setup tazer server as a forwarding server
export TAZER_NETWORK_CACHE=1 
export TAZER_SHARED_MEM_CACHE=1
export TAZER_SHARED_MEM_CACHE_SIZE=$(( 16*1024*1024*1024 )) 
export TAZER_BOUNDED_FILELOCK_CACHE=1 
export TAZER_BOUNDED_FILELOCK_CACHE_SIZE=$(( 500*1024*1024*1024 )) 
#------------ you can change this (probably should keep on /files0 for now tho...)
export TAZER_BOUNDED_FILELOCK_CACHE_PATH=/files0/${USER}/tazer_server_cache
#---------------------
export TAZER_SERVER_CONNECTIONS=conns.meta 
fi

if [ "${mode}" == "2" ] || [ "${mode}" == "4" ] ; then
#------------launch a server on ib net --------------
# gdb --batch --command=tazer.gdb --arg ${TAZER_ROOT}/bin/server 5001 "$(hostname -a)".ibnet &> "$(hostname -a)".ibnet.log &
${TAZER_ROOT}/bin/server 5001 "$(hostname -a)".ibnet &> "$(hostname -a)".ibnet.log &
fi

#------------launch a server on ethernet--------------
# gdb --batch --command=tazer.gdb --arg ${TAZER_ROOT}/bin/server 5001 "$(hostname -a)" &> "$(hostname -a)".log
 ${TAZER_ROOT}/bin/server 5001 "$(hostname -a)" &> "$(hostname -a)".log

wait