#!/bin/bash

GET_TASK_HOST=$1
GET_TASK_PORT=$2
CORES_PER_NODE=$3
SCRIPTS_DIR=`pwd`

JOBID=${SLURM_JOB_ID}_${SLURM_NODEID}
RANDOM_START=2  #0: all tasks start at the same time, 1: tasks on a node start at same time, nodes start at random times, 2: tasks start at random times

MY_HOSTNAME=`hostname`
echo "$MY_HOSTNAME $GET_TASK_HOST $CORES_PER_NODE"

#clean up from previous experiments
rm /dev/shm/*tazer*
rm -r /tmp/*tazer*
rm -r /tmp/${USER}
mkdir -p /tmp/tazer${USER}/tazer_cache

touch ${JOBID}_time.txt
date +%s >> ${JOBID}_time.txt

module unload gcc
rm -r /state/partition1/tazer${USER}

mkdir -p /files0/${USER}/tazer_cache/${MY_HOSTNAME}/fc
ln -s /files0/${USER}/tazer_cache/${MY_HOSTNAME}/fc /tmp/tazer${USER}/tazer_cache

mkdir -p /state/partition1/tazer${USER}/tazer_cache/bbc
ln -s /state/partition1/tazer${USER}/tazer_cache/bbc  /tmp/tazer${USER}/tazer_cache/

mkdir -p /files0/${USER}/tazer_cache/gc
ln -s /files0/${USER}/tazer_cache/gc /tmp/tazer${USER}/tazer_cache

np=$CORES_PER_NODE #24

mkdir $JOBID
mv ${JOBID}_time.txt $JOBID
cd $JOBID

date +%s >> ${JOBID}_time.txt

cp $SCRIPTS_DIR/ParseTazerOutput.py .

cat <<EOT > send_task_msg.py
import sys
import socket
import struct
import time
import random
import os

def get_running_jobs():
    pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
    jobids=""
    for pid in pids:
        try:
            proc=open(os.path.join('/proc', pid, 'cmdline'), 'r').read()
            if "job_" in str(proc):
                job=proc.split("job_")[1].split(".sh")[0]
                if job not in jobids:
                    jobids+=job+","
        except IOError: # proc has already terminated
            continue
    return jobids.strip(",")

def send_msg(sock,msg):
    # Prefix each message with a 4-byte length (network byte order)
    #print len(msg)
    msg = struct.pack('<I', len(msg)) + msg.encode()
    #print len(msg)
    sock.sendall(msg)

def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data

def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock,4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('<I', raw_msglen)[0]
    #print msglen
    return recvall(sock,msglen)

def request(instr):
    temp = instr.split("=")
    #print temp
    req_type=temp[0]
    magic=1
    received = 0
    while received == 0:  
         
        try:
            if(req_type=="0"):
                hostname=temp[1]
                jobid=temp[2]
                data=get_running_jobs()
                msg = "%s %s %s %s %s" %(str(req_type),str(magic),str(hostname),str(jobid),str(data)) 
            elif(req_type=="1"):
                hostname=temp[1]
                jobid=temp[2]
                data=""
                msg = "%s %s %s %s %s" %(str(req_type),str(magic),str(hostname),str(jobid),str(data))
            elif(req_type=="2"):
                hostname=temp[1]
                jobid=temp[2]
                data=temp[3]
                msg = "%s %s %s %s %s" %(str(req_type),str(magic),str(hostname),str(jobid),str(data))
            elif(req_type=="6"):
                hostname=temp[1]
                jobid=temp[2]
                data=""
                msg = "%s %s %s %s %s" %(str(req_type),str(magic),str(hostname),str(jobid),str(data))
            elif(req_type=="7"):
                #print "trying" 
                hostname=temp[1]
                jobid=temp[2]
                data=get_running_jobs()
                msg = "%s %s %s %s %s" %(str(req_type),str(magic),str(hostname),str(jobid),str(data))
                #print msg

            port=${GET_TASK_PORT}+random.randint(0,2)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(("${GET_TASK_HOST}", port))
            
            send_msg(sock,msg)
            result=recv_msg(sock)
            if result == None:
                print ("result was none, port:",port,msg)
                sock.close()
                received=0
                time.sleep(random.randint(5,10))

            else:
                received = 1
                sock.close()
        except socket.error as e:
            print ("something went wrong, port",port,msg)
            received = 0
            time.sleep(random.randint(5,10))
    if(req_type=="7"):
        jobid=temp[2]
        if 'all tasks done' not in result.decode():
            with open('job_'+jobid+'.sh','w') as f:
                for line in result.decode():
                    #print line
                    f.write(line)      
    #print result


if __name__ == "__main__":
    instr=sys.argv[1]
    request(instr)
EOT

cat <<EOT > subtask.sh
#!/bin/bash

if [ "$RANDOM_START" -eq "2" ]; then
    twait=\`shuf -i 1-200 -n 1\`
    sleep \$twait
fi

cnt=0
python send_task_msg.py "7=${MY_HOSTNAME}=${JOBID}_\$1_\$cnt"
while [ -f job_${JOBID}_\$1_\$cnt.sh ]; do
    chmod +x job_${JOBID}_\$1_\$cnt.sh
    
    time JOBID=${JOBID}_\$1_\$cnt ./job_${JOBID}_\$1_\$cnt.sh >> job_${JOBID}_\$1_\$cnt.out 2>&1
    if [ -f job_${JOBID}_\$1_\$cnt.sh ]; then
        mv job_${JOBID}_\$1_\$cnt.sh job_${JOBID}_\$1_\$cnt.sh.old
    fi
    cnt=\$(( cnt+1 ))
    python send_task_msg.py "7=${MY_HOSTNAME}=${JOBID}_\$1_\$cnt"
echo "${JOBID}_\$1 done"
done

EOT

chmod +x subtask.sh

for p in `seq 1 $np`; do
    if [ "$RANDOM_START" -eq "1" ]; then
        twait=`shuf -i 1-60 -n 1`
        sleep $twait
    fi
    ./subtask.sh $p &
    echo "$p"
done

echo "need to wait"

wait
echo "done" >>  ${JOBID}_time.txt
date +%s >> ${JOBID}_time.txt
