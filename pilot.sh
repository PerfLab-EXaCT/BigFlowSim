#!/bin/bash

GET_TASK_HOST=$1
GET_TASK_PORT=$2
CORES_PER_NODE=$3

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

mkdir -p /files0/tazer${USER}/tazer_cache/${MY_HOSTNAME}/fc
ln -s /files0/tazer${USER}/tazer_cache/${MY_HOSTNAME}/fc /tmp/tazer${USER}/tazer_cache

mkdir -p /state/partition1/tazer${USER}/tazer_cache/bbc
ln -s /state/partition1/tazer${USER}/tazer_cache/bbc  /tmp/tazer${USER}/tazer_cache/

mkdir -p /files0/${USER}/tazer_cache/gc
ln -s /files0/${USER}/tazer_cache/gc /tmp/tazer${USER}/tazer_cache

np=$CORES_PER_NODE #24

mkdir $JOBID
mv ${JOBID}_time.txt $JOBID
cd $JOBID

date +%s >> ${JOBID}_time.txt

cat <<EOT > parseTazerOutput.py
import sys


def getVals(t, data):
    input_time = 0.0
    input_accesses = 0.0
    input_amount = 0.0
    output_time = 0.0
    output_accesses = 0.0
    output_amount = 0.0
    destruction_time = 0.0
    for line in data:
        if "[TAZER] "+t in line:
            # print(line)
            vals = line.split(" ")
            if vals[2] in ["open", "access", "stat", "seek", "in_open", "in_close", "in_fopen", "in_fclose"]:
                input_time += float(vals[3])
            elif vals[2] in ["read", "fread"]:
                input_time += float(vals[3])
                input_accesses += float(vals[4])
                input_amount += float(vals[5])
            elif vals[2] in ["close", "fsync", "out_open", "out_close"]:
                output_time += float(vals[3])
            elif vals[2] in ["write", "fwrite"]:
                output_time += float(vals[3])
                output_accesses += float(vals[4])
                output_amount += float(vals[5])
            elif vals[2] in ["destructor"]:
                destruction_time += float(vals[3])
    # print(input_time, input_accesses, input_amount, output_time,
    #       output_accesses, output_amount, destruction_time)
    return input_time, input_accesses, input_amount, output_time, output_accesses, output_amount, destruction_time


def getCacheData(type, name, data):
    hits = 0
    hit_time = 0
    hit_amount = 0
    misses = 0
    miss_time = 0.0
    prefetches = 0
    stalls = 0
    stall_time = 0.0
    stall_amount = 0
    ovh_time = 0.0
    reads = 0
    read_time = 0.0
    read_amt = 0
    destruction_time = 0.0

    for line in data:
        if name+" "+type in line:
            vals = line.split(" ")
            if vals[3] == "hits":
                hit_time += float(vals[4])
                hits += int(vals[5])
                hit_amount += int(vals[6])
            elif vals[3] == "misses":
                miss_time += float(vals[4])
                misses += int(vals[5])
            elif vals[3] == "prefetches":
                prefetches += int(vals[5])
            elif vals[3] == "stalls":
                stall_time += float(vals[4])
                stalls += int(vals[5])
                stall_amount += int(vals[6])
            elif vals[3] == "ovh":
                ovh_time += float(vals[4])
            elif vals[3] == "read":
                read_time += float(vals[4])
                reads += int(vals[5])
                read_amt += int(vals[6])
            elif vals[3] == "destructor":
                destruction_time += float(vals[4])
            elif vals[3] == "constructor":
                destruction_time += float(vals[4])

    return hits, hit_time, hit_amount, misses, miss_time, prefetches, stalls, stall_time, stall_amount, ovh_time, reads, read_time, read_amt, destruction_time


def getConnectionData(data):
    cons = []
    acceses = []
    amounts = []
    times = []
    for line in data:
        if "connection:" in line:
            vals = line.split(" ")
            if vals[2] in cons:
                i = cons.index(vals[2])
                acceses[i] += int(vals[4])
                amounts[i] += float(vals[6])
                times[i] += float(vals[9])
            else:
                cons.append(vals[2])
                acceses.append(int(vals[4]))
                amounts.append(float(vals[6]))
                times.append(float(vals[9]))
    return cons, acceses, amounts, times


if __name__ == "__main__":
    infile = sys.argv[1]

    tazer_input_total = 0
    tazer_input_mem = 0
    tazer_input_shmem = 0
    tazer_input_global = 0
    tazer_input_network = 0
    tazer_output = 0
    tazer_destructor = 0.0
    local_input = 0
    local_output = 0
    sys_input = 0
    sys_output = 0

    data = []
    with open(sys.argv[1]) as file:
        for line in file:
            data.append(line)

    types = ["sys", "local", "tazer"]
    names = ["input_time", "input_accesses", "input_amount", "output_time",
             "output_accesses", "output_amount", "destruction_time"]
    vals = []
    labels = []
    for t in types:
        vs = getVals(t, data)
        vals += vs
        for n in names:
            labels.append(t+"_"+n)

    # hits,hit_time,hit_amount,misses,miss_time,prefetches,stalls,stall_time,stall_amount,ovh_time,reads,read_time,read_amt,destruction_time

    # vs = getCacheData("request", "base", data)
    # labels += ["cache_accesses", "cache_time", "cache_amount",
    #            "base_cache_ovh", "base_destruction"]
    # vals += [vs[0], vs[1], vs[10], vs[8], vs[11]]

    caches = ["base","privatememory","sharedmemory","burstbuffer", "boundedfilelock","network"]
    types = ["request", "prefetch"]
    names = ["hits", "hit_time", "hit_amount", "misses", "miss_time", "prefetches",
             "stalls", "stall_time", "stall_amt", "ovh_time", "reads", "read_time", "read_amt", "destruction_time"]

    for t in types:
        for c in caches:
            vs = getCacheData(t, c, data)
            vals += vs
            for n in names:
                labels.append(c+"_"+t+"_"+n)
    # for t in types:
    #     vs = getCacheData(t, "network", data)
    #     labels += ["network_accesses", "network_time",
    #                "network_total_amount", "network_used_amount", "network_ovh_time"]
    #     vals += vs[0:3]
    #     vals.append(vs[10])
    #     vals.append(vs[8])

    cons, acceses, amounts, times = getConnectionData(data)
    names = ["_accesses", "_amount", "_time"]
    for i in range(len(cons)):
        vals += [acceses[i], amounts[i], times[i]]
        for n in names:
            labels.append("con_"+cons[i]+n)

    label_str = "labels:"
    vals_str = "vals:"
    for i in range(len(labels)):
        label_str += labels[i]+","
        vals_str += str(vals[i])+","
        # print (labels[i],str(vals[i]))

    print(label_str[:-1])
    print(vals_str[:-1])

EOT

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
        twait=`shuf -i 1-300 -n 1`
        sleep $twait
    fi
    ./subtask.sh $p &
    echo "$p"
done

echo "need to wait"

wait
echo "done" >>  ${JOBID}_time.txt
date +%s >> ${JOBID}_time.txt
