#/bin/bash
module load python/anaconda3.2019.3
scriptDir=`pwd`


ioratio=$1
iorate=$(( 125*ioratio ))

tpf=$2
numTasks=$3
cores_per_node=$4
nodes=$5
timelimit=$8
numCores=$((cores_per_node * nodes))
iointensity=`python File_access_pattern_gen.py ${ioratio} ${tpf} ${numTasks} ${numCores} ${iorate}_${tpf}_accesses.txt`
echo "$iointensity"
num_files=$(( numTasks/tpf ))
# 1 -- 125 -- 0.041666666666666664 -- 2 -- 1500 3000
echo "${ioratio} -- ${iorate} -- ${iointensity} -- ${tpf} -- ${num_files} ${numTasks} ${numCores}"


tazer_servers=$6
use_local_server=$7

if (( use_local_server < 3 )); then
    use_bounded_filelock=1
else
    use_bounded_filelock=0
fi

echo "use_local_server: ${use_local_server} use_bounded_filelock: ${use_bounded_filelock}"

cat <<EOT > tasks/${iorate}_${tpf}.sh
#!/bin/bash
MY_HOSTNAME=\`hostname\`
ulimit -n 4096

TAZER_PATH=${TAZER_ROOT}/lib
echo "\$JOBID"
TASKDIR="./"
mkdir -p \$TASKDIR\$JOBID
cp send_task_msg.py \$TASKDIR\$JOBID
cp ParseTazerOutput.py \$TASKDIR\$JOBID
cd \$TASKDIR\$JOBID

loop=\`python send_task_msg.py "6=\$MY_HOSTNAME=\$JOBID"\`
while [ "\$loop" == "0" ] ; do
    start=\$(date +%s)
    end=\$(date +%s)
    tdiff=\$(( end - start ))
    twait=\`shuf -i 10-30 -n 1\`
    while [ \$tdiff -le \$twait ]; do
        end=\$(date +%s)
        tdiff=\$(( end - start ))
        #echo "\$start \$end \$tdiff"
    done
    echo  "checking to start..."
    loop=\`python send_task_msg.py "6=\$MY_HOSTNAME=\$JOBID"\`
done;

N=1
expName="tazer_$iorate_$tpf"
taskType="tazer"
ioType="tazer"

t=\$(date +%s)
var_names="StartTime" && var_vals="\${t}" && var_times="\${t}"
var_names="\${var_names},N" && var_vals="\${var_vals},\${N}" && var_times="\${var_times},\${t}"
var_names="\${var_names},ExpName" && var_vals="\${var_vals},\${expName}" && var_times="\${var_times},\${t}"
var_names="\${var_names},TaskType" && var_vals="\${var_vals},\${taskType}" && var_times="\${var_times},\${t}"
var_names="\${var_names},Host" && var_vals="\${var_vals},\${MY_HOSTNAME}" && var_times="\${var_times},\${t}"
var_names="\${var_names},IOType" && var_vals="\${var_vals},\${ioType}" && var_times="\${var_times},\${t}"
var_names="\${var_names},Slot" && var_vals="\${var_vals},\${SLOT}" && var_times="\${var_times},\${t}"

WORKDIR=\`pwd\`
echo "### Working directory on ###"
echo \$WORKDIR

module load gcc/8.1.0 
module load python/anaconda3.2019.3 

echo "### Running on ###"
hostname
tazer_lib=\${TAZER_PATH}/libclient.so 
out_dir=./
data_dir=./
mkdir -p \$out_dir
# mkdir -p \$data_dir

t=\$(date +%s)
var_names="\${var_names},StartSetup" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"

echo "### Creating tazer meta files"


compression=0
blocksize=\$(( 1024*1024 )) #16777216

# fnum=\`shuf -i1-${num_files} -n1\` #for 2:1 task to file ratio
# fnum=$fnum
fnum=\$(( \${MYID} / ${tpf} ))
infile="belle2_data/tazer_data/tazer8GB_\${fnum}.dat"
t=\$(date +%s)
var_names="\${var_names},InputDataSet" && var_vals="\${var_vals},\${fnum}" && var_times="\${var_times},\${t}"

if [ "${use_local_server}" == "0" ]; then
    server="130.20.68.151"  #this is the IP address for blueskys head node (which we have a TAZER server running on, or we have an ssh tunnel forwarding ports to a remote TAZER server)
    echo "\${server}:5101:\${compression}:0:0:\${blocksize}:\${infile}|\${server}:5201:\${compression}:0:0:\${blocksize}:\${infile}|\${server}:5301:\${compression}:0:0:\${blocksize}:\${infile}|\${server}:5401:\${compression}:0:0:\${blocksize}:\${infile}|" | tee \$data_dir/\tazer8GB.dat.\${fnum}.meta.in
else
    server_list=""
    if [ "${use_local_server}" == "1" ] || ["${use_local_server}" == "3" ]; then
        for server in `python ParseSlurmNodelist.py $tazer_servers`; do
            server_list="\${server_list}\${server}:5001:\${compression}:0:0:\${blocksize}:\${infile}|"
        done
    else
        for server in `python ParseSlurmNodelist.py $tazer_servers`; do
            server_list="\${server_list}\${server}.ibnet:5001:\${compression}:0:0:\${blocksize}:\${infile}|\${server}:5001:\${compression}:0:0:\${blocksize}:\${infile}|"
        done
    fi
    echo "\${server_list}" | tee \$data_dir/\tazer8GB.dat.\${fnum}.meta.in
fi


out_port=4
#echo "130.20.68.151:5\${out_port}01:\${compression}:0:0:\${blocksize}:output/\${JOBID}.root|" | tee tazer_output.dat.meta.out

for server in `python ParseSlurmNodelist.py $tazer_servers`; do
    echo "\${server}:5001:\${compression}:0:0:\${blocksize}:output/\${JOBID}.root|" | tee tazer_output.dat.meta.out
    break
done

echo "### Copying input files ###"
t=\$(date +%s)
var_names="\${var_names},StartInputTx" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"
t=\$(date +%s)
var_names="\${var_names},StopInputTx" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"

outfile=workload_sim_\${JOBID}.txt

t=\$(date +%s)
var_names="\${var_names},StartExp" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"

TAZER_SHARED_MEM_CACHE_SIZE=\$(( 16*1024*1024*1024 ))
TAZER_BB_CACHE_SIZE=\$(( 195*1024*1024*1024 ))
TAZER_BOUNDED_FILELOCK_CACHE_SIZE=\$(( 512*1024*1024*1024 ))

echo "sizes: \${TAZER_SHARED_MEM_CACHE_SIZE} \${TAZER_BB_CACHE_SIZE} \${TAZER_BOUNDED_FILELOCK_CACHE_SIZE}"


time TAZER_PREFETCH=0 \
TAZER_SHARED_MEM_CACHE=1 TAZER_SHARED_MEM_CACHE_SIZE=\${TAZER_SHARED_MEM_CACHE_SIZE} \
TAZER_BB_CACHE=1 TAZER_BB_CACHE_SIZE=\${TAZER_BB_CACHE_SIZE} \
TAZER_BOUNDED_FILELOCK_CACHE=${use_bounded_filelock} TAZER_BOUNDED_FILELOCK_CACHE_SIZE=\${TAZER_BOUNDED_FILELOCK_CACHE_SIZE} \
LD_PRELOAD=\${preload}:\${tazer_lib} ${scriptDir}/workloadSim -f ${scriptDir}/${iorate}_${tpf}_accesses.txt -i ${iointensity} -m ".\${fnum}.meta.in" -t ${timelimit} >& \${out_dir}\${outfile}

t=\$(date +%s)
var_names="\${var_names},StopExp" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"

parsed=\`python ParseTazerOutput.py \${out_dir}\${outfile}\`
tmp_names=\`echo "\$parsed" | grep -oP '(?<=labels:).*'\` 
tmp_vals=\`echo "\$parsed" | grep -oP '(?<=vals:).*'\` 
var_names="\${var_names},\${tmp_names}" && var_vals="\${var_vals},\${tmp_vals}"

#rm -r \$data_dir

t=\$(date +%s)
var_names="\${var_names},FinishedTime" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"
python send_task_msg.py "2=\$MY_HOSTNAME=\$JOBID=\$JOBID;\$var_names;\$var_vals"
python send_task_msg.py "1=\$MY_HOSTNAME=\$JOBID"
wait
EOT

# done

echo "creating task list "
touch CurTasks.dat
for i in `seq 0 $(( numTasks-1 ))`; do
# echo "${iorate}_${tpf}_$(( i/tpf )).sh" >> CurTasks.dat
echo "${iorate}_${tpf}.sh" >> CurTasks.dat

# for i in `seq 1 ${cores_per_node}`; do
# echo "${iorate}_${tpf}.sh" >> CurTasks.dat
done
module unload python/anaconda3.2019.3
