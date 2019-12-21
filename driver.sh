#!/bin/bash
module purge
module load gcc/8.1.0
#module unload python/anaconda3.2019.3

#---------------CHANGE ME!!!!---------------------
# this should point to the install dir for tazer
export TAZER_ROOT=${HOME}
#-------------------------------------------------

#------- find directory this script is located----------

# SOURCE="${BASH_SOURCE}"
# dirname "${BASH_SOURCE}"
# echo "S: ${SOURCE}"
# while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
#     DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
#     echo "D: ${DIR}"
#     SOURCE="$(readlink "$SOURCE")"
#     [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
#     echo "S: ${SOURCE}"
# done

SCRIPT_NAME=$(basename $(test -L "$0" && readlink "$0" || echo "$0"));
scripts_dir=$(cd $(dirname "$0") && pwd);
# scripts_dir="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
#------------------------------------------------------

#----------directory this script is executed---------------
exp_base_dir=`pwd`
#----------------------------------------------------------

echo "scripts_dir=${scripts_dir}"
echo "exp_base_dir=${exp_base_dir}"
echo "TAZER_ROOT=${TAZER_ROOT}"



# ------------- experiment parameters: -----------
cores_per_node=24
nodes=25
numRounds=1
use_ib=0
numTasks=$(( cores_per_node * nodes * numRounds ))
echo "Number of tasks = ${numTasks}"
#-------------------------------------------------

task_server_port=5555


# for ioratio in  1 2 4 8 16; do #with respect to 125MB/s
    # for tpf in 2 4 8 16; do
for ioratio in 1; do
    for tpf in 2; do
        rm -r /files0/${USER}/tazer_cache/ #cleanup from previous experiment
        mkdir $((ioratio*125))MBs_io_${tpf}_tpf 
        cd $((ioratio*125))MBs_io_${tpf}_tpf
        cp -r ${scripts_dir}/* .
        #--------------- build utils ---------------------
        make 
        #-------------------------------------------------
        chmod +x *.sh
        if [ "${use_ib}" == "1" ]; then
            echo "launching tazer servers"
            tazer_server_task_id=$(sbatch --exclude=node33 -N2 --parsable ./launch_ib_tazer.sh) #node33 infiniband is not working?
            tazer_server_nodes=`squeue -j ${tazer_server_task_id} -h -o "%N"`
            while [ -z "$tazer_server_nodes" ]; do
                sleep 1
                tazer_server_nodes=`squeue -j ${tazer_server_task_id} -h -o "%N"`
            done
        else
            tazer_server_nodes="bluesky" 
        fi
        

        echo "creating tasks"
        ./create_task.sh ${ioratio} ${tpf} ${numTasks} ${cores_per_node} ${nodes} ${tazer_server_nodes} ${use_ib}
        
        echo "launching task server"
        get_task_id=`sbatch --parsable ./get_task.sh ${task_server_port}`
        get_task_node=`squeue -j ${get_task_id} -h -o "%N"`
        while [ -z "$get_task_node" ]; do
        sleep 1
        get_task_node=`squeue -j ${get_task_id} -h -o "%N"`
        done
        sleep 15 #wait for get_task to start server to initialize...
        workers_task_id=`sbatch --exclude=node33 --parsable -d after:${get_task_id} -N ${nodes} ./launch_workers.sh ${get_task_node} ${task_server_port} ${cores_per_node} ${nodes} ${numRounds}`
        
        salloc -N1 -d afterany:${workers_task_id} scancel ${get_task_id}
	if [ "${use_ib}" == "1" ]; then
            echo "closing servers"
            for server in `python ParseSlurmNodelist.py $tazer_server_nodes`; do 
               echo "closing $server"
               CloseTazerServer ${server}.ibnet 5001
               CloseTazerServer $server 5002
            done
        fi
        echo "${get_task_id} ${get_task_node}"
        scancel ${get_task_id}
        ./create_plots.sh
        pkill tazer #hmmm this seems suspicious...
        cd ..
    done
done
