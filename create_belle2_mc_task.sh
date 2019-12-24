#/bin/bash
module load python/anaconda3.2019.3
scriptDir=`pwd`


ioratio=$1
iorate=$(( 125*ioratio ))

tpf=$2
numTasks=$3
cores_per_node=$4
nodes=$5
numCores=$((cores_per_node * nodes))
iointensity=`python File_access_pattern_gen.py ${ioratio} ${tpf} ${numTasks} ${numCores} ${iorate}_${tpf}_accesses.txt`
echo "$iointensity"
num_files=$(( numTasks/tpf ))
# 1 -- 125 -- 0.041666666666666664 -- 2 -- 1500 3000
echo "${ioratio} -- ${iorate} -- ${iointensity} -- ${tpf} -- ${num_files} ${numTasks} ${numCores}"




tazer_servers=$6
use_ib=$7

if [ "${use_ib}" == "0" ]; then
    use_bounded_filelock=1
else
    use_bounded_filelock=0
fi

echo "use_ib: ${use_ib} use_bounded_filelock: ${use_bounded_filelock}"

cat <<EOT > tasks/mc_tazer.sh
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
expName="tazer"
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



echo "### Running on ###"
hostname
tazer_lib=\${TAZER_PATH}/libclient.so 
out_dir=./
data_dir=./data
mkdir -p \$out_dir
mkdir -p \$data_dir

echo "### Setup basf2 ###"
t=\$(date +%s)
var_names="\${var_names},StartSetup" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"
source /cvmfs/belle.cern.ch/sl7/tools/b2setup
cd /cvmfs/belle.cern.ch/sl7/releases/release-00-09-03/
b2setup
server="130.20.68.151" 

module load gcc/8.1.0  

cd \$WORKDIR
echo "### Working directory on ###"
echo \$WORKDIR
ls

echo "### Creating tazer meta files"


compression=0
blocksize=\$(( 1024*1024 )) #16777216

dset=\`shuf -i0-4 -n1\`
t=\$(date +%s)
var_names="\${var_names},InputDataSet" && var_vals="\${var_vals},\${dset}" && var_times="\${var_times},\${t}"
files="BHWide_ECL-phase3-optimized.root Coulomb_HER_ECL-phase3-optimized.root RBB_ECL-phase3-optimized.root Touschek_LER_ECL-phase3-optimized.root \
BHWideLargeAngle_ECL-phase3-optimized.root Coulomb_HER_PXD-phase3-optimized.root RBB_PXD-phase3-optimized.root Touschek_LER_PXD-phase3-optimized.root \
BHWideLargeAngle_PXD-phase3-optimized.root Coulomb_HER_usual-phase3-optimized.root RBB_usual-phase3-optimized.root Touschek_LER_usual-phase3-optimized.root \
BHWideLargeAngle_usual-phase3-optimized.root Coulomb_LER_ECL-phase3-optimized.root Touschek_HER_ECL-phase3-optimized.root twoPhoton_ECL-phase3-optimized.root \
BHWide_PXD-phase3-optimized.root Coulomb_LER_PXD-phase3-optimized.root Touschek_HER_PXD-phase3-optimized.root twoPhoton_PXD-phase3-optimized.root \
BHWide_usual-phase3-optimized.root Coulomb_LER_usual-phase3-optimized.root Touschek_HER_usual-phase3-optimized.root twoPhoton_usual-phase3-optimized.root"

file_paths="" 

for file in \$files; do
    echo "\${server}:5101:\${compression}:0:0:\${blocksize}:belle2_data/set\${dset}/\${file}|\${server}:5201:\${compression}:0:0:${blocksize}:belle2_data/set\${dset}/\${file}|\${server}:5301:\${compression}:0:0:\${blocksize}:belle2_data/set\${dset}/\${file}|\${server}:5401:\${compression}:0:0:\${blocksize}:belle2_data/set\${dset}/\${file}|" | tee \$data_dir/\${file}.meta.in
    #echo "172.17.205.100:5001:\${compression}:0:0:\${blocksize}:belle2_data/set\${dset}/\${file}|" | tee \$data_dir/\${file}.meta.in

    cat \$data_dir/\${file}.meta.in
    file_paths="\$file_paths\$data_dir/\${file}.meta.in "
done

out_port=`shuf -i3-4 -n1`
# out_port=4
echo "\${server}:5\${out_port}01:\${compression}:0:0:\${blocksize}:output/\${JOBID}.root|" | tee \${JOBID}.root.meta.out
file_paths="\$file_paths\${JOBID}.root.meta.out"

echo "### Copying input files ###"
t=\$(date +%s)
var_names="\${var_names},StartInputTx" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"
t=\$(date +%s)
var_names="\${var_names},StopInputTx" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"

echo "### Creating file ###"
cat >> basf2_job_\${JOBID}.py <<EOF
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Descriptor: B0_Kstar0nunubar

#############################################################
# Steering file for official MC production of signal samples
# with beam backgrounds (BGx1).
#
# January 2017 - Belle II Collaboration
#############################################################

from basf2 import *
from simulation import add_simulation
from reconstruction import add_reconstruction, add_mdst_output
from L1trigger import add_tsim
from ROOT import Belle2
import glob

# decay file
decfile = Belle2.FileSystem.findFile('decfiles/dec/1150020000.dec')

# background (collision) files
#bg = glob.glob('/group/belle2/users/jbennett/BG15th/phase3/set0/*.root') # if you run at KEKCC
#bg = glob.glob('./BG/*.root')
bg = glob.glob('\${data_dir}/*meta.in')

#: number of events to generate, can be overriden with -n
#num_events = 10000
num_events = 100
#: output filename, can be overriden with -o
# output_filename = "\${JOBID}.root.meta.out.tmp"
output_filename = "\${JOBID}.root"

# create path
main = create_path()

# specify number of events to be generated
main.add_module("EventInfoSetter", expList=1, runList=1, evtNumList=num_events)

# generate BBbar events
evtgeninput = register_module('EvtGenInput')
evtgeninput.param('userDECFile', decfile)
main.add_module(evtgeninput)

# detector simulation
add_simulation(main, bkgfiles=bg)

# remove the cache for background files to reduce memory
#set_module_parameters(main, "BeamBkgMixer", cacheSize=0)

# trigger simulation
add_tsim(main)

# reconstruction
add_reconstruction(main)

# Finally add mdst output
add_mdst_output(main, filename=output_filename, additionalBranches=['TRGGDLResults', 'KlIds', 'KLMClustersToKlIds'])

# process events and print call statistics
process(main)
print(statistics)

EOF

echo "## Running basf2 ##"

t=\$(date +%s)
var_names="\${var_names},StartExp" && var_vals="\${var_vals},\${t}" && var_times="\${var_times},\${t}"

outfile=basf2_local_test_\${JOBID}.txt 

TAZER_SHARED_MEM_CACHE_SIZE=\$(( 4*1024*1024*1024 ))
TAZER_BB_CACHE_SIZE=\$(( 8*1024*1024*1024 ))
TAZER_BOUNDED_FILELOCK_CACHE_SIZE=\$(( 64*1024*1024*1024 ))
export LANG=en_US
export LC_ALL=en_US

echo "sizes: \${TAZER_SHARED_MEM_CACHE_SIZE} \${TAZER_BB_CACHE_SIZE} \${TAZER_BOUNDED_FILELOCK_CACHE_SIZE}"


time TAZER_PREFETCH=0 \
TAZER_SHARED_MEM_CACHE=1 TAZER_SHARED_MEM_CACHE_SIZE=\${TAZER_SHARED_MEM_CACHE_SIZE} \
TAZER_BB_CACHE=1 TAZER_BB_CACHE_SIZE=\${TAZER_BB_CACHE_SIZE} \
TAZER_BOUNDED_FILELOCK_CACHE=${use_bounded_filelock} TAZER_BOUNDED_FILELOCK_CACHE_SIZE=\${TAZER_BOUNDED_FILELOCK_CACHE_SIZE} \
LD_PRELOAD=\${preload}:\${tazer_lib} basf2  basf2_job_\${JOBID}.py -l INFO >& \${out_dir}\${outfile}


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
echo "mc_tazer.sh" >> CurTasks.dat

# for i in `seq 1 ${cores_per_node}`; do
# echo "${iorate}_${tpf}.sh" >> CurTasks.dat
done
module unload python/anaconda3.2019.3