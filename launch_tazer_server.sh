#!/bin/bash
mode=$1
data_dir=$2
echo "tazer args $mode $data_dir"
srun ./tazer_server.sh ${mode} ${data_dir}