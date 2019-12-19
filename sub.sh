#!/bin/bash
module unload python/anaconda3.2019.3
python ./load_task_queues.py CurTasks.dat $1 $2
python clearHold.py $1 $2 
# module unload python/anaconda3.2019.3




