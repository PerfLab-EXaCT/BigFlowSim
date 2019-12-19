#!/bin/bash
#SBATCH --exclusive 
#SBATCH --time=2880

./sub.sh $1 $2 

sleep 10
srun pilot.sh $1 $2 $3
sleep 30

