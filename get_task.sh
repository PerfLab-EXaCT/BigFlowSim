#!/bin/bash
#SBATCH --exclusive 
#SBATCH -N 1 
#SBATCH --time=2880


module purge
module load gcc/8.1.0

./gettask $1 &
wait