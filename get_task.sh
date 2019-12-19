#!/bin/bash
#SBATCH --exclusive 
#SBATCH -N 1 
#SBATCH --time=2880



./gettask $1 &
wait