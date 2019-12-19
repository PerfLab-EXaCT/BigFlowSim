#!/bin/bash
module load python/anaconda3.2019.3
python plot_core_times.py
python plot_latencies.py
module unload python/anaconda3.2019.3