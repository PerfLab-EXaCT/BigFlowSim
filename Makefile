all: WorkloadSim

WorkloadSim: src/WorkloadSim.cpp src/xxhash.c
	g++ -O3 -g -Iinc/ src/WorkloadSim.cpp src/xxhash.c -o workloadSim
