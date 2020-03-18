all: WorkloadSim

WorkloadSim: src/WorkloadSim.cpp
	g++ -O3 -g src/WorkloadSim.cpp -o workloadSim
