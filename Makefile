all: GetTask WorkloadSim

GetTask: src/GetTask.cpp src/Connection.cpp inc/Connection.h
	g++ -O3 -g --std=c++14 src/GetTask.cpp src/Connection.cpp -Iinc/ -lpthread -lstdc++fs -o gettask 

WorkloadSim: src/WorkloadSim.cpp
	g++ -O3 -g src/WorkloadSim.cpp -o workloadSim
