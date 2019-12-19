#include <algorithm>
#include <chrono>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

const double billion = 1000000000.0;

uint64_t getCurrentTime() {
    auto now = std::chrono::high_resolution_clock::now();
    auto now_ms = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    auto value = now_ms.time_since_epoch();
    uint64_t ret = value.count();
    return ret;
}

char *getStringArg(char **argBegin, char **argEnd, const std::string &option, const std::string &optionFull, const std::string &def) {
    char **it = std::find(argBegin, argEnd, option);
    if (it != argEnd && ++it != argEnd) {
        return *it;
    }
    it = std::find(argBegin, argEnd, optionFull);
    if (it != argEnd && ++it != argEnd) {
        return *it;
    }

    return (char *)def.c_str();
}

int64_t getIntArg(char **argBegin, char **argEnd, const std::string &option, const std::string &optionFull, int64_t def) {
    char **it = std::find(argBegin, argEnd, option);
    if (it != argEnd && ++it != argEnd) {
        return atoll(*it);
    }
    it = std::find(argBegin, argEnd, optionFull);
    if (it != argEnd && ++it != argEnd) {
        return atoll(*it);
    }
    return def;
}

double getDoubleArg(char **argBegin, char **argEnd, const std::string &option, const std::string &optionFull, double def) {
    char **it = std::find(argBegin, argEnd, option);
    if (it != argEnd && ++it != argEnd) {
        return atof(*it);
    }
    it = std::find(argBegin, argEnd, optionFull);
    if (it != argEnd && ++it != argEnd) {
        return atof(*it);
    }
    return def;
}

void loadData(std::string file, std::vector<std::string> &files, std::vector<uint64_t> &offsets, std::vector<uint64_t> &counts, uint64_t &largest) {
    std::vector<std::vector<std::string>> data;
    std::ifstream in(file);
    std::string line;

    std::string f;
    uint64_t offset;
    uint64_t count;
    uint64_t dummy1, dummy2;
    std::string curFile = "";
    int index = 0;
    while (std::getline(in, line)) {
        std::stringstream ss(line);
        ss >> f >> offset >> count >> dummy1 >> dummy2;
        files.push_back(f);
        offsets.push_back(offset);
        counts.push_back(count);
        if (count > largest) {
            largest = count;
        }
    }
}

std::tuple<double, double, double, double, double> executeTrace(std::vector<std::string> &files, std::string inFileSuffix, std::vector<uint64_t> &offsets, std::vector<uint64_t> &counts, double ioIntensity, double timeVar, uint64_t largest) {
    auto tempStart = getCurrentTime();
    double sumCpuTime = 0;
    double actualCpuTime = 0;
    double ioTime = 0;
    double closeTime = 0;
    double openTime = 0;

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(-1.0, 1.0);

    uint64_t numAccesses = files.size();
    int div = numAccesses / 10;
    char *buf = new char[largest];
    int fd = 0;
    std::string curFile = "";
    bool output = false;
    for (int i = 0; i < numAccesses; i++) {
        auto start = getCurrentTime();
        if (files[i] != curFile) {
            if (fd) {
                close(fd);
                closeTime += getCurrentTime() - start;
            }
            if (files[i].find("output") != std::string::npos) {
                fd = open((files[i] + ".meta.out").c_str(), O_WRONLY);
                output = true;
                std::cout << files[i] + ".meta.out" << std::endl;
                openTime += getCurrentTime() - start;
            }
            else {
                fd = open((files[i] + inFileSuffix).c_str(), O_RDONLY);
                output = false;
                std::cout << files[i] + inFileSuffix << std::endl;
                openTime += getCurrentTime() - start;
            }
            curFile = files[i];
        }
        if (output) {
            write(fd, buf, counts[i]);
        }
        else {
            lseek(fd, offsets[i], SEEK_SET);
            read(fd, buf, counts[i]);
        }
        ioTime += getCurrentTime() - start;
        double cpu = 0.0;
        uint64_t cput = 0;
        if (!output) {
            cpu = ((double)counts[i] / 1000000.0) / ioIntensity;
            cpu += cpu * dis(gen) * timeVar;
            cput = cpu * billion;
        }
        start = getCurrentTime();
        while (getCurrentTime() - start < cput) {
            std::this_thread::yield();
        }
        // std::this_thread::sleep_for(std::chrono::duration<double>(cpu * .7));
        sumCpuTime += cpu;
        actualCpuTime += getCurrentTime() - start;

        if (i % div == 0) {
            std::cout << (i / (double)numAccesses) * 100 << "% sim: " << sumCpuTime << "s actual: " << actualCpuTime / billion << " io: " << ioTime / billion << " open: " << openTime / billion << " close: " << closeTime / billion << " " << (getCurrentTime() - tempStart) / billion << std::endl;
        }
    }
    auto start = getCurrentTime();
    if (fd) {
        close(fd);
        closeTime += getCurrentTime() - start;
    }
    delete buf;
    return std::make_tuple(sumCpuTime, ioTime / billion, actualCpuTime / billion, openTime / billion, closeTime / billion);
}

int main(int argc, char *argv[]) {

    std::string dataFile = getStringArg(argv, argv + argc, "-f", "--infile", "access_new_in_out.txt");
    std::string inFileSuffix = getStringArg(argv, argv + argc, "-m", "--inmetasuffix", ".meta.in");
    double ioIntensity = getDoubleArg(argv, argv + argc, "-i", "--iointensity", 1000.0);
    double timeVar = 0.05;

    double cpuTime = 0;
    double ioOpenTime = 0;
    double ioReadTime = 0;
    double ioCloseTime = 0;
    double actualCpuTime = 0.0;

    uint64_t startTime = getCurrentTime();
    std::vector<std::string> files;
    std::vector<uint64_t> offsets;
    std::vector<uint64_t> counts;
    uint64_t largest = 0;
    loadData(dataFile, files, offsets, counts, largest);
    uint64_t setupTime = getCurrentTime() - startTime;
    uint64_t simTime = getCurrentTime();
    auto vals = executeTrace(files, inFileSuffix, offsets, counts, ioIntensity, timeVar, largest);
    simTime = getCurrentTime() - simTime;
    uint64_t totTime = getCurrentTime() - startTime;
    std::cout << "Setup time: " << setupTime / billion << std::endl;
    std::cout << "Total sim time: " << simTime / billion << std::endl;
    std::cout << "sim cpu time: " << std::get<0>(vals) << std::endl;
    std::cout << "actual cpu time: " << std::get<2>(vals) << std::endl;
    std::cout << "io time: " << std::get<1>(vals) << std::endl;
    std::cout << "open time: " << std::get<3>(vals) << std::endl;
    std::cout << "close time: " << std::get<4>(vals) << std::endl;
    std::cout << "fulltime: " << totTime / billion << std::endl;
}