#include <atomic>
#include <chrono>
#include <condition_variable>
#include <experimental/filesystem>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <system_error>

#include <math.h>
#include <string.h>
#include <time.h>

#include <pthread.h>
#include <thread>

#include <arpa/inet.h>
#include <fcntl.h>
#include <iomanip>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <queue>
#include <regex>
#include <signal.h>
#include <sstream>
#include <stack>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "Connection.h"

std::string tasks_dir = "./tasks/";
std::string sleepTask = "sleep.sh";
std::string longSleepTask = "longSleep.sh";
std::string doneTask = "done.sh";

uint32_t hold = 1;
uint32_t scheduling = 0;

std::queue<std::string> curTasks;
std::unordered_map<std::string, uint32_t> curTasksNew;
std::unordered_map<std::string, uint32_t> allocTasks;

std::unordered_map<std::string, std::unordered_map<uint32_t, std::queue<std::string>>> nodeQueues;
std::atomic_int finishedTasks;
std::atomic_int failedTasks;
std::atomic_int tasks;
std::atomic_int allocatedTasks;

std::unordered_map<std::string, std::stack<uint32_t>> node_slots;
std::unordered_map<std::string, int32_t> node_cnts;
std::unordered_map<std::string, std::mutex *> node_locks;
std::unordered_map<std::string, std::unordered_set<std::string>> node_jobs;

std::unordered_map<std::string, std::unordered_map<std::string, std::pair<std::string, uint32_t>>> job_data;
std::unordered_map<std::string, std::unordered_map<std::string, std::chrono::time_point<std::chrono::system_clock>>> start_times;
std::unordered_map<std::string, std::unordered_map<std::string, int32_t>> failedTaskCnts;

std::mutex nMutex;
std::mutex tMutex;
std::mutex lMutex;
std::mutex dbMutex;
std::mutex fMutex;
std::mutex subMutex;
std::queue<std::string> pendingDbTasks;
std::unordered_map<std::string, std::string> failedTasksMap;

std::queue<int> work_queue_fds;
std::mutex wqMutex;
std::condition_variable wqCv;

uint64_t theids;

// 17 * 6 + 74 * 4 + 22 * 4 + 12 * 1 + 1 * 4
int32_t getMaxCnt(std::string node) {
    if (std::regex_match(node, std::regex(".*node52.*cluster.*"))) { //seapearl haswell 2
        return 32;                                                   // 2;
    }
    else if (std::regex_match(node, std::regex(".*node51.*cluster.*"))) { //seapearl haswell 1
        return 16;                                                        // 2;
    }
    else if (std::regex_match(node, std::regex(".*node(0[1-9]|1[0-9]|2[0-9]|3[0-2]|49|50).*cluster.*"))) { //seapearl ivy
        return 20;                                                                                         // 2;
    }
    else if (std::regex_match(node, std::regex(".*node(3[3-9]|4[0-8]).*cluster.*"))) { //seapearl amd
        return 4;                                                                      // 2;
    }
    else if (std::regex_match(node, std::regex(".*local.*"))) { //bluesky
        return 24;                                              // 2;
    }
    else {
        return 100;
    }
}

void printQueues() {
    std::stringstream ss;
    for (auto wn : nodeQueues) {
        int cnt = 0;
        for (auto qs : wn.second) {
            cnt += qs.second.size();
        }
        if (cnt > 0) {
            ss << wn.first << ":" << cnt << " ";
        }
    }
    ss << std::endl;
    std::cout << ss.str();
}

void getNewTask(Connection *connection, std::string worker_node, std::string jobid, std::string data, bool sendFile) {
    if (!hold) {
        std::cout << "new task: " << jobid << " " << worker_node << " " << node_cnts[worker_node] << " " << getMaxCnt(worker_node) << std::endl;
        std::cout << " " << node_slots[worker_node].size() << " " << tasks << " " << finishedTasks << " " << failedTasks << std::endl;
        std::cout << data << std::endl;
    }

    std::string task = "";
    std::string origTask = "";
    uint32_t slot = 0;
    bool empty = true;
    for (auto q : nodeQueues[worker_node]) {
        if (!q.second.empty()) {
            empty = false;
            break;
        }
    }
    if (curTasks.empty() && empty) {
        //printQueues();
        if (tasks > 0) {
            task = longSleepTask;
            origTask = longSleepTask;
        }
        else {
            task = doneTask;
            origTask = doneTask;
        }
    }
    else {
        if (hold == 1) {
            task = sleepTask;
            origTask = sleepTask;
        }
        else {
            std::unique_lock<std::mutex> nlock(*(node_locks[worker_node]));
            int32_t cur_cnt = node_cnts[worker_node];
            int32_t max_cnt = getMaxCnt(worker_node);
            if (cur_cnt < max_cnt && !node_slots[worker_node].empty()) {
                if (node_slots[worker_node].empty()) {
                    std::cout << worker_node << " dont think i should be here... " << cur_cnt << " " << max_cnt << std::endl;
                }
                slot = node_slots[worker_node].top();

                if (nodeQueues[worker_node][slot].empty()) { //no task in local queue, check global queue
                    //printQueues();
                    std::unique_lock<std::mutex> tlock(tMutex);
                    if (curTasks.empty()) {
                        if (tasks > 0) {
                            task = longSleepTask;
                            origTask = longSleepTask;
                        }
                        else {
                            task = doneTask;
                            origTask = doneTask;
                        }
                    }
                    else {
                        node_slots[worker_node].pop();
                        if (!scheduling) {
                            origTask = curTasks.front();
                            task = curTasks.front() + "-" + std::to_string(slot);
                            curTasks.pop();
                        }
                        else {
                            double min = 1.1;
                            double semiMin = 1.1;
                            std::string bestTask = "";
                            std::string semiBest = "";
                            for (auto task : curTasksNew) {
                                double perc = (double)curTasksNew[task.first] / (double)tasks;
                                double allocPerc = 0;
                                if (allocatedTasks > 0) {
                                    allocPerc = (double)allocTasks[task.first] / (double)allocatedTasks;
                                }
                                std::cout << task.first << " (" << curTasksNew[task.first] << "," << tasks << ") " << perc << " (" << allocTasks[task.first] << "," << allocatedTasks << ")" << allocPerc << std::endl;
                                if (allocPerc <= perc && allocPerc < min) {
                                    bestTask = task.first;
                                    min = allocPerc;
                                }
                                else {
                                    if (fabs(allocPerc - perc) < semiMin) {
                                        semiBest = task.first;
                                        min = fabs(allocPerc - perc);
                                    }
                                }
                            }
                            if (bestTask == "") {
                                bestTask = semiBest;
                            }
                            origTask = bestTask;
                            task = bestTask + "-" + std::to_string(slot);
                            curTasksNew[bestTask]--;
                            allocTasks[bestTask]++;
                            allocatedTasks++;
                        }
                        node_cnts[worker_node] += 1;
                        node_jobs[worker_node].emplace(jobid);
                        job_data[worker_node][jobid] = std::make_pair(origTask, slot);
                        start_times[worker_node][jobid] = std::chrono::system_clock::now();
                        tasks--;
                    }
                    tlock.unlock();
                }
                else {
                    node_slots[worker_node].pop();
                    origTask = nodeQueues[worker_node][slot].front();
                    task = nodeQueues[worker_node][slot].front() + "-" + std::to_string(slot);
                    nodeQueues[worker_node][slot].pop();
                    node_cnts[worker_node] += 1;
                    node_jobs[worker_node].emplace(jobid);
                    job_data[worker_node][jobid] = std::make_pair(origTask, slot);
                    start_times[worker_node][jobid] = std::chrono::system_clock::now();
                    tasks--;
                }
            }
            else if (false) { //check if any jobs are registered but not running on node anymore
                if (hold) {
                    std::cout << worker_node << " full" << std::endl;
                }
                else {
                    std::vector<std::string> jobs_to_remove;
                    auto temp_t = std::chrono::system_clock::now();
                    //todo: need to check if number of jobs on a node match the node_cnt and available slots...
                    if (!node_jobs[worker_node].empty()) {
                        for (auto job : node_jobs[worker_node]) {
                            std::chrono::duration<double> temp_e = temp_t - start_times[worker_node][job];
                            if (temp_e.count() > 100000000 && false) {
                                if (data.find(job) == std::string::npos) { //a job must have failed on the node
                                    if (job_data[worker_node].count(job) > 0) {
                                        std::unique_lock<std::mutex> flock(tMutex);
                                        if (failedTaskCnts[worker_node].count(job) == 0) {
                                            failedTaskCnts[worker_node][job] = 0;
                                        }

                                        failedTaskCnts[worker_node][job]++;
                                        if (failedTaskCnts[worker_node][job] > max_cnt * 3) {
                                            std::cout << "possibly stalled: " << job << " " << temp_e.count() << " " << failedTaskCnts[worker_node][job] << std::endl;
                                            auto data = job_data[worker_node][job];
                                            std::string failed_task = job + " " + worker_node + " " + std::to_string(data.second) + " " + data.first;
                                            allocTasks[data.first]--;
                                            allocatedTasks--;
                                            node_slots[worker_node].push(data.second);
                                            node_cnts[worker_node]--;
                                            if (node_cnts[worker_node] < 0) {
                                                node_cnts[worker_node] = 0;
                                            }
                                            failedTasks++;
                                            failedTasksMap[job] = failed_task;
                                            start_times[worker_node].erase(job);
                                            jobs_to_remove.push_back(job);
                                        }
                                        flock.unlock();
                                    }
                                    else {
                                        jobs_to_remove.push_back(job);
                                    }
                                }
                            }
                        }
                        for (auto job : jobs_to_remove) {
                            std::cout << "erase: " << job << " " << node_jobs.size() << " " << job_data[worker_node].size() << std::endl;
                            node_jobs[worker_node].erase(job);
                            job_data[worker_node].erase(job);
                            start_times[worker_node].erase(job);
                            failedTaskCnts[worker_node].erase(job);
                        }
                    }
                    else {
                        std::cout << worker_node << " mismatch between count of jobs on node and active jobs" << std::endl;
                        while (!node_slots[worker_node].empty()) {
                            node_slots[worker_node].pop();
                        }
                        for (int i = getMaxCnt(worker_node); i >= 1; i--) {
                            node_slots[worker_node].push(i);
                        }
                        node_cnts.emplace(worker_node, 0);
                    }
                }
                task = sleepTask;
                origTask = sleepTask;
            }
            else {
                task = sleepTask;
                origTask = sleepTask;
            }
            nlock.unlock();
        }
    }

    if (!sendFile) {
        if (!hold) {
            std::cout << "sending: " << task << std::endl;
        }
        connection->sendMsg(task, 3);
    }
    else {
        std::ifstream taskFile;
        std::string line;
        std::stringstream task_ss;
        taskFile.open(tasks_dir + origTask);

        std::getline(taskFile, line);
        task_ss << line << std::endl;
        task_ss << "SLOT=" << slot << std::endl;
        task_ss << "MYID=" <<theids++<<std::endl;
        while (std::getline(taskFile, line)) {
            task_ss << line << std::endl;
        }
        taskFile.close();
        if (!hold) {
            std::cout << "sending: " << tasks_dir << origTask << std::endl;
        }
        connection->sendMsg(task_ss.str(), 3);
    }
    int total = 0;
    for (auto cnt : node_cnts) {
        total += cnt.second;
    }
    std::cout << "total allocated: " << total << std::endl;
}

void finishTask(Connection *connection, std::string worker_node, std::string jobid, std::string data) {
    uint32_t slot = 0;

    std::unique_lock<std::mutex> nlock(*(node_locks[worker_node]));
    if (job_data[worker_node].count(jobid) > 0) {
        allocTasks[job_data[worker_node][jobid].first]--;
        allocatedTasks--;
        slot = job_data[worker_node][jobid].second;
        node_cnts[worker_node]--;
        if (node_cnts[worker_node] < 0) {
            node_cnts[worker_node] = 0;
        }
        node_slots[worker_node].push(slot);
        node_jobs[worker_node].erase(jobid);
        job_data[worker_node].erase(jobid);
        start_times[worker_node].erase(jobid);
        failedTaskCnts[worker_node].erase(jobid);
        finishedTasks++;
    }
    else {
        finishedTasks++;
        failedTasks--; //reported as having failed
        if (failedTasks < 0) {
            failedTasks = 0;
        }
        std::unique_lock<std::mutex> flock(tMutex);
        failedTasksMap.erase(jobid);
        flock.unlock();
    }
    connection->sendMsg(doneTask, 3);
    nlock.unlock();
    if (tasks % 100 == 0) {
        std::unique_lock<std::mutex> flock(tMutex);
        std::ofstream logFile;
        logFile.open("logs/failed_tasks_" + std::to_string(tasks) + ".txt", std::ofstream::out);
        for (auto task : failedTasksMap) {
            logFile << task.second << std::endl;
        }
        logFile.close();
        flock.unlock();
    }
    bool empty = true;
    for (auto q : nodeQueues[worker_node]) {
        if (!q.second.empty()) {
            empty = false;
            break;
        }
    }
    std::cout << "finished task: " << worker_node << " " << slot << " " << node_cnts[worker_node] << " " << getMaxCnt(worker_node) << " " << node_slots[worker_node].size() << " " << tasks << " " << finishedTasks << " " << failedTasks << std::endl;
}

void addTask(Connection *connection, std::string worker_node, std::string task, std::string data) {
    if (worker_node.compare("all") == 0) {

        int count;
        std::unique_lock<std::mutex> tlock(tMutex);
        if (curTasksNew.count(task) == 0) {
            curTasksNew[task] = 0;
        }
        if (allocTasks.count(task) == 0) {
            allocTasks[task] = 0;
        }

        curTasksNew[task]++;
        curTasks.push(task);
        tlock.unlock();
        std::cout << "adding: " << task << " to global queue" << tasks + 1 << " jobs (" << curTasksNew[task] << ")" << std::endl;
    }
    else {
        uint32_t slot = atoi(data.c_str());
        std::unique_lock<std::mutex> nlock(*node_locks[worker_node]);
        nodeQueues[worker_node][slot].push(task);
        std::cout << "adding: " << task << " to " << worker_node << " " << slot << " queue " << nodeQueues[worker_node][slot].size() << " jobs" << std::endl;
        nlock.unlock();
    }
    tasks++;
}

//there is a race condition here, when triggered will result in more calls to dirac get_task, it does not
//result in a slot being allocated to multiple tasks simultaneously.
void checkSlotAvail(Connection *connection, std::string worker_node, std::string jobid, std::string data) {
    int32_t cur_cnt = node_cnts[worker_node];
    int32_t max_cnt = getMaxCnt(worker_node);
    if (cur_cnt < max_cnt) {
        connection->sendMsg("1", 3);
    }
    else {
        connection->sendMsg("0", 3);
    }
}

void saveData(Connection *connection, std::string worker_node, std::string jobid, std::string data) {
    std::unique_lock<std::mutex> dblock(dbMutex);
    pendingDbTasks.push(data);
    dblock.unlock();
    //std::cout << "storing data: " << worker_node << jobid << std::endl;
    connection->sendMsg(doneTask, 3);
    std::unique_lock<std::mutex> nlock(*(node_locks[worker_node]));
    failedTaskCnts[worker_node][jobid] = 0;
    nlock.unlock();
}

void setHold(Connection *connection, std::string worker_node, std::string jobid, std::string data) {
    hold = atoi(data.c_str());
    connection->sendMsg(doneTask, 3);
}

void checkHold(Connection *connection, std::string worker_node, std::string jobid, std::string data) {
    if (hold == 1) {
        //std::cout << worker_node << " " << jobid << " holding" << std::endl;
        connection->sendMsg("0", 3);
    }
    else {
        connection->sendMsg("1", 3);
    }
}

void handleConnection(Connection *connection) {
    auto start = std::chrono::system_clock::now();
    uint32_t size;
    connection->recvMsg(&size, sizeof(size), 3);
    // std::cout << size << std::endl;
    try {
        if (size > 10 && size < 10000000) { // filter out illformed messages
            if (!hold) {
                std::cout << curTasks.size() << " " << hold << " " << connection << std::dec << " " << size << std::endl;
            }
            char *buf = new char[size + 1];
            connection->recvMsg(buf, size, 3);
            buf[size] = 0;
            std::stringstream ss(buf);
            //std::cout << ss.str() << std::endl;
            uint32_t type = 1000;
            uint32_t magic = 10000;
            //uint32_t slot;
            std::string jobid;
            std::string worker_node;
            std::string data;

            //ss >> type >> magic >> worker_node >> slot >> jobid;
            ss >> type >> magic >> worker_node >> jobid;
            getline(ss, data);
            std::cout << type << " " << magic << " wn: " << worker_node << " " << jobid << " " << data << std::endl;
            std::unique_lock<std::mutex> lock(nMutex);
            if (node_locks.count(worker_node) == 0) {
                node_locks.emplace(worker_node, new std::mutex());
                node_slots.emplace(worker_node, std::stack<uint32_t>());
                nodeQueues.emplace(worker_node, std::unordered_map<uint32_t, std::queue<std::string>>());
                for (int i = getMaxCnt(worker_node); i >= 1; i--) {
                    node_slots[worker_node].push(i);
                    nodeQueues[worker_node].emplace(i, std::queue<std::string>());
                }
                node_cnts.emplace(worker_node, 0);
            }
            lock.unlock();

            if (magic == (uint32_t)MAGIC) {

                if (type == 0) { //request new task
                    getNewTask(connection, worker_node, jobid, data, false);
                }
                else if (type == 1) { //finished task
                    finishTask(connection, worker_node, jobid, data);
                }
                else if (type == 2) { //store data
                    saveData(connection, worker_node, jobid, data);
                }
                else if (type == 3) {
                    checkSlotAvail(connection, worker_node, jobid, data);
                }
                else if (type == 4) { //update hold value
                    setHold(connection, worker_node, jobid, data);
                }
                else if (type == 5) { //add task
                    addTask(connection, worker_node, jobid, data);
                }
                else if (type == 6) { //check if execution can start
                    checkHold(connection, worker_node, jobid, data);
                }
                else if (type == 7) {
                    getNewTask(connection, worker_node, jobid, data, true);
                }
            }

            // std::cout << "exiting" << std::endl;
            auto end = std::chrono::system_clock::now();
            std::chrono::duration<double> elapsed_seconds = end - start;
            std::unique_lock<std::mutex>(lMutex);
            std::ofstream logFile;
            logFile.open("logs/times2.txt", std::ofstream::out | std::ofstream::app);
            logFile << worker_node << " " << elapsed_seconds.count() << std::endl;
            logFile.close();
            delete[] buf;
        }
    } catch (const std::system_error &e) {
        std::cout << "Handle connection Error: " << e.code() << " " << e.code().message() << " " << e.what() << std::endl;
    }
    // std::cout << "here" << std::endl;

    delete connection;
}

void processDbRequests() {
    while (true) {
        if (pendingDbTasks.empty()) {
            sleep(1);
        }
        else {
            try {
                std::cout << "processDbRequests: " << pendingDbTasks.size() << std::endl;
                std::ofstream dbfile;
                dbfile.open("logs/data.txt", std::ofstream::out | std::ofstream::app);
                std::unique_lock<std::mutex> dblock(dbMutex);
                std::stringstream data;
                int cnt = 0;
                while (!pendingDbTasks.empty()) {
                    dbfile << pendingDbTasks.front() << std::endl;
                    data << pendingDbTasks.front() << "|";
                    pendingDbTasks.pop();
                    cnt++;
                }
                dblock.unlock();
                dbfile.close();

                //std::system(cmd.c_str());
                //std::cout<<cmd<<std::endl;
            } catch (const std::system_error &e) { //should specifically make this catch resource unavailable
                std::cout << "server caught error..." << e.code() << ": " << e.what() << std::endl;
            }
        }
    }
}

int thread_cnt = 0;
void *helperThread() {

    while (true) {
        int fd = -1;
        // if (!hold) {
        // std::cout << "work_queue_size: " << work_queue_fds.size() << std::endl;
        // }
        std::unique_lock<std::mutex> wqLock(wqMutex);
        if (work_queue_fds.empty()) {
            wqCv.wait(wqLock, [] { return !work_queue_fds.empty(); });
        }
        fd = work_queue_fds.front();
        work_queue_fds.pop();
        wqLock.unlock();
        if (fd > 0) {
            Connection *connection = new Connection(thread_cnt++, fd);
            // std::cout << "handling connection: " << fd << std::endl;
            handleConnection(connection);
            // std::cout << "done with connection" << std::endl;
        }
    }
}

void listenOnPort(int portno) {
    struct sockaddr_in serv_addr, cli_addr;
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("ERROR opening socket");
        exit(1);
    }

    int enable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0) {
        perror("setsockopt(SO_REUSEADDR) failed");
    }
    bzero((char *)&serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;

    serv_addr.sin_port = htons(portno);
    if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("ERROR on binding");
        exit(1);
    }

    signal(SIGCHLD, SIG_IGN); //hack for now, possibly implement a child handler?
    listen(sockfd, 128);
    while (1) {
        socklen_t clilen;
        clilen = sizeof(cli_addr);
        int newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
        // std::cout << "accepted on port: " << portno << std::endl;
        if (newsockfd != -1) {
            std::unique_lock<std::mutex> wqLock(wqMutex);
            work_queue_fds.push(newsockfd);
            wqLock.unlock();
            wqCv.notify_all();
        }
    }
}

int main(int argc, char *argv[]) {
    std::cout << "Starting server" << std::endl;
    finishedTasks = 0;
    failedTasks = 0;
    tasks = 0;
    allocatedTasks = 0;
    theids=0;

    int numHelperThreads = 10;

    int startPort = 8834;

    if (argc >= 2){
        startPort = atoi(argv[1]);
    }
    int numPorts = 3;

    if (std::experimental::filesystem::exists("logs")) {
        auto now = std::chrono::system_clock::now();
        auto in_time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << "logs_" << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d_%H:%M:%S");
        std::experimental::filesystem::rename("logs", ss.str());
    }
    std::experimental::filesystem::create_directory("logs");

    std::thread dbthread(processDbRequests);
    dbthread.detach();

    for (int i = 0; i < numHelperThreads; i++) {
        std::thread ht(helperThread);
        ht.detach();
    }

    std::vector<std::thread> threads;
    for (int i = 0; i < numPorts; i++) {
        threads.push_back(std::thread(listenOnPort, startPort + i));
    }

    for (int i = 0; i < threads.size(); i++) {
        threads[i].join();
    }

    return 0;
}
