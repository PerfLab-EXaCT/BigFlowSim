#ifndef CONNECTION_H_
#define CONNECTION_H_

#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <mutex>

#define MAGIC 1

class Connection {
public:
    Connection(int id, int sock);
    ~Connection();
    //int initiate();
    //void finishTransfer();
    int64_t sendMsg(std::string msg, int retrys);
    //int64_t sendMsg(uint8_t type, std::string msg, int retrys);
    int64_t sendMsg(void* msg, int64_t msgSize, int retrys);
    int64_t recvMsg(void* buf, int64_t bufSize, int retrys);
    int sock();    
    template <class T>
    Connection& operator << (const T & val) {
        std::cerr << val;
        return *this;
    }

    Connection& operator << (std::ostream & (*f)(std::ostream &)) {
        f(std::cerr);
        return *this;
    }

    Connection& operator << (std::ostream & (*f)(std::ios &)) {
        f(std::cerr);
        return *this;
    }

    Connection& operator << (std::ostream & (*f)(std::ios_base &)) {
        f(std::cerr);
        return *this;
    }
    std::mutex _Mutex;
    int task_cnt;
    int _retry;
private:
    int  _id;
    int  _port;
    std::string _hostAddr;
    int _sock;
};

#endif /* CONNECTION_H_ */