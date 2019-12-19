#include <arpa/inet.h>
#include <mutex>
#include <netinet/in.h>
#include <sstream>
#include <string.h>
#include <unistd.h>

#include <cstring>
#include <iomanip>
#include <iostream>

#include "Connection.h"

Connection::Connection(int id, int sock) : _id(id),
                                           _sock(sock) {
}

Connection::~Connection() {
    if (_sock > -1) {
        close(_sock);
        _sock = -1;
    }
    // *this << " connection deconstructing " << _id << std::endl;
}

int64_t Connection::sendMsg(std::string msg, int retrys) {
    uint32_t size = msg.size() + 1 + sizeof(uint32_t);
    char cmsg[size];
    *(uint32_t *)cmsg = size - sizeof(uint32_t);
    msg.copy(cmsg + sizeof(uint32_t), msg.size(), 0);
    cmsg[size - 1] = '\0';
    return sendMsg(cmsg, size, retrys) - 1 - sizeof(uint32_t);
}

int64_t Connection::sendMsg(void *msg, int64_t msgSize, int retrys) {
    int retryCnt = 0;
    size_t sentSize = 0;
    while (sentSize < msgSize && retryCnt <= retrys) {
        int64_t ret = send(_sock, msg + sentSize, msgSize - sentSize, MSG_NOSIGNAL);
        if (ret <= 0) {
            retryCnt++;
            usleep(1000);
            *this << " Error in sendMsg: " << strerror(errno) << std::endl;
        }
        else {
            sentSize += ret;
            retryCnt = 0;
        }
    }
    if (retryCnt > retrys) {
        //_connected = false;
        if (errno == EPIPE) {
            return -2;
        }
        else {
            return -1;
        }
    }
    return sentSize;
}

int64_t Connection::recvMsg(void *buf, int64_t bufSize, int retrys) {
    int retryCnt = 0;
    size_t recvSize = 0;
    while (recvSize < bufSize && retryCnt < retrys) {
        int64_t ret = read(_sock, buf + recvSize, bufSize - recvSize);
        if (ret <= 0) {
            retryCnt++;
            usleep(1000);
        }
        else {
            retryCnt = 0;
            recvSize += ret;
        }
    }
    if (retryCnt > retrys) {
        //_connected = false;
    }
    return recvSize;
}

int Connection::sock() {
    return _sock;
}