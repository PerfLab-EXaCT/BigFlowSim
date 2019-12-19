import os
import io
import sys
import argparse
import time
import threading
import glob
import numpy as np

# import ctypes

full_start = time.time()
# clk_tik = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
# _timer = ctypes.CDLL('libtimer.so')
# _timer.mysleep.argtypes = (ctypes.c_double,)
# _timer.mysleep2.argtypes = (ctypes.c_double, ctypes.c_long)
filecnts = {}


def simCpu(sec):
    tstart = time.time()
    tend = time.time()
    while tend - tstart < sec:
        tend = time.time()


def readEvents(ofds, fileEvents, eventList, readWait, fileWait, ioIntensity, timevar, numAccesses):
    sumCpuTime = 0.0
    actCpuTime = 0.0
    readTime = 0.0
    otherTime = 0.0
    openTime = 0.0
    closeTime = 0.0
    fds = ofds
    if ofds == []:
        start = time.time()
        for f in u:
            temp = f.split("/")[-1]
            if "output" not in f:
                # fd = open("data/"+temp+".meta.in", 'rb',buffering=0)
                ifile = glob.glob("data/*meta.in")
                print(ifile)
                fd = open(ifile[0], 'rb', buffering=0)
            else:
                # fd = open("data/"+temp+".meta.out", 'wb')
                fd = open("data/"+temp, 'wb')

            fds.append([f, fd])
        openTime = time.time()-start
    tempT = time.time()
    cnt = 0
    div = int(numAccesses/10)
    for f, fd in fds:
        tsum = 0
        tsum2 = 0
        tcnt = 0
        for e in np.array(eventList):
            # start = time.time()
            event = fileEvents[f][e]
            # otherTime += time.time()-start
            for r in event:

                start = time.time()
                if "output" in f:
                    print(f)
                    fd.write(os.urandom(int(r[2])))
                else:
                    # print(r)
                    fd.seek(int(r[1]), 0)

                    tmp = fd.read(int(r[2]))
                    #print(int(r[1]), int(r[2]), len(tmp))
                readTime += time.time()-start
                start = time.time()
                tcnt += 1
                tsum += int(r[2])
                filecnts[f] += int(r[2])
                cpus = time.time()
                if readWait:
                    cpu = np.random.uniform(readWait[0], readWait[1])
                else:
                    cpu = (int(r[2])/1000000.0)/ioIntensity
                    cpu = np.random.uniform(
                        cpu-cpu*timevar, cpu+cpu*timevar)
                # print(int(r[2]), int(r[2])/1000000.0, ioIntensity, cpu)
                simCpu(cpu)
                sumCpuTime += cpu
                actCpuTime += time.time() - cpus
                otherTime += time.time()-start
                cnt += 1
                if cnt % div == 0:
                    print(int(cnt/numAccesses*100), "%",
                          sumCpuTime, "s", readTime, "s")
        # start=time.time()
        if fileWait:
            cpu = np.random.uniform(fileWait[0], fileWait[1])
            sumCpuTime += cpu
            simCpu(cpu)
        # print(f.split("/")[-1], "reads: ", tcnt,
        #      "Mbytes read", tsum/1000000.0)
        # otherTime += time.time()-start

    tempT = time.time()-tempT
    # start = time.time()
    if ofds == []:
        for f, fd in fds:
            fd.close()
    # closeTime = time.time()-start

    # print("Temp time: ", tempT, otherTime)

    return sumCpuTime, openTime, readTime, closeTime, actCpuTime


def event_thread(fileEvents, eventList, readWait, fileWait, f, fd, event):
    for r in event:
        fd.seek(int(r[1]), 0)
        fd.read(int(r[2]))
        if readWait:
            cpu = np.random.uniform(readWait[0], readWait[1])
            simCpu(cpu)
            sumCpuTime += cpu


def event_thread2(fileEvents, eventList, readWait, fileWait, f, fd):
    tsum = 0
    tcnt = 0
    sumCpuTime = 0.0
    for e in np.array(eventList):
        event = fileEvents[f][e]
        for r in event:
            fd.seek(int(r[1]), 0)
            fd.read(int(r[2]))
            tcnt += 1
            tsum += int(r[2])
            filecnts[f] += int(r[2])
            if readWait:
                cpu = np.random.uniform(readWait[0], readWait[1])
                simCpu(cpu)
                sumCpuTime += cpu
    time.sleep(np.random.uniform(fileWait[0], fileWait[1]))
    print(f, "reads: ", tcnt, "bytes read", tsum/1000000.0)


def readEventsPar(ofds, fileEvents, eventList, readWait, fileWait):
    fds = ofds
    if fds == []:
        for f in u:
            temp = f.split("/")[-1]
            fd = open("data/"+temp+".meta.in", 'rb', buffering=0)
            fds.append([f, fd])

    tids = []
    for f, fd in fds:
        tids.append(threading.Thread(target=event_thread2, args=(
            fileEvents, eventList, readWait, fileWait, f, fd)))
        tids[-1].start()
    for t in tids:
        t.join()

    # for e in np.array(eventList):

    #     for f, fd in fds:
    #         event = fileEvents[f][e]
    #         tids.append(threading.Thread(target=event_thread, args=(
    #             fileEvents, eventList, readWait, fileWait, f, fd, event)))
    #         tids[-1].start()
    #     for t in tids:
    #         t.join()

    if ofds == []:
        for f, fd in fds:
            fd.close()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infile", type=str,
                        help="belle2 access pattern file", default="${HOME}/access_new_in_out.txt")
    parser.add_argument("-b", "--batchsize", type=int,
                        help="num events per batch", default=1)
    parser.add_argument("-a", "--accesspattern", type=int,
                        help="sequential or random event access", default=0)
    parser.add_argument("-r", "--readtime", type=float,
                        help="cpu time between reads", default=0)
    parser.add_argument("-f", "--filetime", type=float,
                        help="cpu time between accessing files", default=0)
    parser.add_argument("-e", "--eventtime", type=float,
                        help="cpu time between events", default=0)
    parser.add_argument("-t", "--timevar", type=float,
                        help="percent to vary time parameters (+- <arg> percent)", default=0.25)
    parser.add_argument("-c", "--concurrentfiles",
                        help="files are accessed concurrently", action='store_true')
    parser.add_argument("-w", "--iointensity", type=float,
                        help="MB read per second of compute time", default=1000)

    args = parser.parse_args(sys.argv[1:])
    dataFile = args.infile
    batchSize = args.batchsize
    accessPattern = args.accesspattern
    readTime = None
    fileTime = None
    eventTime = None
    if(args.readtime > 0):
        readTime = (args.readtime-args.readtime*args.timevar,
                    args.readtime+args.readtime*args.timevar)
    if(args.filetime > 0):
        fileTime = (args.filetime-args.filetime*args.timevar,
                    args.filetime+args.filetime*args.timevar)
    if(args.eventtime > 0):
        readtime = (args.eventtime-args.eventtime*args.timevar,
                    args.eventtime+args.eventtime*args.timevar)

    ioIntensity = args.iointensity

    np.random.seed(int(time.time()))

    cpuTime = 0.0
    ioOpenTime = 0.0
    ioReadTime = 0.0
    ioCloseTime = 0.0
    actCpuTime = 0.0

    print("DEFAULTBUFF SIZE:", io.DEFAULT_BUFFER_SIZE)
    # ----------------- load in access data -----------------------
    start = time.time()
    data = np.genfromtxt(dataFile, dtype='str', delimiter=" ")
    numAccesses = len(data)
    print("numAccesses", numAccesses)

    # getfile names and index they were first encountered
    u, i = np.unique(data.T[0], return_index=True)
    # print (data[65536::])
    # print (u,i)
    u = u[np.argsort(i)]  # place in order actually opened in app
    # print(u)
    prev = 0
    # print(u)

    # calculate where each event starts within the trace (every time file0 is read from again)
    eventStarts = []
    for n in np.where(data.T[0] == u[0])[0]:
        if int(n) != prev+1:
            eventStarts.append(n)
        prev = int(n)
    eventStarts.append(len(data))
    # print(eventStarts, len(eventStarts))

    fileEvents = {}
    for f in u:
        print(f)
        if len(f) > 0:
            filecnts[f] = 0
            fileEvents[f] = []
            prev = eventStarts[0]
            for i in range(1, len(eventStarts)):
                # print(f, prev, eventStarts[i])
                tmp = data[prev +
                           np.where(data[prev:eventStarts[i]].T[0] == f)[0]]
                fileEvents[f].append(tmp)
                prev = eventStarts[i]

    # print(fileEvents)
    del(data)
    setupTime = time.time()-start
    # -------------------------------------------------------------------------

    # ------------------ playback accesses-------------------------------------
    sstart = time.time()
    numEvents = len(fileEvents[u[0]])
    if accessPattern == 0:
        eventOrder = range(1, numEvents)
    elif accessPattern == 1:
        np.random.shuffle(eventOrder)

    fds = []
    start = time.time()
    for f in u:
        tt = time.time()
        temp = f.split("/")[-1]
        print(f)
        if "output" not in f:
            ifile = glob.glob("data/*meta.in")
            print(ifile)
            fd = open(ifile[0], 'rb', buffering=0)
        else:
            # fd = open("data/"+temp+".meta.out", 'wb')
            fd = open("data/"+temp, 'wb')
        fds.append([f, fd])

        # evSt = []
        # curInd = 0
        # for e in range(len(eventOrder)):
        #     event = fileEvents[f][e]
        #     evSt.append([curInd, len(event)])
        #     curInd += len(event)
        # print(f, evSt)
        # with open("file_accesses/"+temp+"_accesses.txt", "wb") as of, open("file_accesses/"+temp+"_events.txt", "wb") as ef:
        #     #evSt = []
        #     curInd = 0
        #     ef.write("access file offset, number of reads in event\n")
        #     of.write("read offset, read size\n")
        #     for e in range(len(eventOrder)):
        #         event = fileEvents[f][e]
        #         ef.write(str(curInd+1)+", "+str(len(event))+"\n")
        #         for r in event:
        #             of.write(r[1]+", "+r[2]+"\n")
        #         #evSt.append([curInd+1, len(event)])
        #         curInd += len(event)
        # print(f, time.time()-tt)
    ioOpenTime += time.time()-start

    # perform init:
    if args.concurrentfiles:
        readEventsPar(fds,
                      fileEvents, [0], readTime, fileTime)
    else:
        times = readEvents(fds,
                           fileEvents, [0], readTime, fileTime, ioIntensity, args.timevar, numAccesses)
        cpuTime += times[0]
        ioOpenTime += times[1]
        ioReadTime += times[2]
        ioCloseTime += times[3]
        actCpuTime += times[4]

    event_i = 0
    while event_i < len(eventOrder):
        event_end = event_i+batchSize
        if event_end > len(eventOrder):
            event_end = len(eventOrder)

        if args.concurrentfiles:
            readEventsPar(fds,
                          fileEvents, eventOrder[event_i:event_end], readTime, fileTime)
        else:
            times = readEvents(fds,
                               fileEvents, eventOrder[event_i:event_end], readTime, fileTime, ioIntensity, args.timevar, numAccesses)
            cpuTime += times[0]
            ioOpenTime += times[1]
            ioReadTime += times[2]
            ioCloseTime += times[3]
            actCpuTime += times[4]

        if eventTime:
            cpu = np.sum(np.random.uniform(
                eventTime[0], eventTime[1], event_end-event_i))
            cpuTime += cpu
            # print(cpu)
            simCpu(cpu)
        event_i += batchSize
        print("--- Events processed: ", event_i,
              " of "+str(len(eventOrder))+" -----------------------------")

    start = time.time()
    for f, fd in fds:
        fd.close()
    ioCloseTime += time.time()-start

    totalSum = 0.0
    for f in u:
        print(f, filecnts[f]/1000000.0)
        totalSum += filecnts[f]/1000000.0
    print("total MB:", totalSum)
    simTime = time.time()-sstart
    print("Setup time:", setupTime)
    print("Total Sim time:", simTime)
    print("Simulated CPU time: ", cpuTime, actCpuTime)
    print("io times:", ioOpenTime, ioReadTime, ioCloseTime)
    print("full_time: ", time.time()-full_start)
    # -------------------------------------------------------------------------
