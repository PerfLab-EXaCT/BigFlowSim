import numpy as np
import matplotlib.pyplot as plt
import sys

# dsize = total data size
# nfiles = number of files to simulate (dsize/nfiles = size of each file)
# rsize = average read size
# rstddev = read size standar deviation (0 will make all reads equal to rsize)
# rprob = probabiliy of performing a read for a given offset
# batch = number of events per "cycle" of a file..
# nreps = number of repetitions (number of times to cycle through the data)
# osize = total size of output
# wtype = output writing behavior (strided = write after each "event", batch = write at end of all events)


def genAccessPattern(dsize, nfiles, rsize, rstddev, rprob, batch, nreps, osize, wtype, outName):
    fsize = int(dsize/nfiles)  # size of individual file

    fileStarts = np.array(range(nfiles))*fsize
    curInds = np.array(range(nfiles))*fsize

    numEvents = batch*nreps
    wsize = int(osize/numEvents)

    readsPerBatch = int((fsize/rsize)/batch)
    accesses = []
    outInd = 0
    with open(outName, 'w') as of:
        for c in range(nreps):  # number of times to repeat
            #print (c)
            valid = np.ones(nfiles)
            curInds = np.array(range(nfiles))*fsize
            while np.any(valid):
                for i in range(len(curInds)):  # go through each file
                    # print(i)
                    if valid[i]:
                        # of reads to batch for each file read
                        for j in range(readsPerBatch):
                            # print(j)
                            actual_read_size = int(
                                np.random.normal(rsize, rstddev))
                            if rprob > np.random.rand():
                                accesses.append(curInds[i])
                                #of.write(str(i)+" "+str(curInds[i]-fileStarts[i])+" "+str(rsize)+" 0 0\n")
                                # of.write("tazer_"+str(i)+".dat"+" " +
                                #          str(curInds[i])+" "+str(rsize)+" 0 0\n")
                                of.write("tazer8GB.dat"+" " +
                                         str(curInds[i])+" "+str(actual_read_size)+" 0 0\n")
                            curInds[i] += actual_read_size
                            if curInds[i] >= fileStarts[i]+fsize:
                                valid[i] = 0
                                break
                if wtype is "strided":
                    of.write("tazer_output.dat "+str(outInd) +
                             " "+str(wsize) + " 0 0\n")
                    outInd += wsize
        if wtype is "batch":
            of.write("tazer_output.dat "+str(outInd)+" "+str(osize) + " 0 0\n")

    return accesses


ioIntRatio = float(sys.argv[1])
taskToFileRatio = float(sys.argv[2])
numTasks = int(sys.argv[3])
numCores = int(sys.argv[4])
outName = sys.argv[5]


rsize = 2**14  # 16k
rstddev = 100
nfiles = 1  # 2**0  # 1 files
batch = 1
rprob = 1
nreps = 1
osize = 2**23  # 8MB

np.set_printoptions(precision=3, suppress=True)
baseRate = 125*(1000000)  # 125MBs
expTime = 300  # 5 min
diskCacheSize = 512 * 1024**3
bbCacheSize = 200 * 1024**3
memCacheSize = 16.0 * 1024**3

totalIoRate = (baseRate*ioIntRatio)
taskIoRate = totalIoRate/numCores
totalSize = totalIoRate*expTime
taskSize = totalSize/numCores
dsize = taskSize
accesses = genAccessPattern(
    dsize, nfiles, rsize, rstddev, rprob, batch, nreps, osize, "strided", outName)

diskRatio = (taskSize/taskToFileRatio)*numTasks / diskCacheSize
memRatio = (taskSize/taskToFileRatio)*60.0 / memCacheSize
ioTime = ((float(taskSize)*numTasks)/(1080*1000000.0))/60.0
# print(totalIoRate/1000000.0, taskIoRate/1000000.0,
#       totalSize/1000000000.0, taskSize/1000000.0)
# print(diskRatio, memRatio)
print((taskIoRate)/1000000.0)  # rate in MB/s
