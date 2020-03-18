import numpy as np
import matplotlib.pyplot as plt
import sys
import argparse

def genAccess(pyhsicalFileSize, rsize, rcofv, rprob, fileStarts, batchStarts, bsize, bIdx, fIdx, accesses, of):
    # reset indice to start from starting index
    fileInd = batchStarts[bIdx]+fileStarts[fIdx]
    while fileInd < batchStarts[bIdx]+fileStarts[fIdx]+bsize:
        actual_read_size = abs(int(np.random.normal(rsize, rsize*rcofv)))
        while actual_read_size == 0:
            abs(int(np.random.normal(rsize, rsize*rcofv)))
        if rprob > np.random.rand():
            accesses.append((fileInd, actual_read_size, fIdx))
            of.write("tazer8GB.dat" + " " + str(fileInd) +
                     " " + str(actual_read_size)+" 0 0\n")
        fileInd += actual_read_size


def genAccessPattern(dsize, physicalFileSize, nfiles, rsize, rcofv, rprob, batchSize, nreps, osize, wtype, outName):
    fsize = int(dsize/nfiles)
    bsize = batchSize if batchSize > 0 else fsize
    print(bsize % fsize)
    nbatchs = int(fsize/bsize) + (1 if bsize % fsize > 0 else 0)
    fileStarts = np.array(range(nfiles))*fsize
    batchStarts = np.array(range(nbatchs))*bsize

    wsize = int(osize/nbatchs)

    accesses = []
    outInd = 0
    with open(outName, 'w') as of:
        for f in range(nfiles):
            for b in range(nbatch)s:
                for _ in range(nreps):
                    genAccess(physicalFileSize, rsize, rcofv, rprob, fileStarts,
                              batchStarts, bsize, b, f, accesses, of)
                if wtype is "strided":
                    of.write("tazer_output.dat "+str(outInd) +
                                " "+str(wsize) + " 0 0\n")
                    outInd += wsize
        if wtype is "batch":
            of.write("tazer_output.dat "+str(outInd)+" "+str(osize) + " 0 0\n")

    return np.array(accesses)


# linear read: numCycles=1 readProbability=1 batchSize=1 numFiles=1 readProbability-1
# sparse linear read: numCycles=1 readProbability=1 batchSize=1 numFiles=1 readProbability=.1
#
#
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    # experiment/architecture parameters
    parser.add_argument("-r", "--ioRatio", type=float,
                        help="experiment wide ioratio (MB/s)", default=125)
    parser.add_argument("-t", "--tasksPerCore", type=int,
                        help="number of tasks", default=1)
    parser.add_argument("-C", "--numCores", type=int,
                        help="number of cores in experiment", default=1)
    parser.add_argument("-T", "--execTime", type=int,
                        help="desired cpu time (seconds)", default=300)
    parser.add_argument("-S", "--maxFileSize", type=int,
                        help="max pyhsical file size", default=8 * 2**30)

    # file access parameters
    parser.add_argument("-f", "--numFiles", type=int,
                        help="number of files to simulate", default=1)
    parser.add_argument("-c", "--numCycles", type=int,
                        help="number of cycles through a file", default=1)
    parser.add_argument("-b", "--batchSize", type=int,
                        help="number of batches per file (0 = fileSize)", default=0)

    # individual read parameters
    parser.add_argument("-s", "--readSize", type=int,
                        help="average read size (B)", default=2**14)
    parser.add_argument("-d", "--readCofV", type=float,
                        help="read size coefficient of variation", default=.1)
    parser.add_argument("-p", "--readProbability", type=float,
                        help="(0.0-1.0) probability of a read occuring", default=1.0)

    # miscellaneous generator parameters
    parser.add_argument("-o", "--outputSize", type=int,
                        help="average output size write", default=2**23)
    parser.add_argument("-v", "--verbose", action='store_true',
                        help="enable verbose output")
    parser.add_argument("-P", "--plot", type=str,
                        help="filename to save plot of resulting access pattern", default="accesses.png")
    parser.add_argument(
        "-F", "--fileName", help="access pattern trace output name", default="accesses.txt")

    args = parser.parse_args()
    print(args)

    args.ioRatio *= 10**6
    # data to read, not necessarily file size...
    totalSize = (args.ioRatio*args.execTime*args.tasksPerCore) / \
        args.readProbability
    taskSize = float(totalSize)/float(args.numCores*args.tasksPerCore)
    maxFileSize = taskSize/args.numCycles

    if maxFileSize <= args.maxFileSize:
        accesses = genAccessPattern(maxFileSize, args.maxFileSize, args.numFiles, args.readSize, args.readCofV,
                                    args.readProbability, args.batchSize, args.numCycles, args.outputSize, "strided", args.fileName)
    else:
        print("Physical File Size is smaller than required given paramemters")

    if args.verbose:

        maxUniqueSize = maxFileSize * args.numCores
        baseLineRate = 125*10**6  # 110 MB/s
        print("total experiment data read:", totalSize/10.0**9,
              "GB, tx time @ 1gbps =", totalSize/baseLineRate, "seconds")
        print("total task file size:", taskSize/10.0**9,
              "GB, tx time @ 1gbps =", taskSize/baseLineRate, "seconds")
        print("actual data read:", np.sum(accesses, axis=0)[1]/10.0**9, "GB")

    if args.plot:
        print(np.unique(accesses.T[2]))
        for f in np.unique(accesses.T[2]):

            fileAccess = np.argwhere(accesses.T[2] == f)
            plt.plot(fileAccess, accesses.T[0][fileAccess]/(1024*1024), "o")
        plt.tight_layout()
        plt.xlabel("access")
        plt.ylabel("file offset (MB)")
        plt.savefig(args.plot)
