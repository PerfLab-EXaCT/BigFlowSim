import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import re
import matplotlib
matplotlib.rcParams.update({'font.size': 22})


def updateDiskLatencyAndBw(latency, bw, intensity, vals, labels, baseline):
    amount = (float(vals[labels.index("local_input_amount")])
              # )
              + float(vals[labels.index("boundedfilelock_request_read_amt")])
              + float(vals[labels.index("boundedfilelock_request_stall_amt")]))
    time = (float(vals[labels.index("local_input_time")]) +
            float(vals[labels.index("boundedfilelock_request_hit_time")]) +
            float(vals[labels.index("boundedfilelock_request_stall_time")]))
    accesses = (float(vals[labels.index("local_input_accesses")]) +
                float(vals[labels.index("local_output_accesses")]) +
                float(vals[labels.index("boundedfilelock_request_hits")]) +
                float(vals[labels.index("boundedfilelock_request_stalls")]))

    cache_accesses = float(vals[labels.index("tazer_input_accesses")])
    cache_amount = float(vals[labels.index("tazer_input_amount")])
    cache_time = float(vals[labels.index("tazer_input_time")])

    if accesses > 0:
        latency.append(time/(accesses))
        bw.append(amount/(time+1))
        if(cache_time > 0):
            intensity.append(min((1, time/(cache_time))))  # for tazer/xrootd
        else:
            intensity.append(1)  # for local
    else:
        latency.append(0)
        bw.append(0)
        intensity.append(0)


def updateMemLatencyAndBw(latency, bw, intensity, vals, labels, baseline):
    amount = (float(vals[labels.index("privatememory_request_read_amt")])
              + float(vals[labels.index("sharedmemory_request_read_amt")])  # )
              + float(vals[labels.index("privatememory_request_stall_amt")])
              + float(vals[labels.index("sharedmemory_request_stall_amt")]))
    time = (float(vals[labels.index("privatememory_request_hit_time")]) +
            float(vals[labels.index("sharedmemory_request_hit_time")]) +
            float(vals[labels.index("privatememory_request_stall_time")]) +
            float(vals[labels.index("sharedmemory_request_stall_time")]))
    accesses = (float(vals[labels.index("privatememory_request_hits")]) +
                float(vals[labels.index("sharedmemory_request_hits")]) +
                float(vals[labels.index("privatememory_request_stalls")]) +
                float(vals[labels.index("sharedmemory_request_stalls")]))

    cache_accesses = float(vals[labels.index("tazer_input_accesses")])
    cache_amount = float(vals[labels.index("tazer_input_amount")])
    cache_time = float(vals[labels.index("tazer_input_time")])

    if accesses > 0:
        latency.append(time/(accesses))
        bw.append(amount/(time+1))
        intensity.append(min((1, time/(cache_time))))
    else:
        latency.append(0)
        bw.append(0)
        intensity.append(0)


def updateNetLatencyAndBw(latency, bw, intensity, vals, labels, baseline):
    amount = (float(vals[labels.index("network_request_read_amt")])  # )
              + float(vals[labels.index("network_request_stall_amt")]))
    time = (float(vals[labels.index("network_request_hit_time")]) +
            float(vals[labels.index("network_request_stall_time")]))
    accesses = (float(vals[labels.index("network_request_hits")]) +
                float(vals[labels.index("network_request_stalls")]))

    cache_accesses = float(vals[labels.index("tazer_input_accesses")])
    cache_amount = float(vals[labels.index("tazer_input_amount")])
    cache_time = float(vals[labels.index("tazer_input_time")])

    if accesses > 0:
        latency.append(time/(accesses))
        bw.append(amount/(time+1))
        intensity.append(min((1, time/(cache_time))))
    else:
        latency.append(0)
        bw.append(0)
        intensity.append(0)


def updateBBLatencyAndBw(latency, bw, intensity, vals, labels, baseline):
    amount = (float(vals[labels.index("burstbuffer_request_read_amt")])  # )
              + float(vals[labels.index("burstbuffer_request_stall_amt")]))
    time = (float(vals[labels.index("burstbuffer_request_hit_time")]) +
            float(vals[labels.index("burstbuffer_request_stall_time")]))
    accesses = (float(vals[labels.index("burstbuffer_request_hits")]) +
                float(vals[labels.index("burstbuffer_request_stalls")]))

    cache_accesses = float(vals[labels.index("tazer_input_accesses")])
    cache_amount = float(vals[labels.index("tazer_input_amount")])
    cache_time = float(vals[labels.index("tazer_input_time")])

    if accesses > 0:
        latency.append(time/(accesses))
        bw.append(amount/(time+1))
        intensity.append(min((1, time/(cache_time))))
    else:
        latency.append(0)
        bw.append(0)
        intensity.append(0)


def updateTazerLatencyAndBw(latency, bw, intensity, vals, labels, baseline):
    amount = float(vals[labels.index("tazer_input_amount")])
    time = float(vals[labels.index("tazer_input_time")])
    accesses = float(vals[labels.index("tazer_input_accesses")])

    cache_accesses = float(vals[labels.index("tazer_input_accesses")])
    cache_amount = float(vals[labels.index("tazer_input_amount")])
    cache_time = float(vals[labels.index("tazer_input_time")])

    if cache_time > 0:
        latency.append(time/(accesses))
        bw.append(amount)
        intensity.append(min((1, time/(cache_time))))
    else:
        latency.append(0)
        bw.append(0)
        intensity.append(0)
    return float(vals[labels.index("local_input_accesses")])+float(vals[labels.index("tazer_input_accesses")])


def updateOverheadByLevel(level, latency, bw, intensity, vals, labels, baseline):
    amount = float(vals[labels.index("tazer_input_amount")])
    time = float(vals[labels.index(level+"_request_ovh_time")])
    # accesses =float(vals[labels.index(level+"_request_misses")])+float(vals[labels.index(level+"_request_hits")]) #
    accesses = float(vals[labels.index(level+"_request_reads")])

    cache_time = float(vals[labels.index("tazer_input_time")])

    if accesses > 0 and cache_time > 0:
        latency.append((time)/(accesses))
        bw.append(amount/(time))
        intensity.append(min((1, (time)/(cache_time))))
    else:
        latency.append(0)
        bw.append(0)
        intensity.append(0)


def updateOverheadLatencyAndBw(latency, bw, intensity, vals, labels, baseline):
    amount = float(vals[labels.index("tazer_input_amount")])
    time = (float(vals[labels.index("privatememory_request_ovh_time")]) +
            float(vals[labels.index("sharedmemory_request_ovh_time")]) +
            float(vals[labels.index("burstbuffer_request_ovh_time")]) +
            float(vals[labels.index("boundedfilelock_request_ovh_time")]) +
            float(vals[labels.index("network_request_ovh_time")]))

    accesses = float(vals[labels.index("tazer_input_accesses")])
    cache_accesses = float(vals[labels.index("tazer_input_accesses")])
    cache_time = float(vals[labels.index("tazer_input_time")])

    if accesses > 0 and cache_time > 0:
        latency.append((time)/(accesses))
        bw.append(amount/(time))
        intensity.append(min((1, (time)/(cache_time))))
    else:
        latency.append(0)
        bw.append(0)
        intensity.append(0)


def calcHits(machine, vals, labels, hits, misses):
    hits[machine][0].append(float(vals[labels.index("cache_accesses")]))
    hits[machine][1].append(
        float(vals[labels.index("privatememory_request_hits")]))
    hits[machine][2].append(
        float(vals[labels.index("sharedmemory_request_hits")]))
    hits[machine][3].append(
        float(vals[labels.index("burstbuffer_request_hits")]))
    hits[machine][4].append(
        float(vals[labels.index("boundedfilelock_request_hits")]))
    hits[machine][5].append(float(vals[labels.index("network_request_hits")]))
    misses[machine][0].append(
        float(vals[labels.index("privatememory_request_misses")]))
    misses[machine][1].append(
        float(vals[labels.index("sharedmemory_request_misses")]))
    misses[machine][2].append(
        float(vals[labels.index("burstbuffer_request_misses")]))
    misses[machine][3].append(
        float(vals[labels.index("boundedfilelock_request_misses")]))
    misses[machine][4].append(
        float(vals[labels.index("network_request_hits")]))


def updateLatencies(machine, vals, labels, latencies, bws, intensities, baseline):

    updateDiskLatencyAndBw(latencies[machine]["disk"], bws[machine]
                           ["disk"], intensities[machine]["disk"], vals, labels, baseline)
    updateNetLatencyAndBw(latencies[machine]["network"], bws[machine]
                          ["network"], intensities[machine]["network"], vals, labels, baseline)
    updateMemLatencyAndBw(latencies[machine]["memory"], bws[machine]
                          ["memory"], intensities[machine]["memory"], vals, labels, baseline)
    updateMemLatencyAndBw(latencies[machine]["burstbuffer"], bws[machine]
                          ["burstbuffer"], intensities[machine]["burstbuffer"], vals, labels, baseline)

    accesses = updateTazerLatencyAndBw(
        latencies[machine]["overall"], bws[machine]["overall"], intensities[machine]["overall"], vals, labels, baseline)
    updateOverheadLatencyAndBw(latencies[machine]["overhead"], bws[machine]
                               ["overhead"], intensities[machine]["overhead"], vals, labels, baseline)

    for level in ["privatememory", "sharedmemory", "burstbuffer", "boundedfilelock", "network"]:
        updateOverheadByLevel(level, latencies[machine][level+"_overhead"], bws[machine]
                              [level+"_overhead"], intensities[machine][level+"_overhead"], vals, labels, baseline)
    return accesses


def plotData(path, title, fname, baseline=False, save=False):
    global tot_sum
    tot_sum = 0
    global io_sum
    io_sum = 0
    data = {}

    with open(path) as rdata:
        for l in rdata:
            temp = l.split(";")
            if temp[0] not in data:
                data[temp[0]] = [[], []]
            names = [x.strip().strip("\"") for x in temp[1].split(",")]
            vals = [x.strip().strip("\"") for x in temp[2].split(",")]
            data[temp[0]][0] += names
            data[temp[0]][1] += vals

    bluesky = {}
    ivy = {}
    amd = {}
    haswell_1 = {}
    haswell_2 = {}
    ioCnt = 0
    nioCnt = 0
    maxt = 0
    mint = 99999999999999999
    temp = 0

    latencies = {}
    bws = {}
    intensities = {}
    start_time = {}
    for mach in ["bluesky"]:
        latencies[mach] = {}
        bws[mach] = {}
        intensities[mach] = {}
        start_time[mach] = []
        for t in ["network", "disk", "memory", "burstbuffer", "overall", "overhead"]:
            latencies[mach][t] = []
            bws[mach][t] = []
            intensities[mach][t] = []
        for level in ["privatememory", "sharedmemory", "burstbuffer", "boundedfilelock", "network"]:
            latencies[mach][level+"_overhead"] = []
            bws[mach][level+"_overhead"] = []
            intensities[mach][level+"_overhead"] = []

    hits = {"bluesky": [[], [], [], [], []]}

    accesses = []
    a_sum = 0
    for jobid in data:
        labels = data[jobid][0]
        vals = data[jobid][1]
        host = vals[labels.index("Host")]
        slot = int(vals[labels.index("Slot")])

        if mint > int(vals[labels.index("StartTime")]):
            mint = int(vals[labels.index("StartTime")])
        if maxt < int(vals[labels.index("FinishedTime")]):
            maxt = int(vals[labels.index("FinishedTime")])
        if temp < int(vals[labels.index("FinishedTime")])-int(vals[labels.index("StartTime")]):
            temp = int(vals[labels.index("FinishedTime")]) - \
                int(vals[labels.index("StartTime")])

        a_sum += float(vals[labels.index("local_input_accesses")])
        # print (jobid)
        mach_name = "bluesky"

        accesses.append(updateLatencies(mach_name, vals, labels,
                                        latencies, bws, intensities, baseline))

        start_time[mach_name].append(int(vals[labels.index("StartTime")]))

    print("avg accesses:", np.mean(accesses),
          "total accesses", np.sum(accesses), a_sum)

    blue = matplotlib.colors.to_rgba('b')
    green = matplotlib.colors.to_rgba('g')
    red = matplotlib.colors.to_rgba('r')
    yellow = matplotlib.colors.to_rgba('y')
    cyan = matplotlib.colors.to_rgba('c')
    magenta = matplotlib.colors.to_rgba('m')
    print(blue)
    print(green)
    # colors = {"bluesky":"g","ivy":"g","amd":"g","haswell_1":"g","haswell_2":"g"}
    colors = {"network": red, "disk": green, "burstbuffer": magenta,
              "memory": cyan, "overhead": yellow, "overall": blue}
    markers = {"network": "<", "disk": "s", "burstbuffer": "^",
               "memory": "p", "overhead": "d", "overall": "o"}

    level_colors = {"network": red, "boundedfilelock": green,
                    "privatememory": cyan, "sharedmemory": cyan, "burstbuffer": magenta}

    fig = plt.figure(figsize=(17, 10))
    cnt = 1
    ax = None
    for i in ["bluesky"]:  # , "ivy","amd","haswell_1","haswell_2"]:
        print(cnt)
        ax = plt.subplot(1, 1, cnt)
        print(title)
        markersize = 200
        j = 0
        if len(start_time[i]) > 0:
            for t in ["network", "disk", "memory", "burstbuffer", "overhead", "overall"]:
                if np.any(latencies[i][t]):
                    plt.scatter(start_time[i][0]-mint, latencies[i][t][0],
                                marker=markers[t], c=colors[t], label=t, s=markersize)
                    plt.scatter(start_time[i][0]-mint, latencies[i]
                                [t][0], marker=markers[t], c="w", s=markersize)
            for level in ["privatememory", "sharedmemory", "burstbuffer", "boundedfilelock", "network"]:
                if np.any(latencies[i][level+"_overhead"]):
                    plt.scatter(start_time[i][0]-mint, latencies[i][level+"_overhead"][0],
                                marker=markers["overhead"], c=level_colors[level], label=level+"_overhead", s=markersize)
                    plt.scatter(start_time[i][0]-mint, latencies[i]
                                [level+"_overhead"][0], marker=markers["overhead"], c="w", s=markersize)

        markersize = 80
        for t in ["overall", "network", "disk", "memory", "burstbuffer", "overhead"]:
            sts = np.array(start_time[i])-mint
            rbgas = np.zeros((len(latencies[i][t]), 4))
            rbgas[:] = colors[t]
            rbgas[:, 3] = np.array(intensities[i][t])*.995 + .005
            plt.scatter(sts, latencies[i][t], c=rbgas,
                        marker=markers[t], s=markersize)
        for level in ["privatememory", "sharedmemory", "burstbuffer", "boundedfilelock", "network"]:
            sts = np.array(start_time[i])-mint
            rbgas = np.zeros((len(latencies[i][level+"_overhead"]), 4))
            rbgas[:] = level_colors[level]
            rbgas[:, 3] = np.array(
                intensities[i][level+"_overhead"])*.995 + .005
            plt.scatter(sts, latencies[i][level+"_overhead"], c=rbgas,
                        marker=markers["overhead"], s=markersize)

        plt.yscale('log')

        plt.ylim((.00000001, 4))
        plt.grid(True)
        if "tazer-access" in fname or "Local" in title:
            plt.xlim((0, 4600))
        if cnt == 1:
            plt.ylabel("Avg. Latency/access")
            plt.yticks([10, 1, .1, .01, .001, .0001, .00001, .000001, .0000001], [
                       "10s", "1s", "100ms", "10ms", "1ms", "100us", "10us", "1us", "100ns"])
            plt.legend()
        else:
            plt.yticks(
                [10, 1, .1, .01, .001, .0001, .00001, .000001, .0000001], [])
            plt.legend()

        if i is "bluesky":
            plt.title("node type 1")
        elif i is "ivy":
            plt.title("node type 2")
        else:
            plt.title(i)
        plt.xlabel("Task start time")

        cnt += 1
    fig.suptitle(title)
    plt.tight_layout()
    if save:
        fig.savefig(fname+"_latencies.pdf")
    # plt.show()
    # print(latency)
    return tot_sum, io_sum


res = []
res.append(plotData("./logs/data.txt",
                    "graph_title", "tazer", True,True))
