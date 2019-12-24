from matplotlib.patches import Patch
from matplotlib.lines import Line2D
import numpy as np
import scipy.stats as stats
import matplotlib.pyplot as plt
import re
import matplotlib
matplotlib.rcParams.update({'font.size': 20})


def getData(path):
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
    ioCnt = 0
    nioCnt = 0
    maxt = 0
    mint = 99999999999999999
    temp = 0

    exp_times = []
    latencies = []
    start_time = []
    amounts = []

    

    for jobid in data:
        labels = data[jobid][0]
        vals = data[jobid][1]

        

        if mint > int(vals[labels.index("StartTime")]):
            mint = int(vals[labels.index("StartTime")])
        if maxt < int(vals[labels.index("FinishedTime")]):
            maxt = int(vals[labels.index("FinishedTime")])
        if temp < int(vals[labels.index("FinishedTime")])-int(vals[labels.index("StartTime")]):
            temp = int(vals[labels.index("FinishedTime")]) - \
                int(vals[labels.index("StartTime")])

        accesses = float(vals[labels.index("tazer_input_accesses")])
        time = float(vals[labels.index("tazer_input_time")])
        amount = float(vals[labels.index("tazer_input_amount")])

        mach_name = "bluesky"

        if (accesses > 0):
            exp = int(vals[labels.index("FinishedTime")]) - \
                int(vals[labels.index("StartTime")])
            # print (mach_name,exp/60.0,accesses,time)

            exp_times.append(time/60.0)
            latencies.append(time/accesses)
            amounts.append(amount)
            start_time.append(int(vals[labels.index("StartTime")]))
        else:
            pass
            # print(vals)

    

    return [exp_times, latencies, amounts]


def plot_exp(res, ax, ax2, line, name, xs):

    avg_exp = []
    avg_exp1 = []
    sum_exp = []
    avg_lat = []
    avg_lat1 = []
    sum_lat = []
    # xs = []
    # for r in[("16:1", 1/16), ("8:1", 1/8), ("4:1", 1/4), ("2:1", 1/2), ("1:1", 1/1), ("full", 2)]:
    for r in res:

        tmp_exps = []
        tmp_lats = []

        xs.append(r[3])
        tmp_exps += r[0]
        tmp_lats += r[1]
        avg_exp.append(stats.mstats.gmean(tmp_exps))
        avg_exp1.append(np.mean(tmp_exps))
        sum_exp.append(np.sum(tmp_exps))
        avg_lat.append(stats.mstats.gmean(tmp_lats))
        avg_lat1.append(np.mean(tmp_lats))
        sum_lat.append(np.sum(tmp_lats))
        print(r[3], np.mean(tmp_exps), stats.mstats.gmean(
            tmp_exps), np.sum(tmp_exps))
        print(r[3], np.mean(tmp_lats), stats.mstats.gmean(
            tmp_lats), np.sum(tmp_lats))

    print(xs)

    lns1 = ax.plot(xs, np.array(avg_exp1), color="b", marker=line, markersize=10, lw=2,
                   label=name+"task exec time")

    lns2 = ax2.plot(xs, np.array(avg_lat1), color="g", lw=2,
                    marker=line, markersize=10, label=name+"access latency (s)")

    lns = lns1+lns2
    labs = [l.get_label() for l in lns]
    return (lns, labs, avg_exp, avg_exp1, avg_lat, avg_lat1, sum_exp, sum_lat)


# 512*1024*1024*1024


fig = plt.figure(figsize=(12, 8))
cache_labels = [r"$\frac{1}{8}$", r"$\frac{1}{4}$", r"$\frac{1}{2}$",  r"$\frac{1}{1}$",
                r"$\frac{2}{1}$", r"$\frac{4}{1}$", r"$\frac{8}{1}$", r"$\frac{16}{1}$", r"$\frac{32}{1}$", r"$\frac{64}{1}$"]
# xs = [1/16, 1/8, 1/4, 1/2, 1/1, 2, 4, 8]
plt.ylabel("Avg. IO time (min)")
# plt.xlabel("Ratio of last level cache to total footprint")
plt.xlabel("cluster wide i/o intensity (MB/s)")

ax = plt.gca()


ax2 = ax.twinx()

xs = []
res = []
numTasks = 3000.0
llc_size = 512*1024*1024*1024.0
markers = {2: "o", 16: "s"}
for fold in ["./"]:  # , "no_ib_resource_balancing/"]:
    for j in [2, 16]:
        xs = []
        res = []
        for i in [1, 2, 4, 8, 16]:
            infile = fold + str(i*125)+"MBs_io_"+str(j)+"_tpf/logs/data.txt"
            try:
                data = getData(infile)
                print("avg data: ", np.mean(data[2])/1000000000.0, "raw data: ", (np.mean(
                    data[2])*numTasks)/1000000000.0, "tots data:", ((np.mean(data[2])*numTasks)/float(j))/1000000000.0)
                # ratio = llc_size/((np.mean(data[2])*numTasks)/float(j))
                ratio = 125*i
                data.append(ratio)
                res.append(data)
            except:
                pass

        lns, labs, avg_exp, avg_exp1, avg_lat, avg_lat1, sum_exp, sum_lat = plot_exp(
            res, ax, ax2, markers[j], "tasks_per_file: "+str(j), xs)

        print(avg_exp, avg_exp1, avg_lat, avg_lat1, sum_exp, sum_lat)

        # ax.set_xticks([.125, .25, .5, 1, 2, 4, 8, 16, 32, 64])
        # ax.set_xticklabels(cache_labels)
        # ax.set_yticks([2, 10, 20, 100, 200, 300])
        # ax.set_xticks([125, 250, 500, 1000, 2000])
        # ax.set_xticklabels([125, 250, 500, 1000, 2000])
        # ax.set_yticks([.1, 1, 10, 100])

        # fmt = matplotlib.ticker.StrMethodFormatter("{x:g}")
        # ax.yaxis.set_major_formatter(fmt)
        # ax.tick_params(axis='x', which='minor', bottom=False)

        # ax2.set_xticks(xs)
        # ax2.set_xticks([125, 250, 500, 1000, 2000])
        # ax2.set_xticklabels([125, 250, 500, 1000, 2000])
        # ax2.set_yticks([0.05, .01, .001, .0001, .00004])
        # ax2.set_yticklabels(["50ms", "10ms", "1ms", "100us", "40us"])
        plt.ylabel("Avg. Access Latency")

lgd_elems = [Line2D([0], [0], color="b", lw=2, label="execution time"),
             Line2D([0], [0], color="g", lw=2, label="access latency"),
             Line2D([0], [0], marker="o", color="w", markerfacecolor="w", markeredgecolor="k",
                    markersize=10, label="2 tpf"),
             Line2D([0], [0], marker="s", color="w", markerfacecolor="w", markeredgecolor="k",
                    markersize=10, label="16 tpf"), ]
plt.legend(handles=lgd_elems)
plt.tight_layout(pad=0)
plt.show()


print()
print(avg_exp[-3], avg_exp[-2], avg_exp1[-3],
      avg_exp1[-2], sum_exp[-3], sum_exp[-2])
print(avg_exp[-3]/avg_exp[-2], avg_exp1[-3] /
      avg_exp1[-2], sum_exp[-3]/sum_exp[-2])
print(avg_lat[-3], avg_lat[-2], avg_lat1[-3],
      avg_lat1[-2], sum_lat[-3], sum_lat[-2])
print(avg_lat[-3]/avg_lat[-2], avg_lat1[-3] /
      avg_lat1[-2], sum_lat[-3]/sum_lat[-2])

# for i in range(len(avg_exp)):

#     print(avg_exp[i]/avg_exp[-2], avg_exp1[i] /
#           avg_exp1[-2], sum_exp[i]/sum_exp[-2])
#     print(avg_lat[i]/avg_lat[-2], avg_lat1[i] /
#           avg_lat1[-2], sum_lat[i]/sum_lat[-2])
#     print()
