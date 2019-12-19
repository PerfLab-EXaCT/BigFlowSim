import sys


tazer_hosts=sys.argv[1]

addrs = ""

if "[" in tazer_hosts:
    temp = tazer_hosts.split("[")
    base = temp[0]
    temp =  temp[1].strip("]").split(",")
    for t in temp:
        addrs+=base+t+" " #+".ibnet "

print(addrs)


