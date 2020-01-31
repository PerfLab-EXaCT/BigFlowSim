import sys


tazer_hosts=sys.argv[1]

addrs = ""

if "[" in tazer_hosts:
    temp = tazer_hosts.split("[")
    base = temp[0]
    temp =  temp[1].strip("]").split(",")
    for t in temp:
        if "-" in t:
            end_nodes = t.split("-")
            for node in range(int(end_nodes[0]),int(end_nodes[-1])+1):
                addrs+=base+str(node)+" "
        else:
            addrs+=base+t+" " #+".ibnet "

print(addrs)


