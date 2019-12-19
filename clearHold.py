import sys
import socket
import struct
from os import walk


def send_msg(sock, msg):
    # Prefix each message with a 4-byte length (network byte order)
    print len(msg)
    msg = struct.pack('<I', len(msg)) + msg
    print len(msg)
    sock.sendall(msg)


def recvall(sock, n):
    # Helper function to recv n bytes or return None if EOF is hit
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data


def recv_msg(sock):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('<I', raw_msglen)[0]
    print msglen
    return recvall(sock, msglen)


req_type = 4
magic = 1

task_host = sys.argv[1]
task_port = int(sys.argv[2])
print ("task server at:",task_host,task_port)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((task_host, task_port))
hostname = "all"  # node+".cluster.pnnl.gov"
data = "0"
msg = "%s %s %s %s %s" % (str(req_type), str(
    magic), str(hostname), str(0), str(data))
print msg
send_msg(sock, msg)
print recv_msg(sock)
sock.close()
