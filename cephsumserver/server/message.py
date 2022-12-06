import json
import socket
from time import perf_counter

MAX_READ=4048

def send(sock, msg: dict):
    """Send message via socket.
    
    The message sent consists of 4 bytes (big endian) int that corresponds to the 
    size of the remaing part of the message.
    The message itself if converted to json (utf8 and sent as bytes).
    In case no dict is passed in, it is assume a sentinal, and only 4 bytes of 0's is sent.
    """
    if msg is None:
        # sentinal
        msg_length = int(0).to_bytes(4,'big')
        sock.send(msg_length)
    else:
        msg = (json.dumps(msg)).encode('utf8')
        msg_length = int(len(msg)).to_bytes(4,'big')
        # send the length and the message
        sock.sendall(msg_length+msg)


# def inner_recv(sock, read_size, retries=5):
#     _retries=retries
#     while retries:
#         try:
#             data = sock.recv(read_size)
#         except socket.timeout:
#             _retries -=1
#             if _retries == 0:
#                 print("inner timeout")
#                 raise socket.timeout
#             continue
#         return data

def recv(sock) -> dict:
    """Receive the data via the socket. 
    
    First 4 bytes is an int (big endian) representing the size of the message 
    (not including those 4 bytes). 
    Data is read in chunks up to the expected size, and converted into a dict

    """

    data = sock.recv(4)
    msg_length = int.from_bytes(data,'big')
    if msg_length==0:
        # print("message over")
        return {}

    data = b''
    bytes_read=0
    while bytes_read < msg_length:
        tmp = sock.recv(min(MAX_READ, msg_length-bytes_read))
        #tmp = inner_recv(sock, min(MAX_READ, msg_length-bytes_read), 5)
        data += tmp
        bytes_read += len(tmp)
    assert bytes_read == msg_length

    json1 = perf_counter()
    msg = json.loads(data.decode('utf8'))
    print("JSON: ", perf_counter() - json1)
    return msg