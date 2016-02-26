#!/usr/bin/python

import socket
import sys
import time
import json

node_list={}
node_list['cz7644']={'ip':'172.16.164.80', 'port':7777}
node_list['cz7645']={'ip':'172.16.164.81', 'port':7777}
node_list['cz7646']={'ip':'172.16.164.82', 'port':7777}


def send_message(node, message):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_addres = (node['ip'], node['port'])
    sock.connect(server_addres)
    sock.sendall(message)
    result_string=''
    while 1:
        data = sock.recv(1000)
        if not data: break
        result_string += data
    sock.close()
    return result_string

def main():
    print 'waiting nodes'
    for node in node_list:
        while 1:
            status = send_message(node_list[node],  'Status')
            if status == 'Ready': break
            if status == 'Finished': send_message(node_list[node], 'Cancel')
            time.sleep(5)
        print 'node' + node + ' is ready for test.'
    print 'starting'
    with open('workload-definition.json', 'r') as content_file:
        workload = json.loads(content_file.read())
    
    workload['start_at']=int(time.strftime("%s"))+30

    for node in node_list:
        workload['node_name']=node
        send_message(node_list[node], json.dumps(workload))

    for node in node_list:
        while 1:
            status = send_message(node_list[node], 'Status')
            if status == 'Finished': break
            time.sleep(5)
    
    result_file = open("result.csv", "w")
    result_file.write('Node name,Work name,Thread ID,Object number,Op. type,Start time,Duration,Object size,Status\n')
    for node in node_list:
        results = send_message(node_list[node], 'Receive')
        result_file.write(results)
    result_file.close()

if __name__ == "__main__":
    print 'hello'
    main()
