# Ceph performance testing tool (cptt)

CPTT was created for testing Ceph object storage through librados library.

Simple to use:

1. Launch: ```cptt -p 7777 &```
2. Start test: ```cat workload-definition.json | nc localhost 7777```
3. Check status: ```echo "Status" | nc localhost 7777```
4. Cancel test: ```echo "Cancel" | nc localhost 7777```
5. Shutdown: ```echo "Shutdown" | nc localhost 7777```
