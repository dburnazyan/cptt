# Ceph performance testing tool (cptt)

CPTT was created for testing Ceph object storage through librados library.

Simple to use:

1. Launch: ```cptt -p 7777 &```
2. Start test: ```cat workload-definition.json | nc localhost 7777```
3. Check status: ```echo "Status" | nc localhost 7777```
4. Cancel test: ```echo "Cancel" | nc localhost 7777```
5. Shutdown: ```echo "Shutdown" | nc localhost 7777```

Sample workload-definition.json file:

```json
{
 "pool_name":"testpool_1",
 "start_at":1454523048,
 "works":
   [
    {
      "thread_count":4,
      "obj_start_num":1,
      "obj_end_num":10000,
      "op_type":1,
      "object_size":80000000
    }
  ]
}
```
