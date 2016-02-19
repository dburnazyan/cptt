# Ceph performance testing tool (cptt)

CPTT was created for testing Ceph object storage through librados library.

Simple to use:

0. Build: install.sh
1. Launch: ```cptt 7777 &```
2. Start test: ```cat workload-definition.json | nc localhost 7777```
3. Check status: ```echo "Status" | nc localhost 7777``` -> Whait for "Ready" status
4. View results in results.csv file in working dir

To cancel test: ```echo "Cancel" | nc localhost 7777```

To shutdown: ```echo "Shutdown" | nc localhost 7777```

Sample workload-definition.json file:

```json
{
 "mon_host":"1.2.3.4",
 "cluster_name":"ceph",
 "user":"client.admin",
 "key":"AQBV9qBWABBBBBAAZHxjNs9sWgB4KZuG3rDHlw==",
 "pool_name":"testpool_1",
 "start_at":1454523048,
 "works":
   [
    {
      "thread_count":4,
      "total_ops":1000,
      "op_type":1,
      "object_size":80000000
    },
    {
      "thread_count":8,
      "total_ops":2000,
      "op_type":2,
      "object_size":40000000
    }
  ]
}
```
op_type: 1 - write, 2 - read, 3 - remove.

object_size - in bytes.

stat_at - timestamp when test will start.
