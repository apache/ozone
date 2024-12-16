## Request Replay from client

In old flow, request is submitted to ratis directly with ClientId and CallId (From HadoopRPC callback), and same used by ratis to handle request replay (or retry).
If ratis finds the another request with same ClientId and CallId, it returns response of last request itself matching.
Additionally, mapping of ClientId and CallId is present in memory and gets flushed out on restart, so it does not have consistent behaviour for handling same.

For new flow, since only db changes are added to ratis, so this mechanism can not be used. To handle this, need to have similar mechanism, and improve over persistence for replay.

`Client Request --> Gatekeeper --> check for request exist in cache for ClientId and CallId
--> If exist, return cached response
--> Else continue request handling`

### Client request replay at leader node
- When request is received at leader node, it will cache the request in replayCache immediately
- When request is received again with same ClientId and CallId, it will check replayCache and if entry exist,
  - If response in cache is not available (request handling in progess), wait for availability and timeout - 60 sec
  - If response available, return immediately
- If entry does not exist, it will process handling request normally


### Replay cache distribution to other nodes
Request - response will be cached to other node via ratis distribution
    - It will be added to memory cache with expiry handling
    - Also will be added to DB for persistence for restart handing

Below information will be sync to all nodes via ratis:
```
message ClientRequestInfo {
  optional string uuidClientId = 1;
  optional uint64 callId = 2;
  optional unint64 timestamp = 5;
  optional OMResponse response = 3;
}
```

DB Persistence:
```
New rocksDB table: ClientRequestReplyTable
Key: String: ClientId#CallId
Value: ClientRequestInfo
```

### Memory caching:
```
Memory Map: ClientId#CallId Vs Response
Expiry: 10 minute (as current default for ratis)
```

Memory expiry handling:
```
Handling cleanup of expired entry:
Timer: 30 sec
RequestReply Bucket Map:
Time vs List<ClientId#CallId>, where Time is with granularity of 10 sec
```

Steps:
- Retrieve all bucket for time < (currTime - 10min as expiry)
- Delete from DB and Map for the entry
