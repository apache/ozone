# Request persistence and distribution

## Change recorder

When a request is executed, the changes to be persisted needs to be captured and send to all nodes to persist to DB.
To abstract how the changes are cached and transferred with respect to request execution, Change recorder will provider below interfaces,
1. add(table, key, value)
2. remove(table, key, value)
3. addBucketQuota(index, bucketInfo, incBytes, incNamespace)
4. ...

## Change distribution to nodes

There are multiple features having different api. Handling of those api at leader and other nodes can differ, like:
- key creation: key can be prepared to operate at leader and the changes only needs to be updated to db at all nodes.
- key deletion: key needs to be moved to deleted table for soft delete, so preparation can be done at leader, but movement to be done in db for all nodes.
- snapshot creation: involves rocksdb checkpoint creation, which needs to be executed only at all nodes. This can have basic preparation at leader side.
- upgrade: all nodes need update bookkeeping for the operation at all nodes.

So based on different nature and optimization of request, flexibility is required for handling such request at leader and to the nodes.

### Control Request

Control request provides capability for flexibility to the other nodes for way of further execution for cases given above.
This is additional information send with request send to other nodes via ratis. This can be further extended for various purpose on need basis.

Example:
1. DBPersisRequest; This will hold information about data changes to be persisted to db directly, like table, key, value for add
2. Control Request: This will provide additional information such as index, client request info for replay handling

```
message ExecutionControlRequest {
  repeated ClientRequestInfo requestInfo = 1;
}

message ClientRequestInfo {
  optional string uuidClientId = 1;
  optional uint64 callId = 2;
  optional unint64 timestamp = 5;
  optional OMResponse response = 3;
}
```

This control message and request can be embedded to same Proto request body:
```
OMRequest.Builder omReqBuilder = OMRequest.newBuilder()

// control message
.setExecutionControlRequest(controlReq.build())

// db change message
.setPersistDbRequest(reqBuilder.build())
.setCmdType(OzoneManagerProtocolProtos.Type.PersistDb)
.setClientId(lastReqCtx.getRequest().getClientId());
```


request Command Type (message to be send to all nodes via ratis):
- PersistDb
- CreateSnapshot
- DeleteSnapshot
- UpgradePrepare
- UpgradeCancel

### DB Persist Request capturing db changes to send via ratis

DB persist message will capture:
- table record add and delete
- move of record from one table to other table
- bucket quota changes

Message format
```
message PersistDbRequest {
  repeated PersistDbInfo info = 1;
}

message PersistDbInfo {
  optional int64 index = 1;
  repeated DBTableUpdate tableUpdates = 2;
  repeated DBTableMove tableMoves = 3;
  repeated DBBucketQuotaUpdate bucketQuotaUpdate = 4;
}

message DBBucketQuotaUpdate {
required string volName = 1;
required string bucketName = 2;
required int64 diffUsedBytes = 3;
required int64 diffUsedNamespace = 4;
optional uint64 bucketObjectId = 5;
}
message DBTableMove {
  optional string srcTableName = 1;
  optional string destTableName = 2;
  optional string srcKey = 3;
  optional string destKey = 4;
}
message DBTableUpdate {
  required string tableName = 1;
  repeated DBTableRecord records = 2;
}
message DBTableRecord {
  required bytes key = 1;
  optional bytes value = 2;
}
```
