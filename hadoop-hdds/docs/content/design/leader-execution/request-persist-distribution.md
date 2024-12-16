## Request persistence and distribution

There are multiple features having different api. Handling of those api at leader and other nodes can differ, like:
- key creation: key can be prepared to operate at leader and the changes only needs to be updated to db at all nodes.
- key deletion: key needs to be moved to deleted table for soft delete, so preparation can be done at leader, but movement to be done in db for all nodes.
- snapshot creation: involves rocksdb checkpoint creation, which needs to be executed only at all nodes. This can have basic preparation at leader side.
- upgrade: all nodes need update bookkeeping for the operation at all nodes.

So based on different nature and optimization of request, flexibility is required for handling such request at leader and to the nodes.

```
message ExecutionControlRequest {
  repeated ClientRequestInfo requestInfo = 1;
  Optional int32 action = 2;
}
```

actions:
- PersistDb
- CreateSnapshot
- DeleteSnapshot
- UpgradePrepare
- UpgradeCancel

Message format
```
message PersistDbRequest {
  repeated PersistDbInfo info = 1;
  repeated DBBucketQuotaUpdate bucketQuotaUpdate = 2;
}

message PersistDbInfo {
  optional ClientRequestInfo clientInfo = 1;
  optional int64 index = 2;
  repeated DBTableUpdate tableUpdates = 3;
}
message ClientRequestInfo {
  optional string uuidClientId = 1;
  optional uint64 callId = 2;
  optional unint64 timestamp = 5;
  optional OMResponse response = 3;
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
