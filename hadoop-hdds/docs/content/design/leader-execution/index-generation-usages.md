
## Index generation
In old flow, ratis index is used for `object Id` of key and `update Id` for key update.
For new flow, it will not depend on ratis index, but will have its own managed index (`leader index`).

Leader index initialization / update:
- First time startup: 0
- On restart (leader): last preserved index + 1
- On Switch over: last index + 1
- Request execution: index + 1
- Upgrade: Last Ratis index + 1


## Index Persistence:

Index Preserved in TransactionInfo Table with new KEY: "#KEYINDEX"
Format: <timestamp>#<index>
Time stamp: This will be used to identify last saved transaction executed
Index: leader index for the request

Sync the Index to other nodes:
Special request body having metadata: ExecutionControlRequest.

```
message ExecutionControlRequest {
  repeated ClientRequestInfo requestInfo = 1;
}
message ClientRequestInfo {
  optional string uuidClientId = 1;
  optional uint64 callId = 2;
  optional OMResponse response = 3;
  optional int64 index = 4;
  optional unin64 timestamp = 5;
}
```

Eg:
```
OMRequest.Builder omReqBuilder = OMRequest.newBuilder().setPersistDbRequest(reqBuilder.build())
.setExecutionControlRequest(controlReq.build())
.setCmdType(OzoneManagerProtocolProtos.Type.PersistDb)
.setClientId(lastReqCtx.getRequest().getClientId());
```

## Index usages
Index currently used for `object Id` of key and `update Id`. UpdateId reflect changes done to the existing object.
This is done by updating the updateId with transactionId. This also handled replay of raft-log by checking transactionId increase.

This case is no more required as the request is executed at leader node. It can be simply increment of previous value.

`updateId = previous updateId + 1`

## Step-by-step incremental changes for existing flow

1. for increment changes, need remove dependency with ratis index. For this, need to use leader index in both old and new flow.
2. objectId generation: need follow old logic of index to objectId conversion.

