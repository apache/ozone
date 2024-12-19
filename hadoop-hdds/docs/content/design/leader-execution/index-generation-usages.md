
## Index generation
In old flow, ratis index is used for `object Id` of key and `update Id` for key update.
For new flow, it will not depend on ratis index, but will have its own **`managed index`**.

Index initialization / update:
- First time startup: 0
- On restart (leader): last preserved index + 1
- On Switch over: last index + 1
- Request execution: index + 1
- Upgrade: Last Ratis index + 1


## Index Persistence:

Index Preserved in TransactionInfo Table with new KEY: "#KEYINDEX"
Format: <timestamp>#<index>
Time stamp: This will be used to identify last saved transaction executed
Index: index identifier of the request

Sync the Index to other nodes:
Special request body having metadata: [Execution Control Message](request-persist-distribution.md#control-request).


## Step-by-step incremental changes for existing flow

1. for increment changes, need remove dependency with ratis index. For this, need to use om managed index in both old and new flow.
2. objectId generation: need follow old logic of index to objectId mapping.

