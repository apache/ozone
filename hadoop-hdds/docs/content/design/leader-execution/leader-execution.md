# Background

Here is the summary of the challenges:

- The current implementation depends on consensus on the order of requests received and not on consensus on the processing of the requests.
- The double buffer implementation currently is meant to optimize the rate at which writes get flushed to RocksDB but the effective batching achieved is 1.2 at best. It is also a source of continuous bugs and added complexity for new features.
- The number of transactions that can be pushed through Ratis currently caps out around 25k.
- The Current performance envelope for OM is around 12k transactions per second. The early testing pushes this to 40k transactions per second.

## Execution at leader node needs deal with below cases
1. Parallel execution: ratis serialize all the execution in order. With control, it is possible to execute the request in parallel which are independent.
2. Optimized locking: Locks are taken at bucket level for both read and write flow. Here, focus to remove lock between read and write flow, and have more granular locking.
3. Cache Optimization: Cache are maintained for write operation and read also make use of same for consistency. This creates complexity for read to provide accurate result with parallel operation.
4. Double buffer code complexity: Double buffer provides batching for db update. This is done with ratis state machine and induces issues managing ratis state machine, cache and db updates.
5. Request execution flow optimization: Optimize request execution flow, removing un-necessary operation and improve testability.
6. Performance and resource Optimization: Currently, same execution is repeated at all nodes, and have more failure points. With leader side execution and parallelism, need improve performance and resource utilization.

### Object ID generation
Currently, the Object ID is tied to Ratis transaction metadata. This has multiple challenges in the long run.

- If OM adopts multi Ratis to scale writes further, Object IDs will not longer be unique.
- If we shard OM, then across OMs the object ID will not be unique.
- When batching multiple requests, we cannot utilize Ratis metadata to generate object IDs.

Longer term, we should move to a UUID based object ID generation. This will allow us to generate object IDs that are globally unique. In the mean time, we are moving to a persistent counter based object ID generation. The counter is persisted during apply transaction and is incremented for each new object created.

## Prototype Performance Result:

| sno | item                                     | old flow result               | leader execution result |
|-----|------------------------------------------|-------------------------------|------------------------|
| 1   | Operation / Second (key create / commit) | 12k+                          | 40k+                   |
| 2   | Key Commit / Second                      | 5.9k+                         | 20k+ (3.3 times)       |
| 3   | CPU Utilization Leader | 16% (unable to increase load) | 33%                    |
| 4   | CPU Utilization Follower | 6% above                      | 4% below               |

Refer [performance prototype result](performance-prototype-result.pdf)

# Leader execution

![high-level-flow.png](high-level-flow.png)

Client --> OM --> Gatekeeper ---> Executor --> Batching (ratis request) --{Ratis sync to all nodes}--> apply transaction {db update}


### Gatekeeper
Gatekeeper act as entry point for request execution. Its function is:
1. orchestrate the execution flow
2. granular locking
3. execution of request
4. validate om state like upgrade
5. update metrics and return response
6. handle client replay of request
7. managed index generation (remove dependency with ratis index for objectId)

### Executor
This prepares context for execution, process the request, communicate to all nodes for db changes via ratis and clearing up any cache.

### Batching (Ratis request)
All request as executed parallel are batched and send as single request to other nodes. This helps improve performance over network with batching.

### Apply Transaction (via ratis at all nodes)
With new flow as change,
- all nodes during ratis apply transaction will just only update the DB for changes.
- there will not be any double buffer and all changes will be flushed to db immediately.
- there will be few specific action like snapshot creation of db, upgrade handling which will be done at node. 

## Description

### Index generation
refer [index generation and usages](index-generation-usages.md)

### No-Cache for write operation

In old flow, a key creation / updation is added to PartialTableCache, and cleanup happens when DoubleBuffer flushes DB changes.
Since DB changes is done in batches, so a cache is maintained till flush of DB is completed. Cache is maintained so that OM can serve further request till flush is completed.

This adds complexity during read for the keys, as it needs ensure to have the latest data from cache or DB.
Since there can be parallel operation of adding keys to cache, removal from cache and flush to db, this induces bug to the code if this is not handled properly.

For new flow, partial table cache is removed, and changes are visible as soon as changes are flushed to db.
For this to achieve,
- granular locking for key operation to avoid parallel update till the existing operation completes. This avoids need of cache as data is available only after changes are persisted.
- Double buffer operation removal for the flow, flush is done immediately before response is returned. This is no more needed as no need to serve next request as current reply is not done.
- Bucket resource is handled in such a way that its visible only after db changes are flushed. This is required as quota is shared between different keys operating parallel.
Note: For incremental changes, quota count will be available immediately for read for compatibility with older flow till all flows are migrated to new flow.

### Quota handling

refer [bucket quota](bucket-reserve-quota.md)

### Granular locking
Gateway: Perform lock as per below strategy for OBS/FSO
On lock success, trigger execution of request to respective executor queue

#### OBS Locking
refer [OBS locking](obs-locking.md)

#### FSO Locking
TODO

Challenges compared to OBS,
1. Implicit directory creation
2. file Id depends on parent directory /<volId>/<bucketId>/<parent ObjectId>/<file name>
So due to hierarchy in nature and parallel operation at various level, FSO locking is more complicated.

#### Legacy Locking:
Not-in-scope

### Optimized new flow

Currently, a request is handled as:
- Pre-execute: does request static validation, authorization
- validateAndUpdateCache: locking, handle request, update cache
- Double buffer to update DB using cache happening in background

Request execution Template: every request handling need follow below template of request execution.

- preProcess: basic request validation, update parameter like user info, normalization of key
- authorize: perform ranger or native acl validation
- lock: granular level locking
- unlock: unlock locked keys
- process: process request like:
  - Validation after lock like bucket details
  - Retrieve previous key, create new key, quota update, and so on
  - Record changes for db update
  - Prepare response
  - Audit and logging
  - Metrics update
- Request validator annotation: similar to existing, where compatibility check with ozone manager feature version and client feature version, and update request to support compatibility if any.

Detailed request processing:
OBS:
- [Create key](request/obs-create-key.md)
- [Commit key](request/obs-commit-key.md)

### Execution persist and distribution

refer [request-process-distribution](request-persist-distribution.md)

### Replay of client request handling

refer [request-replay-handling](request-replay.md)

### Testability framework

With rework on flow, a testability framework for better test coverage for request processing.

Complexity in existing framework for request:
1. Flow handling is different 1 node / 3 node HA deployment
2. Check for double buffer cache flush
3. cache related behaviour testing
3. Ratis sync and failure handling
4. Too much mocking for unit testing

Proposed handling:
Since execution is leader side and only db update is synchronized to all other nodes, so unit test will focus behavior, but not on env.

1. Test data preparation
  - Utility to prepare Key, File, volume, bucket
  - Insert to DB to different table
2. Short-circuit for db update (without ratis)
3. simplified mocking for ranger authorization (just mock a method)
4. behavior test from End-to-End perspective

This will have the following advantages:
- Speed of test scenarios (sync wait and double buffer flush wait will be avoided)
- optimized test cases (avoid duplicate test cases)
- test code complexity will be less

TODO: With echo, define test utils and sample test cases
- Capture test cases based on behavior.

## Step-by-step integration of existing request (interoperability)

Leader side execution have changes for all flows. This needs to be done incrementally to help better quality, testability.
This needs below integration points in current code:
1. dependency over ratis index removal (for old flow also)
2. bucket quota handling integration (such that old flow have no impact)
3. Granular locking for old flow, this ensures `no cache` for new flow do not have impact
4. OmStateMachine integration for new flow, so that both old and new flow can work together
5. Request segregation for new flow which is incrementally added.

With above, Enable for old and new flow execution will be done with Feature flag, to switch between them seamlessly.
And old flow can be removed with achieving quality, performance and compatibility for new flow execution.

## Impacted areas
1. With Leader side execution, metrics and its capturing information can change.
   - Certain metrics may not be valid
   - New metrics needs to be added
   - Metrics will be updated at leader side now like for key create. At follower node, its just db update, so value will not be udpated.
