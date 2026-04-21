# Ozone Multi-Raft Design Document

## Abstract

This document proposes a multi-raft architecture for Apache Ozone's Ozone Manager (OM) to improve write throughput and scalability by distributing bucket write requests across multiple independent RAFT groups, eliminating the single-leader bottleneck in the current architecture.

## Background

### Current Architecture Limitations

Apache Ozone currently uses a single RAFT consensus group for the Ozone Manager (OM) in high availability (HA) deployments. While this provides strong consistency and automatic failover, it has several limitations:

1. **Single Leader Bottleneck**: All write operations must go through a single OM leader, limiting write throughput regardless of the number of OM replicas
2. **RAFT Log Contention**: A single RAFT log serializes all metadata updates, creating a scalability bottleneck
3. **Resource Underutilization**: In a 3-node OM cluster, only one node actively processes write requests
4. **Limited Horizontal Scalability**: Adding more OM nodes improves read capacity (with follower reads) but not write capacity

### Scalability Requirements

As Ozone deployments grow to support:
- Thousands of buckets across multiple volumes
- Millions of concurrent client operations
- Petabytes of data with billions of objects

The current single-raft architecture becomes a significant bottleneck for metadata operations.

## Goal
**Improve Write Throughput**: Distribute write load across multiple RAFT leaders to achieve near-linear scaling with the number of OM nodes

## Architecture

### High-Level Design

The multi-raft architecture partitions buckets write request across a configurable number of RAFT groups (default: 6). Each RAFT group:
- Has its own RAFT leader, followers, and log
- Processes write requests independently and in parallel
- Uses the same OM nodes but with different leaders

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Application                        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
         ┌─────────────────────────┐
         │  OzoneClient Library    │
         │  - Bucket→OMProxy Cache │
         │  - Routing Logic        │
         └───────────┬─────────────┘
                     │
        ┌────────────┼────────────┐
        │            │            │
        ▼            ▼            ▼
   ┌────────┐  ┌────────┐  ┌────────┐
   │ OM1    │  │ OM2    │  │ OM3    │
   │        │  │        │  │        │
   │ Group1 │  │ Group1 │  │ Group1 │
   │ Leader │  │Follower│  │Follower│
   │        │  │        │  │        │
   │ Group2 │  │ Group2 │  │ Group2 │
   │Follower│  │ Leader │  │Follower│
   │        │  │        │  │        │
   │ Group3 │  │ Group3 │  │ Group3 │
   │Follower│  │Follower│  │ Leader │
   │        │  │        │  │        │
   │ Group4 │  │ Group4 │  │ Group4 │
   │ Leader │  │Follower│  │Follower│
   │   ...  │  │   ...  │  │   ...  │
   └────────┘  └────────┘  └────────┘
```

### Bucket To RAFT-group Assignment

#### Mechanism for assigning buckets to RAFT groups:
1. Client's write request to bucket sent to specific OM node
2. OM node extracts bucket path from request
3. OM node tries to determine RAFT group for bucket:
   - If bucket already assigned to the group, use existing group assignment
   - If doesn't, selects most underutilized RAFT group and assigns bucket to that group
4. Assignment stored in bucket metadata and client cache for future requests
5. If client sends request to non-leader OM for that bucket, OM responds with OMNotLeaderException including correct leader info for client cache update
6. Following requests for that bucket routed directly to correct OM leader for its RAFT group

#### Assignment Metadata

The bucket-to-group assignment is stored:
1. **In Bucket Metadata** (RocksDB): Each bucket stores its assigned RAFT group ID
2. **Client-Side Cache**: Clients cache the mapping of buckets to OM instances to avoid repeated lookups
3. **OM Request Context**: The OMRequest protobuf includes routing hints (raftGroupId that the request should be handled in)

### Request Routing

#### Write Path

```
1. Client: Create key in bucket "vol1/bucket1"
2. OzoneClient checks cache for bucket→group mapping
   - Cache miss:
     - make request to proposed OM node
     - if OMNonLeaderException received, extract leaderOMNodeId from response
     - update cache with bucket→leaderOMNodeId mapping
   - Cache hit: Use cached OM Proxy
3. OzoneRetryInvocationHandler.invokeMethod():
   - Extract bucket path from OMRequest
   - Call proxyProvider.selectProxyInfo(bucketPath)
   - Route to appropriate OM leader for that RAFT group
4. OM Leader processes write through BucketStateMachine
5. RAFT replication to followers
```

### Component Architecture

#### 1. BucketStateMachine

New state machine for multi-raft groups:

```java
public class BucketStateMachine extends BaseStateMachine {
  private final RaftGroupId currentRaftGroupId;
  private final OzoneManagerDoubleBuffer ozoneManagerDoubleBuffer;
  private final ExecutorService executorService;
  private final RequestHandler handler;

  @Override
  public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
    // 1. Acquire semaphore permit
    ozoneManagerDoubleBuffer.acquireUnFlushedTransactions(1);

    // 2. Process request asynchronously
    CompletableFuture<OMResponse> future = CompletableFuture.supplyAsync(
        () -> runCommand(request, termIndex), executorService);

    // 3. Add response to double buffer
    future.thenApply(omResponse -> {
      ozoneManagerDoubleBuffer.add(omClientResponse, termIndex);
      return ratisFuture;
    });
  }
}
```

**Key Features**:
- One instance per RAFT group
- Independent double buffer for parallel flushing
- Separate executor service for transaction processing
- Per-group semaphore for flow control


#### 2. OzoneRetryInvocationHandler

Handles multi-raft request routing:

```java
protected Object invokeMethod(Method method, Object[] args) throws Throwable {
  T proxy = null;
  if (args.length == 2 && args[1] instanceof OMRequest) {
    // Extract bucket path from OMRequest
    String bucketPath = ((OMFailoverProxyProviderBase)proxyDescriptor.getProxyProvider())
        .getWriteRequestBucketPath((OMRequest) args[1]);

    if (bucketPath != null) {
      // Select the correct OM proxy based on bucket's RAFT group
      proxy = (T) ((OMFailoverProxyProviderBase) proxyDescriptor.getProxyProvider())
          .selectProxyInfo(bucketPath);
    }
  }

  if (proxy == null) {
    proxy = proxyDescriptor.getProxy(); // Fallback to default
  }

  return method.invoke(proxy, args);
}
```

#### 4. HadoopRpcOMFailoverProxyProvider

Extended to support multi-raft routing:

```java
public String getWriteRequestBucketPath(OMRequest omRequest) {
  // Extract bucket path from various request types
  if (omRequest.hasCreateKeyRequest()) {
    KeyArgs keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
    return keyArgs.getVolumeName() + "/" + keyArgs.getBucketName();
  }
  // Similar for other request types...
}

public Object selectProxyInfo(String bucketPath) {
  // Determine RAFT group from bucket path
  RaftGroupId groupId = omRaftGroupManager.getRaftGroupIdForBucket(bucketPath);
  // Return proxy pointing to current leader of that group
  return getProxyForBucket(bucketPath);
}
```

### Configuration

#### Core Multi-Raft Configuration

```xml
<!-- Enable multi-raft feature -->
<property>
  <name>ozone.om.multi.raft.bucket.enabled</name>
  <value>true</value>
  <description>
    Enable multi-raft bucket metadata distribution across multiple RAFT groups.
    When enabled, bucket metadata is partitioned across the configured number
    of RAFT groups for improved write throughput and scalability.
    Default: false
  </description>
</property>

<!-- Number of RAFT groups -->
<property>
  <name>ozone.om.multi.raft.bucket.groups</name>
  <value>6</value>
  <description>
    Number of RAFT groups for bucket metadata partitioning. Each RAFT group
    has its own leader, followers, and log, allowing parallel write processing.
    Recommended values: 3, 6, 12, 24 based on cluster size and load.
    Higher values provide better parallelism but increase resource usage.
    Default: 6
  </description>
</property>
```

#### Safe Mode Configuration

```xml
<property>
  <name>ozone.om.safemode.enabled</name>
  <value>true</value>
  <description>
    Enable safe mode for OzoneManager during startup. When enabled, OM enters
    safe mode until all RAFT groups are healthy and synchronized. In multi-raft
    deployments, this ensures all RAFT groups are available before accepting writes.
    Default: true
  </description>
</property>
```

#### RAFT Group Reconciliation

```xml
<property>
  <name>ozone.om.bucket.raft.groups.reconciler.interval</name>
  <value>60s</value>
  <description>
    Interval at which the bucket RAFT groups reconciler runs. The reconciler
    verifies the health state of all RAFT groups and recreates any unhealthy
    or missing groups. This ensures the configured number of RAFT groups is
    maintained and all groups are in a healthy state.
    Default: 60s
  </description>
</property>

<property>
  <name>ozone.om.ratis.unhealthy.peer.timeout</name>
  <value>30s</value>
  <description>
    Timeout duration to consider a RAFT peer unhealthy. If a RAFT peer
    doesn't respond within this timeout, it's marked as unhealthy and
    the reconciler may take corrective action.
    Default: 30s
  </description>
</property>
```

#### Leadership Balancing Configuration

```xml
<property>
  <name>ozone.om.multi.raft.bucket.group.transfer.leader.timeout</name>
  <value>1s</value>
  <description>
    Timeout for transferring RAFT group leadership from one OM node to another.
    Used by the leadership balancer to distribute RAFT group leaders evenly
    across OM nodes for optimal resource utilization.
    Default: 1s
  </description>
</property>

<property>
  <name>ozone.om.multi.raft.bucket.group.transfer.leader.initial.delay</name>
  <value>30s</value>
  <description>
    Initial delay before starting the leadership balancer service. This delay
    allows the cluster to stabilize after startup before attempting to balance
    leadership distribution across OM nodes.
    Default: 30s
  </description>
</property>

<property>
  <name>ozone.om.multi.raft.bucket.group.transfer.leader.period</name>
  <value>60s</value>
  <description>
    Period at which the leadership balancer runs to redistribute RAFT group
    leaders across OM nodes. The balancer ensures each OM node leads approximately
    the same number of RAFT groups, preventing resource imbalance.
    Default: 60s
  </description>
</property>
```

### Leadership Balancing

To prevent all RAFT groups from having leaders on the same OM node:

```java
public class OmRaftGroupsLeadershipBalancer {

  /**
   * Ensures RAFT group leaders are distributed across OM nodes.
   * Target: Each OM node should be leader for ~equal number of groups.
   */
  public void balanceLeadership() {
    Map<String, Integer> nodeToLeaderCount = getCurrentLeaderDistribution();

    // If imbalance detected (max - min > threshold)
    if (isImbalanced(nodeToLeaderCount)) {
      // Transfer leadership from overloaded to underloaded nodes
      for (RaftGroupId group : getOverloadedGroups()) {
        String targetNode = selectUnderloadedNode();
        transferLeadership(group, targetNode);
      }
    }
  }
}
```

**Balancer Strategy**:
- Runs periodically (default: every 5 minutes)
- Transfers leadership via RAFT `transferLeadership()` API
- Considers node health and load
- Graceful transfers to avoid disruption

### Group Reconciliation

Provide configured count of required bucket RAFT-groups and periodically verify a health state of the groups:

```java
public class BucketRaftGroupsReconciler {

  /**
   * Periodically verifies and corrects bucket→group assignments.
   */
  public void reconcile() {
    List<RaftGroup> existingRaftGroups = (List<RaftGroup>) omRatisServer.getServer().getGroups();
    if (existingRaftGroups.size() == 1) {
      List<RaftGroupId> raftGroupIds = generateRaftGroups(currentMultiRaftTerm, expectedRaftGroupsCount);
      ozoneManager.createRaftGroups(raftGroupIds.stream().map(RaftId::getUuid).collect(Collectors.toList()), true);
    } else {
      for (RaftGroup raftGroup : existingRaftGroups) {
        checkHealthStateAndRecreateIfNeeded(raftGroup);
      }
    }
  }
}
```

## Upgrade Path

### From Single-Raft to Multi-Raft

**Preparation Phase**:
1. Upgrade OM nodes to version supporting multi-raft (rolling upgrade)
2. Set `ozone.om.multi.raft.bucket.enabled=false` initially
3. Verify all nodes running new version

**Enablement Phase**:
1. Stop all OM nodes gracefully
2. Set `ozone.om.multi.raft.bucket.enabled=true`
3. Set `ozone.om.multi.raft.bucket.groups=6`
4. Start OM nodes

### Rollback Procedure

If issues arise:
1. Stop all OM nodes
2. Set `ozone.om.multi.raft.bucket.enabled=false`
3. System operates as single-raft again

## Performance Considerations

### Expected Performance Improvements

With 6 RAFT groups on 3 OM nodes:
- **Write Throughput**: ~3x improvement (near-linear with OM node count)
- **Latency**: Unchanged (still single RAFT round-trip per operation)
- **CPU Utilization**: More balanced across OM nodes
- **Memory**: Slightly higher (6x double buffers, 6x thread pools)

### Resource Requirements

Per OM node with 6 RAFT groups:
- **Threads**:
  - 6 flush daemon threads (OzoneManagerDoubleBuffer)
  - 6 StateMachineUpdater threads
  - 6 apply transaction executors
  - ~50-60 additional threads total
- **Memory**:
  - 6x double buffer queues (~100MB per buffer at capacity)
  - 6x RAFT log caches
  - Estimated: +1-2 GB per OM node
- **Disk I/O**: Distributed across RAFT groups (reduced contention on RocksDB)

### Tuning Parameters

For optimal performance, consider adjusting these parameters based on your workload:

#### For High-Load Clusters

```xml
<!-- Increase unflushed transaction buffer for multi-raft -->
<property>
  <name>ozone.om.ratis.server.max-unflushed-transaction-count</name>
  <value>50000</value>
  <description>
    Higher values allow more buffering per RAFT group, reducing backpressure
    under high load. Monitor unflushed transaction metrics to tune this value.
  </description>
</property>

<!-- More RAFT groups for large clusters -->
<property>
  <name>ozone.om.multi.raft.bucket.groups</name>
  <value>12</value>
  <description>
    For clusters with 5+ OM nodes or very high write workloads, increase
    the number of RAFT groups to improve parallelism. Ensure you have
    sufficient CPU and memory resources on OM nodes.
  </description>
</property>

<!-- Balance CPU vs latency -->
<property>
  <name>ozone.om.ratis.server.request.timeout</name>
  <value>60s</value>
  <description>
    Increase timeout for clusters with high latency or slow RocksDB writes.
    Default is 3000ms which may be too aggressive for loaded clusters.
  </description>
</property>
```

#### Leadership Balancing Tuning

```xml
<!-- More aggressive balancing for dynamic workloads -->
<property>
  <name>ozone.om.multi.raft.bucket.group.transfer.leader.period</name>
  <value>30s</value>
  <description>
    Reduce period for more frequent rebalancing in clusters with frequent
    OM node additions/removals or uneven load distribution. Default: 60s
  </description>
</property>

<!-- Faster initial balancing -->
<property>
  <name>ozone.om.multi.raft.bucket.group.transfer.leader.initial.delay</name>
  <value>10s</value>
  <description>
    Reduce delay for faster initial balancing after cluster startup.
    Useful in test/dev environments. Default: 30s
  </description>
</property>
```

#### Reconciliation Tuning

```xml
<!-- More frequent health checks -->
<property>
  <name>ozone.om.bucket.raft.groups.reconciler.interval</name>
  <value>30s</value>
  <description>
    Reduce interval for more frequent RAFT group health checks and faster
    detection of unhealthy groups. Default: 60s
  </description>
</property>

<!-- More conservative peer timeout -->
<property>
  <name>ozone.om.ratis.unhealthy.peer.timeout</name>
  <value>60s</value>
  <description>
    Increase timeout to avoid false positives in high-latency or loaded
    clusters. Default: 30s
  </description>
</property>
```

**Tuning Guidelines**:

- Start with defaults and monitor metrics
- Increase `max-unflushed-transaction-count` if you see frequent backpressure
- Increase `bucket.groups` only if CPU utilization on OM nodes is low (< 50%)
- Adjust balancer period based on cluster stability (stable = longer period)
- Monitor leadership distribution via metrics to validate balancer effectiveness


## Monitoring and Observability

### Metrics

Per RAFT Group:
- `omha_metrics_ozone_manager_bucket_raft_group_leader_state{nodeid="<omNodeId>",raftgroupid="<raftGroupId>",hostname="<omNodeHostname>"}` - raft group leadership state
- `omha_metrics_ozone_manager_raft_group_leader_state{nodeid="<omNodeId>",raftgroupid="<mainRaftGroupId>",hostname="<omNodeHostname>"}` - main raft group leadership state

Global:
- `omha_multi_raft_metrics_raft_groups_count`
- `omha_multi_raft_metrics_raft_groups_expected_count`
- `omha_multi_raft_metrics_om_in_safe_mode`
