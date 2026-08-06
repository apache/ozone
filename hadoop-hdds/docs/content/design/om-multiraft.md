---
title: OM Multiraft Design Document
summary: Design for the OM multi-raft architecture to improve write throughput and scalability by distributing bucket write requests across multiple independent RAFT groups.
date: 2026-04-21
jira: HDDS-15069
status: in review
author: Slava Tutrinov
---
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

New state machine for multi-raft groups. 

**Key Features**:
- One instance per RAFT group
- Independent double buffer for parallel flushing
- Separate executor service for transaction processing
- Per-group semaphore for flow control


#### 2. OzoneRetryInvocationHandler

Handles multi-raft request routing. Request has a hint of the bucket path, which is used to determine the correct 
RAFT group and leader OM node.


#### 4. HadoopRpcOMFailoverProxyProvider

Extended to support multi-raft routing. As like as the previous one, it maintains a list of OM proxies and uses 
the bucket path to select the correct proxy for the RAFT group leader.

### Configuration

#### Core Multi-Raft Configuration

1. Switch to enable multi-raft feature
2. Number of raft groups to handle bucket write requests

#### Safe Mode Configuration

Switch to enable OM safe mode (OM requests are not available until all RAFT groups are healthy)

#### RAFT Group Reconciliation

Configuration to properties for periodic reconciliation of bucket→RAFT group assignments and health checks.

#### Leadership Balancing Configuration

Bucket RAFT-groups leadership should be balanced across OM nodes. Configuration properties to control balancing frequency and thresholds.

### Leadership Balancing

It's need to prevent all RAFT groups from having leaders on the same OM node

**Balancer Strategy**:
- Runs periodically (default: every 5 minutes)
- Transfers leadership via RAFT `transferLeadership()` API
- Considers node health and load
- Graceful transfers to avoid disruption

### Group Reconciliation

Provide configured count of required bucket RAFT-groups and periodically verify a health state of the groups

## Snapshot & Install-Snapshot

### Problem

Ratis snapshot and install-snapshot assume a **1:1 relationship between a Raft log
and the persisted state** it protects. The multi-raft design breaks that assumption:
there are `N` bucket RAFT groups plus the main RAFT group (The main RAFT group here is the one that previously handled 
requests through RATIS alone (the group that the OM instances in HA mode worked in), 
but for the purposes of this design document, its purpose is to only handle non-bucket
write requests and ensure the functionality of the bucket RAFT groups.), but they all apply into a
**single shared OM RocksDB**. This means a snapshot taken for one group, and an
install-snapshot triggered by one group, unavoidably touch state owned by the other
groups.

Two distinct mechanisms must be considered separately:

- **`takeSnapshot()` (log purging)** — works per group. Each `BucketStateMachine`
  persists its own last-applied position under a group-scoped key
  `#TRANSACTIONINFO<raftGroupId>` in the `transactionInfo` table, and returns its own
  applied index to Ratis so that group's log can be purged. Because a RocksDB flush is
  global, flushing one group's double buffer also durably persists the others' pending
  writes — so per-group log purging is safe.

- **`notifyInstallSnapshot()` (follower bootstrap)** — does **not** work per group with
  a shared DB, and is the focus of this section.

### Why per-group install-snapshot is unsafe with a shared DB

When the leader of bucket group *g* triggers `notifyInstallSnapshot` to a lagging
follower, the follower today (`OzoneManager#installSnapshotFromLeader` →
`installCheckpoint`):

1. Downloads the **entire** OM DB checkpoint from the leader — the checkpoint contains
   `keyTable`/`fileTable`/`bucketTable`/`volumeTable` and the `#TRANSACTIONINFO<*>`
   markers for **every** group, not just group *g*.
2. Pauses **only one** state machine and replaces the **whole** DB directory
   (`replaceOMDBWithCheckpoint`), then reloads and unpauses.

With a single shared DB this produces three correctness problems:

1. **Concurrent appliers over a DB being swapped.** Only the triggering group is paused;
   the other groups' `BucketStateMachine`s keep applying into a DB that is being stopped
   and replaced underneath them → lost writes, RocksDB errors, or a crash.
2. **Cross-group rollback.** The checkpoint reflects the *source node's* view of the
   other groups, where that node is typically a **follower** (leadership is spread across
   OM nodes by the balancer). Installing it rewinds groups whose state on the receiver
   was independently ahead — silently rolling their data back.
3. **Log/DB divergence.** After the swap, each non-triggering group's in-memory
   `lastAppliedTermIndex` no longer matches its `#TRANSACTIONINFO<group>` value in the
   freshly installed DB, so its Raft log position and persisted state disagree.

There is no globally consistent cut in an arbitrary whole-DB checkpoint: it is an
interleaving of `N+1` independent logs at `N+1` unrelated indices.

### Design: node-level coordinated install-snapshot

Because the DB is shared, **snapshot and install-snapshot are treated as a node-level
operation coordinated across all groups**, not as an independent per-group action. The
main RAFT group leader is the coordinator (it already drives cross-group concerns such
as group reconciliation).

**Producing a consistent checkpoint (source):**
1. Quiesce all state machines — pause and flush the double buffers of the main SM (the state machine 
   that handles the main RAFT-groups transactions) and every `BucketStateMachine`.
2. Record the **vector** of `(raftGroupId → appliedIndex)`. This is already materialized
   as the per-group `#TRANSACTIONINFO<raftGroupId>` keys.
3. Take the RocksDB checkpoint, then resume all state machines.

**Installing a checkpoint (receiver):** when **any** group requires install-snapshot,
1. Pause **all** `N+1` state machines (not just the triggering group) and clear their
   double buffers.
2. Replace the OM DB directory **once**.
3. Re-seed **each** group's `lastAppliedTermIndex` from its own `#TRANSACTIONINFO<group>`
   marker in the installed DB.
4. Unpause all groups; each then replays its own log tail from its restored index.

> **Implementation note:** the current `installCheckpoint` pauses/unpauses only
> `omRatisServer.getOmStateMachine()` (the main SM). It must be extended to pause/unpause
> the full set of state machines and to reload every group's transaction-info marker.
> The global pause must be ordered against `waitForMainStateMachineCatchUp()` to avoid a
> deadlock between the install barrier and a bucket group waiting on the main SM.

### Mitigation: keep install-snapshot rare

Install-snapshot is the expensive, disruptive path. Followers should catch up via
`AppendEntries` whenever possible, so per-group log retention is sized so that purging
rarely outruns a temporarily lagging follower. Because there are now `N+1` logs, this
increases aggregate log disk usage proportionally; size retention and disk accordingly.

This mitigation reduces frequency but cannot eliminate install-snapshot: a **newly
bootstrapped or re-added OM** has no state for any group and must obtain an initial
checkpoint for all groups. That case is handled by the node-level install above.

### Future direction: per-group state isolation (Phase 2)

The clean long-term fix is to remove the shared-DB constraint by partitioning OM state
per group — e.g. group-scoped RocksDB column families (or separate RocksDB instances) —
so a group's checkpoint and install touch only that group's data and the native Ratis
1:1 log↔state model is restored. This is a larger change: `keyTable`/`fileTable`/
`openKeyTable`/`deletedTable` are global today, buckets are assigned to groups
dynamically (so a bucket's keys would migrate partitions on reassignment), and shared
metadata (volumes, the bucket table, snapshots, S3 secrets) still needs an owning group.
It is therefore deferred to a follow-up phase; the node-level coordinated install above
is the correct near-term behavior.

## Upgrade Path

### From Single-Raft to Multi-Raft

**Preparation Phase**:
1. Upgrade OM nodes to version supporting multi-raft (rolling upgrade)
2. Switch off multi-raft functionality initially
3. Verify all nodes running new version

**Enablement Phase**:
1. Stop all OM nodes gracefully
2. Switch on multi-raft functionality
3. Set raft group count (default: 6)
4. Start OM nodes

### Rollback Procedure

If issues arise:
1. Stop all OM nodes
2. Switch off multi-raft functionality
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


## Monitoring and Observability

### Metrics

Per RAFT Group:
- `omha_metrics_ozone_manager_bucket_raft_group_leader_state{nodeid="<omNodeId>",raftgroupid="<raftGroupId>",hostname="<omNodeHostname>"}` - raft group leadership state
- `omha_metrics_ozone_manager_raft_group_leader_state{nodeid="<omNodeId>",raftgroupid="<mainRaftGroupId>",hostname="<omNodeHostname>"}` - main raft group leadership state

Global:
- `omha_multi_raft_metrics_raft_groups_count`
- `omha_multi_raft_metrics_raft_groups_expected_count`
- `omha_multi_raft_metrics_om_in_safe_mode`
