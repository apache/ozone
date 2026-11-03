# RFC: Write Pipeline V3 — Ratis Streaming without Raft (CHAINED Replication)

| Field   | Value                                     |
|---------|-------------------------------------------|
| Status  | Draft 2026-11-02                          |
| Author  | Tsz-Wo Nicholas Sze, Rui Wang             |
| AI      | Claude                                    |
| Related | HDDS-15757, HDDS-16008, HDDS-16746, HDDS-16910, HDDS-17093 |

---

## 1. Executive Summary & Goals

### 1.1 Background

Apache Ozone currently has three write pipelines:

| Pipeline | Data path | PutBlock (metadata commit) | Raft log entries per block | Replication type |
|---|---|---|---|---|
| **V1** | `WriteChunk` via Raft (`XceiverClientRatis`) | `PutBlock` via Raft | 1 per chunk + 1 per PutBlock | `RATIS` |
| **V2** | Ratis DataStream (`StreamInit` + stream packets) | `PutBlock` via Raft, linked to the stream | 1 per block (stream link) + 1 per flush PutBlock | `RATIS` |
| **EC** | Direct gRPC writes of data and parity chunks to each datanode | `PutBlock` direct to each datanode | 0 | `EC` |

In V1 and V2, every write is ordered by the Raft leader and written to the Raft log, which costs:

- **Raft log I/O.** Each entry is written to the Raft log on every datanode,
  in addition to the container data and RocksDB metadata.
- **Leader CPU.** The leader is a hotspot for ordering, replicating and applying log entries.
- **Latency.** Each PutBlock waits for a Raft round trip, majority commit and apply.

Raft totally orders writes **across blocks in a container**.
Ozone blocks have a single writer and are immutable after commit, so that ordering is not needed for block data.

### 1.2 Proposal

**Write Pipeline V3** sends **both StreamData and PutBlock over Ratis DataStream** and writes **no Raft log entries for any write**.
V3 is identified by the previously unused `ReplicationType.CHAINED` (`= 3` in `hdds.proto`):

- **Keys and containers.** CHAINED keys are written to CHAINED containers.
- **Pipelines.** CHAINED containers live on dedicated CHAINED pipelines.
- **Raft group.** Each CHAINED pipeline keeps its Raft group.
  The group provides peer membership, the datastream server and pipeline management.
  It is never used for write log entries.
- **Durability.** Data and PutBlock are ack'ed on **every** datanode in the chain before the client is acknowledged (all-ack).
- **bcsId.** The block commit sequence ID is always `0` for CHAINED blocks and containers.

Branch `master` already has the datanode/client mechanism,
the **PutBlock-without-Raft** stream path (HDDS-15757/16008/16746/16910/17093).
This RFC turns it into a first-class replication type,
with SCM, OM and ReplicationManager support.

### 1.3 Functional Goals

1. **Zero Raft log entries per block.** No entries for writes, PutBlock (close, flush or hsync), or stream close.
2. **Same durability as V1/V2, or better.** An acknowledged PutBlock is ack'ed on all replicas, not just a majority.
3. **hflush/hsync support** through stream commands (`DataStreamOutput#commandAsync`).
4. **A first-class `CHAINED` replication type.**
   - Supported configs: `CHAINED/ONE` and `CHAINED/THREE`.
   - Selected through the bucket/key replication config, as with EC.
   - Accepted by `ozone.replication.allowed-configs`.
5. **Dedicated CHAINED pipelines and containers.** They are never mixed with RATIS pipelines or containers.
6. **CHAINED containers are never QUASI_CLOSED.** Lifecycle: `OPEN → CLOSING → CLOSED`.
7. **Full ReplicationManager coverage.** Under-, over- and mis-replication handling for CHAINED containers,
   using the existing replicated-container (Ratis-style) handlers.
8. **Rolling-upgrade safe.**
   - Version gates: `HDDSVersion` / `OzoneManagerVersion`.
   - Layout finalization before CHAINED can be used.

### 1.4 Non-Functional Requirements

| NFR | Target |
|---|---|
| Write throughput (large sequential keys, CHAINED/THREE vs V2) | **TBD** (e.g. ≥ X% improvement; to be set from benchmarks) |
| PutBlock latency p50/p99 | **TBD**. Expected to drop by roughly one Raft commit + apply round trip. |
| Raft log write bytes per GB written | **0** for writes (control traffic only) |
| Leader CPU | No per-block work on the Raft leader. Work is spread along the chain. |
| RocksDB writes | No more than V2 (one PutBlock per flush/close per replica) |
| Durability | Ack only after data + PutBlock are synced on all replicas (HDDS-17093) |
| Compatibility | No change to existing RATIS/EC data paths. No new protobuf fields. No RocksDB layout change. |
| Availability | One failed or slow datanode fails the in-flight block. The client retries on another pipeline (§4.1.3). |

---

## 2. Out of Scope / Future Works

- **EC with streaming.** EC keys keep the existing direct-write path.
  EC may switch to `CHAINED/ONE` in the future.
- **Raft-less pipeline management.** Creating and closing pipelines still uses the existing Raft group commands
  (`CreatePipelineCommand`, `ClosePipelineCommand`).
  Close pipelines will remove the Raft group.
- **Migrating existing RATIS keys or containers to CHAINED.** CHAINED applies only to new writes.
- **Replication factors other than ONE and THREE.**
- **Tolerating a partial ack.** A write that is not acked by every replica is a failed write.
  Majority/quorum acks for streaming are not proposed.
- **Orphan-block garbage collection.** Blocks written but never committed to OM are the same issue as for EC and V2 today,
  and are handled separately.
- **Multi-writer appends.** Ozone blocks stay single-writer.
- **Container layouts other than FILE_PER_BLOCK.** FILE_PER_BLOCK is the only supported layout.

---

## 3. Proposed Architecture

### 3.1 Component View

```
                              ┌──────────────────────┐
                              │         OM           │
        ┌────────────────────►│ openKey/allocateBlock│
        |       allocateBlock │ commitKey / hsync    │
        |     (CHAINED/THREE) └──────────────────────┘
        |                                  ▲ pipeline/conatainers
        |                                  │ (CHAINED/THREE) 
        |                                  │
 ┌──────┴───────┐             ┌──────────────────────────────────────┐
 │   Client     │             │                 SCM                  │
 │ (OFS / S3G / │             │ PipelineFactory[CHAINED] =           │
 │  RpcClient)  │             │      ChainedPipelineProvider         │
 │              │             │ WritableContainerFactory[CHAINED]    │
 │ KeyDataStream│             │ ReplicationManager (Ratis handlers)  │
 │  Output      │             │ CloseContainer: force=true(non-RATIS)│
 │ BlockData-   │             └───────▲────────────────┬─────────────┘
 │ StreamOutput │                     │ reports        │ Create/ClosePipeline,
 └──────┬───────┘                     │ (container,    │ CloseContainer(force)
        │                             │  pipeline)     ▼
        │ Ratis DataStream   ┌────────┴──────────── CHAINED pipeline ────────────────┐
        │ (header, packets,  │                                                       │
        │  commands, CLOSE)  │  ┌────────────┐  stream  ┌────────────┐ stream ┌────────────┐
        └───────────────────►│  │ Primary DN │─────────►│    DN2     │───────►│    DN3     │
          ◄── all-ack reply ─┤  │ (closest)  │◄─ ack ───│            │◄─ ack ─│            │
                             │  └─────┬──────┘          └─────┬──────┘        └──────┬─────┘
                             │        │ KeyValueStreamDataChannel (FILE_PER_BLOCK)   │
                             │        ▼ data file + RocksDB PutBlock, synced         ▼
                             │                                                       │
                             │  ┄┄┄┄┄┄┄┄┄┄┄┄ Raft group (control only) ┄┄┄┄┄┄┄┄┄┄┄┄┄ │
                             │  membership, datastream server, group add/remove;     │
                             │  NO log entries for WriteChunk / PutBlock / close     │
                             └───────────────────────────────────────────────────────┘
```

### 3.2 Write Sequence (one block)

```
Client                    Primary DN               DN2                 DN3           OM
  │ allocateBlock(CHAINED/THREE) ───────────────────────────────────────────────────►│
  │◄───────────────────────────────────────── block + CHAINED pipeline + token ──────│
  │ stream(header: StreamInitWithPutBlock, routing table P→2→3)                      │
  │──────────────────────────►│──────────────────►│─────────────────►│               │
  │  (each DN: validate token, open KeyValueStreamDataChannel; no container2BCSIDMap)│
  │ writeAsync(data packet)*  │                   │                  │               │
  │──────────────────────────►│──────────────────►│─────────────────►│               │
  │ [hsync] commandAsync(PutBlock)                │                  │               │
  │──────────────────────────►│──────────────────►│─────────────────►│ persist+sync  │
  │◄──────── reply (all peers OK, identical) ─────┴──────────────────┘               │
  │ hsync(key, length) ─────────────────────────────────────────────────────────────►│
  │ writeAsync(tail = |data|PutBlock|int len|, CLOSE)                                │
  │──────────────────────────►│──────────────────►│─────────────────►│               │
  │      each DN: closeWithStreamPutBlock → write data, persist PutBlock, sync       │
  │◄──────── reply (all peers OK, identical) ─────┴──────────────────┘               │
  │ commitKey(block, length) ───────────────────────────────────────────────────────►│
```

### 3.3 Key Architectural Properties

- **All-ack.** The Ratis datastream server replies to a CLOSE or command only after every peer in the routing table succeeds with an identical reply.
  Ratis performs this check in `DataStreamManagement#checkSuccessRemoteWrite`.
- **Chain routing.** The primary is `pipeline.getClosestNode()`,
  and the chain comes from `RatisHelper#getRoutingTable(pipeline)` (`ozone.client.datastream.pipeline.mode=true`).
- **No Raft ordering of writes.** Each block is a separate stream with a single writer.
  Ordering inside a block is the stream's byte order, and there is no cross-block ordering.
- **bcsId = 0.** No Raft index exists to assign. Because every CHAINED replica has bcsId 0,
  replicas cannot disagree on bcsId, which is why QUASI_CLOSED is unnecessary.

---

## 4. Detailed Component Design

### 4.1 Client

#### 4.1.1 Replication selection
- **Selecting V3.** Users choose V3 through the bucket or key replication config:
  - `ozone sh bucket create --type CHAINED --replication THREE /vol/bucket`
  - or a key-level `ReplicationConfig`
- **`ChainedReplicationConfig`** (new, in `hadoop-hdds/common`):
  - Implements `ReplicatedReplicationConfig` and reuses `ReplicationFactor`, like `RatisReplicationConfig`.
  - Valid instances: `CHAINED/ONE` and `CHAINED/THREE`.
  - Wired into `ReplicationConfig.fromProto`, `fromProtoTypeAndFactor`, `parse` and `getDefault`.
- **Other client wiring.**
  - Remove `@Deprecated` from `ReplicationType.CHAINED`.
  - The shell's `ReplicationOptions` stops rejecting CHAINED.

#### 4.1.2 Stream output
- **`RpcClient#createDataStreamOutput` / `KeyDataStreamOutput`.**
  A CHAINED key always uses the stream path, with `putBlockWithoutRaft = true`,
  whatever `ozone.client.datastream.putblock.without.raft.enabled` says.
  That flag keeps its V2 meaning for RATIS keys.
- **`XceiverClientCreator`.** `case CHAINED` returns `XceiverClientRatis`,
  which provides the DataStream API for the pipeline.
- **`BlockDataStreamOutput`:**
  - The header uses `Type.StreamInitWithPutBlock` with `WriteChunkRequestProto` (block ID, checksum) and `writePipelineVersion`.
  - Flush and hsync send `PutBlock` with `DataStreamOutput#commandAsync`.
  - Close sends the tail `|data|PutBlockRequestProto|int32 length|` with `StandardWriteOption.CLOSE`.
    The PutBlock is at most 1 MB (`PUT_BLOCK_REQUEST_LENGTH_MAX`), a design constant.
  - The existing version gate `allDatanodesSupportStreamPutBlock` (`HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT`)
    is a hard requirement for CHAINED.
    SCM only builds CHAINED pipelines from datanodes that support it (§4.3.1).

#### 4.1.3 Failure handling and retry
- **Failed or unmatched reply.** If a CLOSE or command reply is a failure or a mismatch, the block write fails.
- **Retry.** `KeyDataStreamOutput` reacts the same way it does today for V1/V2:
  - Add the pipeline to the `ExcludeList`; on `CONTAINER_NOT_OPEN` / `CLOSED_CONTAINER_IO`, add the container instead.
  - Allocate a new block from OM and SCM.
  - Rewrite the buffered, unacked data (`ozone.client.datastream.window.size`).
- **The abandoned block** keeps the length of its last all-acked PutBlock, if there was one;
  otherwise it is dropped from the key. Data beyond that length is never committed (§5.4).
- **Known issue (prerequisite fix).** In `BlockDataStreamOutput#executePutBlock`,
  a failed CLOSE reply re-calls `executePutBlock` on a stream that is already closed.
  This can recurse without bound. It must be fixed before CHAINED is GA.

### 4.2 Datanode

| Area | Behavior for CHAINED |
|---|---|
| Stream init | `ContainerStateMachine#stream` → `HddsDispatcher` (op `STREAM_INIT`) validates the block token. Auto-creating the container works as for V2. **Change:** for a `StreamInitWithPutBlock` request, the container is added to `container2BCSIDMap` only when the group's recorded type is RATIS. When the type is CHAINED or unknown, it is not added. Plain `StreamInit` (V2) is unchanged. |
| Data | `KeyValueStreamDataChannel` writes packets to the FILE_PER_BLOCK block file. |
| Flush/hsync | `ContainerStateMachine#streamCommand` persists the PutBlock (`ContainerStateMachine#streamPutBlock`) and syncs it before replying. |
| Close | `KeyValueStreamDataChannel#closeWithStreamPutBlock` parses the tail, writes the remaining data, persists the PutBlock with bcsId 0 (`BlockManagerImpl#persistPutBlock`), and syncs data and metadata before replying (HDDS-17093). |
| Container close | **Change:** `CloseContainerCommandHandler` closes CHAINED containers directly (`controller.closeContainer`) even if the pipeline's Raft group exists. It never submits CloseContainer through Raft and never quasi-closes them. |
| Raft group removal | `ContainerStateMachine#notifyGroupRemove` only goes through `container2BCSIDMap`. CHAINED containers are not in it, so they are never quasi-closed. |
| Restart | `ContainerSet#buildMissingContainerSetAndValidate` checks containers against the Raft snapshot's bcsId map. CHAINED containers are not in it, so they are not marked UNHEALTHY. The recorded group types are in memory only, so every group reloaded at startup has an unknown type. SCM closes the restarted datanode's CHAINED pipelines (§4.3.2). |
| Pipeline creation | **Change:** `CreatePipelineCommandHandler` creates the Raft group (`XceiverServerRatis#addGroup`) as today. It also records `CreatePipelineCommandProto.type` for the group in `XceiverServerRatis`, on every command, including when the group already exists (`isExist`). |

**How the datanode knows a container is CHAINED.** A CHAINED pipeline creates only CHAINED containers, so the datanode goes by the pipeline's type.
- **Recording the type.** `XceiverServerRatis` keeps an in-memory `RaftGroupId → ReplicationType` map, filled from `CreatePipelineCommandProto.type`. It is not persisted. Nothing is added to disk and the protobuf doesn't change.
  - The type must be recorded even when the group already exists. The datanode that handles the command first adds the group on its peers through `getGroupManagementApi(peer).add(group)`. When a peer's own command arrives, `isExist` is already true, and today the handler skips its body.
  - So the state machine can exist before its type is known. The type is looked up at stream init, from the request's group (`ContainerStateMachine#getGroupId`), not when the state machine is built.
- **Stream init.** A `StreamInitWithPutBlock` request adds its container to `container2BCSIDMap` only when the group is known to be RATIS. CHAINED and unknown groups are treated the same way: the container is not added.
  - **Unknown type for CHAINED.** This can happen just after pipeline creation, or after a restart until SCM's `ClosePipelineCommand` arrives. The Ratis and DataStream servers start in `VersionEndpointTask` (`ozoneContainer.start`), before the datanode registers. Not adding the container is exactly the CHAINED behavior, so this window is harmless.
  - **Unknown type for RATIS.** This only happens after a restart. A RATIS container that is first auto-created by `StreamInitWithPutBlock` (`ozone.client.datastream.putblock.without.raft.enabled`) in this state is not tracked in `container2BCSIDMap`, so the bcsId check at restart skips it. That costs nothing: such writes have no Raft write entries, so the bcsId is 0 anyway.
- **Close.** SCM sends `force=true` for every non-RATIS container. The datanode closes directly when `force` is set and the container is not tracked in `container2BCSIDMap`, i.e. it was never written through Raft. This holds whether the group's type is known or not. A RATIS container written through Raft is always in that map, so a forced close of a RATIS container keeps today's behavior.

### 4.3 SCM

#### 4.3.1 `ChainedPipelineProvider` (new)
- **Why it is needed.**
  - `PipelineFactory` maps each `ReplicationType` to one `PipelineProvider` and registers only STAND_ALONE, RATIS and EC today.
  - `RatisPipelineProvider` extends `PipelineProvider<RatisReplicationConfig>`
    and hardcodes `RatisReplicationConfig.getInstance(factor)` in `newPipelineBuilder`,
    so it can only build RATIS pipelines.
- **What it reuses.** It extends `PipelineProvider<ChainedReplicationConfig>`
  and reuses the Raft-group logic pulled out of `RatisPipelineProvider` rather than copied:
  - datanode choice: `pickNodesNotUsed` for ONE, `PipelinePlacementPolicy` for THREE;
  - `LeaderChoosePolicy`;
  - `CreatePipelineCommand(id, CHAINED, factor, dns, suggestedLeader)` to each datanode;
  - `ClosePipelineCommand` on close.
- **Differences from RATIS:**
  - **Datastream port.** The `RATIS_DATASTREAM` port is required strictly.
    RATIS falls back to datanodes without it in `chooseThreeFactorDatanodes`; CHAINED does not.
  - **Datanode version.** Only datanodes at `HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT` or later are eligible.
  - **Pipeline limit.** The per-datanode limit (`ozone.scm.datanode.pipeline.limit`) counts **both** RATIS/THREE and CHAINED/THREE Raft groups. Today `PipelinePlacementPolicy#currentRatisThreePipelineCount` and `RatisPipelineProvider#exceedPipelineNumberLimit` count RATIS/THREE only.

#### 4.3.2 Other SCM pipeline changes
- **`PipelineFactory`** registers `CHAINED → ChainedPipelineProvider`.
- **`BackgroundPipelineCreator#getReplicationConfigs`** also creates CHAINED pipelines
  when CHAINED is in `ozone.replication.allowed-configs`:
  - THREE always;
  - ONE under the existing `ozone.scm.pipeline.creation.auto.factor.one` rule.
- **`PipelineManagerImpl`:**
  - `closePipelinesMissingDataStreamPort` includes CHAINED.
  - `factorOne` handles CHAINED.
  - Remove `case CHAINED: // Not supported`.
- **Datanode restart.** When a datanode SCM already knows registers again (the `else` branch of `SCMNodeManager#register`),
  the leader SCM closes every CHAINED pipeline that contains it. That datanode has lost its in-memory group types (§4.2).
  - Today that branch fires no pipeline event. `StaleNodeHandler` closes pipelines only after `ozone.scm.stale.node.interval`,
    which a fast restart never reaches. So this needs a new event and handler.
  - RATIS pipelines are not closed.
  - Re-registering without a restart (for example after a `ReregisterCommand`) also triggers the close.
    That is unnecessary, but safe.
  - If SCM restarts as well, the datanode registers as a new node, so this trigger doesn't fire.
    The datanode stays correct, because unknown-type groups are handled safely (§4.2).
- **`PipelineReportHandler#setPipelineLeaderId`.** A CHAINED/ONE pipeline has no leader flag, the same as RATIS/ONE.
- **Safe mode.** The pipeline rules count RATIS/THREE only, so safe mode is unchanged.
- **Admin CLI.** `CreatePipelineSubcommand` accepts CHAINED.

#### 4.3.3 Containers
- **Allocation.** `WritableContainerFactory#getContainer` adds `case CHAINED`, which uses `WritableRatisContainerProvider`.
  That provider is already typed on `ReplicationConfig`, so it finds or creates an OPEN container on an OPEN CHAINED pipeline.
  No new allocation field is needed: `AllocateScmBlockRequestProto` already carries type and factor.
- **Close.** No change. `CloseContainerEventHandler` already sets `force=true` for any non-RATIS type.
- **All replicas unhealthy.** No change. `ClosingContainerHandler` quasi-closes only RATIS containers, so CHAINED goes to CLOSED.
- **ReplicationManager.** Most `ReplicationType.RATIS` checks need an audit; there are about 41 in 34 main source files.
  - **Treat CHAINED like RATIS (must change).** Without these changes CHAINED containers get no replication handling:
    - `RatisReplicationCheckHandler`
    - `RatisContainerReplicaCount`
    - the Ratis under-, over- and mis-replication handlers
    - `ReconciliationEligibilityHandler`
  - **Treat CHAINED like EC (already correct):** forced close, no QUASI_CLOSED, CLOSED when every replica is unhealthy.
  - **Not applicable:** `QuasiClosedContainerHandler` and `QuasiClosedStuckReplicationCheck` stay RATIS-only.
- **Replica selection.** bcsId-based choices compare equal at 0. Any healthy CLOSED replica is a valid source.

### 4.4 OM
- **No new RPCs.** `ChainedReplicationConfig` round-trips through the existing `KeyInfo` / `BucketInfo` `type` + `factor` fields.
- **Validation and gating.** Bucket/key creation accepts CHAINED only after finalization (§8.3)
  and only if it is in `ozone.replication.allowed-configs`.
- **Lease recovery.** Lease recovery for an hsync'd CHAINED key must use the all-acked length (§5.4).

### 4.5 API and Wire Formats

| Interaction | Transport | Payload | Returns |
|---|---|---|---|
| `allocateBlock` (client → OM → SCM) | Hadoop RPC | existing `AllocateScmBlockRequestProto { size, numBlocks, type=CHAINED, factor, owner, excludeList }` | `AllocatedBlock { blockID, pipeline(type=CHAINED, nodes), token }` |
| Stream header | Ratis DataStream `stream(header, routingTable)` | `ContainerCommandRequestProto { cmdType=StreamInitWithPutBlock, containerID, datanodeUuid, writeChunk{blockID, chunkData}, encodedToken, writePipelineVersion }` | `DataStreamOutput` |
| Data packet | `DataStreamOutput#writeAsync(buf)` | raw bytes | `DataStreamReply` (bytes written) |
| Flush / hsync | `DataStreamOutput#commandAsync(buf)` | `ContainerCommandRequestProto { cmdType=PutBlock, putBlock{blockData, eof=false, sync=true for hsync} }` | `DataStreamReply` wrapping `ContainerCommandResponseProto { result, putBlock{committedBlockLength} }`, only after every peer succeeds |
| Close | `writeAsync(tail, CLOSE)` | `\|data\|PutBlockRequestProto (≤ 1 MB)\|int32 length\|` | `DataStreamReply` wrapping `ContainerCommandResponseProto`, only after every peer has persisted and synced |
| Commit (client → OM) | Hadoop RPC | existing `commitKey(keyArgs, locationList{blockID, length})` / `hsync` | existing responses |
| Pipeline create (SCM → DN) | heartbeat command | existing `CreatePipelineCommandProto { pipelineID, type=CHAINED, factor, datanode[], priority[] }` | pipeline report |
| Container close (SCM → DN) | heartbeat command | existing `CloseContainerCommandProto { containerID, pipelineID, force=true }` | container report (`CLOSED`) |

**No protobuf schema changes.** CHAINED (=3) already exists in `ReplicationType`. Every message above uses existing fields.

---

## 5. Data Schema & Flows

### 5.1 Schema

| Item | Change |
|---|---|
| `ReplicationType.CHAINED = 3` (`hdds.proto`) | Now used. Previously defined but unused. |
| `ContainerInfoProto.replicationType/replicationFactor` | `CHAINED` / `ONE`, `THREE` |
| `Pipeline` (SCM DB) | `type=CHAINED`. Stored in the existing pipeline table. |
| `KeyInfo`, `BucketInfo` (OM DB) | `type=CHAINED`. Existing fields. |
| `BlockData` (datanode RocksDB) | Unchanged. `blockCommitSequenceId = 0`. |
| Container bcsId (datanode RocksDB, `ContainerInfoProto.sequenceId`, `ContainerReplicaProto.blockCommitSequenceId`) | Always `0` for CHAINED |
| RocksDB layout / column families | Unchanged |

### 5.2 Lifecycles

```
CHAINED pipeline:  ALLOCATED ──(all DNs report group)──► OPEN ──(close/scrub)──► CLOSED ──► removed
CHAINED container: OPEN ──(full / pipeline close / DN failure)──► CLOSING ──(force close, direct)──► CLOSED
                   (never QUASI_CLOSED; UNHEALTHY replicas handled by ReplicationManager as today)
```

**Why QUASI_CLOSED never applies.** QUASI_CLOSED exists for RATIS containers
whose replicas may have applied the Raft log to different indexes,
i.e. different bcsIds. SCM needs time to choose the replica with the highest bcsId.
Every CHAINED replica has bcsId 0, so replicas can never disagree.

### 5.3 Flows

**Write (happy path).** Covered in §3.2. The client commits to OM only after the CLOSE reply,
which guarantees all replicas have persisted and synced the block.

**hsync.**
1. `commandAsync(PutBlock)` is sent and the all-ack reply returns.
2. The client calls OM `hsync` with the acked length L.
3. Later bytes are not visible to OM until the next acked PutBlock.

**Container close (SCM-initiated).** Triggered when the container is full, the pipeline closes, or a datanode goes stale or dead.
1. SCM moves the container to CLOSING and sends `CloseContainerCommand(force=true)`.
2. Each datanode marks the container CLOSING and closes it directly.
3. Streams still in flight get `CONTAINER_NOT_OPEN` / `CLOSED_CONTAINER_IO` and retry on a new block (§4.1.3).
   Container close is not ordered with streams, and it doesn't need to be: an unacked block is never committed.
4. Replicas report CLOSED, and SCM moves the container to CLOSED.

**Datanode failure during a write.**
1. The chain breaks, and the CLOSE or command reply fails or times out.
2. The client excludes the pipeline and rewrites the unacked data to a new block on another CHAINED pipeline.
3. SCM detects the stale datanode, closes the pipeline, and force-closes its containers.
4. ReplicationManager re-replicates under-replicated CHAINED containers using the Ratis-style handlers.

**Pipeline creation.**
1. `BackgroundPipelineCreator` or the admin CLI calls `ChainedPipelineProvider#create`.
2. `CreatePipelineCommand(type=CHAINED)` is sent to each datanode.
3. Each datanode adds the Raft group, or finds it was already added by a peer.
   Either way it records the type CHAINED for the group. The pipeline reports move the pipeline to OPEN.

**Datanode restart.**
1. The datanode reloads its Raft groups from Ratis storage.
   Their types are unknown, because the recorded types were in memory only.
2. Its Ratis and DataStream servers start before it registers. Until step 4,
   a stream on a CHAINED group is still handled correctly:
   an unknown type keeps the container out of `container2BCSIDMap` (§4.2).
3. The datanode registers again.
   The leader SCM closes every CHAINED pipeline containing it and force-closes their containers.
   Each one closes directly, because it is not in `container2BCSIDMap`.
4. The datanode receives `ClosePipelineCommand` and removes the groups.
   `notifyGroupRemove` quasi-closes nothing, because no CHAINED container is in the map.
5. `BackgroundPipelineCreator` creates new CHAINED pipelines, and the restarted datanode is eligible for them.

**Lease recovery of an hsync'd CHAINED key.**
1. OM `recoverLease` runs; `LeaseRecoveryClientDNHandler` finalizes the last block.
2. **Change for CHAINED:**
   - The final length must be the **all-acked** length: the minimum committed length across replicas,
     or the OM-recorded hsync length.
   - It must not come from one replica.
   - `FinalizeBlock` is a Raft write today (`HddsUtils#isReadOnly` returns false),
     so CHAINED needs a non-Raft path: a stream command, or a direct call to each replica.

### 5.4 Replica Consistency

The client sees a PutBlock or CLOSE succeed only when **all** replicas succeed with identical replies,
and data and metadata are synced before the reply. Therefore:

- **Committed data is identical on every replica.** No ContainerChecksums-based divergence detection or new SCM logic is needed.
- **Replicas can differ only in data that was not all-acked:**
  - an abandoned block, or
  - the tail after the last all-acked PutBlock of a block.
- **OM never references that data**, so the difference is harmless, provided the two readers below stay within the acked length:
  1. **Readers of an under-construction (hsync'd) block.**
     - `BlockInputStream` uses `blockData.getSize()` from whichever datanode answers, so a reader may see bytes that were not all-acked.
     - Proposal: document this, or bound such reads to the OM hsync length (open question 5).
  2. **Lease recovery.** It must use the all-acked length (§5.3).
- **Bit rot and disk failures** are handled as today: the container scanner marks the replica UNHEALTHY and ReplicationManager repairs it.

---

## 6. Alternatives Considered & Trade-offs

### 6.1 (a) Keep V2: stream data, Raft PutBlock
- **Pros:** already shipped; total order and bcsId tie-breaking; tolerates a minority failure without failing the write.
- **Cons:** a Raft entry, leader round trip and apply for every PutBlock; Raft log I/O; leader hotspot; QUASI_CLOSED complexity.

### 6.2 (b) EC-style direct gRPC replication (client writes to each replica)
- **Pros:** no Raft group at all; proven in Ozone's EC path.
- **Cons:**
  - The client sends N× the data, using N× its egress bandwidth, where the chain uses 1×.
  - It needs a new replicated (non-EC) client output stream and a new pipeline type without a Raft group.
  - It doesn't reuse the Ratis datastream server, with its zero-copy and Netty path.
  - It doesn't reuse the PutBlock-without-Raft code already on branch `master`.

### 6.3 (c) CHAINED containers on shared RATIS pipelines, or a bcsId == -1 marker on RATIS containers
- **Pros:** no new pipeline provider; no extra Raft groups per datanode.
- **Cons:**
  - **Shared pipelines** break the rule that a container's type matches its pipeline's type.
    SCM code that assumes this (pipeline lookup by `ReplicationConfig`, `getMatchingContainer`) breaks.
  - **The -1 marker** is implicit and needs special cases:
    - `BlockUtils#verifyBCSId` (-1 < 0 makes every read fail)
    - `ContainerSet#buildMissingContainerSetAndValidate`
    - the close paths

    It also displays as `18446744073709551615` in the uint64 replica field, and reconciliation can overwrite it.
- **Rejected.**

### 6.4 (d) Tolerate a partial ack (majority ack)
- **Pros:** lower tail latency; a slow datanode doesn't fail the write.
- **Cons:**
  - Without Raft there is no sequence number to tell the replicas apart,
    so committed data could differ across replicas.
  - That needs divergence detection, repair (reconciliation on the write path) and QUASI_CLOSED-like handling,
    which brings back what V3 removes.
- **Rejected for V3** (open question 2).

### 6.5 Trade-off Summary

| Option | Raft entries/block | Client egress | Latency | Consistency of committed data | Complexity |
|---|---|---|---|---|---|
| (a) V2 | ≥ 1 per PutBlock | 1× | + Raft round trip | Majority + bcsId | Existing |
| (b) EC-style direct | 0 | N× | Lowest (parallel) | All-ack | High (new stream + pipeline) |
| (c) Shared / -1 marker | 0 | 1× | Chain | All-ack | Medium, but brittle special cases |
| (d) Majority ack | 0 | 1× | Lower p99 | **Divergent** without repair | High |
| **V3 (CHAINED)** | **0** | **1×** | **Chain, no Raft** | **All-ack, synced** | **Medium (reuses `am` path)** |

---

## 7. External Dependencies & Security

### 7.1 Dependencies
- **Apache Ratis DataStream** (already a dependency): `DataStreamOutput`, `RoutingTable`
  and the datastream server's all-ack reply.
  - No new third-party dependency.
  - No Ratis change is required.
- **Existing Ozone mechanisms:**
  - block tokens
  - `HDDSVersion.STREAM_PUTBLOCK_WITHOUT_RAFT(101)`
  - the FILE_PER_BLOCK container layout

### 7.2 Security (per THREAT_MODEL.md; secure mode `ozone.security.enabled=true` is assumed)
- **Authorization.**
  - The block token is validated once at stream init (op `STREAM_INIT`, in `HddsDispatcher`).
  - Later packets, commands and CLOSE belong to that authorized stream, so they skip per-request token checks by design. This matches V2.
  - Tokens are issued by OM for CHAINED keys exactly as for RATIS keys.
- **Transport.**
  - The datastream port (`RATIS_DATASTREAM`) uses the Ratis Netty TLS configuration (mTLS in secure mode).
  - Datanode-to-datanode forwarding along the chain uses the same TLS.
  - Non-secure mode is out of model (THREAT_MODEL.md §5a).
- **Service peers.** CHAINED pipelines are created only by SCM through the existing datanode command channel.
  Peers are authenticated as for RATIS pipelines.
- **Data-integrity risks.** These are not new trust-boundary findings: they are correctness properties covered by §5.4 and §8.
  - **Orphan blocks.** Data written but never committed takes space, as with EC and V2 (out of scope, §2).
  - **Visibility of the non-all-acked tail** to hsync readers and lease recovery (§5.3, §5.4).
- **Byzantine datanodes.** A bounded-Byzantine datanode could return a forged success.
  The all-ack check requires identical replies from every peer, so a forged reply that differs fails the write.
  Stored data stays protected by chunk checksums verified on read and by the scanner.

---

## 8. Operational Considerations

### 8.1 Configuration

| Key | Default | Notes |
|---|---|---|
| `ozone.replication.allowed-configs` | existing | Must include `CHAINED/ONE` and/or `CHAINED/THREE` to enable them. |
| `ozone.client.datastream.putblock.without.raft.enabled` | `false` | V2 RATIS keys only. CHAINED keys always commit PutBlock through the stream. |
| `ozone.client.datastream.pipeline.mode` | `true` | Chain routing from the closest node. |
| `ozone.client.datastream.window.size` | `64MB` | Client retry buffer for unacked data. |
| `ozone.client.datastream.buffer.flush.size` | `16MB` | PutBlock (stream command) boundary. |
| `ozone.client.datastream.min.packet.size` | `1MB` | Packet size. |
| `hdds.container.ratis.datastream.enabled` | existing | Must be `true` on datanodes. |
| `ozone.scm.datanode.pipeline.limit` | existing | Now limits RATIS/THREE and CHAINED/THREE Raft groups combined. |
| `ozone.scm.pipeline.creation.auto.factor.one` | existing | Also controls whether CHAINED/ONE pipelines are created automatically. |

### 8.2 Metrics (new or extended)
- **Datanode:**
  - stream commands (count, latency)
  - stream-committed PutBlocks
  - CHAINED containers closed directly
  - stream failures by result code
- **Client:**
  - CHAINED block retries / excluded pipelines
  - CLOSE reply latency (p50/p99)
- **SCM:**
  - CHAINED pipelines by state
  - CHAINED containers by state; QUASI_CLOSED **must be 0**, so alert if it isn't
  - Raft groups per datanode (RATIS + CHAINED)
- **OM:** CHAINED lease recoveries.

### 8.3 Deployment and Upgrade
1. **Upgrade** SCM, OM, datanodes, then clients. The gates:
   - new `HDDSVersion` / `OzoneManagerVersion` entries;
   - a new layout feature on SCM and OM (name TBD);
   - the existing `STREAM_PUTBLOCK_WITHOUT_RAFT(101)` covers the datanode.
2. **Finalize** SCM and OM. Before finalization, CHAINED configs are rejected.
3. **Enable** `CHAINED/THREE` in `ozone.replication.allowed-configs`.
   SCM's background creator starts creating CHAINED pipelines.
4. **Canary** with one bucket (`--type CHAINED`) used by S3G or OFS.
   Compare throughput and latency with V2, and confirm QUASI_CLOSED = 0.
5. **Roll out** by changing bucket defaults.

### 8.4 Rollback
- **Before finalization:** fully reversible. CHAINED is not usable yet.
- **After CHAINED data exists:** **no downgrade.** Older components throw `UnsupportedOperationException`
  for CHAINED in `ReplicationConfig.fromProto`.
- **Mitigation:**
  - Stop new CHAINED writes by removing CHAINED from `allowed-configs` and changing bucket defaults back to RATIS.
  - Existing CHAINED keys stay readable.

### 8.5 Capacity
- **Raft groups.** Raft groups per datanode now cover RATIS and CHAINED together under one limit.
  Running both types splits the pipeline budget between them.
- **Memory.** Each CHAINED Raft group has an idle log and its state machine, but no write traffic.

### 8.6 Gotchas
- **Prerequisite fix.** The close-retry recursion in `BlockDataStreamOutput#executePutBlock` (§4.1.3).
- **Slow datanodes.** Chain latency is the sum of hop latencies. One slow datanode slows or fails every block on that pipeline,
  so watch the client-exclusion metrics.
- **Held-back tail.** The last ≤ 1 MB PutBlock plus data is held back for the CLOSE packet, which adds memory per open stream.
- **hsync visibility.** Readers of an hsync'd, still-open CHAINED block may see bytes that were not all-acked (§5.4).
- **Lease recovery** must use the all-acked length (§5.3).
- **Mixing types.** RATIS and CHAINED containers never share pipelines.
  Admin tooling that lists pipelines or containers by type must include CHAINED.

---

## 9. Open Questions

1. **Performance targets.** What throughput and latency improvements vs V2 are required for GA (§1.4)?
2. **Partial ack.** Is all-ack acceptable for availability, given that one slow or failed datanode fails the in-flight block,
   or is a degraded mode needed later?
3. **Orphan blocks.** Should CHAINED share the EC orphan-block cleanup, or get its own?
4. **Known bugs.** Must the `executePutBlock` close-retry recursion be fixed before CHAINED ships?
5. **hsync reads.** Document the non-all-acked visibility, or limit under-construction reads to the OM hsync length?
6. **Lease recovery.** Use the minimum length across replicas, or the OM-recorded hsync length?
   And which non-Raft path should replace `FinalizeBlock` for CHAINED?
7. **Layout feature.** The name and number for the SCM/OM layout feature that gates CHAINED.
