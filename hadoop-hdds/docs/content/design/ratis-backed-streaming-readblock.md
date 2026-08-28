---
title: Ratis-Backed Streaming ReadBlock
summary: Design for group-aware and closed-replica ReadBlock over Ratis DataStream
date: 2026-07-11
status: draft
author: Lixucheng
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Ratis-Backed Streaming ReadBlock

## Summary

Ozone already has a direct streaming `ReadBlock` implementation over the
datanode gRPC service. The branch adds a second path that sends a `ReadBlock`
through Ratis DataStream and uses a live Raft group to perform the configured
Ratis read check before streaming data.

Before the resolver described below, this path worked only when the original
Ratis group was available and normally fell back for `CLOSED` containers. SCM
gives a non-open container a synthetic read pipeline whose random ID has no
Raft division. Ratis therefore failed division lookup before it could perform
the read check, and `RatisDataStreamBlockInputStream` switched to the existing
gRPC or block-read path.

That fallback was not a sufficient final design because most historical key
data is stored in immutable `CLOSED` containers. Those reads do not need leader
state, ReadIndex, or applied-index checks, but they should still be able to use
the packetized Ratis DataStream transport.

This design makes container mutability the policy boundary:

| Container state | Read path | Ratis consistency work |
| --- | --- | --- |
| `OPEN` or `CLOSING`, live group | Group-aware Ratis DataStream | Division lookup and configured read check |
| `CLOSED` | Direct Ratis DataStream to a selected replica | No Raft division, leader, or ReadIndex check |
| Other or unknown local state | Reject direct claim; group lookup or read fails | No bypass |

Both Ratis modes preserve Ozone `ReadBlock` status, token, block commit
sequence ID (BCSID), replica-index, range, and checksum validation. The
existing gRPC and normal block-read paths remain factory-selected compatibility
paths for older pipelines, but they are not the intended steady-state path for
`CLOSED` data.

The feature remains disabled by default:

~~~properties
ozone.client.ratis.stream.readblock.enable=false
~~~

## Goals

- Stream one `ReadBlock` request as multiple bounded response packets.
- Use Ratis DataStream for both live-group and immutable closed-replica reads.
- Preserve existing Ozone authorization, status, offset, BCSID, replica-index,
  and checksum semantics.
- Keep mutable-container reads behind the existing Ratis group and read check.
- Route `CLOSED` reads without depending on a write-pipeline group that may no
  longer exist.
- Keep memory bounded and make Netty buffer ownership explicit.
- Keep the additional Ratis change narrow, optional, and wire compatible.
- Retain a safe mixed-version compatibility path during rollout.

## Non-Goals

- Keeping closed write pipelines or their Raft groups alive.
- Creating a new Raft group for every closed container.
- Weakening consistency for `OPEN` or `CLOSING` containers.
- Changing EC reads.
- Making the path zero-copy end to end.
- Switching transport or replica inside `RatisDataStreamBlockInputStream`.
- Treating `QUASI_CLOSED` or `UNHEALTHY` as immutable without a separate
  correctness decision.

## State and Lifetime Boundaries

Three different objects must not be conflated:

- A **container** owns block data and has lifecycle states such as `OPEN`,
  `CLOSING`, `QUASI_CLOSED`, and `CLOSED`.
- An SCM **pipeline descriptor** identifies candidate datanodes. A descriptor
  created for reading does not imply that a Raft group exists.
- A Ratis **Raft group** is live server state addressed by a group ID and
  exposed as a `RaftServer.Division`.

`ContainerInfo.isOpen()` covers `OPEN` and `CLOSING`. SCM tries to return the
original pipeline for those states. For other states, or when the original
pipeline is unavailable, SCM calls `createPipelineForRead(...)`. The RATIS
provider builds an `ALLOCATED` pipeline with a random ID and the current
replica datanodes; it does not create a Raft group.

A `CLOSED` container is logically immutable even if the old write group still
happens to exist. Conversely, a missing group does not prove that a container
is immutable. The target route must therefore be chosen from authoritative
container state, not from pipeline state or `GroupMismatchException`.

The datanode's local container state is the final authority. Client or OM state
can become stale between lookup and read, so the group-independent handler
must re-check that the local container is exactly `CLOSED` before emitting the
first byte.

## Existing Path: Direct Streaming ReadBlock

### Client Flow

The existing high-throughput read path is `StreamBlockInputStream`:

~~~text
KeyInputStream
  -> BlockInputStreamFactoryImpl
  -> StreamBlockInputStream
  -> XceiverClientGrpc.initStreamRead()
  -> XceiverClientGrpc.streamRead(ReadBlock)
  -> one selected datanode gRPC stream
~~~

`XceiverClientGrpc.initStreamRead()` tries sorted pipeline datanodes until it
opens `XceiverClientProtocolService.send`. The bidirectional stream can carry
successive `ReadBlock` requests. `StreamBlockInputStream` owns setup retry,
block-location refresh, timeout, response queueing, and stream completion.

`BlockExtendedInputStream.setPipeline()` converts RATIS pipelines to
STAND_ALONE for this read path. No Raft group, Ratis division, leader check, or
ReadIndex is involved.

### Datanode Flow

The direct gRPC server path is:

~~~text
GrpcXceiverService.send()
  -> HddsDispatcher.streamDataReadOnly()
  -> KeyValueHandler.readBlockImpl()
  -> RandomAccessFileChannel
~~~

`KeyValueHandler.readBlockImpl()` verifies replica index and BCSID, aligns the
read to checksum boundaries, reads the FILE_PER_BLOCK file in response-sized
buffers, and calculates the checksum metadata.

Each gRPC response is a complete `ContainerCommandResponseProto`:

~~~text
ContainerCommandResponseProto
  ReadBlockResponseProto
    offset
    checksumData
    data = ByteString(block bytes)
~~~

On the client, `StreamBlockInputStream.StreamingReader.onNext()` verifies the
checksum over `ReadBlock.data`, queues the response, adjusts for checksum
alignment, and copies bytes into the caller's buffer.

### Properties of the Existing Path

- It can read without a live Raft group.
- It embeds payload bytes in protobuf `ByteString`.
- Ozone owns its transport lifecycle and retry policy.
- It is the current compatibility path for closed-container reads.
- It does not exercise the Ratis DataStream transport.

## Current Prototype: Group-Aware Ratis Streaming ReadBlock

### Client Flow

When `ozone.client.ratis.stream.readblock.enable` is true and the replication
type is RATIS, `BlockInputStreamFactoryImpl` creates
`RatisDataStreamBlockInputStream` only when every datanode in the pipeline
advertises `RATIS_DATASTREAM_READ_BLOCK_SUPPORT`. It does not inspect container
state. A mixed or older pipeline is routed to `StreamBlockInputStream` when the
older streaming feature is enabled and supported, or to `BlockInputStream`
otherwise.

`RatisDataStreamBlockInputStream.openStream()`:

1. Computes a bounded request length.
2. Builds the normal Ozone `ContainerCommandRequestProto(ReadBlock)`.
3. Wraps it as a `ContainerCommandRequestMessage`.
4. Acquires `XceiverClientRatis` for the supplied pipeline.
5. Calls `DataStreamApi.streamReadOnly(ByteBuffer)`.

Ratis wraps the message in a `RaftClientRequest` of type `READ`. Its group ID
is the Ozone pipeline ID, and the DataStream primary is the closest pipeline
datanode.

### Ratis Server Flow

The current server ordering is important:

~~~text
ReadStreamManagement.processImpl()
  -> parse STREAM_HEADER as RaftClientRequest(READ)
  -> server.getDivision(groupId)
  -> submitClientRequestAsync(dummy READ)
  -> configured read check succeeds
  -> division.getStateMachine().data().query(message, stream)
~~~

Division lookup happens before the read check. With the default Ratis read
option, the dummy request checks leader state. With the linearizable read
option, it obtains or serves ReadIndex and waits for the local applied index.
Only after that check succeeds does Ratis invoke
`StateMachine.DataApi.query(Message, WritableByteChannel)`.

In Ozone, the selected division owns a `ContainerStateMachine`. Its
`query(Message, WritableByteChannel)` implementation calls the same
`HddsDispatcher.streamDataReadOnly()` and `KeyValueHandler.readBlockImpl()`
used by the gRPC path.

### Response Framing

The live branch uses
`DataStreamObserver<ContainerDispatcher.ReadBlockResponse>`. The observer
carries:

- a `ContainerCommandResponseProto` whose `ReadBlock.data` is empty; and
- a separate raw `ByteBuffer` containing block bytes.

`ContainerStateMachine` encodes each Ratis `STREAM_DATA` reply as:

~~~text
4-byte metadataLength
ContainerCommandResponseProto metadata
raw block bytes
~~~

There is no magic prefix in the current frame. The metadata length uses the
normal Java `ByteBuffer` integer encoding, and the metadata keeps the existing
`ReadBlock` offset and checksum data.

After the last data packet, Ratis emits a terminal `STREAM_HEADER` containing
a serialized, request-aware `RaftClientReply`. Data packets and final command
success are separate; the client must validate both.

### Client Buffer Ownership

`DataStreamInput.readAsync()` returns:

~~~text
CompletableFuture<ReferenceCountedObject<DataStreamReply>>
~~~

`RatisDataStreamBlockInputStream` retains the wrapper while a Netty-backed
payload slice is visible and releases it after consumption. Terminal and
unexpected replies are released immediately after inspection.

### Request Window

The first stream after open, seek, unbuffer, or client release requests only
the application length. Once an adjacent stream reaches a successful terminal
reply, the next sequential request may use:

~~~text
max(applicationLength + preReadSize, readWindowSize)
~~~

The current defaults are 32 MiB pre-read and a 256 MiB Ratis read window. This
reduces stream setup overhead for sequential reads without prefetching 256 MiB
after a random seek.

### Copy and Backpressure Boundaries

The current path is not zero-copy. `KeyValueHandler` keeps raw block bytes out
of protobuf, but `ContainerStateMachine` still concatenates the four-byte
length, serialized metadata, and raw payload into one contiguous `ByteBuffer`.

`ContainerStateMachine.writeFully()` waits for each channel write and handles a
Ratis channel implementation that may report the byte count without advancing
the source `ByteBuffer` position. This bounds the server's production of
response frames.

## Historical Closed-Container Fallback Problem

For a non-open container, SCM returns a synthetic RATIS read pipeline with a
random ID and current replica datanodes. The descriptor is sufficient for the
existing direct Ozone read paths, but no datanode has a Raft division for that
random ID.

Before the group-independent resolver, the sequence was:

~~~text
CLOSED container
  -> SCM synthetic RATIS read pipeline
  -> RatisDataStreamBlockInputStream
  -> streamReadOnly(READ, synthetic groupId)
  -> ReadStreamManagement.getDivision(groupId)
  -> GroupMismatchException
  -> close Ratis stream and client
  -> StreamBlockInputStream or BlockInputStream fallback
~~~

The failure occurred before the configured Ratis read check. Merely marking the
request non-linearizable or skipping the dummy check is insufficient because
`getDivision(groupId)` fails first.

This fallback was conservative at the byte-stream boundary, but it was the wrong
steady-state architecture:

- Most historical data was in `CLOSED` containers, so fallback became common.
- Every such read paid a failed Ratis setup and error round trip.
- The payload then left the Ratis DataStream transport.
- `GroupMismatchException` conflates a normal immutable-container read with a
  genuinely missing group for mutable data.
- The current client does not receive container lifecycle state:
  `ContainerWithPipeline` contains it, but `KeyManagerImpl` copies only the
  pipeline into `OmKeyLocationInfo`.

Removing fallback alone would not have solved the problem. The missing
capability was a group-independent DataStream read for an immutable local
replica. The resolver now provides that capability, and the datanode-version
gate routes older pipelines to a compatibility stream before
`RatisDataStreamBlockInputStream` is constructed. Once constructed, the Ratis
stream never switches transport.

## Proposed Architecture

### Routing Policy

The route is chosen before data is emitted:

| Authoritative state | Ratis route | Failure policy |
| --- | --- | --- |
| `OPEN` or `CLOSING` | Existing group-aware read | Refresh or fail if the live group is unavailable |
| Local `CLOSED` | Group-independent direct read | Retry another eligible replica only before the first data frame |
| Any other local state | Do not bypass the group | Preserve conservative existing behavior |

The existing client request already identifies the container and target
datanode, so the first implementation does not need to propagate container
state through OM or SCM protocols. The datanode resolver validates local
`CLOSED` state before claiming the request. This keeps the routing decision at
the authoritative replica and prevents stale metadata from weakening
consistency.

### Open or Closing Container

The current group-aware path remains unchanged:

~~~text
RatisDataStreamBlockInputStream
  -> RaftClientRequest(READ, live pipeline groupId)
  -> ReadStreamManagement
  -> division lookup
  -> configured Ratis read check
  -> ContainerStateMachine.data().query()
  -> STREAM_DATA* + terminal STREAM_HEADER
~~~

This path keeps the Ratis leader/read-option behavior for mutable containers.

### Closed Container

The target closed-replica path uses the same client request and wire framing:

~~~text
RatisDataStreamBlockInputStream
  -> RaftClientRequest(READ, synthetic read pipeline ID)
  -> selected datanode RATIS_DATASTREAM port
  -> optional application resolver claims local CLOSED ReadBlock
  -> group-independent StateMachine.DataApi.query()
  -> HddsDispatcher.streamDataReadOnly()
  -> STREAM_DATA* + terminal STREAM_HEADER
~~~

The resolver runs before `server.getDivision(groupId)`. When it positively
claims a request, Ratis does not resolve a division and does not submit the
dummy read check. The synthetic group ID remains request correlation metadata;
it does not identify a live Raft division in this mode.

The resolver must not claim a request because division lookup failed. It claims
only an Ozone `ReadBlock` whose target datanode hosts the requested container
in exactly `CLOSED` state. All other requests continue through the existing
group-aware code.

The direct path reuses the current frame format, request executor, Netty
backpressure, reply ownership, and terminal reply. The only semantic change is
how Ratis obtains the `StateMachine.DataApi` used for the query.

## Technical Design

### Minimal Additional Ratis Change

Zero Ratis changes cannot provide a group-independent read on the current
RATIS_DATASTREAM listener: `ReadStreamManagement` requires a
`RaftServer.Division` before it reaches Ozone.

The minimal Ratis change is one optional server-side resolver:

~~~java
interface DataStreamReadResolver {
  StateMachine.DataApi resolve(RaftClientRequest request) throws IOException;
}
~~~

A non-null result means the application positively claims the request. A null
result means Ratis must execute the current group-aware path.

The resolver is installed through the existing Ratis `Parameters` passed to
`NettyServerStreamRpc`. `ReadStreamManagement` checks it immediately after
parsing a `READ` header and before division lookup:

~~~text
dataApi = resolver == null ? null : resolver.resolve(request)

if dataApi != null:
  create request-aware success terminal reply
  dataApi.query(request.message, readStream)
else:
  division = server.getDivision(request.groupId)
  readCheck = server.submitClientRequestAsync(dummyRead)
  division.stateMachine.data().query(request.message, readStream)
~~~

Ratis remains responsible for:

- recognizing and framing a read stream;
- running the query on the existing request executor;
- writing `STREAM_DATA` replies;
- sending the terminal `STREAM_HEADER`;
- constructing request-aware success and error replies; and
- closing and cleaning up the stream.

The application resolver is responsible only for selecting a `DataApi`.
Container policy and data access do not move into Ratis.

The Ratis implementation is limited to:

1. A small resolver SPI in `ratis-server-api`.
2. Typed `Parameters` access in the Netty DataStream configuration.
3. Passing the optional resolver from `NettyServerStreamRpc` to
   `ReadStreamManagement`.
4. A claimed-request branch before `getDivision(...)`.
5. Focused tests in `TestDataStreamManagement`.

This design requires no change to Raft protobufs, `DataStreamApi`,
`DataStreamInput`, client framing, or the normal group-aware request path.
When no resolver is registered, behavior is identical to the current Ratis
implementation.

### Required Ozone Changes

#### 1. Keep the Client Request Unchanged

`RatisDataStreamBlockInputStream` already sends the container ID, target
datanode, token, block ID, range, and synthetic read-pipeline ID. The resolver
can choose the safe path from this request and the local container state.

No OM or SCM wire change is required for the initial implementation:

- a locally `CLOSED` container is claimed before division lookup;
- every other state remains unclaimed and follows the existing group-aware
  path; and
- a client chooses this path only when every pipeline datanode advertises the
  resolver capability.

Client-visible container state may be added later to make mutable-container
group mismatch handling independent of the synthetic pipeline-state proxy, but
it is not a correctness dependency for the direct `CLOSED` handler.

#### 2. Register the Direct Read Resolver

`XceiverServerRatis` should install the Ozone resolver in the Ratis
`Parameters` used to build the DataStream server.

For each incoming Ratis `READ` request, the resolver:

1. Parses the `ContainerCommandRequestProto`.
2. Returns unclaimed unless the command is `ReadBlock`.
3. Looks up the local container.
4. Returns unclaimed unless the local container is exactly `CLOSED`.
5. Verifies that the request targets the local datanode when target identity is
   present.
6. Returns the group-independent Ozone `StateMachine.DataApi`.

The resolver does not trust a client-supplied "skip consistency" flag and does
not claim `OPEN`, `CLOSING`, `QUASI_CLOSED`, `RECOVERING`, `UNHEALTHY`, or
unknown containers.

#### 3. Reuse the Existing Ozone Read Implementation

The direct `DataApi` should call the same
`HddsDispatcher.streamDataReadOnly()` and `KeyValueHandler.readBlockImpl()`
used by both current paths.

Ozone should extract only the shared Ratis response framing from
`ContainerStateMachine` so the group-aware and direct handlers use identical:

- metadata/data separation;
- four-byte metadata length;
- `writeFully()` behavior;
- error conversion; and
- channel close semantics.

The extraction should remain package-local and have exactly two callers. No new
transport abstraction is needed.

The normal dispatcher continues to enforce:

- block-token validation;
- container existence;
- replica-index validation;
- requested BCSID;
- offset and length bounds;
- response status;
- checksum metadata; and
- audit and metrics.

### Rollout and Follow-Ups

`DatanodeVersion.RATIS_DATASTREAM_READ_BLOCK_SUPPORT` advertises the
group-independent resolver. `BlockInputStreamFactoryImpl` selects the Ratis
streaming path only when every datanode in the supplied pipeline advertises
that version. This makes mixed-version routing proactive instead of first
discovering an old server through `GroupMismatchException`.

The compatibility choice is:

| Pipeline capability | Selected client path |
| --- | --- |
| Every datanode supports the Ratis resolver | `RatisDataStreamBlockInputStream` |
| Older datanode, old streaming path enabled and supported | `StreamBlockInputStream` |
| Older datanode without old streaming support | `BlockInputStream` |

#### Failure Behavior

Once the factory selects `RatisDataStreamBlockInputStream`, the read stays on
Ratis DataStream. Group mismatch, resolver rejection, query failure, timeout,
and terminal reply failure propagate to the caller. The stream does not create
a `StreamBlockInputStream` or `BlockInputStream` at any point.

This intentionally prefers consistency over availability during a close race.
SCM can synthesize a read pipeline when an `OPEN` or `CLOSING` container's
original pipeline is unavailable. If the selected local replica is still
`CLOSING` or `QUASI_CLOSED`, the resolver does not claim the request and the
synthetic group cannot be resolved. A later caller refresh or retry may succeed
after the replica is exactly `CLOSED`; the current stream does not perform that
refresh itself.

The following changes remain follow-ups.

#### Select and Retry Closed Replicas

For `CLOSED` data, the client selects an eligible replica from the SCM read
pipeline and uses its RATIS_DATASTREAM address. A failed direct read may refresh
metadata and try another eligible replica only if no `STREAM_DATA` payload has
been exposed.

After the first payload frame, the client must fail the read window rather than
splice data from another transport or replica. A later caller retry may reopen
a new window at an independently validated position.

#### Metrics

Add separate counters for:

- group-aware Ratis reads;
- closed-replica direct Ratis reads;
- pre-data replica retries;
- capability-routed compatibility reads; and
- rejected direct claims by local container state.

These metrics are needed before changing the feature default.

## Correctness and Security

### Authoritative Closed-State Check

The direct path is safe only for a locally `CLOSED` container. Checking at the
datanode protects against stale client, OM, or SCM state. A request that is not
locally closed remains unclaimed and cannot bypass the Ratis group check.

`QUASI_CLOSED` is deliberately excluded from the initial design because
replicas may not agree on the final BCSID. It can be considered separately with
an explicit replica-selection rule.

### Existing Ozone Validation

The direct path must retain all normal `ReadBlock` validation. In particular,
`BlockUtils.verifyBCSId()` rejects a replica whose container BCSID is behind the
requested block, and `BlockUtils.verifyReplicaIdx()` prevents reading the wrong
replica index.

Token verification happens before the handler emits data. A token failure,
missing container, invalid range, BCSID mismatch, or checksum failure is an
operation failure, not permission to bypass through a different consistency
mode.

### Deletion and Replica Movement

A closed container may be deleted or moved while a read is in progress. The
handler holds only the existing Ozone read resources; it does not pin a Raft
group. Deletion races surface as normal container-not-found or I/O failures.
Replica retry remains a pre-data operation.

### Terminal Status

Receiving all `STREAM_DATA` frames is not sufficient for success. The client
must also receive and validate the terminal request-aware `RaftClientReply`.
The direct path preserves this rule even though it does not use a Raft
division.

## Compatibility and Rollout

1. Land the optional Ratis resolver. Without a registered resolver, behavior
   and wire compatibility remain unchanged.
2. Register the Ozone resolver on datanodes. The existing client feature flag
   continues to control use of Ratis streaming reads.
3. Advertise resolver support through `DatanodeVersion` and require every
   pipeline datanode to advertise it before selecting the Ratis stream.
4. Remove in-stream fallback; older pipelines are already routed to a
   compatibility path before stream construction.
5. Collect route, rejection, retry, and compatibility metrics before changing
   the feature default.

No protocol flag is required. Old clients continue to send the same
`RaftClientRequest(READ)`. New clients avoid the Ratis path when the supplied
pipeline contains an old datanode.

## Testing

### Ratis Tests

Extend `TestDataStreamManagement` to verify:

- an unclaimed request still performs division lookup and the dummy read check;
- a claimed request with no matching division invokes the resolver `DataApi`;
- a claimed request does not call `submitClientRequestAsync()`;
- resolver or query failure produces a terminal error;
- the terminal success reply preserves request metadata; and
- existing group-aware and follower linearizable-read tests remain unchanged.

### Ozone Unit Tests

Add focused coverage for:

- direct selection for local `CLOSED` containers;
- rejection of direct claims for every other local state;
- rejection of missing containers, other commands, and requests for another
  datanode;
- existing adaptive request-window behavior;
- Ratis stream selection when all datanodes advertise resolver support;
- proactive compatibility routing for a mixed-version pipeline.

### Ozone Integration Tests

Update `TestRatisDataStreamReadBlock` so that:

- an open-container read is a `RatisDataStreamBlockInputStream` using the
  group-aware path;
- a `CLOSED` container read is a `RatisDataStreamBlockInputStream` using the
  direct resolver path;
- the closed read still works after the original write group is removed;
- checksum verification, seek, unbuffer, and close preserve current behavior.

Add at least one RATIS/THREE follow-up. The current RATIS/ONE test proves basic
framing and the direct read after the original group is removed, but it does
not prove replica choice or failover.

## Alternatives Considered

### Keep an In-Stream Fallback

This required no new Ratis code, but closed data did not use Ratis DataStream
and every attempt paid a failed group lookup. It did not meet the goal.

### Keep Closed Write Groups Alive

This preserves the current group-aware path but changes pipeline-close
semantics, retains failed write-group state, and scales with historical
pipelines. It is rejected.

### Create a Permanent Single-Node Carrier Group per Datanode

This avoids a Ratis source change, but it creates synthetic group identity,
storage, recovery, election, reporting, and close-protection work. It also
continues to run an irrelevant read check. It is a smaller Ratis diff and a
larger system design, so it is rejected.

### Add a New Wire Flag or Packet Type

A `skipLinearizable` flag still needs a group-independent data handler and
makes consistency bypass client-controlled. A new packet type expands the
mixed-version surface without changing the required server behavior. Both are
rejected.

### Use Direct gRPC for All Closed Containers

This is a valid zero-Ratis-change route and remains a factory-selected
compatibility behavior. It does not satisfy the requirement that
closed-container payloads use the Ratis DataStream transport.

## Current Limitations and Follow-Ups

- The feature is disabled by default.
- Mixed-version pipelines are proactively routed to the existing streaming or
  block-read compatibility path.
- Capability-aware per-replica selection and direct-read metrics are not yet
  implemented.
- `RatisDataStreamBlockInputStream` does not switch transport or refresh block
  locations after a resolver rejection or group mismatch; caller retry or
  refresh is required.
- The server still performs one frame-concatenation copy.
- `DataStreamInputImpl` still uses an unbounded client reply queue.
- `QUASI_CLOSED`, `UNHEALTHY`, and recovering replicas remain outside the direct
  route.
- The benchmark below uses MiniOzoneCluster and does not measure the proposed
  group-independent closed-container path.

## Conclusion

The current prototype proves that Ozone can stream multi-packet `ReadBlock`
responses through Ratis while preserving terminal status, checksum semantics,
and explicit buffer ownership. Its group-aware design is appropriate for
mutable containers with a live pipeline.

The former fallback problem was architectural, not an error-handling corner:
`CLOSED` containers normally have a synthetic read pipeline but no matching
Raft division. The final design therefore has two Ratis modes. Mutable data
uses the current group-aware path and configured read check. Immutable
`CLOSED` data uses the same DataStream transport through a minimal optional
server resolver and an Ozone-owned direct handler.

This keeps Ratis generic and nearly unchanged: no new wire format, client API,
or synthetic carrier group. Ozone remains responsible for container state,
authorization, data validation, replica policy, framing, and compatibility
routing.

## Appendix A: Existing Prototype Benchmark

The latest local benchmark was run on June 21, 2026 after updating the Ozone
adapter for the Ratis `DataStreamInput.readAsync()` return type
`CompletableFuture<ReferenceCountedObject<DataStreamReply>>`. The run used the
Ozone worktree on branch `ratis-stream-read-poc` and a local Ratis
`3.3.0-SNAPSHOT` install from branch `RATIS-2546-stream-read-client`.

This benchmark supersedes the older May 27 gathered-buffer numbers for the
current compatibility path. It also did not reproduce the earlier 32 MiB
no-MD5 regression: in this run, Ratis-backed stream read is faster than
streaming ReadBlock for all no-MD5 32 MiB rows.

Benchmark settings:

- MiniOzoneCluster;
- RATIS/ONE replication;
- bytes per checksum: 16 KiB;
- key sizes: 256 MiB, 500 MiB, 1 GiB;
- application read buffers: 32 MiB, 8 MiB, 1 MiB, 4 KiB;
- one no-MD5 sequential read and one MD5-verifying sequential read for each
  path and buffer;
- random read buffers: 1 MiB and 4 KiB;
- random reads per key and buffer: 32.

The no-MD5 reads are the clearer transport comparison:

| Key size | Buffer | ReadBlock stream | Ratis data stream | Ratis / ReadBlock bandwidth |
| --- | ---: | ---: | ---: | ---: |
| 256 MiB | 32 MiB | 831.60 MB/s | 1823.26 MB/s | 2.19x bandwidth |
| 256 MiB | 8 MiB | 1931.80 MB/s | 2718.68 MB/s | 1.41x bandwidth |
| 256 MiB | 1 MiB | 1177.33 MB/s | 1358.01 MB/s | 1.15x bandwidth |
| 500 MiB | 32 MiB | 1625.92 MB/s | 2695.45 MB/s | 1.66x bandwidth |
| 500 MiB | 8 MiB | 2384.70 MB/s | 3109.47 MB/s | 1.30x bandwidth |
| 500 MiB | 1 MiB | 1631.86 MB/s | 2745.81 MB/s | 1.68x bandwidth |
| 500 MiB | 4 KiB | 985.58 MB/s | 3686.01 MB/s | 3.74x bandwidth |
| 1 GiB | 32 MiB | 1693.30 MB/s | 3206.32 MB/s | 1.89x bandwidth |
| 1 GiB | 8 MiB | 2633.54 MB/s | 3492.42 MB/s | 1.33x bandwidth |
| 1 GiB | 1 MiB | 1639.57 MB/s | 2344.10 MB/s | 1.43x bandwidth |
| 1 GiB | 4 KiB | 1060.42 MB/s | 3676.22 MB/s | 3.47x bandwidth |

The same benchmark also ran MD5-verifying reads:

| Key size | Buffer | ReadBlock stream with MD5 | Ratis data stream with MD5 | Ratis / ReadBlock bandwidth |
| --- | ---: | ---: | ---: | ---: |
| 256 MiB | 32 MiB | 534.48 MB/s | 569.58 MB/s | 1.07x bandwidth |
| 256 MiB | 8 MiB | 652.07 MB/s | 607.30 MB/s | 0.93x bandwidth |
| 256 MiB | 1 MiB | 643.63 MB/s | 595.04 MB/s | 0.92x bandwidth |
| 256 MiB | 4 KiB | 633.92 MB/s | 643.04 MB/s | 1.01x bandwidth |
| 500 MiB | 32 MiB | 654.77 MB/s | 646.86 MB/s | 0.99x bandwidth |
| 500 MiB | 8 MiB | 730.53 MB/s | 654.55 MB/s | 0.90x bandwidth |
| 500 MiB | 1 MiB | 620.98 MB/s | 624.35 MB/s | 1.01x bandwidth |
| 500 MiB | 4 KiB | 721.04 MB/s | 666.94 MB/s | 0.92x bandwidth |
| 1 GiB | 32 MiB | 679.32 MB/s | 645.72 MB/s | 0.95x bandwidth |
| 1 GiB | 8 MiB | 726.82 MB/s | 654.54 MB/s | 0.90x bandwidth |
| 1 GiB | 1 MiB | 651.98 MB/s | 642.44 MB/s | 0.99x bandwidth |
| 1 GiB | 4 KiB | 649.88 MB/s | 635.07 MB/s | 0.98x bandwidth |

The random reads use `seek()` before each read and therefore exercise the
adaptive non-promoted request length. Throughput is less meaningful for 4 KiB
because only 128 KiB total is read per row, so elapsed time is shown too:

| Key | Random read | ReadBlock | Ratis | BW ratio | IOPS ratio | ReadBlock time | Ratis time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 MiB | 1 MiB | 74.79 MB/s | 242.15 MB/s | 3.24x bandwidth | 3.24x IOPS | 0.428 s | 0.132 s |
| 256 MiB | 4 KiB | 0.26 MB/s | 1.37 MB/s | 5.21x bandwidth | 5.21x IOPS | 0.474 s | 0.091 s |
| 500 MiB | 1 MiB | 103.10 MB/s | 432.56 MB/s | 4.20x bandwidth | 4.20x IOPS | 0.310 s | 0.074 s |
| 500 MiB | 4 KiB | 0.46 MB/s | 1.59 MB/s | 3.44x bandwidth | 3.44x IOPS | 0.270 s | 0.079 s |
| 1 GiB | 1 MiB | 67.30 MB/s | 387.87 MB/s | 5.76x bandwidth | 5.76x IOPS | 0.475 s | 0.083 s |
| 1 GiB | 4 KiB | 0.26 MB/s | 1.97 MB/s | 7.43x bandwidth | 7.43x IOPS | 0.472 s | 0.064 s |

The adaptive window makes the first stream after seek smaller, so random reads
do not accidentally prefetch a 256 MiB window. The MD5 rows remain clustered
around 0.5-0.7 GiB/s, so checksum/digest CPU dominates that mode and hides
most transport differences. The largest MD5 rows should still be watched, but
the no-MD5 transport regression that motivated the rerun is not visible in
this benchmark.

## Appendix B: Reproduction Steps

Install the local Ratis snapshot required by Ozone. The snapshot must contain
the RATIS-1240 server hook from
`31bfd388748551f7d5a71e705e07b76b4a194f64` and the client-side
`DataStreamApi.streamReadOnly(ByteBuffer)` /
`CompletableFuture<ReferenceCountedObject<DataStreamReply>>` prototype:

```bash
cd /Users/lixucheng/Documents/oss/apache/ratis
./mvnw -DskipTests -DskipShade -DskipJavadoc -DskipRat -DskipCheckstyle install
```

Run the Ozone correctness test:

```bash
cd /Users/lixucheng/Documents/oss/apache/ozone
mvn -pl :ozone-integration-test -am \
  -Dtest=org.apache.hadoop.ozone.client.rpc.read.TestRatisDataStreamReadBlock \
  -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DskipShade -DskipRecon \
  -Dskip.npx -Dskip.installnodenpm -Dskip.npm -Dskip.yarn \
  test
```

Run the correctness test and PR-6613-style trend benchmark in one pass:

```bash
cd /Users/lixucheng/Documents/oss/apache/ozone
mvn -pl :ozone-integration-test -am \
  -Dtest=TestRatisDataStreamReadBlock,TestRatisDataStreamReadBlockBenchmark#comparePr6613StyleReadTrend \
  -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -DskipShade -DskipRecon \
  -Dskip.npx -Dskip.installnodenpm -Dskip.npm -Dskip.yarn \
  -Dozone.ratis.datastream.benchmark=true \
  -Dozone.ratis.datastream.benchmark.key.sizes=256M,500M,1G \
  -Dozone.ratis.datastream.benchmark.buffer.sizes=32M,8M,1M,4k \
  -Dozone.ratis.datastream.benchmark.random.buffer.sizes=1M,4k \
  -Dozone.ratis.datastream.benchmark.random.reads=32 \
  test
```

Run the PR-6613-style trend benchmark:

```bash
cd /Users/lixucheng/Documents/oss/apache/ozone
mvn -pl :ozone-integration-test -am \
  -Dtest=org.apache.hadoop.ozone.client.rpc.read.TestRatisDataStreamReadBlockBenchmark#comparePr6613StyleReadTrend \
  -Dozone.ratis.datastream.benchmark=true \
  -Dozone.ratis.datastream.benchmark.key.sizes=256M,500M,1G \
  -Dozone.ratis.datastream.benchmark.buffer.sizes=32M,8M,1M,4k \
  -Dozone.ratis.datastream.benchmark.random.buffer.sizes=1M,4k \
  -Dozone.ratis.datastream.benchmark.random.reads=32 \
  -DskipRecon -DskipShade \
  -Dskip.npx -Dskip.installnodenpm -Dskip.npm -Dskip.yarn \
  test
```

Extract the benchmark lines:

```bash
cd /Users/lixucheng/Documents/oss/apache/ozone
REPORT_DIR=hadoop-ozone/integration-test/target/surefire-reports
REPORT=org.apache.hadoop.ozone.client.rpc.read.TestRatisDataStreamReadBlockBenchmark
rg -n "PR-6613-style|with .* bytes|createStreamKey|readStreamKey|randomReadKey|improvement|Tests run" \
  "$REPORT_DIR/$REPORT-output.txt" \
  "$REPORT_DIR/$REPORT.txt"
```

Check formatting:

```bash
cd /Users/lixucheng/Documents/oss/apache/ozone
git diff --check
```
