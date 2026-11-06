---
title: Ozone Protobuf Oneof Optimization
summary: Use oneof groups in Proto files to Optimize memory usage in Ozone.
date: 2026-10-07
jira: HDDS-16984
status: draft
author: Rishabh Patel
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

## Problem Statement

Ozone uses [Protobuf](https://protobuf.dev/) as its serialization framework. As part of its API surface, Ozone supports several types of requests and responses for each of its components (OM, SCM, DN). These requests are serialized via Protobuf. The existing Protobuf definitions for the Request/Response wrappers are suboptimal. As per the wrappers, any number of requests and responses can be part of the same Proto object. In practice, each wrapper Proto object is usually used for only a single type of request or response at a time. This proposal aims to address this discrepancy and optimize the Protobuf object design for the wrappers.

Consider the [OMRequest](https://github.com/apache/ozone/blob/0a9bd0d999aca77d3bea8246ceb6e1c04672cb81/hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto#L179-L333) proto object. It is a wrapper object that encapsulates all requests to the OM. It allows over a hundred types of requests. At a time, the OM will allow only one type of request like `CreateBucketRequest` or the `CreateKeyRequest` but not both. 

```proto
message OMRequest {
  required Type cmdType = 1; // Type of the command

  // A string that identifies this command, we generate  Trace ID in Ozone
  // frontend and this allows us to trace that command all over ozone.
  optional string traceID = 2;

  required string clientId = 3;

  optional UserInfo userInfo = 4;
  optional uint32 version = 5;

  optional LayoutVersion layoutVersion = 6;

  optional ReadConsistencyHint readConsistencyHint = 7;

  optional CreateVolumeRequest              createVolumeRequest            = 11;
  optional SetVolumePropertyRequest         setVolumePropertyRequest       = 12;
  optional CheckVolumeAccessRequest         checkVolumeAccessRequest       = 13;
  optional InfoVolumeRequest                infoVolumeRequest              = 14;
  optional DeleteVolumeRequest              deleteVolumeRequest            = 15;
  optional ListVolumeRequest                listVolumeRequest              = 16;
  .... // many more optional Request fields
}
```

The protoc compiler however will [generate](https://protobuf.dev/reference/java/java-generated/#fields) the Java classes with a fixed layout. For each proto field, the generated classes will contain one reference to store the tag number and one reference to the object. 

```java
// optional .hadoop.ozone.CreateVolumeRequest createVolumeRequest = 11;
public static final int CREATEVOLUMEREQUEST_FIELD_NUMBER = 11;
private org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest createVolumeRequest_;
```

This results in memory being used for all such presence fields and reference fields. With the current design, the space occupied by each wrapper class increases linearly with the number of fields in the proto message definitions.
The field exists even if it is not set. A parsed `OMRequest` carrying a `CreateBucketRequest` still has a slot for `createVolumeRequest_`, for `deleteVolumeRequest_`, and for every other request type, all holding null.

The memory cost comes from the generated Java wrapper class, not from serialized protobuf bytes or from allocating every child payload. In the current form, each `optional FooRequest fooRequest \= N` becomes its own instance field in every wrapper object. Even when the field value is `null`, the parent object still contains a fixed reference slot for it, plus presence bookkeeping. With dozens of mutually exclusive payload fields, every parsed or built wrapper object pays for many unused reference slots.

```
Today: one reference slot per request type     With oneof: one slot plus a tag

+--------------------------------+             +--------------------------------+
| OMRequest                504 B |             | OMRequest                 96 B |
+--------------------------------+             +--------------------------------+
| cmdType = CreateBucket         |             | cmdType = CreateBucket         |
| traceID, clientId, userInfo .. |             | traceID, clientId, userInfo .. |
| bitField0_ .. bitField3_       |             | bitField0_                     |
| createVolumeRequest_      null |             | bodyCase_ = 21                 |
| setVolumePropertyRequest_ null |             | body_ ---------------------------> CreateBucketRequest
| checkVolumeAccessRequest_ null |             +--------------------------------+
| ...                            |
| createBucketRequest_ ----------> CreateBucketRequest
| ...                            |
| ~100 more slots, all null      |
+--------------------------------+
```


## Protobuf `oneof` keyword

Protobuf provides the [`oneof`](https://protobuf.dev/programming-guides/proto2/#oneof) keyword to enforce the presence of at most a single field within a group of fields.  
In other words, it makes the group mutually exclusive. Setting one field invalidates the previously set field within the group. Setting at least one field is not required though. 

`oneof` changes that object layout. Instead of generating one reference field per possible payload, protoc stores all mutually exclusive payloads in a single shared `Object` field, together with a small discriminator indicating which field is active. The selected payload is still stored normally, but the unselected alternatives no longer consume per-instance reference slots in the wrapper object.

```proto
private int bodyCase_ = 0;
private java.lang.Object body_;
```

This is useful for Ozone because request and response wrappers are created frequently and can pass through RPC, Ratis, queues, caches and GC-heavy paths. The change preserves the same field numbers and wire compatibility while reducing the in-memory footprint of each wrapper. The on-wire size does not change as optional fields are by-default [not transferred](https://protobuf.dev/programming-guides/encoding/#optional) over the wire.

## Scope

Eight dispatcher wrapper messages across four `.proto` files:

| Proto file                                                                                                                         | Wrapper messages                                                |
|:-----------------------------------------------------------------------------------------------------------------------------------|:----------------------------------------------------------------|
| `https://github.com/apache/ozone/blob/master/hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto`                  | `OMRequest`, `OMResponse`                                       |
| `https://github.com/apache/ozone/blob/master/hadoop-hdds/interface-client/src/main/proto/DatanodeClientProtocol.proto`             | `ContainerCommandRequestProto`, `ContainerCommandResponseProto` |
| `https://github.com/apache/ozone/blob/master/hadoop-hdds/interface-admin/src/main/proto/ScmAdminProtocol.proto`                    | `ScmContainerLocationRequest`, `ScmContainerLocationResponse`   |
| `https://github.com/apache/ozone/blob/master/hadoop-hdds/interface-server/src/main/proto/ScmServerDatanodeHeartbeatProtocol.proto` | `SCMCommandProto`, `SCMDatanodeRequest, SCMDatanodeResponse`    |
| `https://github.com/apache/ozone/blob/master/hadoop-hdds/interface-server/src/main/proto/ScmServerProtocol.proto`                  | `SCMBlockLocationRequest, SCMBlockLocationResponse`             |

As part of this change we will rearrange all the mutually exclusive requests into a `oneof` body within the same proto message. Some requests are however not mutually exclusive or are not supported by `oneof` bodies. 

For example today a `CreateVolumeRequest` implicitly piggybacks a `CreateTenantRequest` as a volume is automatically created for each new tenant. As such, the two requests are no longer mutually exclusive and both cannot be part of the `oneof` block.

Similarly, `GetS3SecretRequest` is paired with `UpdateGetS3SecretRequest` and `TenantAssignUserAccessIdRequest` is paired with `UpdateGetS3SecretRequest`.

Repeated fields like `SetSnapshotPropertyRequests` are also [not allowed](https://protobuf.dev/programming-guides/proto2/#oneof) to be part of the `oneof` block.   
Refactoring `SetSnapshotPropertyRequests` into its own message with a repeated list and then placing that new message into the oneof block does not yield much benefit when compared to the cost of deprecating the old field.

The shape of the change, using `OMRequest`:

```proto
message OMRequest {
  required Type cmdType = 1;
  optional string traceID = 2;
  required string clientId = 3;
  optional UserInfo userInfo = 4;
  optional uint32 version = 5;
  optional LayoutVersion layoutVersion = 6;
  optional ReadConsistencyHint readConsistencyHint = 7;

  // Mutually exclusive payloads. Same field numbers as today.
  oneof body {
    SetVolumePropertyRequest  setVolumePropertyRequest  = 12;
    CheckVolumeAccessRequest  checkVolumeAccessRequest  = 13;
    // ...
    CreateBucketRequest       createBucketRequest       = 21;
    // ...
    CreateKeyRequest          createKeyRequest          = 31;
    // ...
  }

  // Not mutually exclusive with the block above, so they stay flat.
  optional S3Authentication             s3Authentication            = 95;
  optional CreateVolumeRequest          createVolumeRequest         = 11;
  optional UpdateGetS3SecretRequest     updateGetS3SecretRequest    = 82;
  repeated SetSnapshotPropertyRequest   SetSnapshotPropertyRequests = 143;
}
```

## Compatibility

Three surfaces must remain byte-identical for the changes to be safe:

**1. The wire.** This is what a client emits and what a server parses. The Protobuf
[encoding guide](https://protobuf.dev/programming-guides/encoding/#oneofs) is explicit that oneof fields are encoded
the same as if they were not in a oneof. So an old client and a new client put the same bytes on the wire, and an old
server and a new server read them the same way. Every combination of old and new works.

**2. Ratis log segments.** `OMRequest` and `ContainerCommandRequestProto` are shipped through the raft log, and what
goes into the log is the wire bytes. Since those bytes do not change, neither does the log. A new binary calling
`OMRequest.parseFrom(oldBytes)` walks the same tags and puts the payload in the slot the tag maps to; only the
bookkeeping around it differs. The reverse is also fine: if a new leader writes a segment and the cluster then rolls
back to an old binary, the old `parseFrom` reads the same tags and lands each payload in its old slot.

**3. RocksDB.** RocksDB stores domain objects, never the RPC wrappers. None of the wrappers has a codec, so none of
them could have been persisted. There is nothing to migrate.

## Performance

### Java Object Layout

By using [Java Object Layout](https://github.com/openjdk/jol), the measured instance shrinkage of object sizes is shown in the following table.  
Here shallow means the memory for the object itself only, i.e the object header plus its own fields, rounded for JVM alignment.
It does not include anything the object points to, such as nested protobuf submessages, strings, lists, byte arrays, or other referenced objects. 
In JOL terms, that’s the ClassLayout / Instance size number, as opposed to the GraphLayout footprint, which includes the whole reachable object graph.

| Wrapper                         | Shallow Baseline (Bytes) | Shallow Oneof (Bytes) |                               Shrinkage |
|:--------------------------------|-------------------------:|----------------------:|----------------------------------------:|
| `OMRequest`                     |                      504 |                    96 |                                   5.25× |
| `OMResponse`                    |                      480 |                    64 |                                   7.50× |
| `ScmContainerLocationRequest`   |                      240 |                    56 |                                   4.29× |
| `ScmContainerLocationResponse`  |                      240 |                    56 |                                   4.29× |
| `ContainerCommandRequestProto`  |                      152 |                    72 |                                   2.11× |
| `ContainerCommandResponseProto` |                      136 |                    56 |                                   2.43× |
| `SCMCommandProto`               |                      104 |                    64 |                                   1.63× |
| `SCMDatanodeRequest`            |                        — |                     — | (small, tracked with `SCMCommandProto`) |

One place this shows up directly: `OzoneManagerRatisServer` keeps a retry cache with a 10 minute timeout and no
size bound. Every entry holds an `OMResponse`.

- At 5000 sustained writes/second the cache holds ~3 million entries at steady state
- At 480 B per `OMResponse` wrapper that is ~1.5 GB of pure wrapper overhead  
- Under `oneof` the same arithmetic (64 B) yields ~192 MB  
- The saving per OM is on the order of 7X, and it lives in a cache that has no other bounds

### Bare-metal 1-node cluster

Performed a Freon `ockrw` mixed workload (60% read, 10% list, 30% write), 15 Million operations per run, 16 client threads, 32 GB OM heap, G1GC, JDK 17, 80-core Xeon machine with 503 GB RAM.  
The following results compare the average of two runs each.

**Wrapper allocation, from async-profiler as leaf frame:**

| Wrapper class        | Baseline (GB) | `oneof` (GB) |   Δ    |
|:---------------------|--------------:|-------------:|:------:|
| `OMRequest`          |         44.56 |         6.96 | −84.4% |
| `OMResponse`         |         41.70 |         4.14 | −90.1% |
| `OMRequest$Builder`  |         85.77 |        49.37 | −42.4% |
| `OMResponse$Builder` |         80.09 |        43.20 | −46.1% |

Both `OMRequest` and `OMResponse` dropped out of the top-30 leaf allocators after the change.

**CPU cost on the response-side dispatcher, as % of run total:**

| Frame                                            | Baseline % | `oneof` % | Δ (rel) |
|:-------------------------------------------------|-----------:|----------:|:-------:|
| `AbstractParser.parseFrom` (all proto parses)    |     16.908 |    16.729 |  −1.1%  |
| `OMRatisHelper.getOMResponseFromRaftClientReply` |      0.839 |     0.767 |  −8.6%  |
| `OzoneManagerRatisServer.getOMResponse`          |      0.852 |     0.791 |  −7.3%  |
| `OMResponse.parseFrom`                           |      0.524 |     0.490 |  −6.4%  |

**Steady-state latency and throughput (trimmed bottom 98% of ops):**

| Metric             |    Baseline |     `oneof` |   Δ    | Baseline noise |
|:-------------------|------------:|------------:|:------:|:--------------:|
| latency mean       |    3.267 ms |    3.212 ms | −1.7%  |     0.02%      |
| est. throughput    | 4897.7 op/s | 4981.8 op/s | \+1.7% |     0.02%      |
| p98 latency        |   17.207 ms |   16.408 ms | −4.6%  |     0.05%      |
| bytes/op allocated |     1.55 MB |     1.55 MB |  flat  |       —        |

## Risks

With the flat layout, a builder could hold two request fields at once and nothing complained. 
One risk that needs to be called out is that a future change could set two `oneof` fields on one builder assuming the old flat semantics. 
This requires a consistency and change in programming methodology that does not rely on multiple requests implicitly piggybacking each other.
Any future change that needs two requests to travel together has to either keep both fields outside the `oneof` block, as the tenant and S3 secret cases do today, or define a compound request
that bundles everything the operation needs

Another possible option is to create a validator which flags the mismatch between cmdType and oneof field that is set.

## Acknowledgments
Thanks to Sumit Agrawal, Sammi Chen, Andrey Yarovoy and Rui Wang for the initial review of this proposal.
