<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 -->

# Jobworker foundation

The standalone Jobworker registers with OM, sends heartbeats and worker reports,
receives commands in heartbeat responses, dispatches registered handlers asynchronously,
and reports command status/results.

This module is the generic foundation only. No production job handler is registered:
key migration, lifecycle scheduling, rewrite APIs and migration metadata are not included.

## Enable and start

OM does not create the Jobworker endpoint or its background services by default.
Set `ozone.om.jobworker.service.enabled=true` in the OM configuration and restart OM
to enable the framework.

For HA jobs, upgrade and enable the endpoint on every OM member and finalize the
`JOBWORKER_HA` layout feature before submitting work. The first durable submission
verifies each member's finalized protocol and cluster/service identity. The proof
is committed with the job, allowing subsequent failover with an OM unavailable.
Membership changes require revalidation. An unavailable or older member blocks
initial activation instead of receiving an unsupported Ratis request.

The OM gRPC port is configured by `ozone.om.jobworker.grpc.port` (default `8982`).
HA deployments can set `ozone.om.jobworker.grpc.port.<serviceId>.<nodeId>`.
Worker endpoint discovery uses the host from the corresponding `ozone.om.address`
configuration together with the Jobworker gRPC port, not the ordinary OM RPC port.

On the worker, configure:

- `ozone.jobworker.om.service.ids`: OM service IDs to connect to in HA mode, with their
  ordinary per-node OM addresses and Jobworker ports. All services must belong to one cluster.
- `ozone.om.address`: the OM host for non-HA discovery.
- `ozone.jobworker.storage.volume.dirs`: worker-local workspace directories.
- `ozone.jobworker.heartbeat.interval`: heartbeat interval (default `30s`).
- `ozone.jobworker.rpc.timeout`: per-request deadline (default `10s`).
- `ozone.jobworker.metadata.dir`: persistent identity and command journal directory;
  defaults to `jobworker-state` under the first configured storage volume.
- `ozone.jobworker.journal.max.records`: retained receipt limit (default `10000`).

Launch the packaged service with `ozone jobworker`; the launcher also supports the
standard daemon options. Use separate worker-local workspace directories, not datanode
data directories. An OM that loses its in-memory registration requests re-registration.

## Contract and limitations

The transport follows the reference implementation and uses **plaintext gRPC**.
It does not provide a complete worker authentication/authorization mechanism and must
not be presented as secure-cluster support. Do not expose this endpoint to an
untrusted network.

Worker membership and delivery cursors are reconstructible local state. Assignments,
attempt identities and results are committed through OM Ratis into `jobworkerJobTable`.
The leader commits an assignment before dispatch and a result before acknowledging it.
A worker keeps the same identity across restart, persists a receipt before enqueue,
and persists EXECUTING before invoking the handler. An OM election does not create
a new attempt or restart an executing handler.

Use an explicit, positive, stable workflow ID when constructing a durable command.
Do not reuse that ID for different logical work. The JVM-local implicit ID constructor
is only suitable for transient control commands; durable submission rejects it.
Result listeners are notified at least once and must commit their effects idempotently
before returning. Override `decodeCommand` if a listener needs a command-specific
Java type reconstructed after failover. Worker handlers are statically registered
through `JobworkerService.createCommandHandlers`; none is registered by default.

Worker restart converts a receipt that was EXECUTING to UNCERTAIN, without rerunning it.
A missing receipt during reconciliation also stays UNCERTAIN. The owning workflow
must inspect authoritative effects and, where necessary, fence obsolete mutations
before calling the internal `resolveUncertain` operation with the current revision.
There is no automatic reassignment of uncertain work and no generic exactly-once
guarantee for arbitrary external effects.

Terminal results are retained until an explicit committed acknowledgement, not merely
a successful heartbeat. Acknowledged receipts remain as duplicate-suppression tombstones.
The journal manifest detects missing admitted receipts, and missing/corrupt identity,
manifest or receipt files fail startup closed. Never delete or roll back this metadata
to make space, and do not share/reuse it between workers or clusters.

Admission stops at the configured receipt limit without dropping safety state.
Automatic journal/job retirement is not implemented in this foundation; capacity and
the dependent workflow's retention policy must be planned explicitly. Terminal OM
job records are also retained. This is a deployment constraint, not permission to
delete receipts or blindly retry timed-out work.

Handlers and OM listeners use the existing typed registration/dispatch abstractions.
A dependent job adds its own command payload/decoder, worker handler, OM listener and
authorization requirements. This foundation does not introduce arbitrary remote code
loading or grant namespace permissions for a future job.
