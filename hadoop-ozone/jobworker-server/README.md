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

In HA, enable the endpoint on every OM member. Workers register with and heartbeat to
every member, but only the ready leader sends commands and processes their status.

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

Launch the packaged service with `ozone jobworker`; the launcher also supports the
standard daemon options. Use separate worker-local workspace directories, not datanode
data directories. An OM that loses its in-memory registration requests re-registration.

## Contract and limitations

The transport follows the reference implementation and uses **plaintext gRPC**.
It does not provide a complete worker authentication/authorization mechanism and must
not be presented as secure-cluster support. Do not expose this endpoint to an
untrusted network.

### Term fencing and idempotency

Command dispatch state lives only in the memory of the OM leader; nothing about a
command is persisted by this framework. The leader stamps every command with its Raft
term. A worker remembers the highest term it has seen for each OM service and fails a
command from an older term with `STALE_TERM` instead of running it. A status echoes
the term of its command, and the leader ignores a status from another term.

When an OM stops being the ready leader, or the term changes, it drops all in-flight
and queued commands and reports each of them to its listener through
`onStatusUpdateTimeout`. The new leader starts with no in-flight commands. A worker
that stops heartbeating becomes stale and its commands fail or time out. A worker keeps
no state across restarts and registers with a new identity each time it starts.

As a result a command may run more than once, for example when the old leader's command
is still executing while the owning workflow re-issues the work on the new leader.
Workflows must keep what work remains in their own persisted state, make every command
idempotent (for key rewrites, use the existing atomic rewrite with the expected key
generation), and change persisted state from listener callbacks only when the OM is
the ready leader. Worker handlers are statically registered through
`JobworkerService.createCommandHandlers`; none is registered by default.

Handlers and OM listeners use the existing typed registration/dispatch abstractions.
A dependent job adds its own command payload/decoder, worker handler, OM listener and
authorization requirements. This foundation does not introduce arbitrary remote code
loading or grant namespace permissions for a future job.
