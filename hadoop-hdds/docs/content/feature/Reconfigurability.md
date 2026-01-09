---
title: "Dynamic Property Reload"
weight: 11
menu:
   main:
      parent: Features
summary: Dynamically reload configuration properties without restarting Ozone services.
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Ozone supports dynamic reloading of certain configuration properties without restarting services. This enables operators to tune cluster behavior, adjust limits, and update settings in production without service disruption.

## Overview

When a property is marked as reconfigurable, you can:
1. Modify the property value in the configuration file (`ozone-site.xml`)
2. Invoke the reconfig command to apply the changes to the running service

The reconfiguration is performed asynchronously, and you can check the status to verify completion.

## Command Reference

```shell
ozone admin reconfig --service=[OM|SCM|DATANODE] --address=<ip:port> <operation>
```

### Options

| Option | Description |
|--------|-------------|
| `--service` | The service type: `OM`, `SCM`, or `DATANODE` |
| `--address` | RPC address of the target server (e.g., `hadoop1:9862`) |
| `--in-service-datanodes` | (DataNode only) Apply to all IN_SERVICE datanodes |

### Operations

| Operation | Description |
|-----------|-------------|
| `start` | Execute reconfiguration asynchronously |
| `status` | Check the status of a reconfiguration task |
| `properties` | List all reconfigurable properties for the service |

## Reconfigurable Properties Reference

### Ozone Manager (OM)

| Property | Default | Description |
|----------|---------|-------------|
| `ozone.administrators` | - | Comma-separated list of Ozone administrators |
| `ozone.readonly.administrators` | - | Comma-separated list of read-only administrators |
| `ozone.om.server.list.max.size` | `1000` | Maximum server-side response size for list operations |
| `ozone.om.volume.listall.allowed` | `true` | Allow all users to list all volumes |
| `ozone.om.follower.read.local.lease.enabled` | `false` | Enable local lease for follower read optimization |
| `ozone.om.follower.read.local.lease.lag.limit` | `10000` | Maximum log lag for follower reads |
| `ozone.om.follower.read.local.lease.time.ms` | `5000` | Lease time in milliseconds for follower reads |
| `ozone.key.deleting.limit.per.task` | `50000` | Maximum keys to delete per task |
| `ozone.directory.deleting.service.interval` | `60s` | Directory deletion service run interval |
| `ozone.thread.number.dir.deletion` | `10` | Number of threads for directory deletion |
| `ozone.snapshot.filtering.service.interval` | `60s` | Snapshot SST filtering service run interval |

### Storage Container Manager (SCM)

| Property | Default | Description |
|----------|---------|-------------|
| `ozone.administrators` | - | Comma-separated list of Ozone administrators |
| `ozone.readonly.administrators` | - | Comma-separated list of read-only administrators |
| `hdds.scm.block.deletion.per-interval.max` | `500000` | Maximum blocks SCM processes per deletion interval |
| `hdds.scm.replication.thread.interval` | `300s` | Interval for the replication monitor thread |
| `hdds.scm.replication.under.replicated.interval` | `30s` | Frequency to check the under-replicated queue |
| `hdds.scm.replication.over.replicated.interval` | `30s` | Frequency to check the over-replicated queue |
| `hdds.scm.replication.event.timeout` | `12m` | Timeout for replication/deletion commands |
| `hdds.scm.replication.event.timeout.datanode.offset` | `6m` | Offset subtracted from event timeout for datanode deadline |
| `hdds.scm.replication.maintenance.replica.minimum` | `2` | Minimum replicas required for node maintenance |
| `hdds.scm.replication.maintenance.remaining.redundancy` | `1` | Remaining redundancy required for maintenance (EC) |
| `hdds.scm.replication.datanode.replication.limit` | `20` | Max replication commands queued per datanode |
| `hdds.scm.replication.datanode.reconstruction.weight` | `3` | Weight multiplier for reconstruction commands |
| `hdds.scm.replication.datanode.delete.container.limit` | `40` | Max delete container commands queued per datanode |
| `hdds.scm.replication.inflight.limit.factor` | `0.75` | Factor to scale cluster-wide replication limit |
| `hdds.scm.replication.container.sample.limit` | `100` | Number of containers sampled per state for debugging |
| `ozone.scm.ec.pipeline.minimum` | `5` | Minimum EC pipelines to keep open |
| `ozone.scm.ec.pipeline.per.volume.factor` | `1` | Factor for calculating EC pipelines based on volumes |

### DataNode

| Property | Default | Description |
|----------|---------|-------------|
| `hdds.datanode.block.deleting.limit.per.interval` | `20000` | Maximum blocks deleted per interval on a datanode |
| `hdds.datanode.block.delete.threads.max` | `5` | Maximum threads for block deletion |
| `ozone.block.deleting.service.workers` | `10` | Number of block deletion service workers |
| `ozone.block.deleting.service.interval` | `60s` | Block deletion service run interval |
| `ozone.block.deleting.service.timeout` | `300s` | Block deletion service timeout |
| `hdds.datanode.replication.streams.limit` | `10` | Maximum replication streams per datanode |

## Usage Examples

### List Reconfigurable Properties

To view all properties that can be dynamically reconfigured:

```shell
$ ozone admin reconfig --service=OM --address=hadoop1:9862 properties
OM: Node [hadoop1:9862] Reconfigurable properties:
ozone.administrators
ozone.om.server.list.max.size
ozone.om.volume.listall.allowed
ozone.om.follower.read.local.lease.enabled
ozone.om.follower.read.local.lease.lag.limit
ozone.om.follower.read.local.lease.time.ms
```

### OM Reconfiguration Example

Modify `ozone.administrators` in `ozone-site.xml`, then execute:

```shell
$ ozone admin reconfig --service=OM --address=hadoop1:9862 start
OM: Started reconfiguration task on node [hadoop1:9862].

$ ozone admin reconfig --service=OM --address=hadoop1:9862 status
OM: Reconfiguring status for node [hadoop1:9862]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.
SUCCESS: Changed property ozone.administrators
From: "hadoop"
To: "hadoop,bigdata"
```

### SCM Reconfiguration Example

Modify `ozone.administrators` in `ozone-site.xml`, then execute:

```shell
$ ozone admin reconfig --service=SCM --address=hadoop1:9860 start
SCM: Started reconfiguration task on node [hadoop1:9860].

$ ozone admin reconfig --service=SCM --address=hadoop1:9860 status
SCM: Reconfiguring status for node [hadoop1:9860]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.
SUCCESS: Changed property ozone.administrators
From: "hadoop"
To: "hadoop,bigdata"
```

### DataNode Reconfiguration Example

Modify `hdds.datanode.block.deleting.limit.per.interval` in `ozone-site.xml`, then execute:

```shell
$ ozone admin reconfig --service=DATANODE --address=hadoop1:19864 start
Datanode: Started reconfiguration task on node [hadoop1:19864].

$ ozone admin reconfig --service=DATANODE --address=hadoop1:19864 status
Datanode: Reconfiguring status for node [hadoop1:19864]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.
SUCCESS: Changed property hdds.datanode.block.deleting.limit.per.interval
From: "20000"
To: "30000"
```

### Batch Operations (DataNode Only)

To perform reconfiguration on all IN_SERVICE datanodes simultaneously:

```shell
$ ozone admin reconfig --service=DATANODE --in-service-datanodes start
Datanode: Started reconfiguration task on node [hadoop1:19864].
Datanode: Started reconfiguration task on node [hadoop2:19864].
Datanode: Started reconfiguration task on node [hadoop3:19864].
Reconfig successfully 3 nodes, failure 0 nodes.
```

To list properties across all datanodes:

```shell
$ ozone admin reconfig --service=DATANODE --in-service-datanodes properties
DN: Node [hadoop1:19864] Reconfigurable properties:
hdds.datanode.block.deleting.limit.per.interval
Datanode: Node [hadoop2:19864] Reconfigurable properties:
hdds.datanode.block.deleting.limit.per.interval
Datanode: Node [hadoop3:19864] Reconfigurable properties:
hdds.datanode.block.deleting.limit.per.interval
Reconfig successfully 3 nodes, failure 0 nodes.
```

## Best Practices

1. **Test in non-production first**: Always validate configuration changes in a test environment before applying to production.

2. **Change one property at a time**: When making multiple changes, apply them incrementally to isolate the impact of each change.

3. **Monitor after changes**: Watch cluster metrics and logs after reconfiguration to ensure the changes have the desired effect.

4. **Document changes**: Keep a record of configuration changes for troubleshooting and audit purposes.

5. **Use batch operations carefully**: When using `--in-service-datanodes`, ensure all nodes should receive the same configuration.
