---
title: "Multi-Raft Support in Ozone"
menu:
  main:
    parent: Features
summary: "Enables each Datanode to participate in multiple Ratis pipelines concurrently to improve resource utilization and write throughput."
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

## Multi-Raft in Datanodes

Multi-Raft support allows a Datanode to participate
multiple Ratis replication groups (pipelines) at the same time
for improving write throughput and ensuring better utilization
of disk and network resources.
This is particularly useful when Datanodes have multiple disks
or the network has a very high bandwidth.

### Background

The early Ozone versions supported only one Raft pipeline per Datanode.
This limited its concurrent write handling capacity for replicated data
and led to under-utilization of resources.
The use of Multi-Raft tremendously improved the resource utilization.

## Prerequisites
- Ozone 0.5.0 or later
- Replication type: RATIS
- Multiple metadata directories on each Datanode
  (via `hdds.container.ratis.datanode.storage.dir`)
  - Adequate CPU, memory, and network bandwidth

## How It Works
SCM can now create overlapping pipelines:
each Datanode can join multiple Raft groups
up to a configurable limit.
This boosts concurrency and avoids idle nodes and idle disks.
Raft logs are stored separately on different metadata directories
in order to reduce disk contention.
Ratis handles concurrent logs per node.

## Configuration
- `hdds.container.ratis.datanode.storage.dir` (no default)
    - A list of metadata directory paths.
- `ozone.scm.datanode.pipeline.limit` (default: 2)
    - The maximum number of pipelines per datanode can be engaged in.
      The value 0 means that
      the pipeline limit per datanode will be determined
      by the number of metadata disks reported per datanode;
      see the next property.
- `ozone.scm.pipeline.per.metadata.disk` (default: 2)
    - The maximum number of pipelines for a datanode is determined
      by the number of disks in that datanode.
      This property is effective only when the previous property is set to 0.
      The value of this property must be greater than 0.

## How to Use
1. Configure Datanode metadata directories:
   ```xml
   <property>
     <name>hdds.container.ratis.datanode.storage.dir</name>
     <value>/disk1/ratis,/disk2/ratis,/disk3/ratis,/disk4/ratis</value>
   </property>
   ```
2. Set pipeline limits in `ozone-site.xml`:
   ```xml
   <property>
     <name>ozone.scm.datanode.pipeline.limit</name>
     <value>0</value>
   </property>
   <property>
     <name>ozone.scm.pipeline.per.metadata.disk</name>
     <value>2</value>
   </property>
   ```
3. Restart SCM and Datanodes.
4. Validate with:
   ```bash
   ozone admin pipeline list
   ozone admin datanode list
   ```


## Operational Tips
- Monitor with `ozone admin` CLI and the Recon UI.
- Ensure pipeline count matches expectations.
  For optimal I/O isolation,
  configure `hdds.container.ratis.datanode.storage.dir`
  with paths on multiple distinct physical disks.
  The Ratis pipelines will be distributed accordingly.
  - Be cautious with very high pipeline counts due to memory/CPU overhead.

## Advanced Ratis Configuration

Any Ratis configuration properties can be set using a corresponding prefix.
The following table shows the prefixes in each Ozone component.

| Ozone Components                | Configuration Prefixes  |
|---------------------------------|-------------------------|
| Ozone Manager (OM)              | `ozone.om.ha`           |
| Storage Container Manager (SCM) | `ozone.scm.ha`          |
| Datanode                        | `hdds.ratis`            |
| Ozone Client                    | `hdds.ratis`            |

See also [Apache Ratis configuration
documentation](https://github.com/apache/ratis/blob/ratis-3.2.1/ratis-docs/src/site/markdown/configurations.md).

## Limitations
- Global configuration: cannot set per-node limits
- Requires restart after changing storage dirs
- No effect on Erasure-Coding (EC) pipelines

## References
- Design doc: [HDDS-1564 Ozone multi-raft support](https://ozone.apache.org/docs/edge/design/multiraft.html)
