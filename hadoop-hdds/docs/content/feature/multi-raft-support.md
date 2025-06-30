---
title: "Multi-Raft Support in Ozone"
menu:
  main:
    parent: Features
summary: "Enables each DataNode to participate in multiple Ratis pipelines concurrently to improve resource utilization and write throughput."
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

## Overview
The early Ozone versions supported one Raft pipeline per DataNode, which meant each DataNode could only participate in one Ratis replication group (pipeline) at a time. This limited its concurrent write handling capacity for replicated data and could lead to under-utilization of resources.

Multi-Raft support allows a DataNode to be part of multiple Ratis pipelines at once,
improving write throughput and ensuring better utilization of disk and network resources.
This is particularly useful when DataNodes have multiple disks.

## Prerequisites
- Ozone 0.5.0 or later
- Replication type: RATIS
- Multiple metadata volumes on each DataNode (via `hdds.container.ratis.datanode.storage.dir`)
- Adequate CPU, memory, and network bandwidth

## How It Works
SCM can now create overlapping pipelines: each DataNode can join multiple Raft groups, up to a configurable limit. This boosts concurrency and avoids idle nodes. Raft logs are stored separately on different metadata volumes to reduce disk contention. Ratis handles concurrent logs per node.

## How to Use
1. Configure DataNode metadata volumes:
   ```xml
   <property>
     <name>hdds.container.ratis.datanode.storage.dir</name>
     <value>/disk1/ratis,/disk2/ratis</value>
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
3. Restart SCM and DataNodes.
4. Validate with:
   ```bash
   ozone admin pipeline list
   ozone admin datanode list
   ```

## Configuration
- `ozone.scm.datanode.pipeline.limit` (default: 2)
  - Max number of pipelines per datanode can be engaged in.
    Setting the value to 0 means the pipeline limit per dn will be determined
    by the no of metadata volumes reported per dn.
- `ozone.scm.pipeline.per.metadata.disk` (default: 2)
  - Number of pipelines to be created per metadata volume.
- `hdds.container.ratis.datanode.storage.dir`: list of metadata volume paths

## Operational Tips
- Monitor with `ozone admin` CLI and Recon UI
- Ensure pipeline count matches expectations
For optimal I/O isolation, configure `hdds.container.ratis.datanode.storage.dir` with paths on multiple distinct physical disks. Ratis will distribute pipeline logs accordingly.
- Be cautious with very high pipeline counts (memory/CPU overhead)

## Limitations
- Global config: cannot set per-node limits
- Requires restart after changing storage dirs
- No effect on Erasure-Coding (EC) pipelines

## References
- Design doc: [HDDS-1564 Ozone multi-raft support](https://ozone.apache.org/docs/edge/design/multiraft.html)
