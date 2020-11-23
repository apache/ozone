---
title: "High Availability"
weight: 1
menu:
   main:
      parent: Features
summary: HA setup for Ozone to avoid any single point of failure.
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

Ozone has two leader nodes (*Ozone Manager* for key space management and *Storage Container Management* for block space management) and storage nodes (Datanode). Data is replicated between datanodes with the help of RAFT consensus algorithm.

To avoid any single point of failure the leader nodes also should have a HA setup.

 1. HA of Ozone Manager is implemented with the help of RAFT (Apache Ratis)
 2. HA of Storage Container Manager is [under implementation]({{< ref "scmha.md">}})

## Ozone Manager HA

A single Ozone Manager uses [RocksDB](https://github.com/facebook/rocksdb/) to persiste metadata (volumes, buckets, keys) locally. HA version of Ozone Manager does exactly the same but all the data is replicated with the help of the RAFT consensus algorithm to follower Ozone Manager instances.

![OM HA](HA-OM.png)

Client connects to the Leader Ozone Manager which process the request and schedule the replication with RAFT. When the request is replicated to all the followers the leader can return with the response.

## Configuration

HA mode of Ozone Manager can be enabled with the following settings in `ozone-site.xml`:

```XML
<property>
   <name>ozone.om.ratis.enable</name>
   <value>true</value>
</property>
```
One Ozone configuration (`ozone-site.xml`) can support multiple Ozone HA cluster. To select between the available HA clusters a logical name is required for each of the clusters which can be resolved to the IP addresses (and domain names) of the Ozone Managers.

This logical name is called `serviceId` and can be configured in the `ozone-site.xml`
 
 ```
<property>
   <name>ozone.om.service.ids</name>
   <value>cluster1,cluster2</value>
</property>
```

For each of the defined `serviceId` a logical configuration name should be defined for each of the servers.

```XML
<property>
   <name>ozone.om.nodes.cluster1</name>
   <value>om1,om2,om3</value>
</property>
```

The defined prefixes can be used to define the address of each of the OM services:

```XML
<property>
   <name>ozone.om.address.cluster1.om1</name>
   <value>host1</value>
</property>
<property>
   <name>ozone.om.address.cluster1.om2</name>
   <value>host2</value>
</property>
<property>
   <name>ozone.om.address.cluster1.om3</name>
   <value>host3</value>
</property>
```

The defined `serviceId` can be used instead of a single OM host using [client interfaces]({{< ref "interface/_index.md" >}})

For example with `o3fs://`

```shell
hdfs dfs -ls o3fs://bucket.volume.cluster1/prefix/
```

Or with `ofs://`:

```shell
hdfs dfs -ls ofs://cluster1/volume/bucket/prefix/
```

## Implementation details

Raft can guarantee the replication of any request if the request is persisted to the RAFT log on the majority of the nodes. To achieve high throughput with Ozone Manager, it returns with the response even if the request is persisted only to the RAFT logs.

RocksDB instance are updated by a background thread with batching transactions (so called "double buffer" as when one of the buffers is used to commit the data the other one collects all the new requests for the next commit.) To make all data available for the next request even if the background process is not yet wrote them the key data is cached in the memory.

![Double buffer](HA-OM-doublebuffer.png)

The details of this approach discussed in a separated [design doc]({{< ref "design/omha.md" >}}) but it's integral part of the OM HA design.

## References

 * Check [this page]({{< ref "design/omha.md" >}}) for the links to the original design docs
 * Ozone distribution contains an example OM HA configuration, under the `compose/ozone-om-ha` directory which can be tested with the help of [docker-compose]({{< ref "start/RunningViaDocker.md" >}}).