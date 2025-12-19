---
title: "Datanodes"
date: "2017-09-14"
weight: 7
menu: 
  main:
     parent: Architecture
summary: Ozone supports Amazon's Simple Storage Service (S3) protocol. In fact, You can use S3 clients and S3 SDK based applications without any modifications with Ozone.
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

Datanodes are the worker bees of Ozone. All data is stored on data nodes.
Clients write data in terms of blocks. Datanode aggregates these blocks into
a storage container. A storage container is the data streams and metadata
about the blocks written by the clients.

## Storage Containers

![Container Metadata](ContainerMetadata.png)

A storage container is a self-contained super block. It has a list of Ozone
blocks that reside inside it, as well as on-disk files which contain the
actual data streams. This is the default Storage container format. From
Ozone's perspective, container is a protocol spec, actual storage layouts
does not matter. In other words, it is trivial to extend or bring new
container layouts. Hence this should be treated as a reference implementation
of containers under Ozone.

## Understanding Ozone Blocks and Containers

When a client wants to read a key from Ozone, the client sends the name of
the key to the Ozone Manager. Ozone manager returns the list of Ozone blocks
that make up that key.

An Ozone block contains the container ID and a local ID. The figure below
shows the logical layout of the Ozone block.

![Ozone Block](OzoneBlock.png)

The container ID lets the clients discover the location of the container. The
authoritative information about where a container is located is with the
Storage Container Manager (SCM). In most cases, the container location will be
cached by Ozone Manager and will be returned along with the Ozone blocks.


Once the client is able to locate the container, that is, understand which
data nodes contain this container, the client will connect to the datanode
and read the data stream specified by _Container ID:Local ID_. In other
words, the local ID serves as index into the container which describes what
data stream we want to read from.

### Discovering the Container Locations

How does SCM know where the containers are located ? This is very similar to
what HDFS does; the data nodes regularly send container reports like block
reports. Container reports are far more concise than block reports. For
example, an Ozone deployment with a 196 TB data node will have around 40
thousand containers. Compare that with HDFS block count of million and half
blocks that get reported. That is a 40x reduction in the block reports.

This extra indirection helps tremendously with scaling Ozone. SCM has far
less block data to process and the namespace service (Ozone Manager) as a
different service are critical to scaling Ozone.

## Data Volume Management

### What is a Volume?

In the context of an Ozone DataNode, a "volume" refers to a physical disk or storage device managed by the DataNode. Each volume can store many containers, which are the fundamental units of storage in Ozone. This is different from the "volume" concept in Ozone Manager, which refers to a namespace for organizing buckets and keys.

The status of volumes, including used space, available space and whether or not they are operational (healthy) or failed, can be looked up from DataNode Web UI.

### Defining Volumes with hdds.datanode.dir

The property `hdds.datanode.dir` defines the set of volumes (disks) managed by a DataNode. You can specify one or more directories, separated by commas. Each directory represents a volume.
For example: `/data1/disk1,/data2/disk2`, which configures the DataNode to manage two volumes.

### Volume Choosing Policy

When a DataNode needs to select a volume to store new data, it uses a volume choosing policy. The policy is controlled by the property `hdds.datanode.volume.choosing.policy`. There are two main policies:

- **CapacityVolumeChoosingPolicy (default):**
  This policy randomly selects two volumes with enough available space and chooses the one with lower utilization (i.e., more free space). This approach increases the likelihood that less-used disks are chosen, helping to balance disk usage over time.

- **RoundRobinVolumeChoosingPolicy:**
  This policy selects volumes in a round-robin order, cycling through all available volumes. It does not consider the current utilization of each disk, but ensures even distribution of new containers across all disks.

### Volume-Related Configuration Properties

| Property Name                                 | Default Value                | Description                                                                                  |
|-----------------------------------------------|------------------------------|----------------------------------------------------------------------------------------------|
| hdds.datanode.volume.choosing.policy          | CapacityVolumeChoosingPolicy | The policy used to select a volume for new containers.                                       |
| hdds.datanode.volume.min.free.space           | 20GB                         | Minimum free space required on a volume to be eligible for new containers.                   |
| hdds.datanode.volume.min.free.space.percent   | 0.02                         | Minimum free space percentage required on a volume to be eligible for new containers.        |

### Disk Balancer

Over time, operations like adding or replacing disks can cause uneven disk usage. The Ozone community is developing a Disk Balancer (see [HDDS-5713](https://issues.apache.org/jira/browse/HDDS-5713)) to automatically balance disk usage across DataNode volumes. This feature is under active development.

## Notable configurations

key | default | <div style="width: 300px;">description</div>
----|---------|------------
dfs.container.ratis.datanode.storage.dir | none | This directory is used for storing Ratis metadata like logs.
ozone.scm.datanode.id.dir | none | The path that datanodes will use to store the datanode ID.
hdds.datanode.dir | none | Determines where HDDS data will be stored on the local filesystem.
hdds.datanode.dir.du.reserved | none | Reserved space in bytes per volume. Always leave this much space free for non dfs use.
ozone.metadata.dirs | none | Directory to store persisted data (RocksDB).
ozone.recon.address | 0.0.0.0:9891 | RPC address of the Recon. Use <host:port> to connect Recon.
