---
title: "Storage Container Manager"
date: "2017-09-14"
weight: 3
menu: 
  main:
     parent: Architecture
summary:  Storage Container Manager or SCM is the core metadata service of Ozone. SCM provides a distributed block layer for Ozone.
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

Storage Container Manager (SCM) is the leader node of the *block space management*. The main responsibility is to create and manage [containers]({{<ref "concept/Containers.md">}}) which is the main replication unit of Ozone.


![Storage Container Manager](StorageContainerManager.png)

## Main responsibilities

Storage container manager provides multiple critical functions for the Ozone
cluster.  SCM acts as the cluster manager, Certificate authority, Block
manager and the Replica manager.

SCM is in charge of creating an Ozone cluster. When an SCM is booted up via `init` command, SCM creates the cluster identity and root certificates needed for the SCM certificate authority. SCM manages the life cycle of a data node in the cluster.

 1. SCM is the block manager. SCM
allocates blocks and assigns them to data nodes. Clients
read and write these blocks directly.

 2. SCM keeps track of all the block
replicas. If there is a loss of data node or a disk, SCM
detects it and instructs data nodes make copies of the
missing blocks to ensure high availability.

 3. **SCM's Certificate Authority** is in
charge of issuing identity certificates for each and every
service in the cluster. This certificate infrastructure makes
it easy to enable mTLS at network layer and the block
token infrastructure depends on this certificate infrastructure.

## Main components

For a detailed view of Storage Container Manager this section gives a quick overview about the provided network services and the stored persisted data.

### Network services provided by Storage Container Manager:

 * Pipelines: List/Delete/Activate/Deactivate
   * pipelines are set of datanodes to form replication groups
   * Raft groups are planned by SCM
 * Containers: Create / List / Delete containers
 * Admin related requests
  * Safemode status/modification
  * Replication manager start / stop 
 * CA authority service
  * Required by other sever components
 * Datanode HeartBeat protocol
   * From Datanode to SCM (30 sec by default)
   * Datanodes report the status of containers, node...
   * SCM can add commands to the response

Note: client doesn't connect directly to the SCM

### Persisted state

The following data is persisted in Storage Container Manager side in a specific RocksDB directory

 * Pipelines
   * Replication group of servers. Maintained to find a group for new container/block allocations.
 * Containers
   * Containers are the replication units. Data is required to act in case of data under/over replicated.
 * Deleted blocks
   * Block data is deleted in the background. Need a list to follow the progress.
 * Valid cert, Revoked certs
  * Used by the internal Certificate Authority to authorize other Ozone services

## Notable configurations

key | default | description 
----|---------|------------
ozone.scm.container.size | 5GB | Default container size used by Ozone
ozone.scm.block.size | 256MB |  The default size of a data block.
hdds.scm.safemode.min.datanode | 1 | Minimum number of datanodes to start the real work.
ozone.scm.http-address | 0.0.0.0:9876 | HTTP address of the SCM server
ozone.metadata.dirs | none | Directory to store persisted data (RocksDB).