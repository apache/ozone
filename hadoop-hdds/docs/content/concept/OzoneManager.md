---
title: "Ozone Manager"
date: "2017-09-14"
weight: 2
menu: 
  main:
     parent: Architecture
summary: Ozone Manager is the principal name space service of Ozone. OM manages the life cycle of volumes, buckets and Keys.
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

![Ozone Manager](OzoneManager.png)

Ozone Manager (OM) is the namespace manager for Ozone.

This means that when you want to write some data, you ask Ozone
Manager for a block and Ozone Manager gives you a block and remembers that
information. When you want to read that file back, you need to find the
address of the block and the Ozone Manager returns it to you.

Ozone Manager also allows users to organize keys under a volume and bucket.
Volumes and buckets are part of the namespace and managed by Ozone Manager.

Each ozone volume is the root of an independent namespace under OM.
This is very different from HDFS which provides a single rooted file system.

Ozone's namespace is a collection of volumes or is a forest instead of a
single rooted tree as in HDFS. This property makes it easy to deploy multiple
OMs for scaling(pending future development).

## Ozone Manager Metadata

OM maintains a list of volumes, buckets, and keys.
For each user, it maintains a list of volumes.
For each volume, the list of buckets and for each bucket the list of keys.

Ozone Manager will use [Apache Ratis](https://ratis.apache.org/)(A Raft protocol implementation) to
replicate Ozone Manager state. This will ensure [High Availability for Ozone Manager]({{< ref "feature/OM-HA.md" >}}).


## Ozone Manager and Storage Container Manager

The relationship between Ozone Manager and [Storage Container Manager (SCM)]({{< ref "concept/StorageContainerManager.md" >}}) is best
understood if we trace what happens during a key write and key read.

### Key Write

![Ozone Manager Write Path](OzoneManager-WritePath.png)

* To write a key to Ozone, a client tells Ozone manager that it would like to
write a key into a bucket that lives inside a specific volume. Once Ozone
Manager determines that you are allowed to write a key to the specified bucket,
OM needs to allocate a block for the client to write data.

* To allocate a block, Ozone Manager sends a request to Storage Container
Manager (SCM); SCM is the manager of [data nodes]({{< ref "concept/Datanodes.md" >}}). SCM picks three data nodes
into which client can write data. SCM allocates the block and returns the
[block ID]({{< ref "concept/Datanodes.md" >}}) to Ozone Manager.

* Ozone manager records this block information in its metadata and returns the
block and a [block token]({{< ref "security/SecureOzone.md" >}}) (a security permission to write data to the block)
to the client.

* The client uses the block token to prove that it is allowed to write data to
the block and writes data to the data node.

* Once the write is complete on the data node, the client will update the block
information on Ozone manager.

### Key Reads

![Ozone Manager Read Path](OzoneManager-ReadPath.png)

* Key reads are simpler, the client requests the block list from the Ozone
Manager
* Ozone manager will return the block list and block tokens which
allows the client to read the data from data nodes.
* Client connects to the data  node and presents the block token and reads
the data from the data node.

## Main components of the Ozone Manager

For a detailed view of Ozone Manager this section gives a quick overview about the provided network services and the stored persisted data.

### Network services provided by Ozone Manager:

Ozone provides a network service for the client and for administration commands. The main service calls

 * Key, Bucket, Volume / CRUD
 * Multipart upload (Initiate, Complete…)
   * Supports upload of huge files in multiple steps
 * FS related calls (optimized for hierarchical queries instead of a flat ObjectStore namespace)
   * GetFileStatus, CreateDirectory, CreateFile, LookupFile
 * ACL related
   * Managing ACLs if [internal ACLs]({{< ref "security/SecurityAcls.md" >}}) are used instead of [Ranger]({{< ref "security/SecurityWithRanger.md" >}}) 
 * [Delegation token]({{< ref "security/SecureOzone.md" >}}) (Get / Renew / Cancel)
  * For security
 * Admin APIs
   * Get S3 secret 
   * ServiceList (used for service discovery)
   * DBUpdates (used by [Recon]({{< ref "concept/Recon.md" >}}) downloads snapshots)

### Persisted state

The following data is persisted in Ozone Manager side in a specific RocksDB directory:
 
 * Volume / Bucket / Key tables
   * This is the main responsibility of OM
   * Key metadata contains the block id (which includes container id) to find the data
 * OpenKey table
   * for keys which are created, but not yet committed
 * Delegation token table
   * for security
 * PrefixInfo table
   * specific index table to store directory level ACL and to provide better performance for hierarchical queries
 * S3 secret table
   * For S3 secret management
 * Multipart info table
   * Inflight uploads should be tracked
 * Deleted table
   * To track the blocks which should be deleted from the datanodes

## Notable configurations

key | default | description
----|---------|------------
ozone.om.address | 0.0.0.0:9862 | RPC address of the OM. Required by the client.
ozone.om.http-address | 0.0.0.0:9874 | Default port of the HTTP server.
ozone.metadata.dirs | none | Directory to store persisted data (RocksDB).
