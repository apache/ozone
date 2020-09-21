---
title: Storage Class
summary: New abstraction to configure replication methods.
date: 2020-06-08
jira: HDDS-3755
status: implementing
author: Marton Elek
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


# Abstract

One of the fundamental abstraction of Ozone is the _Container_ which used as the unit of the replication.

Containers have to favors: _Open_ and _Closed_ containers: Open containers are replicated by Ratis and writable, Closed containers are replicated with data copy and read only.

In this document a new level of abstraction is proposed: the *storage class* which defines which type of containers should be used and what type of transitions are supported.

# Goals / Use cases

## [USER] Simplify user interface and improve usability

Users can choose from an admin provided set of storage classes (for example `STANDARD`, `REDUCED`) instead of using implementation specific terms (`RATIS/THREE`, `RATIS/ONE`)

Today the users should use implementation spefific terms when key is created:

```
ozone sh key put --replication=THREE --type=RATIS /vol1/bucket1/key1 source-file.txt
```

There are two problems here:

 1. User should use low-level, technical terms during the usage. User might not know what is `RATIS` and may not have enough information to decide the right replication scheme.

 2. The current keys are only for the *open* containers. There is no easy way to add configuration which can be used later during the lifecycle of containers/keys. (For example to support `Ratis/THREE` --> `Ratis/TWO`)

With the storage-class abstraction the complexity of configuration can be moved to the admin side (with more flexibility). And user should choose only from the available storage-classes (or use the default one).

Instead of the earlier CLI this document proposes to use an abstract storage-class parameter instead:

```
ozone sh key put --storage-class=STANDARD /vol1/bucket1/key1 source-file.txt
```

## [USER] Set a custom replication for a newly created bucket

A user may want to set a custom replication for bucket at the time of creation. All keys in the bucket will respect the specified storage class (subject to storage and quota availability). E.g.

```
ozone sh bucket create --storage-class=INFREQUENT_ACCESS
```


Bucket-level default storage-class can be overridden for any key, but will be used as default.


## [USER] Fine grained replication control when using S3 API

A user may want to set custom replication policies for any key **which uploaded via S3 API**. Storage-classes are already used by AWS S3 API. With first-class support of the same concept in Ozone users can choose from the predefined storage-classes (=replication rules) with using AWS API:


```
aws s3 cp --storage-class=REDUCED file1 s3://bucket/file1
```


## [USER] Set the replication for a specific prefix

A user may want to set a custom replication for a specific key prefix. All keys matching that prefix will respect the specified storage class. This operation will not affect keys already in the prefix (question: consider supporting this with data movement?)

```
ozone sh prefix setClass --storage-class=REDUCED /vol1/bucket1/tmp
```

Prefix-level default storage-class can be overridden for ay key, but will be used as default.

## [ADMIN/DEV] Support multiple replication schemes

Today there are two replication schemes which are hard coded in the code. Storage-class abstraction extends this behavior to support any number of replication schemes.

Keys (and containers) can be categorized by storage-class which determines the replication scheme.

## [ADMIN/USER] Flexible administrations

As it's mentioned above, today it's hard to configure the details of the replications for key/bucket level. The only thing what we can define is the replication type for open containers (RATIS/THREE or RATIS/ONE) which determines the later lifecycle of the keys/containers.

Any specific replication configuration can be configured only on cluster level and not on key level.

A storage-class can define all the parameters for the spefific containers/keys:

As an example this could be a storage-class definitions:

```
name: STANDARD
states:
    - name: open
      replicationType: RATIS
      repliationFactor: THREE
    - name: closed
      replicationType: COPY
      repliationFactor: TWO
      rackPolicy: different
      transitions:
        - target: ec
          trigger:
             ratio: 90%
             used: 30d
    - name: ec
      codec: Reed-Solomon
      scheme:
         data: 6
         parity: 3
```

This defines a replication scheme where only two replicas are enough from closed containers, and container will be erasure encoded under the hood if the 90% of the content is not used in the last 30 days.

Please note that:

 * All the low-level details of the replication rules can be configured here by the administrators
 * Configuration is not global and not cluster-level, one can have different configuration for different storage-classes (which means for different keys/containers)
 * Users dont' need to face with these details as they can use the storage-class (or just use the pre-created buckets and use default storage-class) abstraction

## [DEV] Give flexibility to the developers

Storage-class abstraction provides an easy way to plug in newer replication schemes. New type of replications (like EC) can be supported easily as the system will be prepared to allocate different type of containers.

## [ADMIN] Better upgrade support

Let's imagine that a new type of Open container replication is introduced (`RATIS-STREAM/THREE` instead of `RATIS/THREE`). If storage-classes are stored with the keys and containers instead of the direct replication rules we can:

 1. Easily change the replicaiton method of existing buckets/keys
 2. Turn on experimental features for specific buckets


## [ADMIN] Change the cluster-wide replication

An admin may decide to set a custom policy for an entire cluster.

```
ozone sh prefix setClass --storage-class=EC_6_3 /
```

# Unsupported use cases

The following use cases are specifically unsupported.

## [USER] Change the replication policy for a pre-existing key

Changing the replication policy for a pre-existing key will require data movement and reauthoring containers and hence it is unsupported.

## [USER] Defining storage-classes using Hadoop Compatible File System interface

It's not possible to defined storage-class (or any replication rule) with using *Hadoop Compatible File System* interface. However storage-class defined on bucket level (or prefix level) will be inherited, even if the keys are created view the `o3fs://` or `o3s://` interfaces

# The storage-class as an abstraction

The previos section explained some user facing property of the storage-class concept. This section explains the concept compared to the existing Ozone design.

## Containers in more details

Container is the unit of replication of Ozone. One Container can store multiple blocks (default container size is 5GB) and they are replicated together. Datanodes report only the replication state of the Containers back to the Storage Container Manager (SCM) which makes it possible to scale up to billions of objects.

The identifier of a block (BlockId) containers ContainerId and LocalId (ID inside the container). ContainerId can be used to find the right Datanode which stores the data. LocalId can be used to find the data inside one container.

Container type defines the following:

 * How to write to the containers?
 * How to read from the containers?
 * How to recover / replicate data in case of error
 * How to store the data on the Datanode (related to the *how to write* question?)

THe current definition of *Ratis/THREE* is the following (simplified version):

 * **How to write**: Call standard Datanode RPC API on *Leader*. Leader will replicate the data to the followers
 * **How to read**: Read the data from the Leader (stale read can be possible long-term)
 * **How to replicate / recover**
    * Transient failures can be handled by new leader election
    * Permanent degradation couldn't be handled. (Transition to Closed containers is required)

The current definitions of the *Closed/THREE*:

  * **How to write**: Closed containers are not writeable
  * **How to read**: Read the data from any nodes (Simple RPC call to the DN)
  * **How to replicate / recover**
    * Datanodes provides a GRPC endpoint to publish containers as compressed package
    * Replication Manager (SCM) can send commands to DN to replicate data FROM other Datanode
    * Datanode downloads the compressed package and import it

The definitions of *Closed/ONE*:
  * **How to write**: Closed containers are not writeable
  * **How to read**: Read the data from any nodes (Simple RPC call to the DN)
  * **How to replicate / recover**: No recovery, sorry.


## Storage-class

Let's define the *storage-class* as set of used container **types and transitions** between them during the life cycle of the containers.

The type of the Container can be defined with the implementation type (eg. Ratis, EC, Closed) and with additional parameters related to type (eg. replication type of Ratis, or EC algorithm for EC containers).

Today's implementation of Ozone can be described with two storage-classes:

![](https://i.imgur.com/02rmoFP.png)

The definition of STANDARD Storage class:

 * *First container type/parameters*: Ratis/THREE replicated containers
 * *Transitions*: In case of any error or if the container is full, convert to closed containers
 * *Second container type/parameters*: Closed/THREE container

The definition of REDUCED Storage class:

 * *First container type/parameters*: Ratis/ONE replicated containers
 * *Transitions*: In case the container is full, convert to closed containers
 * *Second container type/parameters*: Closed/ONE container

This proposal suggests to introduce an abstraction and name the two possible scheme. Later we can define other storage classes as well. For example we can define (Ratis/THREE --> Closed/FIVE) storage class, or more specific containers can be used for Erasure Coding or Random Read/Write.

With this approach the storage-class can be an adjustable abstraction to define the rules of replications. Some key properties of this approach:

**Key properties of the storage-class abstraction**:

 * **Storage-class can be defined by configuration**: Storage class is nothing more just the definition of rules to store / replicate containers. They can be configured in config files and changed any time.
 * **Object creation requires storage class**: Right now we should defined *replication factor* and *replication type* during the key creation. They can be replaced with setting only the Storage class
 * **Storage-class defined on key** (and container) **level**
 * **Storage-class is property of the containers**: As the unit of replication in Ozone is container, one specific storage-class should be adjusted for each containers.
 * **Changing definition/configuration of storage class** is allowed for most of the properties. The change will modify the behavior of the Replication Manager, and -- eventually -- the containers will be replaced in a different way.
 * **Changing storage class of a key** is not possible wihout moving data between containers
 * **Buckets and volumes can provide default storage-classes**, to be inherited if not set on the Key level

*Note*: we already support storage class for S3 objects the only difference is that it would become an Ozone level abstraction and it would defined *all* the container types and transitions.

# Storage-class inside Ozone

First of all, we can configure different replication levels easily with this approach (eg. Ratis/THREE --> Closed/TWO). Ratis need quorum but we can have different replication number after closing containers.

We can also define topology related transitions (eg. after closing, one replica should be copied to different rack) or storage specific constrains (Right now we have only one implementation of the storage: `KeyValueContainer` but we can implement more and storage class provides an easy abstraction to configure the required storage).

Datanode also can provide different disk type for containers in a certain storage class (eg. SSD for fast access).

## Transitions between storage classes

Storage-class is a property of all the *containers*. If a container is tagged with `STANDARD` storage-class, it defines how the container should be replicated and what transitions should be done (in case of predefined conditions).

For example a `STANDARD` container should be replicated with `RATIS/THREE`, and closed in case the container is full. In closed *state*, the container should be replicated with the standard closed container replication.

When keys are created, blocks are assigned from the appropriate containers. **There is no way to change storage-class without moving data* (distcp).

bucket.storage-class := STANDARD --> REDUCED

container=REDUCED

key1 (storage-class=REDUCED)
key2 (straoge-class=REDUCED)

REDUCED: (RATIS/ONE -- CLOSED/ONE) --> (RATIS/TWO -- CLOSED/TWO)

## Multi-stage storage-class

There is a very specific use case the "temperature" of data. Key (and containers) can become COLD or HOT over the the time. As we need transitions between the different **state** of the containers, the COLD data should be a new state of a container not a storage class.

Therefore the STANDARD storage class can be modified to support three states: OPEN, WARM(=CLOSED), COLD. Transitino between WARM and CLOSED can be managed by SCM (if container is not used, SCM can request to enable erasure coding). Or manually (one specific container can be forced to Erasure Coding / COLD state. Only useful for administrators)

![](https://i.imgur.com/3NWzlm5.png)

## Storage class on the user interface

Today, we need to define the replication factor (like `THREE`) and replication type (like `RATIS`) during the creation of the key. It's not possible to adjust possible replication rule for the key when it become part of a closed containers.

With storage-class support the users don't need to udnerstand the details of the replication. Users can choose from the well-known or custom-defined storage-classes.

# Implementation changes

**Phase 1**: while storage class abstraction can provide a very generic framework for replication, in the first implementation It's proposed to keep it as simple as possible:

 * Don't introduce any new custom container state
 * Don't introduce any new transitions
 * Don't introduce any new custom storage classes

But we can start to use the abstraction for the existing implementation:

 * We will implement two, hard-coded storage-classes (STANDARD, REDUCED) which can configure the behavior of existing replication schemes
 * Ozone Client will be changed to support `--storage-class` instead of `--type` and `--factor`
 * `ofs://` and `o3fs://` doesn't require any modification
 * Storage class will be used as a custom String in RPC protocol (in storage-sensitive places it can be stored as an integer with an additional mapping)
 * All the replication logic (PipelineManager/ReplicationManager) will work exactly as before. Storage-class will be resolved to the required replication config. Pipelines will have the same type as before (eg. Ratis/THREE)
 * SCM allocateContainer logic should be changed. Right now it creates a predefined number of Open containers for each `owner` (high level applocation). It can be extended easily to create open containers for each storage-classes
 * Having slightly more open containers doesn't make any difference (5-10 more open containers per disk vs. the 5000-10000 existing containers)

# Backward compatibility

 * In allocateBlock and intra-service communication we can replace replication type and replication factor with storage-class
 * In client API replication factor and replication type will be optional
 * Legacy type/factor definitions will be mapped to real storage classes under the hood (which defines the real replication scheme). For example `Ratis/THREE --> STANDARD`

# Additional use cases

In addition to the possible, replication related additional options there are two very specific use cases where we can use storage classes. Both requires more design discussion but here I quickly summarize some *possible directions* with the help of the storage class abstraction.

This section demonstrates the power of the abstraction the exact implementation of these features can be (and probably will be slightly) different

## Erasure coding

To store cold data on less bits (less than the data * 3 replicas) we can encode the data and store some additional bits to survive replica loss. In a streaming situation (streaming write) it can be tricky as we need enough chunks to do the Reed-Solomon magic. With containers we are in a better position. After the first transition of the Open containers we can do EC encoding and that time we have enough data to encode.

There are multiple options to do EC, one most simplest way is to encode Containers after the first transition:

Storage class COLD:

 * *First container type/parameters*: Ratis/THREE replicated containers
 * *Transitions*: In case of any error or if the container is full, convert to closed containers
 * *Second container type/parameters*: EC RS(6,2)

With this storage class the containers can be converted to a specific EC container together with other containers (For example instead of 3 x C1, 3 x C2 and 3 x C3 containers can be converted to C1, C2, C3 + Parity 1 + Parity2).

## Random-read write

NFS / Fuse file system might require to support Random read/write which can be tricky as the closed containers are immutable. In case of changing any one byte in a block, the whole block should be re-created with the new data. It can have a lot of overhead especially in case of many small writes.

But write is cheap with Ratis/THREE containers. Similar to any `ChunkWrite` and `PutBlock` we can implement an `UpdateChunk` call which modifies the current content of the chunk AND replicates the change with the help of Ratis.

Let's imagine that we solved the resiliency of Ratis pipelines: In case of any Ratis error we can ask other Datanode to join to the Ratis ring *instead of* closing it. I know that it can be hard to implement, but if it is solved, we have an easy solution for random read/write.

If it works, we can define the following storage-class:

 * **Initial container** type/parameters: Ratis/THREE
 * **Transitions**: NO. They will remain "Open" forever
 * **Second container type**: NONE

This would help the random read/write problem, with some limitations: only a few containers should be managed in this storage-class. It's not suitable for really Big data, but can be a very powerful addition to provide NFS/Fuse backend for containers / git db / SQL db.

# References

 * Storage class in S3: https://aws.amazon.com/s3/storage-classes/


