---
title: Storage Class
summary: New abstraction to configure replication methods.
date: 2020-06-08
jira: HDDS-3755
status: implementing
author: Marton Elek
documentclass: scrartcl
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

Containers have to favors: _Open_ and _Closed_ containers: Open containers are replicated by Ratis and writable, Closed containers are replicated with simple data copy and they are read only.

In this document a new level of abstraction is proposed: the *storage-class* which defines the replication mechanism of the containers and the parameters of the replication.

# Goals / Use cases

## [USER] Simplify user interface and improve usability

Users can choose from an admin provided set of storage classes (for example *STANDARD*, *REDUCED*) instead of using implementation specific terms (*RATIS/THREE*, *RATIS/ONE*)

Today the users should use implementation specific terms when key is created:

```
ozone sh key put --replication=THREE --type=RATIS /vol1/b1/key1 a.txt
```

There are two problems here:

 1. User should use low-level, technical terms during the usage. User might not know what is *RATIS* and may not have enough information to decide the right replication scheme.

 2. The current parameters defines only the replication of the *open* containers and the same value (THREE) is used for *Closed* containers. There is no easy way to add configuration which can be used later during the lifecycle of containers/keys. No easy way to adjust behavior of the replication. (For example to support `Ratis/THREE` --> `Ratis/TWO`)

With the storage-class abstraction the complexity of configuration can be moved to the admin side, where we can provide more flexibility. Users should choose only from the available storage-classes (or use the default one) and the exact replication mechanism is defined by the administrator for each of the storage-.classes

Instead of the current CLI parameters (replication factor and type) this document proposes to use an abstract storage-class parameter instead:

```
ozone sh key put --storage-class=STANDARD /vol1/bucket1/key1 source-file.txt
```

## [USER] Set a custom replication for a newly created bucket

A user may want to set a custom replication for a bucket at the time of creation. All keys in the bucket will respect the specified storage-class (subject to storage and quota availability). E.g.

```
ozone sh bucket create --storage-class=INFREQUENT_ACCESS
```


Bucket-level default storage-class can be overridden for any key, but will be used as default.


## [USER] Fine grained replication control when using S3 API

A user may want to set custom replication policies for any key **which uploaded via S3 API**. Storage-classes are already used by AWS S3 API. With first-class support of the same concept in Ozone, users can choose from the predefined storage-classes (=replication rules) with using AWS API:


```
aws s3 cp --storage-class=REDUCED file1 s3://bucket/file1
```


## [USER] Set the replication for a specific prefix

A user may want to set a custom replication for a specific key prefix. All keys matching that prefix will respect the specified storage class. This operation will not affect keys already in the prefix.

```
ozone sh prefix setClass --storage-class=REDUCED /vol1/bucket1/tmp
```

Prefix-level default storage-class can be overridden for ay key, but will be used as default.

## [ADMIN/DEV] Support multiple replication schemes

Today Ozone uses two favor of one replication scheme which are hard coded in the code both on client and server side.

Storage-class abstraction extends this behavior to support any number of replication schemes between server and client and any new replication scheme can be introduced on server side (with code changes!).

Keys (and containers) can be categorized by storage-class which determines the replication scheme. The proposed solution provides the fundamental API which can be used implementing new replication schemes in the future.

Maintaining the storage-class property of the container makes it possible to implement a container allocation in a generic way (we need open containers for each of the storage-classes).

## [ADMIN/USER] Flexible administration

As it's mentioned above, today it's hard to configure the details of the replications for key/bucket level. The only thing what we can define is the replication type for open containers (RATIS/THREE or RATIS/ONE) which determines the later lifecycle of the keys/containers.

Any specific replication configuration can be configured only on cluster level and not on key or container level.

A storage-class can define all the parameters for the specific containers/keys:

Examples for replication mechanism and possible parameters.


 | Replication mechanism | Possible parameters     |
 |-----------------------|------------------------|
 | Open -> Closed        | ratis quorum (ONE, THREE) and closed replicas
 | Online, striped EC    | Codec algorithm, number of data and parity instances, size of striping cells |
 | Offline, container-level EC | Codec algorithm, number of data and parity, conditions to EC a Closed container. 

Definition of a storage-class means a selection of the replication mechanism (first column) and setting the parameters (depends from the selected replication mechanism).

This approach provide an easy-to-use, high-level abstraction for the user, but administrators can configure all the parameters. One replication mechanism (eg. RATIS->CLOSED) might have only limited parameters but others (like EC) have many, but the complexity will be hidden from the users. 

Configuration is not global and not cluster-level, one can have different configuration for different storage-classes (which means for different keys/containers). For example it's possible to define two storage-class with the same mechanism but with different parameters:

| storage class   | mechanism | parameters |
|-----------------|-----------|------------|
| NORMAL | RATIS->CLOSED | ratis_quorum = 3, required_closed_replicas = 3 
| REDUCED | RATIS->CLOSED | ratis_quorum = 1, required_closed_replicas = 1

Users don't need to face with these details as they can use the storage-class (or just use the pre-created buckets and use default storage-class) abstraction.

## [DEV] Give flexibility to the developers

Storage-class abstraction provides an easy way to plug-in newer replication schemes.

Ozone uses containers are the replication units which can contain multiple blocks, but keys with the same replication mechanism should be placed to the same type of containers. Storage-class is a classification for containers which are replicated in the same way.

In case of a new replication mechanism, SCM should handle the new type of the containers (open enough containers from the new type, manage the container type). Storage-class is a one-time improvement which introduces this container grouping and can guarantee the required for each replication mechanism.

Implementing new replication mechanism requires to implement only the specific part (eg. create a new `Manager` in addition to the existing `ReplicationManager` or `PipelineManager`)

## [ADMIN] Better upgrade support

Let's imagine that a new type of Open container replication is introduced (for example *RATIS-STREAM/THREE* instead of *RATIS/THREE`*. If storage-classes are stored with the keys and containers instead of the direct replication rules we can:

 1. Easily change the replication behavior of existing buckets/keys. (Storage-class could not changed but the replication mechanism or the parameters of the replication can be changed for the groups of keys / containers of that specific storage class.)
 2. Turn on experimental features for specific buckets (define new storage-class with experimental replication type for new buckets.)

As it was mentioned earlier, the storage-class of keys couldn't be changed without data movement. But the replication behavior of a storage-class can be changed (depends from allowed migration path).

Example:

 1. `storage-class=NORMAL` is defined by the administrator as `RATIS/THREE -> CLOSED/THREE` 
 2. User creates key `/key1.txt` and `/key2.txt` with `storage-class=NORMAL`
 3. Next release introduces `RATIS-STREAM/THREE -> CLOSED` replication mechanism which supports migration from `RATIS/THREE -> CLOSED`
 4. As keys are stored in the containers for `storage-class=NORMAL` it's enough to change the definition of `storage-class=NORMAL` to update the replication mechanism of the containers.

And advanced version of the same example:

 1. After the new version of Ozone (which supports `RATIS-STREAM/THREE -> CLOSED`) the administrator may create a new storage class `EXPERIMENTAL` with the new replication mechanism.
 2. New storage class can be used for an experimental bucket or prefix to test if the new replication mechanism is safe and fast enough.
 3. If it works well, the definition of `NORMAL` can also be changed as in the previous example.

 Please note that not all the replication behavor changes can be supported. For example an updat of a storage-class definition from `RATIS/THREE -> CLOSED` to `EC` may or may not be supported, depends from the business requirements.

But the storage-class abstraction can help us to implement the upgrade as it classifies the containers based on the required replication mechanism and parameters. 

## [ADMIN] Change the cluster-wide replication

An admin may decide to set a custom policy for an entire cluster.

```
ozone sh prefix setClass --storage-class=EC_6_3 /
```

# Unsupported use cases

The following use cases are specifically unsupported.

## [USER] Change the replication policy for a pre-existing key

Changing the replication policy for a pre-existing key will require data movement and re-authoring containers and hence it is unsupported.

## [USER] Defining storage-classes using Hadoop Compatible File System interface

It's not possible to defined storage-class (or any replication rule) with using *Hadoop Compatible File System* interface. However storage-class defined on bucket level (or prefix level) will be inherited, even if the keys are created view the `o3fs://` or `o3s://` interfaces

# The storage-class as an abstraction

After the definition of the use cases, let's talk about abstraction level of the storage-classes in the scope of Ozone.

## Containers in more details

Container is the unit of replication of Ozone. One Container can store multiple blocks (default container size is 5GB) and they are replicated together. Datanodes report only the replication state of the Containers back to the Storage Container Manager (SCM) which makes it possible to scale up to billions of objects.

The identifier of a block (BlockId) contains ContainerId and LocalId (ID inside the container). ContainerId can be used to find the right Datanode which stores the data. LocalId can be used to find the data inside one container.

Today Ozone operates with *Open* and *Closed* containers. For both of them we should define the following behaviors.

 * How to write to the containers?
 * How to read from the containers?
 * How to recover / replicate data in case of error
 * How to store the data on the Datanode (related to the *how to write* question?)
 
**Replication mechanism** is the definition how those Containers are used during the lifecycle of the data. It defines the used states, lifecycle and transitions between the states. 

The high level overview of *Open* containers:

 * **How to write**: Call standard Datanode RPC API on *Leader*. Leader will replicate the data to the followers
 * **How to read**: Read the data from the Leader (stale read can be possible long-term)
 * **How to replicate / recover**
    * Transient failures can be handled by new leader election
    * Permanent degradation couldn't be handled. (Transition to Closed containers is required)

The high level overview of *Closed* containers:

  * **How to write**: Closed containers are not writeable
  * **How to read**: Read the data from any nodes (Simple RPC call to the DN)
  * **How to replicate / recover**
    * Datanodes provides a GRPC endpoint to publish containers as compressed package
    * Replication Manager (SCM) can send commands to DN to replicate data FROM other Datanode
    * Datanode downloads the compressed package and import it

The current replication mechanism is the usage of Open and Closed containers with a one-way transitions between them (in case the container is full or in case of error.) 

 1. One which `RATIS/THREE` -> `CLOSED/THREE`
 2. The other one `CLOSED/ONE` -> `CLOSED/ONE`

## Storage-class

Storage class is a selection of replication mechanism and definition of parameters.

As we saw we have one replication mechanism (which uses Open and Closed containers) with two parameters:

 1. `ratis_quorum` (can be `ONE` or `THREE`)
 2. `required_closed_replicas` (can be `ONE` or `THREE`)

Note: Today the second one is explicitly set by the first one, there is no way to adjust it.

Let's call this replication mechanism (which defines the usage of Open and Closed containers and the transitions) as `NORMAL`

Today's implementation of Ozone can be described with two storage-classes:

![Current replication with Storage class](storage-class-today.png)

Both of these use the same replication mechanism but different parameters. 

| storage class   | mechanism | parameters |
|-----------------|-----------|------------|
| NORMAL | NORMAL (RATIS->CLOSED) | ratis_quorum = 3, required_closed_replicas = 3 
| REDUCED | NORMAL (RATIS->CLOSED) | ratis_quorum = 1, required_closed_replicas = 1

Please note that the storage-class is just an abstraction. After choosing the right replication mechanism and parameters based on storage-class, the replication will be exactly as before

Keys with STANDARD storage-class (*NORMAL* replication with parameters *THREE/THREE*) 

 * *First container type/parameters*: Ratis/THREE replicated containers
 * *Transitions*: In case of any error or if the container is full, convert to closed containers
 * *Second container type/parameters*: Closed/THREE container

REDUCED storage-class  (*NORMAL* replication with parameters *ONE/ONE*) 

 * *First container type/parameters*: Ratis/ONE replicated containers
 * *Transitions*: In case the container is full, convert to closed containers
 * *Second container type/parameters*: Closed/ONE container

This proposal suggests to introduce an abstraction and name for these two possible scheme. 

In the future Ozone can be improved in two directions:

 1. Introducing new replication mechanism (like EC)
 2. Make existing replication mechanism configurable (for example support  *Ratis/THREE->CLOSED/TWO*) 

Again: the storage-class is a selection of existing replication mechanism with configuration. Both of these challenges can be implemented with the storage-class abstraction.

**Key properties of the storage-class abstraction**:

 * **Storage-class can be defined by configuration**: Storage class is nothing more just the definition of the required replication with parameters.
 * **Storage-class** is based on existing replication mechanism, implemented in the code. 
 * **Object creation requires storage class**: Right now we should defined *replication factor* and *replication type* during the key creation. They can be replaced with setting only the Storage class
 * **Storage-class is property of the containers**: As the unit of replication in Ozone is container, one specific storage-class should be adjusted for each containers.
 * **Storage-class should be defined for each key** to find the right container to store. But this value can be defined by a bucket/volume/prefix level default.
 * **Changing definition/configuration of storage class** may be allowed for selected properties. (For example `required_closed_replica` of the normal replication mechanism can be changed: The change will modify the behavior of the Replication Manager, and -- eventually -- the new number of replicas will be provided.)
 * **Changing storage class of a key** is not possible without moving data between containers.
 * **Buckets,volumes and prefixes can provide default storage-classes**, to be inherited if not set on the Key level

*Note*: we already support storage-class for S3 objects the only difference is that it would become an Ozone level abstraction and it would defined *all* the container types and transitions.

## Transitions between storage classes

Storage-class is a property of all the *containers*. If a container is tagged with `STANDARD` storage-class, it defines how the container should be replicated (and what are the parameters for that specific replication mechanism).

For example a `STANDARD` container should be replicated with the normal replication mechanism.

The *normal* replication uses:

 1. `RATIS/THREE` for `Open` containers
 2. `CLOSED/THREE` for `Closed` containers
 3. `Open` -> `Closed` transition is defined in case of error or if the container is full
   
Today this replication mechanism have only one configurable options: use factor *ONE* or *THREE* for both type of containers (Open/Closed). But this can be changed for new (or existing) replication mechanism with introducing more and more parameters.

When keys are created, blocks are assigned from the appropriate containers. **There is no way to change storage-class without moving data** (distcp).

## Storage class on the user interface

Today, we need to define the replication factor (like `THREE`) and replication type (like `RATIS`) during the creation of the key. It's not possible to adjust possible replication rule for the key when it become part of a closed containers.

With storage-class support the users don't need to understand the details of the replication. Users can choose from predefined storage class (Like NORMAL, REDUCED). In the future we can provide support additional, custom storage-classes (using existing replication mechanism with different parameters) Which further improves the flexibility.

## Multi-stage storage-class

There is a very specific use case the "temperature" of data. Key (and containers) can become COLD or HOT over the the time. As we need transitions between the different **state** of the containers, the COLD data should be a new state of a container not a storage-class.

COLD and HOT data are not planned to be added, but a good example for the abstraction level of storage-classes.These are not different replication mechanism, but possible new states in an existing replication workflow.

![Storage class transitions](storage-class-transition.png)

# Implementation changes

The goal with the implementation is to introduce a new abstraction level, but use exactly the same replication mechanism as today.


**New interfaces**: To manage the storage-class interfaces we need to define the interfaces:

 * We need configuration classes for the existing replication mechanism to define the replication factor.
    * For normal `RATIS` replication only the `ratis_quorum`, and the `required_closed_replicas` can be configured and only two tuples are valid `(ONE,ONE)`, `(THREE,THREE)`
    * `STANDALONE` replication is deprecated but we need to support it until full removal (without parameters)
 * `StorageClass` is a POJO with the selection of `ReplicationMechanism` and the replication configuration.
 * We need a `StorageClassRegistry` instance which is available both from client side and server side. Today we don't need to make the configurable from external files: both the existing replication mechanisms can be configured from the code. 
 * `StorageClassRegistry` will contain the definition of the three initial storage-classes: STANDARD (RATIS/THREE), REDUCED (RATIS/ONE) and LEGACY (STANDALONE)
 * We need a *legacy mapping table* which maps the `factor/type` from any older requests to `storageClass`! This can help to keep the backward compatibility.
  
**Protocol changes**: 

  * `storageClass` field should be added to all the protocols where we use `factor/type` today. Old `factor/type` fields will be kept for compatibility reasons. 
  * Storage-class will be represented as a custom String in the RPC protocol (in storage-sensitive places it can be stored as an integer with an additional mapping).
  * There are two protocols which should be changed:
   * *Key creation* and *Key information* related structures in `OmClientProtocol` should be updated (`KeyInfo`, `MultipartUploadInfo`, `KeyArgs`,... )
   *  SCMAdmin protocol should be improved to include `storage-class` in `ContainerRequestProto`.

**Ozone Client changes**: Ozone client classes (`OzoneBucket`, `OzoneKey`,...) should be modified to propagate the `storageClass` everywhere instead of `factor/type`. To make it backward compatible we can create legacy mapping table should be used.

**Ozone CLI changes** `ozone sh` CLI will be changed to support `--storage-class` instead of `--type` and `--factor`

**HCFS changes**: `ofs://` and `o3fs://` doesn't require any modification

**Ozone Manager changes**: OM needs the following changes:

  * `storageClass` should be stored for the keys instead of `factor/type`. THe *legacy mapping table* will be used to generate storageClass for old, persisted keys which has `factory/type` in the RocksDB but no storageClass. It can be added to the appropriate codec.
  * `storageClass` should be propagated when SCM is called from OM for new container allocations.
  * If `storageClass` is not defined for a key, the default should be used from the `bucket`/`volume`. If they are not specified the cluster-wide default will be used.

**Storage Container Manager** changes:

  * `ContainerInfo` will include the `storageClass` as a property
  * Internal allocation request will contain the `storageClass` (old requests can be handled with the *legacy mapping table*)
  * The key part of the container allocation is the `SCMContainerManager.getMatchingContainer`. It already classifies the containers based on owner and creates separated containers for different owners. We need to add the `storageClass` to this classification.
  * Choosing the right pipeline requires to have `factor` and `type`. They can be get from the definition of the `storageClass`. All the remaining logic can be the same.
  * If the `storageClass` uses different replication mechanism, we should throw an error. (This part can be extended with implementing the second replication mechanism)
  * Replication Monitor can work as before, but for each container the desired replication number can be retrieved via the storage-class abstraction. (Instead of getting the factor from the container, the replication parameters of the assigned storageClass should be used).
  * Containers with unknown replication mechanism should be ignored.

**S3g** changes: S3g can be simplified. Today it has an own mapping between (s3-)storage-class and Ozone replication factor and type. With this change it can be part of the main Ozone logic. 

# Backward compatibility

As it was mentioned before `storageClass` is defined as an additional field in wire and storage protocol. This change can be fully backward compatible as for legacy requests and persistent data the `storageClass` can be calculated by a conventional *legacy mapping table*. This table can defined the standard `storageClass` names for the existing `RATIS/THREE` and `RATIS/ONE` and `STANDALONE/ONE` (deprecated) configuration options which are used today. They will be mapped to `NORMAL`, `REDUCED` and `LEGACY` storage classes internally. 

Methods of the SCM `*Manager`s supposed to be internal API, they can be changed to use `storageClass` instead of `factor/type` as long as the public interface (RPC/persistence) can support both. (Same applies to OM and client changes.)

Old client will be usable for new Ozone versions as the factor and type can be translated to storageClass with the mapping defined above.

New client can work with Ozone if it's required, with filling both the `storageClass` and legacy fields. For performance reason it should be activated if required.

SCM and OM HA can work without any changes with storage-classes as it's nothing more just a new, more generic parameter instead of the existing factor and type parameters.

From upgrade point of view the usage of `storageClass` means a 

# Additional use cases, feature plans

In addition to the possible, replication related additional options there are two very specific use cases where we can use storage-classes in the future. **Both requires more design discussion, and won't be implemented in the first phase,** but here I quickly summarize some *possible directions* with the help of the storage-class abstraction, to show the power and usefulness of the abstraction.

The exact implementation of these features (depends from further design discussion) can be different, but examples shows that the abstraction can make easier to implement new features.

## Storage-class as configuration unit

This document focuses on configuring replication mechanism for storage classes while the keys can use from pre-defined storage-classes.

It's also possible to extend these options and define different topology rules, disk placement rules (SSD, HDD, ...) on prefix/key level.

Storage-class is nothing more just the classification of containers to define the behavior and a binding between keys and containers. Any container specific configuration can be added leter.

## Random-read write

NFS / Fuse file system might require to support Random read/write which can be tricky as the closed containers are immutable. In case of changing any one byte in a block, the whole block should be re-created with the new data. It can have a lot of overhead especially in case of many small writes.

But write is cheap with Ratis/THREE containers. Similar to any `ChunkWrite` and `PutBlock` we can implement an `UpdateChunk` call which modifies the current content of the chunk AND replicates the change with the help of Ratis.

Let's imagine that we solved the resiliency of Ratis pipelines: In case of any Ratis error we can ask other Datanode to join to the Ratis ring *instead of* closing it. I know that it can be hard to implement, but if it is solved, we have an easy solution for random read/write.

If it works, we can define the following storage-class:

 * **Initial container** type/parameters: Ratis/THREE
 * **Transitions**: NO. They will remain "Open" forever
 * **Second container type**: NONE

This would help the random read/write problem, with some limitations: only a few containers should be managed in this storage-class. It's not suitable for really Big data, but can be a very powerful addition to provide NFS/Fuse backend for containers / git db / SQL db.


## Async, container-level erasure coding

To store cold data on less bits (without the 3x replication overhead) we can encode the data with Erasure Coding and store some additional bits to survive replica loss. In a streaming situation (streaming write) it can be tricky as we need enough chunks to do the Reed-Solomon magic. With containers we are in a better position. After the first transition of the Open containers we can do EC encoding and that time we have enough data to encode.

There are multiple options to do EC, one most simplest (but somewhat limited) way is to encode Containers after the first transition:

Storage class COLD:

 * *First container type/parameters*: Ratis/THREE replicated containers
 * *Transitions*: In case of any error or if the container is full, convert to closed containers
 * *Second container type/parameters*: EC RS(6,2)

With this storage class the containers can be converted to a specific EC container together with other containers (For example instead of 3 x C1, 3 x C2 and 3 x C3 containers can be converted to C1, C2, C3 + Parity 1 + Parity2).

# References

 * Storage class in S3: https://aws.amazon.com/s3/storage-classes/

