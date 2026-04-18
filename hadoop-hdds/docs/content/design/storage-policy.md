---
title: Ozone Storage Policy Support
summary: Support storage policy in Ozone to write key data into specified types of storage media.
date: 2026-03-23
jira: HDDS-11233
status: draft
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

# Terminology

## Definitions

- Storage Policy: Defines where key data replicas should be stored in specific storage tiers.
- Storage Type: The type of each Datanode volume or container replica. Each Datanode volume can be configured with a
  storage type, including SSD, DISK, and ARCHIVE.
- Storage Tier: A specific storage tier is composed of all replicas of a container based on their storage type. For
  example, a 3-replica SSD tier consists of 3 replicas of SSD type.
- Volume: In this document, unless otherwise specified, a volume refers to the volume of a Datanode.
- Key: In this document, a key refers to an object in Ozone, including entries in both the KeyTable and FileTable.

## Storage Policy vs Storage Type vs Storage Tier

![storage-policy](https://issues.apache.org/jira/secure/attachment/13070477/storage-policy.png)

The relationship between Storage Policy, Storage Type, and Storage Tier:

- The storage policy is the property of key/bucket (managed by OM).
- The storage tier is the property of Pipeline and Container (managed by SCM).
- The storage type is the property of volume and container replica (managed by DN).
- Only the storage policy can be modified by the user directly via the ozone command.

Example:

For a keyA, its storage policy is Hot, Its Container tier is SSD tier, the Container has three replicas, all of which
are of the SSD storage type.

# User Scenarios

- User A needs a bucket that supports high-performance IO, so they create a bucket with the storage policy set to Hot.
  Data written by User A to the bucket will automatically be distributed across SSD disks in the cluster.
- User B needs higher IO performance for a specific key. They write a key with the storage policy set to Hot. The
  key's data will be distributed across SSD disks in the cluster.
- User C uses the command `aws s3 cp myfile.txt s3://my-bucket/myfile.txt --storage-class STANDARD` to upload a file
  to the Ozone SSD tier. The key's data will be distributed across SSD disks in the cluster.

# Goals

- Storage Policy: Introduce storage policy and related concepts. Define multiple storage policies and support S3
  storage class.
- Storage Policy Writing: Allow writing keys/files to specified storage tiers based on storage policy. Support S3,
  API, and shell command interfaces.
- Storage Policy Update: Enable setting and unsetting storage policies for buckets, and setting storage tiers for
  containers.
- Storage Policy Display: Support displaying the storage policy attribute of buckets and keys. Support displaying the
  storage tier of SCM containers and pipelines. Support displaying Datanode storage type usage information. Support
  checking whether the key storage policy is satisfied.
- Container Balancer: Support migrating container replicas between Datanodes to volumes of the matching storage type.
  For example, SSD type container replicas will be migrated to SSD type volumes, and will not be migrated to DISK
  type volumes.
- ReplicationManager: Support managing the storage type of container replicas to ensure that container replicas on
  Datanodes reside on the correct volumes. Ensure that the storage types of container replicas forming a storage
  tier are correct. For example, a 3-replica SSD storage tier container in SCM should consist of 3 SSD type container
  replicas, and each container replica should reside on an SSD type volume.
- DiskBalancerService: Support migrating container replicas within a Datanode to volumes of the matching storage type.
  For example, SSD type container replicas will be migrated to SSD type volumes, and will not be migrated to DISK
  type volumes.

# Design

## Supported Storage Policies

- Supported storage policies: Hot / Warm / Cold
- Supported storage tiers: SSD / DISK / ARCHIVE / EMPTY
- Supported storage types: SSD / DISK / ARCHIVE
- Supported bucket layouts: FILE_SYSTEM_OPTIMIZED, OBJECT_STORE, LEGACY
- S3 storage classes: STANDARD / STANDARD_IA / GLACIER

### Storage Policy Map to Storage Tier

| Storage Policy | Storage Tier for Write | Fallback Tier for Write |
|----------------|------------------------|-------------------------|
| Hot            | SSD                    | DISK                    |
| Warm           | DISK                   | EMPTY                   |
| Cold           | ARCHIVE                | EMPTY                   |

- Storage Tier for Write: The primary storage tier where data is written when a storage policy is specified.
- Fallback Tier for Write: If the specified storage policy cannot be satisfied with the primary storage tier, SCM
  will attempt to use this fallback tier to meet the policy requirements. EMPTY means no fallback is available.

### Storage Tier Map to Storage Type

| Tier    | Storage Type of Pipeline | One Replica Container Storage Type | Three Replica Container Storage Type | EC Container Replicas Storage Type |
|---------|--------------------------|-------------------------------------|--------------------------------------|-------------------------------------|
| SSD     | SSD                      | SSD                                 | 3 SSD                                | n SSD                               |
| DISK    | DISK                     | DISK                                | 3 DISK                               | n DISK                              |
| ARCHIVE | ARCHIVE                  | ARCHIVE                             | 3 ARCHIVE                            | n ARCHIVE                           |
| EMPTY   | -                        | -                                   | -                                    | -                                   |

### Fallback Storage Type for Container Replica Replication/Migration

| Container Replica Storage Type | Fallback Storage Types (ordered) [1] |
|--------------------------------|--------------------------------------|
| SSD                            | DISK, ARCHIVE                        |
| DISK                           | ARCHIVE                              |
| ARCHIVE                        | none                                 |

- Fallback Storage Type: During the container replica replication or migration process, if SCM cannot find a suitable
  volume type that matches the original container replica's storage type, it will attempt to use the fallback storage
  types in order.

[1] A container replica does not know the storage policy of the key or the storage tier of the SCM container it belongs
to. The container replica only knows its own expected storage type, which is why the column name is "Fallback Storage
Types" rather than "Fallback Storage Tier".

### AWS S3 StorageClass

| AWS S3 StorageClass | Ozone Storage Policy |
|---------------------|----------------------|
| STANDARD [1]        | Hot                  |
| STANDARD_IA         | Warm                 |
| GLACIER             | Cold                 |
| DEEP_ARCHIVE        | Warm                 |

> AWS StorageClass Valid Values: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING |
> GLACIER | DEEP_ARCHIVE | OUTPOSTS | GLACIER_IR | SNOW | EXPRESS_ONEZONE
> According to AWS S3 documentation, STANDARD is the highest performance S3 StorageClass, but its name is STANDARD,
> which is not straightforward to map to the Ozone SSD tier.

[1] The field names here reuse the AWS S3 field names, but the actual semantics differ from AWS S3. For example, in
Ozone, STANDARD represents the Hot storage policy, while in AWS S3, STANDARD has different semantics.

## Component Changes

### Datanode Container Replica

A storage type field is added to container replicas on Datanodes, which is persisted in the container's metadata YAML
file.

### Bucket, Key

A storage policy attribute is added to buckets and keys on OM.

### SCM Container, Pipeline

A storage tier attribute is added to containers and pipelines on SCM. A pipeline can support multiple storage tiers.
For example, if all Datanodes in a pipeline have both SSD and DISK type volumes, the pipeline's supported storage tier
attributes will include both SSD and DISK.

## Datanode Volume Storage Type

- Referencing HDFS [1], use configuration to define the storage type of each volume. If no storage type information is
  configured for a volume, the storage type will be DISK.
  - For example:
    ```xml
    <property>
      <name>hdds.datanode.dir</name>
      <value>[SSD]/mnt/disk/0/ozone,[DISK]/mnt/disk/1/ozone</value>
    </property>
    ```
    Volume /mnt/disk/0/ozone will be SSD storage type, and Volume /mnt/disk/1/ozone will be DISK storage type.

[1] Refer to https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html section
Configuration.

## Writing Keys with Storage Policy

### Ozone Filesystem

- Support specifying a storage policy when writing a key.
  - If a storage policy is specified when writing a key, the key storage policy is the specified storage policy.
  - If a storage policy is not specified when writing a key, the storage policy of the key will depend on the bucket.
    If the bucket does not have a storage policy set, the default storage policy is determined by the configuration
    `ozone.default.storagepolicy`, the default value is WARM.

- Fallback strategy:
  - When writing a key, if data cannot be written to the specified storage tier, it can be written to the fallback
    storage tier, provided the fallback storage tier is not EMPTY and fallback is allowed.
  - Fallback control: Fallback can be controlled at the bucket granularity. If allowed, the key will attempt to write
    to the fallback storage tier. The default is to allow fallback.

### S3

- If StorageClass is specified in the request, the storage policy is derived from the StorageClass in the request.
- If StorageClass is not specified in the request, the storage policy of the bucket is used.
- If the storage policy of the bucket is null, the storage policy is determined by the configuration
  `ozone.s3.default.storagepolicy`, the default value is STANDARD_IA, which maps to Ozone storage policy WARM.

### Supported APIs

#### Ozone Filesystem

- createKey
- createStreamKey
- createFile
- createStreamFile
- initiateMultipartUpload

#### S3 Request

Refer to
[Using Amazon S3 storage classes - Amazon Simple Storage Service](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html)

- PutObject:
  - Support specifying the StorageClass parameter in the PutObject request to determine the storage policy for the
    object.
- CopyObject:
  - Support specifying a new storage policy (StorageClass) in the CopyObject request and applying the new storage
    policy when copying the object from the source location to the target location. If no new storage policy is
    specified, inherit the storage policy of the source object.
- Multipart Upload:
  - Support specifying the StorageClass parameter in the CreateMultipartUpload request and following the StorageClass
    parameter of CreateMultipartUpload in UploadPart.
- GetObject:
  - Return the current storage policy of the object in the GetObject response.
- HeadObject:
  - Return the metadata of the object, including its storage policy (StorageClass).
- ListObjects:
  - Include the storage policy (StorageClass) information of each object in the ListObjects response.

### Write Key Process

#### Normal Write (No Fallback)

![create-key-with-storage-policy](https://issues.apache.org/jira/secure/attachment/13081145/create-key-with-storage-policy.png)

- Client sends CreateKey request to OM with storage policy Hot.
- OM requests SCM to allocate a container with storage policy Hot.
- SCM allocates a container and pipeline with storage tier SSD according to storage policy Hot.
- Client determines the storage type of the chunk to be SSD according to the storage tier SSD.
- DN creates the container or writes the chunk on the specified storage medium according to the storage type.

#### Fallback Write

![fallback-create-key-with-storage-policy](https://issues.apache.org/jira/secure/attachment/13081146/fallback-create-key-with-storage-policy.png)

- Client sends CreateKey request to OM with storage policy Hot.
- OM requests SCM to allocate a container with storage policy Hot.
- SCM attempts to allocate a container and pipeline with storage tier SSD according to storage policy Hot, but finds
  that there are no available containers or pipelines with SSD tier. SCM then selects the fallback tier DISK.
- Client determines the storage type of the chunk to be DISK according to the fallback storage tier DISK.
- DN creates the container or writes the chunk on the specified storage medium according to the storage type.

### Datanode Container Replica Creation

- When the Ozone client writes data to a container replica on a Datanode, the storage type is included in the request.
- When the Datanode receives a write request for a container replica and the container does not exist, the Datanode
  creates the container by selecting an appropriate volume (VolumeChoosingPolicy) based on the storage type in the
  request.
- If the storage type is not specified when creating a container replica on the Datanode, the container replica will be
  created on a volume of the storage type specified by the configuration `hdds.datanode.default.storagetype` (default
  is DISK). This typically only occurs when a client from an older version that does not support the storage policy
  feature writes data to the Datanode.
- The Datanode does not perform fallback when creating container replicas. If a volume of the storage type specified in
  the client's write request cannot be found, an exception will be thrown. Since SCM selects appropriate Datanodes to
  create pipelines and allocate containers based on Datanode reports, this situation should not occur during normal
  operation.

### SCM Pipeline Creation and Selection

- Datanodes report their storage space information to SCM through StorageReportProto, including the space usage and
  storage type of all volumes (the default storage type for a Datanode volume is DISK).
- SCM calculates the supported volume types and capacities for each Datanode based on StorageReportProto.
- When creating a container, SCM selects appropriate Datanodes to create a pipeline based on the storage tier
  corresponding to the storage policy in the request.
  - For example, the Hot storage policy corresponds to the SSD storage tier. For a 3-replica SSD storage tier, the
    pipeline will consist of 3 Datanodes that have SSD type volumes.
  - For the Hot storage policy with EC 6+3 replication, the pipeline will consist of 9 Datanodes that have SSD
    type volumes.
- If SCM cannot find enough suitable Datanodes to create a pipeline, it will try to create a pipeline using the
  fallback tier of the storage policy. If the fallback tier is EMPTY, or the fallback tier also cannot find enough
  suitable Datanodes, the creation fails.
- SCM does not mix storage types within a storage tier. An SSD storage tier pipeline will only consist of Datanodes
  with SSD type volumes, and will not include Datanodes with only DISK type volumes.
- The storage tier is an attribute of the pipeline, indicating the storage tiers supported by that pipeline. A pipeline
  can support multiple storage tiers. For example, if Datanodes all have both SSD and DISK type volumes, the pipeline
  created on these Datanodes will have supported storage tier attributes of both SSD and DISK.
- When allocating a container through an existing pipeline, SCM filters matching pipelines based on their supported
  storage tiers.


- BackgroundPipelineCreator will attempt to create pipelines for all storage tiers. If the Datanodes in the cluster
  can support a certain storage tier, BackgroundPipelineCreator will automatically create pipelines for that tier.
- The pipeline count limit is calculated independently for each storage tier.

## Storage Policy Update

Setting the bucket storage policy is supported. Existing buckets can be updated with a new storage policy.

- `ozone sh bucket update --storagepolicy <storagePolicyStr>`
  Allowed values: HOT, WARM, COLD, or null to unset.

Setting the SCM container storage tier attribute is supported. This is mainly used to update existing containers in the
cluster so that they have the specified storage tier attribute.

- `ozone admin container setstoragetier --storage-tier=<storageTierStr>`
  Allowed values: SSD, DISK, ARCHIVE, or null to unset.

## Storage Policy Display

Display the bucket storage policy attribute:

- `ozone sh bucket info`

Display the key storage policy attribute:

- `ozone sh key info`

Display the container storage tier, the storage type of its replicas, and the storage type of the volumes where the
replicas reside:

- `ozone admin container info`

List containers filtered by storage tier:

- `ozone admin container list --storage-tier=<storageTierStr>`

Display the storage tiers supported by pipelines associated with Datanodes:

- `ozone admin datanode list`

Display Datanode usage information by storage type:

- `ozone admin datanode usageinfo`

Display cluster-wide space usage information by storage type:

- `ozone admin storagepolicy usageinfo`

Check whether a key's storage policy is satisfied:

- `ozone admin storagepolicy check`

## Container Balancer

- Datanodes report the status information of all containers to SCM through ContainerReplicaProto, including the
  storage type of the container replica itself and the storage type of the volume where it resides.
- Container Balancer supports balancing based on the space usage of each storage type. Each storage type is calculated
  independently. Container replicas are migrated to volumes of the matching storage type based on the container
  replica's storage type.
- Container Balancer does not perform fallback during migration. It will not migrate a container replica to a volume
  of a different storage type.

## ReplicationManager

- Datanodes report the status information of all containers to SCM through ContainerReplicaProto, including the
  storage type of the container replica itself and the storage type of the volume where it resides.
- Datanodes report their storage space information to SCM through StorageReportProto, including the space usage and
  storage type of all volumes (the default storage type for a Datanode volume is DISK).
- ReplicationManager introduces two new container health states: MIS_STORAGE_TYPE_WITH_VOLUME and
  MIS_STORAGE_TYPE_WITH_CONTAINER.
  - MIS_STORAGE_TYPE_WITH_VOLUME: Container replica whose storage type mismatches its volume's storage type.
    - ReplicationManager will select a matching volume and migrate the container replica for this health state.
  - MIS_STORAGE_TYPE_WITH_CONTAINER: Container replica whose storage type mismatches the container's storage tier.
    - ReplicationManager will send a command to update the storage type of the container replica on the Datanode.
  - ReplicationManager will first handle MIS_STORAGE_TYPE_WITH_CONTAINER and then handle
    MIS_STORAGE_TYPE_WITH_VOLUME.
- ReplicationManager uses the SCM container's storage tier as the source of truth to check whether the container
  replicas and the volumes where replicas reside match the storage tier.
  - If the SCM container's storage tier is null, no storage tier checks will be performed. Containers created before
    the storage policy feature was introduced will have a null storage tier.
  - To manage existing containers' replicas (e.g., to prevent these container replicas from occupying SSD type
    volume space), the container storage tier needs to be set. Once set, ReplicationManager will manage the
    container replicas based on the storage tier. Otherwise, container replicas may reside on volumes of any
    storage type.
- When ReplicationManager replicates, migrates, or reconstructs container replicas (e.g., for UNDER_REPLICATED or
  MIS_REPLICATED containers), it selects matching Datanodes based on the storage type corresponding to the SCM
  container's storage tier.
- When ReplicationManager replicates, migrates, or reconstructs container replicas, if it cannot find a matching
  Datanode, it will try to use the fallback storage types defined by the SCM container's storage tier. If the storage
  tier has no defined fallback storage types, or no Datanodes matching the fallback storage types can be found, the
  operation fails.

## DiskBalancerService

- DiskBalancerService calculates space usage and migrates containers between volumes of the same storage type within a
  Datanode.

# Configurations

New configuration keys introduced by the storage policy feature:

- `ozone.default.storagepolicy`
  Default value: WARM. The default storage policy for writing keys when neither the key nor the bucket has a storage
  policy set.

- `ozone.default.storageTier`
  Default value: DISK. The default storage tier.

- `ozone.s3.default.storagepolicy`
  Default value: STANDARD_IA. The default S3 storage class when StorageClass is not specified in the S3 request and
  the bucket does not have a storage policy set. STANDARD_IA maps to Ozone storage policy WARM.

- `hdds.datanode.default.storagetype`
  Default value: DISK. The default storage type used when creating a container replica on a Datanode without a
  specified storage type. This typically applies to write requests from older clients that do not support the storage
  policy feature.

- `ozone.scm.container.allow.null.storage.tier`
  Default value: false. Whether to allow selecting containers with a null storage tier during container allocation.
  This can be used during the upgrade period to allow new keys to be written to existing containers that do not yet
  have a storage tier set.

For a pure SSD cluster (where all Datanode volumes are configured with SSD storage type), the following configurations
should be updated accordingly:

- `ozone.default.storagepolicy` should be set to HOT (Hot maps to SSD storage tier).
- `ozone.default.storageTier` should be set to SSD.
- `ozone.s3.default.storagepolicy` should be set to STANDARD (STANDARD maps to Hot storage policy).
- `hdds.datanode.default.storagetype` should be set to SSD.

Otherwise, when writing a key without specifying a storage policy, the system will default to the WARM storage policy,
which corresponds to the DISK storage tier. Since WARM does not define a fallback tier (its fallback is EMPTY), SCM
cannot fall back to another tier, and the write will fail because no DISK type volumes are available in the cluster.

Note that this situation only occurs when the volumes are explicitly configured as SSD in the `hdds.datanode.dir`
configuration (e.g., `[SSD]/mnt/disk/0/ozone`). Even if the underlying physical disks are SSDs, as long as the volume
configuration does not specify a storage type, the Datanode will treat them as DISK type volumes by default, and no
configuration changes are needed.

# Protobuf Changes

Major protobuf changes:

hdds.proto

```protobuf
enum StorageType {
  DISK_TYPE = 1;
  SSD_TYPE = 2;
  ARCHIVE_TYPE = 3;
  RAM_DISK_TYPE = 4;
  PROVIDED_TYPE = 5;
}

enum StorageTierProto {
  UNKNOWN_TIER = 0;
  DISK_TIER = 1;
  SSD_TIER = 2;
  ARCHIVE_TIER = 3;
}

enum StoragePolicyProto {
  UNKNOWN_POLICY = 0;
  HOT = 1;
  WARM = 2;
  COLD = 3;
}

enum S3StorageClassProto {
  UNKNOWN_STORAGE_CLASS = 0;
  STANDARD = 1;
  STANDARD_IA = 2;
  GLACIER = 3;
  DEEP_ARCHIVE = 4;
}

message Pipeline {
  //...
  repeated StorageTierProto supportedStorageTier = 1;
}

message ContainerInfoProto {
  //...
  optional StorageTierProto storageTier = 1;
}
```

OmClientProtocol.proto

```protobuf
message BucketInfo {
  //...
  optional hadoop.hdds.StoragePolicyProto storagePolicy = 1;
  optional bool allowFallbackStoragePolicy = 1;
}

message KeyInfo {
  //...
  optional hdds.StoragePolicyProto storagePolicy = 1;
}
```

ScmServerDatanodeHeartbeatProtocol.proto

```protobuf
message StorageReportProto {
  //...
  optional StorageType storageTypeProto = 1 [default = DISK_TYPE];
}

message ContainerReplicaProto {
  //...
  optional StorageType storageType = 1;
  optional StorageType VolumeStorageType = 1;
}
```

# HDFS Compatibility

HDFS storage policy-related commands are not supported.

Specifying a storage policy when creating a file using the HDFS Filesystem interface is not supported, as the Hadoop
FileSystem (org.apache.hadoop.fs.FileSystem) does not support passing the storage policy through the create method.

# Backward Compatibility

This section describes the backward compatibility for data created before the storage policy feature is introduced.

- Key: Existing keys will have a null storage policy.
- Bucket: Existing buckets will have a null storage policy. When writing a key/file to such a bucket without specifying
  a storage policy, the key's storage policy will be determined by the configuration `ozone.default.storagepolicy`
  (default value is WARM).
  - The bucket's storage policy can be updated via command.
- SCM Container: Existing SCM containers will have a null storage tier. ReplicationManager will not perform storage
  tier management for containers with a null storage tier, and will not attempt to maintain the storage type of
  the corresponding container replicas or the volumes where they reside.
  - The storage tier of existing containers can be set via the command `ozone admin container setstoragetier`.
- Datanode Container Replica: Existing container replicas will not have a storage type attribute. ReplicationManager
  will not manage the volumes of such container replicas, meaning they may reside on volumes of any storage type.
  - After setting a storage tier for the container via `ozone admin container setstoragetier`, ReplicationManager
    will update the container replicas' storage type accordingly.

# FAQ

## What does it mean for a key's storage policy to be satisfied?

- The key's storage policy is not null.
- The storage type of all container replicas of the key matches the key's storage policy.
- The storage type of each container replica matches the storage type of the volume where it resides.

## Which scenarios will cause the storage policy of the key to be unsatisfied?

- Fallback occurred when the user wrote the key.
- The user modified the configuration, causing the volume's storage type to change.
- ReplicationManager migrated a container replica to a volume with a mismatched storage type (due to fallback during
  replication).

## How to satisfy the key storage policy?

- If the key's storage policy does not match the SCM container's storage tier, the key needs to be rewritten. Because
  except for the upgrade scenario (setting a storage tier for containers that previously had a null storage tier), the
  SCM container's storage tier is immutable. Therefore, the key needs to be rewritten to a matching container.
- If the SCM container's storage tier does not match the Datanode container replica's storage type, or if the Datanode
  container replica's storage type does not match the storage type of the volume where it resides, ReplicationManager
  will use the SCM container's storage tier as the baseline to update the Datanode container replica's storage type or
  migrate the Datanode container replica to a matching volume.

## Is there a plan to add more storage tiers, such as NVME? Can users create custom storage policies?

Currently only a few storage policy types (Hot, Warm, Cold) and storage tiers (SSD, DISK, ARCHIVE) are implemented.
Adding new storage policies or storage tiers in the code is straightforward. Custom user-defined storage policies are
not supported at this time, but the implementation would not be difficult to add in the future.

## How does the storage tier affect pipeline creation?

Introducing a storage tier does not alter the pipeline's behavioral logic; it only affects which Datanodes compose the
pipeline. A pipeline will have a supported storage tier attribute. For example, a pipeline with an SSD storage tier
attribute will be composed of Datanodes that have SSD type volumes. The primary logic change occurs during pipeline
creation, where Datanodes with suitable volumes are selected to form the pipeline. Read and write behavior after
pipeline creation remains unchanged.

## How does the storage tier affect the number of pipelines?

The pipeline count limit is calculated independently for each storage tier. However, since a pipeline can support
multiple storage tiers simultaneously, when Datanodes have both SSD and DISK type volumes, pipelines created on these
Datanodes will support both tiers. In this case, the actual number of pipelines will not increase significantly.

## Can different keys in the same bucket have different storage policies?

Yes, each key can have its own storage policy. The bucket's storage policy only serves as the default when a key does
not specify one.

## What happens when a specific storage type runs out of space?

For example, if all SSD volumes are full, during write, if the fallback tier is defined and allowed, data will be
written to the fallback tier. If the fallback tier is EMPTY or also unavailable, the write fails.

## Does the storage policy feature require any special steps to enable after upgrading?

No special steps are required. The feature is transparent for existing clusters. Existing data will have null storage
policy/tier/type, and the default configuration (WARM/DISK) matches the existing behavior where all volumes are
treated as DISK type. Existing pipelines and containers will continue to work as before.
But for pure SSD clusters where all volumes are explicitly configured as SSD, the default configurations need to be
updated accordingly. See the Configurations section for details.

## What happens if a Datanode volume's storage type configuration is changed after containers are already created on it?

The existing container replicas on that volume will have a mismatch between their storage type and the volume's new
storage type. ReplicationManager will detect this as MIS_STORAGE_TYPE_WITH_VOLUME and attempt to migrate the replicas
to a volume with the matching storage type.

## How to check the storage type distribution of the cluster?

Use `ozone admin storagepolicy usageinfo` to view cluster-wide storage type usage, or
`ozone admin storagepolicy usageinfo --with-datanode` for per-Datanode details.
