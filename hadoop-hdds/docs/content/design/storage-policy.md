---
title: Ozone Storage Policy Support
summary: Support Ozone storage strategy, and support to write key into the specified type of storage medium.
date: 2024-07-25
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

## Terminology

- Storage Policy: Defines where key data replicas should be stored in specific storage tiers.
- Storage Type: The types of disks/Container replicas in a Datanode, storage type could include RAM_DISK, SSD, HDD, ARCHIVE, etc.
- Storage Tier: A set of Container replicas in a cluster that satisfy the storage policy.
- Volume: In this document, unless otherwise specified, a volume refers to the volume of a Datanode..
- prefix: The prefix in this article, unless otherwise specified, refers to the prefix of the storage policy type, not the ACL prefix. The prefix of the storage policy type is used to configure the prefix of the storage policy for the specified prefix.

## Storage Policy vs Storage Type vs Storage Tier

![storage-policy](https://issues.apache.org/jira/secure/attachment/13070477/storage-policy.png)

The relation of Storage Policy, Storage Type and Storage Tier

- The storage policy is the property of key/bucket/ prefix (Managed by OM);
- The storage tier is the property of Pipeline and Container (Managed by SCM);
- The storage type is the property of volume and Container replicas (Managed by DN);
- Only the storage policy can be modified by the user directly via ozone command;

Example:

For a keyA, its storage policy is Hot, its Container 1 tier is SSD tier, and Container 1 has three replicas, all of which are of the SSD storage type.

# User Scenarios

- User A needs a bucket that supports high-performance IO, so create a bucket with the storage policy set to Hot. Data written by User A to bucket will automatically be distributed across the SSD disks in the cluster.
- User B needs higher IO performance for the directory/prefix /project/metadata, so set the storage policy for the prefix /project/metadata to Hot. Subsequently, data written to /project/metadata will be automatically distributed across the SSD disks in the cluster.
- User C has already written key1 to the cluster and requires better IO performance. The storage policy for key1 can be set to Hot, and then a migration can be triggered to move key1 to the SSD disks.
- Use D use command `aws s3 cp myfile.txt s3://my-bucket/myfile.txt --storage-class XXX` upload a file the Ozone SSD tier

# Current Status

- Ozone currently has some support for tiered storage such as storage type, and some parts of this article may already be implemented.
- Currently, in Ozone, when a key is created, the key's Block can appear on any volume of a Datanode. When a key is created, SCM first needs to allocate a Block for the key through Pipelines. The Client then writes the Block to the corresponding Datanode based on the Pipeline information. In this process, the smallest element managed by the SCM Pipeline is the Datanode, and when the Datanode creates a Container, the Container may appear on any volume with enough remaining space. Under the current architecture, Ozone does not support writing data to specific disks

# Goal Requirements Specification

### **Support for Storage Policy Writing and Management**

- **Writing keys**: Allow keys to be written to specified storage tiers based on storage policies.
- **Policy Management**: Enable setting, unsetting, and inheriting storage policies for keys, prefixes, and buckets. Inherit policies based on the longest matching prefix or bucket if no specific policy is set.

### **Support for Data Migration Across Different Storage Policies**

- **Data Migration**: Support data migration across different storage policies via manual triggers, ensuring data is moved to the appropriate storage tiers.

### **Adaptation of AWS S3 StorageClass**

- **S3 StorageClass Mapping**: Map AWS S3 storage classes to Ozone storage policies, supporting related API operations (PutObject, CopyObject, Multipart Upload, GetObject, HeadObject, ListObjects).

### **Management and Monitoring Tools**

- **Storage Policy Commands**: Provide tools to view storage policies of containers, datanode usage, and pipeline information.
- **Metrics and Monitoring**: Enable visibility into storage policy compliance, container storage types, and space information across different storage policies.

### **Future Enhancements**

- **Intelligent Storage Policies**: Plan to support automatic data migration based on access frequency, similar to S3 Intelligent-Tiering.
- **Bucket StorageClass Lifecycle Rules: Support setting storage policies Lifecycle Rules at the bucket level.**
- **Recon Support**: Enhance Recon to display relevant storage tier information.

# Detailed Requirements Specification

## Storage Policy and Storage Types

### Supported Storage Types

- Specify the Storage Type for each volume through configuration. If no Storage Type is specified, the default value will be DISK.
- Support Storage Type:SSD / DISK / ARCHIVE / RAM_DISK

### Supported Storage Policies

Support storage policy: Hot , Warm, Cold

### Storage Policies Map To Storage Tiers

| Storage Policy | Storage Tier for Write | Fallback Tier for Write |
| --- | --- | --- |
| Hot | SSD | DISK |
| Warm | DISK | none |
| Cold | ARCHIVE | none |
- **Storage Tier For Write**: The priority storage tier where data is written when storage policy is specified.
- **Fallback Tier for Write**: If the specified storage policy cannot be satisfied with the priority storage tier, the SCM will attempt to use this fallback tier to meet the policy requirements.

### Storage Tier Map To Storage Type

| Tier | StorageType of Pipeline | One Replication 
Container Replicas Storage Type | Three replication
Container Replicas Storage Type | EC
Container Replicas Storage Type |
| --- | --- | --- | --- | --- |
| SSD | SSD | SSD | 3 SSD | n SSD |
| DISK | DISK | DISK | 3 DISK | n DISK |
| ARCHIVE | ARCHIVE | ARCHIVE | 3 ARCHIVE | n ARCHIVE |

### Fallback Storage Type For Container replicas Replication/Migration

| Container Replicas Type | Container Replicas Fallback Storage Type [1] |
| --- | --- |
| SSD | DISK |
| DISK | none |
| ARCHIVE | none |
- Container Replicas Fallback Storage Type: During the Container replicas replication or migration process, if the SCM cannot find a suitable volume type that matches the original Container replica's storage type, it will attempt to use this fallback storage tier.

[1] For a Container replicas, it will not know the Storage Policy of the Container’s key or the tier of the SCM Container located, the Container replicas just know its own expected storage type, So column name is “Fallback Storage Type”

## Support for Ozone Storage Policy Writing and Management

### Support storage policy writing

- Support specifying a storage policy when writing a key.
    - If a storage policy is specified when writing a key, the key storage policy is the specified storage policy.
    - If no storage policy is specified, the default behavior refers to the "Inheritance of storage policy" section.
    - If a key neither inheriting any storage policy nor specified a storage policy when writing a key, then the key storage policy will be default storage policy (can refers to the "default storage policy" section)
    - If the priority storage policy is not satisfied, support writing to the fallback tier if the fallbackStrategy is “allow”

### Support fallback strategy configuration

- fallbackStrategy
    - Allow (default): In this case, the behavior is similar to HDFS, with automatic fallback, and it does not trigger errors or additional alerts;
    - Prohibit: Prohibit fallback; if a tier that satisfies the storage policy cannot be found, the write operation fails directly.

### Inheritance of storage policies

- If no storage policy is specified (undefined storage policy) when writing a key, the key's storage policy inherits the longest matching prefix. If there is no matching prefix, it inherits the storage policy of the bucket. If the bucket has no effective storage policy [1], the key's storage policy will be the default storage policy .
- If a key is created with an effective storage policy, the storage policy of the key will not change with the storage policy changing of the bucket or prefix.

[1] Effective storage policy means a non-empty storage policy.

### undefined storage policy

- If the user does not specify any storage policy when creating a key, the user's storage policy is undefined storage policy.
- Even if the user's key inherits the storage policy of the bucket/prefix, the user's storage policy is still undefined storage policy.
- Undefined storage policy does not mean the key no storage policy. if the key inherits a storage policy, the key actual storage policy is the inherited storage policy.
- The undefined storage policy will change as the changing of the prefix/bucket storage policy, including when the key is renamed to a prefix with a different storage policy.

### default storage policy

- If a key neither inheriting any storage policy nor specified a storage policy when writing a key, then the key storage policy will be default storage policy
- The default storage policy is the storage policy for existing keys before the storage policy feature is launched. That is, all keys have at least the default storage policy, even if the key was created before the storage policy feature was launched.
- If the user has not configured a default storage policy, the default storage policy should be Warm.
- The default storage policy can be configured.

## Storage policy management

### key storage policy

- Support setting and unsetting the storage policy for keys.
    - After unsetting the storage policy, the actual storage policy of the key refers to the "Inheritance of storage policy" section, or is the default storage policy if key do not inherit any storage policy.
- Support displaying the storage policy in the key list/info results(Include whether the storage policy is the default storage policy).

### Bucket storage policy management

- Support setting and unsetting storage policies for buckets.
    - After unsetting the storage policy, the storage policy is the default storage policy.
- Support specifying a storage policy when creating a bucket.
    - If no storage policy is specified, the storage policy is the default storage policy.
- Support displaying the storage policy in the bucket list/info results (Include whether the storage policy is the default storage policy).

### Prefix management

- Support creating, deleting, setting, getting, and listing prefixes.
- The display of the prefix storage policy should display whether the storage policy is the default storage policy
- A prefix can only have one type of policy.
- Prefixes do not support unsetting storage policies; deleting a prefix is equivalent to unsetting the storage policy.

### Support for persistent storage of storage policy changes:

- Use ozone admin storagepolicies satisfyStoragePolicy to trigger the migration of corresponding changes, and mark the corresponding storage policy changes as completed.

### FSO type buckets:

- Use prefixes to implement directory-level storage policy management, not directly support setting storage policies for directories.
- Do not support setting storage policies for directory-type keys.

## Adaptation of AWS S3

### Adaptation of AWS S3 StorageClass

Not all the StorageClass will be support by the Ozone

A possible solution

| AWS S3 StorageClass | Ozone StoragePolicy |
| --- | --- |
| STANDARD | Hot |
| STANDARD_IA | Warm |
| GLACIER | COLD |

> According to AWS S3 documentation, STANDARD is the highest performance S3 StorageClass, but its name is STANDARD, which is not easy to convert it to OZONE SSD

> AWS StorageClass Valid Values: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | OUTPOSTS | GLACIER_IR | SNOW | EXPRESS_ONEZONE

### Adaptation of AWS S3 Related API

refer to
[Using Amazon S3 storage classes - Amazon Simple Storage Service](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html)

- PutObject:
    - Support specifying the StorageClass parameter in the PutObject request to determine the storage policy for the object.
- CopyObject:
    - Support specifying a new storage policy (StorageClass) in the CopyObject request and applying the new storage policy when migrating the object from the source location to the target location. If no new storage policy is specified, inherit the storage policy of the source object.
- Multipart Upload
    - Support specifying the StorageClass parameter in the CreateMultipartUpload request and following the StorageClass parameter of CreateMultipartUpload in UploadPart.
- GetObject operation:
    - Return the current storage policy of the object in the GetObject response.
- HeadObject:
    - Support the HeadObject request to return the metadata of the object, including its storage policy (StorageClass).
- ListObjects:
    - Include the storage policy (StorageClass) information of each object in the ListObjects response.
- Bucket StorageClass Lifecycle Rules:
    - Support setting storage policies Lifecycle Rules at the bucket level and automatically managing the storage policy conversion of objects through policies and lifecycle rules. For example, automatically transfer objects from SSD to HDD or from HDD to NVMe SSD based on the object's age or access frequency.
- ~~RestoreObject operation: [Not Supported]~~

## Support for Storage Policy Management Commands/Metrics

Lists commands that need to be added/adapted to storage policies, but may not be all commands

- Storage policy management
    - `ozone admin storagepolicies list`, lists all supported policies in the current cluster.
    - `ozone admin storagepolicies get ${key}`, get the key storage policy (include the inherited storage policy)
    - `ozone admin storagepolicies check ${key}`, check whether the key storage policy is satisfied
    - `ozone admin storagepolicies satisfyStoragePolicy -bucket / -prefix / -key`, triggers migrations to satisfy the corresponding data's storage policy.
    - `ozone admin storagepolicies satisfyStoragePolicy status -bucket / -prefix / -key`, checks the migration status of the storage policy.
    - `ozone admin storagepolicies checkSatisfyStoragePolicy -bucket / -prefix / -key`, checks whether the specified resource's storage policy is satisfied.
        - Containers may be migrated to a fallback tier. At this time, the storage policy of the corresponding resource is not satisfied.
- `ozone admin container info`, supports displaying the storage policy of Containers.
- `ozone admin datanode usageinfo`, supports displaying space information according to different storage policies.
- `ozone admin pipeline list`, supports displaying the storage tier supported by Pipelines.
- `ozone admin pipeline create`, supports creating Pipelines that support specified storage tier.
- Datanode volume related Metrics support displaying the storage policies of volumes.
- SCM supports displaying the data space information of different storage policies in the cluster.

## Storage Policy Satisfier Service

- Support data migration across different storage policies.
    - Storage policy migrations are triggered by ozone admin storagepolicies satisfyStoragePolicy. Modifying the storage policy of a bucket/prefix/key does not directly trigger storage policy migrations.
- Support manually setting storage policies for buckets/prefixes/keys and implementing asynchronous migrations.
- Responsible for managing storage policy-related migration work.
- Responsible for responding to user requests and checking whether the specified resources satisfy the storage policy.

## Permissions Management

- Changing the storage policy of buckets/prefixes/keys requires administrator permissions.

## Storage Policy Supported Replicas Types

- Support replica type storage policies.
- Support EC type storage policies.
    - EC types cannot support some storage policies that mix different storage media.

## Storage Policy Supported Bucket Types

- Support all types of buckets.

## Compatible HDFS Commands

HDFS storage policy-related commands are not supported.

Do not support specified storage policy when creating a file using HDFS Filesystem interface, the Hadoop FileSystem (org.apache.hadoop.fs.FileSystem) does not support passing the storage policy by Create method.

- Ozone needs to record the access frequency of keys/buckets.

## Recon Side Feature

Support displaying relevant storage tier information in Recon. Need to design more detailed. // TODO

# Architecture Design

This section aims to describe the architecture and design concepts of the system without delving into specific implementation details. Its purpose is to validate the feasibility and rationality of the technical route, as well as to assess the workload and technical dependencies of related tasks.

## Writing Key Process

Writing data to the specified tier:

- When a user creates a key, they set the storage policy and fallbackStrategy parameters and send the request to the OM:
    - CreateKey
    - CreateFile
    - CreateMultipartKey
- Community Ozone:
    - CreateStreamKey
    - CreateStreamFile
    - CreateMultipartStreamKey
- OM adds the storage policy information to the allocateBlock request sent to the SCM. The actual storage strategy should be calculated based on the storage strategy requirements above.
- SCM, upon receiving the request, selects the Pipeline based on the storage policy and allocates a Container.
    - The storage type of each Container replicas should be added to for the Datanodes in the Pipeline, just like the replicaIndex for the EC key.
    - If the expected storage tier Pipeline cannot be found, attempt to find the fallback tier defined by the storage policy if the fallbackStrategy is allowed. If it cannot be found, the creation process fails.
    - If the SCM needs to create a new Container for a specific storage tier, SCM needs to record the Container’s storage tier for the Container. So the ContainerData should add a storage tier field.
- After receiving the Pipeline with the specified storage type information, the client writes the Chunk replica with the storage type information.
- Datanode writes data to the specified Container replica or creates a Container replica in the specified volume based on the storage type .
- OMKeyInfo needs to persist in the storage policy field, if the storage policy does not be specified, the storage policy should be null.

## DN Container Replica Creation

- DN should select the appropriate volume based on the storage type.
- If the Container includes storage type information, DN should store this information in the Container's replicas YAML configuration.
- When DN starts, it should read the storage type information from the Container's YAML configuration and load it into memory.
- If the storage type of Container is not specified, it defaults to Disk.

## Storage Policy Management

- Implement key, prefix, and bucket storage policy-related commands as needed, and add related fields to persist tier information.

## SCM Pipeline / ContainerReplica Management and Block Allocation

- Pipeline needs to add tier-related fields to distinguish between different tiers of Pipelines.
- For Pipelines tier:
    - Pipeline adds tier attribute, Pipeline belongs to a specific tier
    - If the Pipeline tier is null, then the Pipeline tier is Disk, so all the Pipeline tier created before storage policy release should be Disk.
        - After the storage policy is released, the Container which is allocated by the existing Pipeline may not be created normally due to the change of the storage type of the Datanode volume. For example, if all volumes on a Datanode of the existing Pipeline1 are configured as SSD, but the tier of the existing Pipeline1 is Disk and the storage type of the Container replicas is Disk, the creation of this Container will fail because no suitable volume can be found.
        - For the above scenario the Pipeline should be closed and SCM should create a new Pipeline.
    - SCM should select the appropriate Pipeline based on the storage policy selection information in the request during getPipelines.
    - If no suitable Pipeline is found, select the appropriate Datanode to create the Pipeline.
    - If no suitable Datanode is found, fallback to creating a Pipeline for the defined fallback tier Pipeline.
    - If a fallback Pipeline tier cannot be created, AllocateBlock fails.
    - Currently, Pipelines are selected using a polling method (PipelineStateMap#getPipelines). If there are many Pipelines, performance may be affected, so a more efficient method can be considered.

## Datanode Heartbeat Reporting Process

- Datanode reports the storage type information of each volume through heartbeats. SCM aggregates and manages Datanodes based on different storage types, including the remaining capacity of each storage type.
    - Currently, Datanode reports volume capacity information through NodeReport. The storage type information of volume can be reported to SCM in DATA_VOLUME of getNodeReport.
    - When creating Pipelines, SCM needs to choose appropriate Datanodes based on storage type information.

## Datanode Storage Type Management

- Referencing HDFS [1], use configuration to define the tier type of each volume. If no tier information is configured for a volume, it defaults to the default tier. (default tier should be DISK)
    - When Datanode starts, it should load the tier information into memory based on the configuration.

[1] refer to https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html section Configuration

## Container Replicas Storage Type Management

- Container replicas need to add storage type related fields and persist storage to support Container migration.
- ContainerReport should add storage type information (recorded in the Container YAML) of the Container replicas and its located volume storage type (after cluster upgrade, configuration changes, or fallback during migration, the Container storage type may not match its actual storage type).
    - DiskBalancerService should ensure that the balanced Container resides in the volume of the expected tier during the balancing process.

## SCM Adapts to ReplicationManager

### SCM Adapts to ReplicationManager

- During Container replicas migration, SCM should consider whether the target Datanode contains the corresponding tier and the capacity of that tier.
    - If the expected storage type cannot be found during migration, attempt to find the storage type defined by fallback storage. If it cannot be found, the migration fails.

### Container replicas Storage Type Satisfier

- Checking whether the Container is in the expected tier. If not, trigger the migration process.
- Current ReplicationManager logic can be reused, making this check one of its checks.
- Add a new Container HealthState, such as STORAGE_TYPE_MISMATCH, and support the count the number of the Container in the STORAGE_TYPE_MISMATCH HealthState.

## DN Adapts to Container Migration

- Adapt DiskBalancerService (ReplicationManager) to prioritize migrating Container replicas to their expected storage type.

## Data Migration

TODO

## Backward Compatibility

- Existing keys without any storage policy will be the default storage policy.
- The default storage policy can be configured, the default value of the default storage policy is Warm
- If the volume is not configured as a non-Disk storage type and the default storage policy is Warm, then after the storage policy is released, the storage policy of all data in the cluster is satisfied.

# QA

## Which scenarios will cause the storage policy of the key to be unsatisfied?

1. When the bucket/prefix/key storage policy is modified by the user.
2. Rename the key with storage policy "undefined storage policy" and the name after key rename matches the new storage policy prefix with different storage policy.
3. When ReplicateManager or DiskBalancerService migrates/replicates container replicas, a fallback occurs
4. When the configuration of the volume changes, after restarting, all the Container replica storage types located on the volume will not be satisfied.

## How to satisfy the key storage policy

- For above scenarios 1 ~ 2, they can be found/satisfied by the “Key Storage Policy Satisfier.”
- For above scenarios 3 ~ 4, they can be found/satisfied by the “Container replica Storage Type Satisfier (ReplicationManager).”