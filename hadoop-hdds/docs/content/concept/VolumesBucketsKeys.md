---
title: "Volumes, Buckets, and Keys"
date: "2025-07-03"
menu:
  main:
    parent: "Ozone Manager"
summary: "Understanding the fundamental data hierarchy in Apache Ozone: Volumes, Buckets, and Keys."
---

<!--
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

Apache Ozone organizes data in a three-level hierarchy: Volumes, Buckets, and Keys. This structure provides a flexible and scalable way to manage large datasets, similar to how traditional file systems use directories and files, but optimized for object storage.

## Overview of the Hierarchy

*   **Volumes:** The top-level organizational unit, akin to user accounts or home directories.
*   **Buckets:** Reside within volumes, similar to directories or folders, and contain the actual data objects.
*   **Keys:** The fundamental data objects, analogous to files, stored inside buckets.

```
Volume
└─── Bucket
    ├─── Key 1
    ├─── Key 2
    └─── ...
```

This hierarchy is managed by the [Ozone Manager]({{< ref "OzoneManager.md" >}}), which is the principal namespace service of Ozone.

## Volumes

### What is a Volume?

A **Volume** in Ozone is the highest level of the namespace hierarchy. It serves as a logical container for one or more buckets. Conceptually, a volume can be thought of as a user's home directory or a project space, providing a clear separation of data ownership and management.

**Key Characteristics:**
*   **Administrative Control:** Only administrators can create or delete volumes. This ensures proper resource allocation and access control at the highest level.
*   **Storage Accounting:** Volumes are used as the basis for storage accounting, allowing administrators to track resource usage per volume.
*   **Container for Buckets:** A volume can contain any number of buckets.

### Details

#### Creation and Management
Volumes are typically created and managed using the Ozone command-line interface (CLI). For example:
```bash
ozone sh volume create /myvolume
```
For more details on volume operations, refer to the [Ozone CLI documentation]({{< ref "Cli.md" >}}#volume-operations).

#### Quota Management
Volumes can have quotas applied to them, limiting the total storage space or the number of namespaces (buckets) they can consume. This is crucial for multi-tenant environments to prevent any single user or project from monopolizing resources.
*   **Storage Space Quota:** Limits the total data size within the volume.
*   **Namespace Quota:** Limits the number of buckets that can be created within the volume.

For comprehensive information on configuring and managing quotas, see the [Quota Management documentation]({{< ref "Quota.md" >}}).

#### Access Control Lists (ACLs)
Access to volumes is controlled via ACLs, which define permissions for users and groups. These permissions determine who can create buckets within a volume, list its contents, or perform other operations.
*   **Create:** Allows creating buckets within the volume.
*   **List:** Allows listing buckets within the volume.
*   **Read:** Allows reading metadata of the volume.
*   **Write:** Allows writing metadata of the volume.
*   **Delete:** Allows deleting the volume (if empty or recursively).

ACLs can be set and managed using the Ozone CLI. Refer to the [Security ACLs documentation]({{< ref "SecurityAcls.md" >}}) for more in-depth information.

#### S3 Gateway Integration (`/s3v` Volume)
For compatibility with the S3 API, Ozone uses a special volume, typically `/s3v`. By default, all buckets accessed via the S3 interface are stored under this volume. It's also possible to expose buckets from other Ozone volumes via the S3 interface using "bucket linking."
For more details, refer to the [S3 Protocol documentation]({{< ref "S3.md" >}}) and [S3 Multi-Tenancy documentation]({{< ref "feature/S3-Multi-Tenancy.md" >}}).

#### DataNode Physical Volumes vs. Ozone Manager Logical Volumes
It's important to distinguish between the logical "volumes" managed by the Ozone Manager (as described above) and the physical "volumes" (disks) managed by the DataNodes.
*   **Ozone Manager Volumes:** Logical namespace containers for buckets and keys.
*   **DataNode Volumes:** Physical storage devices (disks) on a DataNode where actual data blocks are stored in containers.
For more information on DataNode volume management, refer to the [DataNodes documentation]({{< ref "Datanodes.md" >}}).

## Buckets

### What is a Bucket?

A **Bucket** is the second level in the Ozone data hierarchy, residing within a volume. Buckets are analogous to directories or folders in a traditional file system. They serve as containers for keys (data objects).

**Key Characteristics:**
*   **Contained within Volumes:** Every bucket must belong to a volume.
*   **Container for Keys:** A bucket can contain any number of keys.
*   **No Nested Buckets:** Unlike directories, buckets cannot contain other buckets.

### Details

#### Creation and Management
Buckets are created within a specified volume.
```bash
ozone sh bucket create /myvolume/mybucket
```
For more details on bucket operations, refer to the [Ozone CLI documentation]({{< ref "Cli.md" >}}#bucket-operations).

#### Bucket Layouts (Object Store vs. File System Optimized)
Ozone supports different bucket layouts, primarily:
*   **Object Store (OBS):** The traditional object storage layout, where keys are stored with their full path names. This is suitable for S3-like access patterns.
*   **File System Optimized (FSO):** An optimized layout for Hadoop Compatible File System (HCFS) semantics, where intermediate directories are stored separately, improving performance for file system operations like listing and renaming.
For more details, refer to the [Prefix FSO documentation]({{< ref "feature/PrefixFSO.md" >}}).

#### Encryption (Transparent Data Encryption - TDE)
Buckets can be configured for Transparent Data Encryption (TDE) at the time of creation. When TDE is enabled, all data written to the bucket is automatically encrypted at rest using a specified encryption key.
For detailed steps on setting up and using TDE, refer to the [Securing TDE documentation]({{< ref "SecuringTDE.md" >}}).

#### Erasure Coding
Erasure Coding (EC) can be enabled at the bucket level to define data redundancy strategies. This allows for more efficient storage compared to replication, especially for large datasets.
For more information, see the [Erasure Coding documentation]({{< ref "feature/ErasureCoding.md" >}}).

#### Snapshots
Ozone's snapshot feature allows users to take point-in-time consistent images of a given bucket. These snapshots are immutable and can be used for backup, recovery, archival, and incremental replication purposes.
For more details, refer to the [Ozone Snapshot documentation]({{< ref "feature/Snapshot.md" >}}).

#### GDPR Compliance
Ozone provides features to support GDPR compliance, particularly the "right to be forgotten." When a GDPR-compliant bucket is created, encryption keys for deleted data are immediately removed, making the data unreadable even if the underlying blocks haven't been physically purged yet.
For more details, refer to the [GDPR documentation]({{< ref "security/GDPR.md" >}}).

#### Bucket Linking
Bucket linking allows exposing a bucket from one volume (or even another bucket) as if it were in a different location, particularly useful for S3 compatibility or cross-tenant access. This creates a symbolic link-like behavior.
For more information, see the [S3 Protocol documentation]({{< ref "S3.md" >}}) and [S3 Multi-Tenancy documentation]({{< ref "feature/S3-Multi-Tenancy.md" >}}).

#### Quota Management
Similar to volumes, buckets can also have storage space and namespace quotas applied to them.
For comprehensive information on configuring and managing quotas, see the [Quota Management documentation]({{< ref "Quota.md" >}}).

#### Access Control Lists (ACLs)
ACLs define permissions for buckets, controlling who can list keys, read/write data, or delete the bucket.
For more details, refer to the [Security ACLs documentation]({{< ref "SecurityAcls.md" >}}).

## Keys

### What is a Key?

A **Key** is the fundamental data object in Ozone, analogous to a file in a traditional file system. Keys are stored within buckets and represent the actual data that users interact with.

**Key Characteristics:**
*   **Contained within Buckets:** Every key must reside within a bucket.
*   **Immutable Data Blocks:** Once written, the underlying data blocks of a key are immutable. Updates or modifications to a key typically result in new versions or new data blocks being written, with the metadata pointing to the latest version.

### Details

#### Creation, Reading, and Management
Keys are created, read, and managed using the Ozone CLI or various client APIs (Java, S3, etc.).
```bash
ozone sh key put /myvolume/mybucket/mykey.txt /path/to/local/file.txt
```
For more details on key operations, refer to the [Ozone CLI documentation]({{< ref "Cli.md" >}}#key-operations).

#### Key Write and Read Process
When a client writes a key, the Ozone Manager handles the metadata (key name, location of data blocks), and the DataNodes store the actual data blocks. For reads, the Ozone Manager provides the client with the locations of the data blocks, which the client then retrieves directly from the DataNodes.
For a deeper dive into the key write and read process, refer to the [Ozone Manager documentation]({{< ref "OzoneManager.md" >}}).

#### Atomic Key Replacement
Ozone supports atomic key replacement, ensuring that a key is only overwritten if it hasn't changed since it was last read. This prevents lost updates in concurrent write scenarios.
For more details, refer to the [Overwriting Key Only If Unchanged design document]({{< ref "design/overwrite-key-only-if-unchanged.md" >}}).

#### Trash
When keys are deleted from File System Optimized (FSO) buckets, they are moved to a trash directory, allowing for recovery. For Object Store (OBS) buckets, keys are permanently deleted.
For more information on the trash feature, refer to the [Trash documentation]({{< ref "feature/Trash.md" >}}).

#### Encryption
If the parent bucket is encrypted, all keys written to that bucket will be transparently encrypted.
For more details, refer to the [Securing TDE documentation]({{< ref "SecuringTDE.md" >}}).

#### Access Control Lists (ACLs)
ACLs can also be applied to individual keys, providing fine-grained control over read and write permissions.
For more details, refer to the [Security ACLs documentation]({{< ref "SecurityAcls.md" >}}).
