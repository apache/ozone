---
title: Service Limits
description: An overview of the scalability and resource limits within Apache Ozone.
---

This document provides an overview of the scalability and resource limits within Apache Ozone. Some of these limits are defined by configuration, while others are practical limits of the system's architecture.

## Naming Conventions

### Volume Names

*   **Minimum Length:** 3 characters
*   **Maximum Length:** 63 characters
*   **Constraints:** Volume names must be DNS-compliant. They cannot contain underscores, start or end with a period or dash, have two contiguous periods, or have a dash after a period.

### Bucket Names

*   **Minimum Length:** 3 characters
*   **Maximum Length:** 63 characters
*   **Constraints:** Bucket names must be DNS-compliant. They cannot contain underscores, start or end with a period or dash, have two contiguous periods, or have a dash after a period.

### Key (Object) Names

*   **Maximum Length:** There is no explicitly defined limit on the length of a key name. The primary constraint is the underlying storage capacity for metadata.
*   **Constraints:** Key names must not contain the following characters: ``, `{`, `}`, `<`, `>`, `^`, `%`, `~`, `#`, `|`, ``, `[`, `]`, or non-printable ASCII characters.

## Object and Storage Limits

### Maximum Blocks per Object

There is **no hard limit** on the number of blocks that can be in a single object. The practical limit is determined by the Ozone Manager's ability to manage the object's metadata.

### Maximum Chunk Size

The maximum size of a single chunk is **32 MB**, as defined by `OZONE_SCM_CHUNK_MAX_SIZE`. Blocks are composed of multiple chunks.

### Maximum Object Size

There is no hard limit on the maximum size of a single object. An object is composed of one or more blocks, and the number of blocks per key is practically unlimited. The block size defaults to **256MB** and is configurable via the `ozone.scm.block.size` property. Therefore, the maximum object size is determined by the number of blocks multiplied by the block size.

### Multipart Uploads

*   **Minimum Part Size:** 5 MB (`OM_MULTIPART_MIN_SIZE`)
*   **Maximum Part Size:** While not explicitly defined by a constant, the maximum part size is generally understood to be **5 GB**, following the de-facto standard of the S3 protocol.
*   **Maximum Number of Parts:** 10,000 parts per upload (`MAXIMUM_NUMBER_OF_PARTS_PER_UPLOAD`)

### Erasure Coding (EC) Constraints
The Erasure Coding policy used for a bucket imposes a requirement on the minimum number of DataNodes available. For an EC policy with `k` data blocks and `m` parity blocks (e.g., `RS-k-m-1024k`):
*   A minimum of **k + m** DataNodes are required to write new data.
*   A minimum of **k** DataNodes are required to reconstruct and read data.
For example, the common `RS-6-3-1024k` policy requires at least 9 DataNodes to create new blocks.

## Cluster and Component Scalability

### Ozone Manager (OM)
*   **Non-HA:** 1 active OM.
*   **HA (High Availability):** Typically 3 OMs are run in an HA setup. This configuration requires an odd number of nodes (e.g., 3, 5, 7) for leader election. 3 is the most common deployment.

### Storage Container Manager (SCM)
*   **Non-HA:** 1 SCM.
*   **HA (High Availability):** An SCM HA cluster also typically consists of 3 nodes.

### Datanode
There is no configured limit to the number of DataNodes. The practical limit is determined by the capacity of the OM and SCM to handle heartbeats and block reports. Clusters can scale to thousands of DataNodes.

### S3 Gateway and HTTPFS Gateway
There is no limit to the number of gateway instances. These services are stateless and can be scaled out horizontally behind a load balancer as needed.

### Recon Server
Typically, only **one** Recon server is run per cluster. It is a centralized service for monitoring and is not designed for an HA configuration.

### Pipelines
There is no configured limit on the number of pipelines. The SCM automatically creates and destroys pipelines based on the number of healthy DataNodes and data replication requirements.

## Resource Limits

### Keys (Objects)
*   **Per Cluster:** This is the most important practical limit of an Ozone cluster. There is no configured limit, but the total number of objects is limited by the Ozone Manager's memory, as it holds all object metadata on its heap.
*   **Per Volume / Per Bucket:** There is no configured limit on the number of keys within a volume or a bucket.

### Volumes
*   **Maximum Volumes per Cluster:** There is no configured limit. The practical limit is determined by the Ozone Manager's capacity to manage the metadata.
*   **Maximum Volumes per User:** There is no configured limit.

### Buckets
*   **Maximum Buckets (Cluster-wide):** The maximum number of buckets per cluster is configurable via `ozone.om.max.buckets`, which defaults to **100,000**. This limit is global and applies to the total number of buckets across all volumes.
*   **Maximum Buckets (Per Volume):** There is no configured limit for the number of buckets per volume.

### Containers
There is no configured limit on the number of containers. The container ID is a 64-bit integer, making the theoretical limit `Long.MAX_VALUE`. The practical limit is determined by the SCM's resources (CPU, memory, disk I/O) to manage container metadata.

### Quotas
The maximum value for a namespace (object count) or storage space (bytes) quota is `Long.MAX_VALUE` (approximately 8 exabytes). For practical purposes, the quota size is unlimited.

### Snapshots
*   **Maximum Snapshots:** The maximum number of filesystem snapshots allowed in an Ozone Manager is configurable via `ozone.om.fs.snapshot.max.limit`, which defaults to **10,000**.

#### Snapshot Diffs
The `snapshot diff` operation, used for disaster recovery and replication, has practical limits to control its resource consumption on the Ozone Manager.
*   **Maximum Concurrent Jobs:** The number of concurrent snapshot diff jobs is limited by `ozone.om.snapshot.diff.thread.pool.size`, which defaults to **10**.
*   **Maximum Key Changes per Job:** A single diff job will fail if it involves more than `ozone.om.snapshot.diff.max.allowed.keys.changed.per.job` keys, which defaults to **10,000,000**. This prevents a single, massive diff from overwhelming the system.

### S3 Multi-Tenancy
There is no configured limit on the number of S3 tenants. The practical limit is determined by the performance of the external Apache Ranger instance used for authorization.

### Users, ACLs, Tags, and Other Metadata
*   **Maximum Users:** The number of users is not explicitly limited.
*   **ACLs (Access Control Lists):** There is no explicit limit on the number of ACLs. The limit is a practical one, constrained by the overall size of the request sent to the Ozone Manager.
*   **Tags:** Similar to ACLs, there is no hard limit on the number of tags.
*   **CORS (Cross-Origin Resource Sharing) and Lifecycle Policies:** Limits on these features are not explicitly defined.

## Component Hardware and Resources

### JVM Heap Size
The heap size for each Java-based service is controlled by JVM flags (`-Xms`, `-Xmx`) and is practically limited by the host machine's physical RAM. The following are general recommendations.
*   **Ozone Manager (OM):** The OM heap is the most critical parameter for cluster scale.
    *   *Minimum:* 8 GB for a test cluster.
    *   *Recommendation:* Size according to the number of objects. Production clusters may need **128 GB, 256 GB, or more**.
*   **Storage Container Manager (SCM):**
    *   *Minimum:* 8 GB.
    *   *Recommendation:* Typically smaller than the OM heap. May require **32-64 GB** for very large clusters.
*   **Recon Server:**
    *   *Minimum:* 8-16 GB.
    *   *Recommendation:* May require **32-64 GB** on large clusters to handle processing metadata from OM and SCM.
*   **Datanode:**
    *   *Minimum:* 4 GB.
    *   *Recommendation:* **8-16 GB** is usually sufficient. Allocating excess memory is often wasteful.
*   **S3 Gateway & HTTPFS Gateway:**
    *   *Minimum:* 4 GB.
    *   *Recommendation:* **8-16 GB** is sufficient for most workloads.

### DataNode Storage
*   **Total Manageable Space:** A DataNode has no internal limit on the total storage it can manage. The limit is determined by the host's OS and hardware (number of disks multiplied by their capacity).
*   **Data Directories:** There is **no configured limit** on the number of data directories a DataNode can use. You can specify multiple paths in the `hdds.datanode.dir` property.
*   **Disk Size:**
    *   *Maximum Disk Size:* Determined by the underlying operating system and filesystem, not Ozone.
    *   *Minimum Free Space:* DataNode volumes must have a minimum amount of free space to be considered for new data. This is controlled by `hdds.datanode.volume.free-space.min`, which defaults to **10 GB**.
