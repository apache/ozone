---
name: Ozone
title: An Introduction to Apache Ozone
menu: main
weight: -10
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

## What is Apache Ozone?

Apache Ozone is a scalable, distributed object store designed for lakehouse workloads,
AI/ML, and cloud-native applications.
Originating from the BigData analytics ecosystem, it handles both small and large files,
supporting deployments up to billions of objects and exabytes of capacity.
Ozone provides strong consistency guarantees,
multiple protocol interfaces (including S3 compatibility), and configurable durability options.

## What it does?

Ozone includes features relevant to large-scale storage requirements:

### Scale

Ozone's architecture separates metadata management from data storage. The Ozone Manager (OM) and Storage Container Manager (SCM) handle metadata operations, while Datanodes manage the physical storage of data blocks. This design allows for independent scaling of these components and supports incremental cluster growth.

### Flexible Durability

Ozone offers configurable data durability options per bucket or per object:
*   **Replication (RATIS):** Uses 3-way replication via the [Ratis (Raft)](https://ratis.apache.org) consensus protocol for high availability.
*   **Erasure Coding (EC):** Supports various EC codecs (e.g., Reed-Solomon) to reduce storage overhead compared to replication while maintaining specified durability levels.

### Secure

Security features are integrated at multiple layers:
*   **Authentication:** Supports Kerberos integration for user and service authentication.
*   **Authorization:** Provides Access Control Lists (ACLs) for managing permissions at the volume, bucket, and key levels. Supports Apache Ranger integration for centralized policy management.
*   **Encryption:** Supports TLS/SSL for data in transit and Transparent Data Encryption (TDE) for data at rest.
*   **Tokens:** Uses delegation tokens and block tokens for access control in distributed operations.

### Performance

Ozone's design considers performance for different access patterns:
*   **Throughput:** Intended for streaming reads and writes of large files. Data can be served directly from Datanodes after initial metadata lookup.
*   **Latency:** Metadata operations are managed by OM and SCM, designed for low-latency access.
*   **Small File Handling:** Includes mechanisms for managing metadata and storage for large quantities of small files.

### Multiple Protocols

Applications can access data stored in Ozone through several interfaces:
*   **S3 Protocol:** Provides an S3-compatible REST API, allowing use with S3-native applications and tools.
*   **Hadoop Compatible File System (OFS):** Offers the `ofs://` scheme for integration with Hadoop ecosystem tools (e.g., Iceberg, Spark, Hive, Flink, MapReduce).
*   **Native Java Client API:** A client library for Java applications.
*   **Command Line Interface (CLI):** Provides tools for administrative tasks and data interaction.

### Efficient Storage Use

Ozone includes features aimed at optimizing storage utilization:
*   **Erasure Coding:** Can reduce the physical storage footprint compared to 3x replication.
*   **Small File Handling:** Manages metadata and block allocation for small files.
*   **Containerization:** Groups data blocks into larger Storage Containers, which can simplify management and disk I/O.

### Storage Management

Ozone uses a hierarchical namespace and provides management tools:
*   **Namespace:** Organizes data into Volumes (often mapped to tenants) and Buckets (containers for objects), which hold Keys (objects/files).
*   **Quotas:** Administrators can set storage quotas at the Volume and Bucket levels.
*   **Snapshots:** Supports point-in-time, read-only snapshots of buckets for data protection and versioning.

### Strong Consistency

Ozone provides strong consistency for metadata and data operations. Reads reflect the results of the latest successfully completed write operations.

## Key Characteristics

The design of Ozone leads to certain characteristics relevant for large-scale data management:

### Storage Costs

Factors influencing storage costs include:
*   **Storage Efficiency:** Erasure Coding can reduce physical storage requirements.
*   **Hardware:** Designed to run on commodity hardware.
*   **Licensing:** Apache Ozone is open-source software under the Apache License 2.0.
*   **Scalability:** Clusters can be expanded by adding nodes or racks. Data rebalancing mechanisms help manage utilization.

### Operations

Aspects related to storage administration include:
*   **Unified Storage:** Can potentially serve as a common storage layer for different types of workloads.
*   **Management Tools:** Includes the Recon web UI for monitoring and CLI tools for administration.
*   **Maintenance:** Supports features like rolling upgrades, node decommissioning, and data balancing.

### Hybrid Cloud Scenarios

Ozone's S3 compatibility allows applications developed for S3 to run on-premises using Ozone. This can be relevant for hybrid cloud strategies or migrating workloads between on-premises and cloud environments.

## Dive Deeper

To learn more about Ozone, refer to the following sections:

*   **New to Ozone?** Try the **[Quick Start Guide]({{< ref "start" >}})** to set up a cluster.
*   **Want to understand the internals?** Read about the **[Core Concepts]({{< ref "concept" >}})** (architecture, replication, security).
*   **Need to use Ozone?** Check the **[User Guide]({{< ref "interface" >}})** for client interfaces and integrations.
*   **Managing a cluster?** Consult the **[Administrator Guide]({{< ref "tools" >}})** for installation, configuration, and operations.
