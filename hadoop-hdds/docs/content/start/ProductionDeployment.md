---
title: Production Deployment
weight: 6
menu:
  main:
    parent: Getting Started
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

This document provides guidance on the requirements and best practices for a production deployment of Apache Ozone.

## Ozone Components

A typical production Ozone cluster includes the following services:

*   **Ozone Manager (OM)**: Manages the namespace and metadata of the Ozone cluster. A production cluster requires 3 OM instances for high availability.
*   **Storage Container Manager (SCM)**: Manages the data nodes and pipelines. A production cluster requires 3 SCM instances for high availability.
*   **DataNode**: Stores the actual data in containers. A production cluster requires at least 3 DataNodes.
*   **Recon**: A web-based UI for monitoring and managing the Ozone cluster. A Recon server is strongly recommended, though not required.
*   **S3 Gateway (S3G)**: An S3-compatible gateway for accessing Ozone. Multiple S3 Gateway instances are strongly recommended to load balance S3 traffic.
*   **HttpFs**: An HDFS-compatible API for accessing Ozone. This is an optional component.

## Requirements

### System Requirements

*   **Hardware**: Bare metal machines are recommended for optimal performance. Virtual machines or containers are not recommended for production deployments.
*   **Operating System**: Linux (recommended distributions: Red Hat 8/Rocky 8+, Ubuntu, SUSE; supported architectures: x86/ARM).
*   **Java Development Kit (JDK)**: Version 8 or higher.
*   **Time Synchronization**: A time synchronization service such as Chrony or ntpd must be enabled to prevent time drift.

### Memory Requirements

*   **Ozone Manager (OM), Storage Container Manager (SCM), and Recon**: Recommended heap size in large production clusters is 64GB.
*   **DataNode, S3 Gateway, and HttpFs**: Recommended heap size is 31GB.

### Storage Requirements

*   **Ozone Manager (OM), Storage Container Manager (SCM), and Recon Metadata Storage**: Use SAS SSD or NVMe SSD for metadata (RocksDB and Ratis) to ensure optimal performance. It is recommended to use RAID 1 (disk mirroring) for the metadata disks to protect against disk failures.
*   **DataNode Storage**:
    *   **Ratis Log**: Use SAS SSD or NVMe SSD for the Ratis log directory for low latency writes.
    *   **Container Data**: Hard disks are acceptable for container data storage.
    *   **Disk Configuration**: It is recommended to use a JBOD (Just a Bunch Of Disks) configuration instead of RAID. Ozone is a replicated distributed storage system and handles data redundancy. Using RAID can decrease performance without providing additional data protection benefits.
*   **Storage Type**: Use direct-attached storage. Do not use Network Attached Storage (NAS) or Storage Area Network (SAN).

### Network Requirements

*   **Network Bandwidth**: A minimum of 25Gbps network card bandwidth is recommended.
*   **Network Topology**: A leaf-spine network topology with an oversubscription ratio below 3:1 is recommended for predictable performance.

### Security Requirements (Optional but Recommended)

*   **Kerberos**: A Kerberos environment, including a Key Distribution Center (KDC), is recommended for enhanced security.

## Recommended Configurations

### Linux Kernel

*   **CPU Governor**: Set the CPU scaling driver to `performance` mode to maximize performance.
*   **Transparent Hugepage**: Disable Transparent Hugepage to avoid performance issues.
*   **SELinux**: Disable SELinux.
*   **Swappiness**: Set `vm.swappiness=1` to minimize swapping.

### Local File System

*   **LVM**: Disable Logical Volume Manager (LVM) for data drives.
*   **File System**: Use `ext4` or `xfs` file systems.
*   **Mount Options**: Mount drives with the `noatime` option to reduce unnecessary disk writes. For SSDs, also add the `discard` option.

### Ozone Configuration

*   **Monitoring**: Install Prometheus and Grafana for monitoring the Ozone cluster. For audit logs, consider using a log ingestion framework such as the ELK Stack (Elasticsearch, Logstash, and Kibana) with FileBeat, or other similar frameworks. Alternatively, you can use Apache Ranger to manage audit logs.
*   **Pipeline Limits**: Increase the number of allowed write pipelines to better suit your workload by adjusting `ozone.scm.datanode.pipeline.limit` and `ozone.scm.ec.pipeline.minimum`.
*   **Heap Sizes**: Configure sufficient heap sizes for Ozone Manager (OM), Storage Container Manager (SCM), Recon, DataNode, S3 Gateway (S3G), and HttpFs services to ensure stability.
