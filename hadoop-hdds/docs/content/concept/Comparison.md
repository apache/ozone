---
title: Comparison with Other Storage Technologies
menu:
  main:
    parent: Architecture
weight: 10
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

This document provides a high-level comparison of Apache Ozone with other storage technologies.

## High-Level Comparison

| Tech                | Type                  | Consistency         | Scale                | Big Data Integration | License               | Notes                                                              |
|---------------------|-----------------------|---------------------|----------------------|----------------------|-----------------------|--------------------------------------------------------------------|
| **Ozone**           | Object / File         | Strong              | PBs, billions keys   | Native               | Apache 2.0            | Modern Hadoop-native object store with S3 API                      |
| **HDFS**            | File                  | Strong              | PBs, billions files  | Native               | Apache 2.0            | Classic Hadoop FS, no S3 API, tight Hadoop integration             |
| **Ceph**            | Object / Block / File | Tunable / Eventual  | Multi-PB, very large | Limited native       | LGPLv2.1 / Ceph Foundation | General-purpose: underlying RADOS (object), RGW (object), CephFS (file) |
| **MinIO**           | Object                | Strong              | Petabyte scale       | None by default      | AGPLv3 (SSPL-like)    | Cloud-native S3 API, lightweight, fast, no FS semantics            |
| **Lustre**          | Parallel File (POSIX) | Strong              | PB scale, HPC        | None                 | GPLv2                 | HPC clusters, high-throughput parallel file system                 |
| **GlusterFS**       | File (POSIX)          | Eventually consistent| Large, multi-PB      | None                 | GPLv3                 | General-purpose scale-out distributed file storage                 |
| **OpenStack Swift** | Object                | Eventual            | Large, multi-PB      | None                 | Apache 2.0            | S3-like multi-tenant object storage for private clouds             |
