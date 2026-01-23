---
title: 生产环境部署
weight: 6
menu:
  main:
    parent: 快速入门
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

本文档旨在为 Apache Ozone 的生产环境部署提供需求和最佳实践的指导。

## 需求

### 系统需求

*   **操作系统**: Linux（推荐发行版：Red Hat 8/Rocky 8+、Ubuntu、SUSE；支持架构：x86/ARM）。
*   **Java 开发工具包 (JDK)**: 版本 8 或更高。
*   **时间同步**: 必须启用时间同步服务（如 Chrony 或 ntpd）以防止时间漂移。

### 存储需求

*   **元数据存储**: 为确保最佳性能，请使用 SAS SSD 或 NVMe SSD 存储元数据（RocksDB 和 Ratis）。
*   **DataNode 存储**: DataNode 数据存储可使用硬盘。
*   **存储类型**: 请使用直接附加存储。不要使用网络附加存储 (NAS) 或存储区域网络 (SAN)。

### 网络需求

*   **网络带宽**: 建议网卡带宽至少为 25Gbps。
*   **网络拓扑**: 为实现可预测的性能，建议采用超分比例低于 3:1 的叶脊网络拓扑。

### 安全需求 (可选但推荐)

*   **Kerberos**: 为增强安全性，建议使用包括密钥分发中心 (KDC) 在内的 Kerberos 环境。

## 推荐配置

### Linux 内核

*   **CPU 调节器**: 将 CPU 调节驱动设置为 `performance` 模式以最大化性能。
*   **透明大页**: 禁用透明大页以避免性能问题。
*   **SELinux**: 禁用 SELinux。
*   **Swappiness**: 设置 `vm.swappiness=1` 以最小化交换。

### 本地文件系统

*   **LVM**: 禁用数据驱动器的逻辑卷管理器 (LVM)。
*   **文件系统**: 使用 `ext4` 或 `xfs` 文件系统。
*   **挂载选项**: 使用 `noatime` 选项挂载驱动器以减少不必要的磁盘写入。对于 SSD，还需添加 `discard` 选项。

### Ozone 配置

*   **监控**: 安装 Prometheus 和 Grafana 以监控 Ozone 集群。
*   **管道限制**: 通过调整 `ozone.scm.datanode.pipeline.limit` 和 `ozone.scm.ec.pipeline.minimum` 来增加允许的写入管道数量，以更好地适应您的工作负载。
*   **堆大小**: 为 Ozone Manager (OM)、Storage Container Manager (SCM)、Recon、DataNode、S3 Gateway (S3G) 和 HttpFs 服务配置足够的堆大小，以确保稳定性。
