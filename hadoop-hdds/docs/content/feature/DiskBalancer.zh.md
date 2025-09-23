---
title: "磁盘均衡器"
weight: 1
menu:
   main:
      parent: 特征
summary: 数据节点的磁盘平衡器.
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

## 概述
**Apache Ozone** 能够有效地将所有容器均匀分布在每个 Datanode 的多个磁盘上。这种初始分布确保了 I/O 负载从一开始就保持平衡。然而，
在集群的整个运行生命周期中，以下原因可能会导致磁盘负载不平衡：
- **添加新磁盘**以扩展数据节点存储空间。
- **用新磁盘**替换损坏的旧磁盘**。
- 大量**块**或**副本删除**。

磁盘利用率不均衡会造成性能瓶颈，因为**过度利用的磁盘**会成为**热点**，限制 Datanode 的整体吞吐量。因此，我们引入了这项新功能**磁盘平衡器**
，以确保 Datanode 内各个磁盘的数据均匀分布。

如果磁盘的`VolumeDataDensity`超过可配置的“阈值”，则该磁盘被视为需要进行平衡。DiskBalancer 可以通过**CLI 命令**手动触发。

![Disk Even](diskBalancer.png)

## 功能标志

磁盘平衡器功能已通过功能标志引入。默认情况下，此功能处于禁用状态。

可以通过在“ozone-site.xml”配置文件中将以下属性设置为“true”来**启用**该功能：
`hdds.datanode.disk.balancer.enabled = false`

## 命令行用法
DiskBalancer 通过 `ozone admin datanode diskbalancer` 命令进行管理。

**注意：**此命令在主帮助信息（`ozone admin datanode --help`）中隐藏。这是因为该功能目前处于实验阶段，默认禁用。隐藏该命令可防止意外使用，
并为普通用户提供清晰的帮助输出。但是，对于希望启用和使用该功能的用户，该命令仍然完全可用。

### **启动 DiskBalancer**
要在所有 Datanode 上使用默认配置启动 DiskBalancer，请执行以下操作：

```shell
ozone admin datanode diskbalancer start -a
```

您还可以使用特定选项启动 DiskBalancer：
```shell
ozone admin datanode diskbalancer start [options]
```

### **更新配置**
要更新 DiskBalancer 配置，您可以使用以下命令：

```shell
ozone admin datanode diskbalancer update [options]
```
**选项包括：**

| 选项                           | 描述                              |                                                                                                                                                             
|------------------------------|---------------------------------|
| `-t, --threshold`            | 与磁盘平均利用率的百分比偏差，超过此偏差，数据节点将重新平衡。 |
| `-b, --bandwithInMB`         | DiskBalancer 每秒的最大带宽。           |
| `-p, --parallelThread`       | DiskBalancer 的最大并行线程。           |
| `-s, --stop-after-disk-even` | 磁盘利用率达到均匀后自动停止 DiskBalancer。    |
| `-a, --all`                  | 在所有数据节点上运行命令。                   |
| `-d, --datanodes`            | 在特定数据节点上运行命令                    |

### **停止 DiskBalancer**
要停止所有 Datanode 上的 DiskBalancer，请执行以下操作：

```shell
ozone admin datanode diskbalancer stop -a
```
您还可以停止特定 Datanode 上的 DiskBalancer：

```shell
ozone admin datanode diskbalancer stop -d <datanode1>
```
### **磁盘平衡器状态**
要检查所有数据节点上的磁盘平衡器状态，请执行以下操作：

```shell
ozone admin datanode diskbalancer status
```
您还可以检查特定 Datanode 上 DiskBalancer 的状态：
```shell
ozone admin datanode diskbalancer status -d <datanode1>
```
### **磁盘平衡器报告**
要获取前**N**个数据节点（按降序显示）的磁盘平衡器**volumeDataDensity**，
默认 N=25，如未指定：

```shell
ozone admin datanode diskbalancer report --count <N>
```

## DiskBalancer Configurations

The DiskBalancer's behavior can be controlled using the following configuration properties in `ozone-site.xml`.

| Property                                                    | Default Value                          | Description                                                                                                                                                                 |
|-------------------------------------------------------------|----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hdds.datanode.disk.balancer.enabled`                       | `false`                                | 如果为 false，则 Datanode 上的 DiskBalancer 服务将被禁用。将其配置为 true 可启用 DiskBalancer。                                                      |                                                            |                                                                                        |                                                                                                                                                                              |
| `hdds.datanode.disk.balancer.volume.density.threshold`      | `10.0`                                 | 百分比（0-100）。如果对于每个卷，其利用率与平均数据节点利用率之差不超过此阈值，则认为数据节点处于平衡状态。    |
| `hdds.datanode.disk.balancer.max.disk.throughputInMBPerSec` | `10`                                   | 平衡器可用于移动数据的最大带宽（以 MB/s 为单位），以避免影响客户端 I/O。                                                                    |
| `hdds.datanode.disk.balancer.parallel.thread`               | `5`                                    | 用于并行移动容器的工作线程数。                                                                                                       |
| `hdds.datanode.disk.balancer.service.interval`              | `60s`                                  | Datanode DiskBalancer 服务检查不平衡并更新其配置的时间间隔。                                                             |
| `hdds.datanode.disk.balancer.stop.after.disk.even`          | `true`                                 | 如果为真，则一旦磁盘被视为平衡（即所有卷密度都在阈值内），DiskBalancer 将自动停止其平衡活动。           |
| `hdds.datanode.disk.balancer.volume.choosing.policy`        | `org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultVolumeChoosingPolicy` | 用于选择平衡的源卷和目标卷的策略类。                                                                                                 |
| `hdds.datanode.disk.balancer.container.choosing.policy`     | `org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy` | 用于选择将哪些容器从源卷移动到目标卷的策略类。                                                                         |
| `hdds.datanode.disk.balancer.service.timeout`               | `300s`                                 | Datanode DiskBalancer 服务操作超时。                                                                                                                    |
| `hdds.datanode.disk.balancer.should.run.default`            | `false`                                | 如果平衡器无法读取其持久配置，则该值决定服务是否应默认运行。                                                       |

