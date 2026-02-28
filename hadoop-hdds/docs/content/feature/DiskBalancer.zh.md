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

### 身份验证和授权

DiskBalancer 命令通过 RPC 直接与数据节点通信，因此需要进行正确的身份验证和授权配置。

#### 身份验证配置

在启用了 Kerberos 的安全集群中，必须在 `ozone-site.xml` 文件中配置数据节点的 Kerberos 主体以进行 RPC 身份验证：

```xml
<property>
<name>hdds.datanode.kerberos.principal</name>
<value>dn/_HOST@REALM.TLD</value>
<description>
  The Datanode service principal. This is typically set to
  dn/_HOST@REALM.TLD. Each Datanode will substitute _HOST with its
  own fully qualified hostname at startup. The _HOST placeholder
  allows using the same configuration setting on all Datanodes.
</description>

</property>
```

**注意**：如果没有此配置，DiskBalancer 命令在安全集群中将因身份验证错误而失败。 客户端使用此主体在建立 RPC 连接时验证数据节点的身份。

#### 授权配置

每个数据节点都使用 `OzoneAdmins` 根据 `ozone.administrators` 配置执行授权检查：

- **管理员操作**（启动、停止、更新）：要求用户位于 `ozone.administrators` 成员列表中，或属于 `ozone.administrators.groups` 中的某个组。

- **只读操作**（状态、报告）：不需要管理员权限 - 任何已认证的用户都可以查询状态和报告。

#### 默认行为

默认情况下，如果未配置 `ozone.administrators`，则只有启动数据节点服务的用户才能启动、停止或更新 DiskBalancer。

这意味着在典型的部署中，如果数据节点以用户 `dn` 的身份运行，则只有该用户拥有 DiskBalancer 操作的 管理员权限。

#### 为其他用户启用身份验证

要允许其他用户执行 DiskBalancer 管理操作（启动、停止、更新），请在 `ozone-site.xml` 文件中配置 `ozone.administrators` 属性：

**Example 1: Single user**
```xml
<property>
  <name>ozone.administrators</name>
  <value>scm</value>
</property>
```

**Example 2: Multiple users**
```xml
<property>
  <name>ozone.administrators</name>
  <value>scm,hdfs</value>
</property>
```

**Example 3: Using groups**
```xml
<property>
  <name>ozone.administrators.groups</name>
  <value>ozone-admins,cluster-operators</value>
</property>
```
**注意**：`ozone-admins` 和 `cluster-operators` 是示例组名称。请将其替换为您环境中的实际组名称。 更新 `ozone.administrators` 配置后，
请重启数据节点服务以使更改生效。

## 命令行用法
DiskBalancer 通过 `ozone admin datanode diskbalancer` 命令进行管理。

**注意：**此命令在主帮助信息（`ozone admin datanode --help`）中隐藏。这是因为该功能目前处于实验阶段，默认禁用。隐藏该命令可防止意外使用，
并为普通用户提供清晰的帮助输出。但是，对于希望启用和使用该功能的用户，该命令仍然完全可用。

### 命令语法
**启动 DiskBalancer：**
```bash
ozone admin datanode diskbalancer start [<datanode-address> ...] [OPTIONS] [--in-service-datanodes]
```

**停止 DiskBalancer：**
```bash
ozone admin datanode diskbalancer stop [<datanode-address> ...] [--in-service-datanodes]
```

**更新配置：**
```bash
ozone admin datanode diskbalancer update [<datanode-address> ...] [OPTIONS] [--in-service-datanodes]
```

**获取状态：**
```bash
ozone admin datanode diskbalancer status [<datanode-address> ...] [--in-service-datanodes] [--json]
```

**获取报告：**
```bash
ozone admin datanode diskbalancer report [<datanode-address> ...] [--in-service-datanodes] [--json]
```

### 命令选项

| Option                              | Description                                                                                                                                                                                                      | Example                                        |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------|
| `<datanode-address>`                | 一个或多个数据节点地址作为位置参数。地址可以是：<br>- 主机名（例如，`DN-1`）- 使用默认的 CLIENT_RPC 端口 (19864)<br>- 带端口的主机名（例如，`DN-1:19864`）<br>- IP 地址（例如，`192.168.1.10`）<br>- 带端口的 IP 地址（例如，`192.168.1.10:19864`）<br>- 标准输入 (`-`) - 从标准输入读取数据节点地址，每行一个 | `DN-1`<br>`DN-1:19864`<br>`192.168.1.10`<br>`-` |
| `--in-service-datanodes`            | 它向 SCM 查询所有 IN_SERVICE 数据节点，并在所有这些数据节点上执行该命令。                                                                                                                                                                    | `--in-service-datanodes`                       |
| `--json`                            | 输出格式设置为JSON。                                                                                                                                                                                                     | `--json`                                       |
| `-t/--threshold-percentage`         | 磁盘使用率阈值百分比（默认值：10.0）。与 `start` 和 `update` 命令一起使用。                                                                                                                                                                 | `-t 5`<br>`--threshold-percentage 5.0`        |
| `-b/--bandwidth-in-mb`              | 最大磁盘带宽，单位为 MB/s（默认值：10）。与 `start` 和 `update` 命令一起使用。                                                                                                                                                             | `-b 20`<br>`--bandwidth-in-mb 50`              |
| `-p/--parallel-thread`              | 并行线程数（默认值：1）。与 `start` 和 `update` 命令一起使用。                                                                                                                                                                        | `-p 5`<br>`--parallel-thread 10`               |
| `-s/--stop-after-disk-even`         | 磁盘平衡完成后自动停止（默认值：false）。与 `start` 和 `update` 命令一起使用。                                                                                                                                                              | `-s false`<br>`--stop-after-disk-even true`    |

### 示例
**启动 DiskBalancer：**

```bash
# 在多个数据节点上启动 DiskBalancer
ozone admin datanode diskbalancer start DN-1 DN-2 DN-3

# 在所有运行中的数据节点上启动 DiskBalancer
ozone admin datanode diskbalancer start --in-service-datanodes

# 使用配置参数启动 DiskBalancer
ozone admin datanode diskbalancer start DN-1 -t 5 -b 20 -p 5

# 从标准输入读取数据节点地址
echo -e "DN-1\nDN-2" | ozone admin datanode diskbalancer start -

# 使用 JSON 输出启动 DiskBalancer
ozone admin datanode diskbalancer start DN-1 --json
```

**停止 DiskBalancer：**

```bash
# 在多个数据节点上停止 DiskBalancer
ozone admin datanode diskbalancer stop DN-1 DN-2 DN-3

# 在所有运行中的数据节点上停止 DiskBalancer
ozone admin datanode diskbalancer stop --in-service-datanodes

# 停止 DiskBalancer 并输出 JSON 信息
ozone admin datanode diskbalancer stop DN-1 --json
```
**更新配置：**

```bash
# 更新多个参数
ozone admin datanode diskbalancer update DN-1 -t 5 -b 50 -p 10

# 更新所有 IN_SERVICE 数据节点
ozone admin datanode diskbalancer update --in-service-datanodes -t 5

# 更新并输出 JSON 格式
ozone admin datanode diskbalancer update DN-1 -b 50 --json
```

**获取状态：**

```bash
# 从多个数据节点获取状态
ozone admin datanode diskbalancer status DN-1 DN-2 DN-3

# 从所有处于服务状态的数据节点获取状态
ozone admin datanode diskbalancer status --in-service-datanodes

# 以 JSON 格式获取状态
ozone admin datanode diskbalancer status --in-service-datanodes --json
```
**获取报告：**

```bash
# 从多个数据节点获取报告
ozone admin datanode diskbalancer report DN-1 DN-2 DN-3

# 从所有处于服务状态的数据节点获取报告
ozone admin datanode diskbalancer report --in-service-datanodes

# 以 JSON 格式获取报告
ozone admin datanode diskbalancer report --in-service-datanodes --json
```

## DiskBalancer Configurations

The DiskBalancer's behavior can be controlled using the following configuration properties in `ozone-site.xml`.

| Property                                                    | Default Value                          | Description                                                                                                                                                                 |
|-------------------------------------------------------------|----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `hdds.datanode.disk.balancer.enabled`                       | `false`                                | 如果为 false，则 Datanode 上的 DiskBalancer 服务将被禁用。将其配置为 true 可启用 DiskBalancer。                                                      |                                                            |                                                                                        |                                                                                                                                                                              |
| `hdds.datanode.disk.balancer.volume.density.threshold.percent` | `10.0`                                 | 百分比（0-100）。如果对于每个卷，其利用率与平均数据节点利用率之差不超过此阈值，则认为数据节点处于平衡状态。    |
| `hdds.datanode.disk.balancer.max.disk.throughputInMBPerSec` | `10`                                   | 平衡器可用于移动数据的最大带宽（以 MB/s 为单位），以避免影响客户端 I/O。                                                                    |
| `hdds.datanode.disk.balancer.parallel.thread`               | `5`                                    | 用于并行移动容器的工作线程数。                                                                                                       |
| `hdds.datanode.disk.balancer.service.interval`              | `60s`                                  | Datanode DiskBalancer 服务检查不平衡并更新其配置的时间间隔。                                                             |
| `hdds.datanode.disk.balancer.stop.after.disk.even`          | `true`                                 | 如果为真，则一旦磁盘被视为平衡（即所有卷密度都在阈值内），DiskBalancer 将自动停止其平衡活动。           |
| `hdds.datanode.disk.balancer.replica.deletion.delay`       | `5m`                                   | 容器成功从源卷移动到目标卷后，在删除源容器副本之前会延迟一段时间。这种延迟删除机制允许在读取持有旧容器副本的线程失败之前留出时间。单位：ns、ms、s、m、h、d。 |
| `hdds.datanode.disk.balancer.volume.choosing.policy`        | `org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultVolumeChoosingPolicy` | 用于选择平衡的源卷和目标卷的策略类。                                                                                                 |
| `hdds.datanode.disk.balancer.container.choosing.policy`     | `org.apache.hadoop.ozone.container.diskbalancer.policy.DefaultContainerChoosingPolicy` | 用于选择将哪些容器从源卷移动到目标卷的策略类。                                                                         |
| `hdds.datanode.disk.balancer.service.timeout`               | `300s`                                 | Datanode DiskBalancer 服务操作超时。                                                                                                                    |
| `hdds.datanode.disk.balancer.should.run.default`            | `false`                                | 如果平衡器无法读取其持久配置，则该值决定服务是否应默认运行。                                                       |

