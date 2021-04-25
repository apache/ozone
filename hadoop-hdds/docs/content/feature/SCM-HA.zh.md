---
title: "高可用SCM"
weight: 1
menu:
   main:
      parent: 特点
summary: Storage Container Manager 的 HA 设置可以避免任何单点故障。
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

Ozone包含两个元数据管理节点(用于键管理的 *Ozone Manager* 和用于块空间管理的 *Storage Container management* )和多个存储节点(数据节点)。通过 RAFT 共识算法实现数据在数据节点之间的复制。

<div class="alert alert-warning" role="alert">
请注意，SCM-HA 尚未准备好在安全环境中部署。 安全工作正在进行中，将很快完成。
</div>

为了避免任何单点故障，元数据管理器节点还应该具有 HA 设置。

Ozone Manager 和 Storage Container Manager 都支持 HA。在这种模式下，内部状态通过 RAFT (使用 Apache Ratis )复制。

本文档解释了 Storage Container Manager (SCM)的 HA 设置，请在[本页]({{< ref "OM-HA" >}})中查看 Ozone Manager (OM)的 HA 设置。虽然它们可以独立地为 HA 进行设置，但可靠的、完全的 HA 设置需要为两个服务启用 HA。

## 配置

Storage Container Manager 的 HA 模式可以在 `ozone-site.xml` 中进行以下设置：

```XML
<property>
   <name>ozone.scm.ratis.enable</name>
   <value>true</value>
</property>
```
一个 Ozone 配置(`ozone-site.xml`)可以支持多个SCM HA节点集，多个 Ozone 集群。要在可用的 SCM 节点之间进行选择，每个集群都需要一个逻辑名称，可以将其解析为 Storage Container Manage 的 IP 地址(和域名)。

这个逻辑名称称为 `serviceId`，可以在 `ozone-site.xml` 中配置。

大多数情况下，你只需要设置当前集群的值：

```XML
<property>
   <name>ozone.scm.service.ids</name>
   <value>cluster1</value>
</property>
```

对于每个已定义的 `serviceId`，应该为每个服务器定义一个逻辑配置名：

```XML
<property>
   <name>ozone.scm.nodes.cluster1</name>
   <value>scm1,scm2,scm3</value>
</property>
```

定义的前缀可以用来定义每个 SCM 服务的地址:

```XML
<property>
   <name>ozone.scm.address.cluster1.scm1</name>
   <value>host1</value>
</property>
<property>
   <name>ozone.scm.address.cluster1.scm2</name>
   <value>host2</value>
</property>
<property>
   <name>ozone.scm.address.cluster1.scm3</name>
   <value>host3</value>
</property>
```

为了获得可靠的 HA 支持，请选择3个独立的节点以形成仲裁。

## 引导

初始化的**第一个** SCM-HA 节点和 none-HA SCM是一样的：

```
bin/ozone scm --init
```

第二个和第三个节点应该被 *bootstrapped*，而不是 init。这些集群将加入到配置的 RAFT 仲裁。当前服务器的 id 通过 DNS 名称标识，也可以通过 `ozone.scm.node.id` 明确设置。大多数时候你不需要设置它，因为基于 DNS 的 id 检测可以很好地工作。

```
bin/ozone scm --bootstrap
```

## 自动引导

在某些环境（例如容器化/ K8s 环境）中，我们需要一种通用的统一方法来初始化 SCM HA 仲裁。 剩下的标准初始化流程如下：

 1. 在第一个“原始”节点上，调用 `scm --init`
 2. 在第二个/第三个节点上，调用`scm --bootstrap`

这可以通过使用 `ozone.scm.primordial.node.id` 更改。您可以定义原始节点。设置这个节点后，你应该在**所有**的节点上**同时**执行 `scm --init` 和 `scm --bootstrap`。

根据 `ozone.scm.primordial.node.id`，初始化进程将在第二个/第三个节点上被忽略，引导进程将在除原始节点外的所有节点上被忽略。

## 实现细节

SCM HA 使用 Apache Ratis 在 SCM HA 仲裁的成员之间复制状态。每个节点在本地 RocksDB 中维护块管理元数据。

这个复制过程是 OM HA 复制过程的一个简单版本，因为它不使用任何双缓冲区(SCM 请求的总体 db 吞吐量更低)。

数据节点将所有报告(容器报告、管道报告……)并发发送给 *所有* SCM 节点。只有 leader 节点可以分配/创建新的容器，并且只有 leader 节点可以将命令返回给数据节点。

## 验证SCM HA设置

启动 SCM-HA 后，可以验证 SCM 节点是否形成了一个仲裁，而不是3个单独的 SCM 节点。

首先，检查所有的 SCM 节点是否存储相同的 ClusterId 元数据：

```bash
cat /data/metadata/scm/current/VERSION
```

ClusterId 包含在版本文件中，并且在所有的 SCM 节点中应该是相同的：

```bash
#Tue Mar 16 10:19:33 UTC 2021
cTime=1615889973116
clusterID=CID-130fb246-1717-4313-9b62-9ddfe1bcb2e7
nodeType=SCM
scmUuid=e6877ce5-56cd-4f0b-ad60-4c8ef9000882
layoutVersion=0
```

如果所有的容器元数据都已复制，您还可以创建数据并使用 `ozone debug` 工具进行双重检查。

```shell
bin/ozone freon randomkeys --numOfVolumes=1 --numOfBuckets=1 --numOfKeys=10000 --keySize=524288 --replicationType=RATIS --numOfThreads=8 --factor=THREE --bufferSize=1048576
 
 
// use debug ldb to check scm db on all the machines
bin/ozone debug ldb --db=/tmp/metadata/scm.db/ ls
 
 
bin/ozone debug ldb --db=/tmp/metadata/scm.db/ scan --with-keys --column_family=containers
```

## 从现有的SCM迁移

可以在任何 Ozone 集群上打开 SCM HA。 首先启用 Ratis（`ozone.scm.ratis.enable`）并为 Ratis ring 配置一个节点（`ozone.scm.nodes.serviceId` 应该有一个元素）。

启动集群并测试它是否正常工作。

如果一切正常，您可以用多个节点扩展集群配置，重新启动 SCM 节点，并使用 `scm --bootstrap` 命令初始化其他节点。
