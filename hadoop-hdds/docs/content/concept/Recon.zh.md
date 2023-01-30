---
title: "Recon"
date: "2020-10-27"
weight: 8
menu: 
  main:
     parent: 概念
summary: Recon 作为 Ozone 的管理和监视控制台。
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

Recon 充当 Ozone 的管理和监视控制台。它提供了 Ozone 的鸟瞰图，并通过基于 REST 的 API 和丰富的网页用户界面（Web UI）展示了集群的当前状态，从而帮助用户解决任何问题。


## 高层次设计

{{<figure src="/concept/ReconHighLevelDesign.png" width="800px">}}

<br/>

在较高的层次上，Recon 收集和汇总来自 Ozone Manager（OM）、Storage Container Manager（SCM）和数据节点（DN）的元数据，并充当中央管理和监视控制台。Ozone 管理员可以使用 Recon 查询系统的当前状态，而不会使 OM  或 SCM 过载。

Recon 维护多个数据库，以支持批处理，更快的查询和持久化聚合信息。它维护 OM DB 和 SCM DB 的本地副本，以及用于持久存储聚合信息的 SQL 数据库。

Recon 还与 Prometheus 集成，提供一个 HTTP 端点来查询 Prometheus 的 Ozone 指标，并在网页用户界面（Web UI）中显示一些关键时间点的指标。

## Recon 和 Ozone Manager

{{<figure src="/concept/ReconOmDesign.png" width="800px">}}

<br/>

Recon 最初从领导者 OM 的 HTTP 端点获取 OM rocks DB 的完整快照，解压缩文件并初始化 RocksDB 以进行本地查询。通过对最后一个应用的序列 ID 的 RPC 调用，定期请求领导者 OM 进行增量更新，从而使数据库保持同步。如果由于某种原因而无法检索增量更新或将其应用于本地数据库，则会再次请求一个完整快照以使本地数据库与 OM DB 保持同步。因此，Recon 可能会显示陈旧的信息，因为本地数据库不会总是同步的。

## Recon 和 Storage Container Manager

{{<figure src="/concept/ReconScmDesign.png" width="800px">}}

<br/>

Recon 还充当数据节点的被动 SCM。在集群中配置 Recon 时，所有数据节点都向 Recon 注册，并像 SCM 一样向 Recon 发送心跳、容器报告、增量容器报告等。Recon 使用它从数据节点得到的所有信息在本地构建自己的 SCM rocks DB 副本。Recon 从不向数据节点发送任何命令作为响应，而只是充当被动 SCM 以更快地查找 SCM 元数据。

## <a name="task-framework"></a> 任务框架

Recon 有其自己的任务框架，可对从 OM 和 SCM 获得的数据进行批处理。一个任务可以在 OM DB 或 SCM DB 上监听和操作数据库事件，如`PUT`、`DELETE`、`UPDATE`等。在此基础上，任务实现`org.apache.hadoop.ozone.recon.tasks.ReconOmTask`或者扩展`org.apache.hadoop.ozone.recon.scm.ReconScmTask`。

`ReconOmTask`的一个示例是`ContainerKeyMapperTask`，它在 RocksDB 中持久化保留了容器 -> 键映射。当容器被报告丢失或处于不健康的运行状态时，这有助于了解哪些键是容器的一部分。另一个示例是`FileSizeCountTask`，它跟踪 SQL 数据库中给定文件大小范围内的文件计数。这些任务有两种情况的实现：
 
 - 完整快照（reprocess()）
 - 增量更新（process()）
 
当从领导者 OM 获得 OM DB 的完整快照时，将对所有注册的 OM 任务调用 reprocess()。在随后的增量更新中，将在这些 OM 任务上调用 process()。

`ReconScmTask`的示例是`ContainerHealthTask`，它以可配置的时间间隔运行，扫描所有容器的列表，并将不健康容器的状态（`MISSING`、`MIS_REPLICATED`、`UNDER_REPLICATED`、`OVER_REPLICATED`）持久化保留在 SQL 表中。此信息用于确定集群中是否有丢失的容器。

## Recon 和 Prometheus

Recon 可以与任何配置为收集指标的 Prometheus 实例集成，并且可以在数据节点和 Pipelines 页面的 Recon UI 中显示有用的信息。Recon 还公开了一个代理端点 ([/指标]({{< ref path="interface/ReconApi.zh.md#metrics" >}})) 来查询 Prometheus。可以通过将此配置`ozone.recon.prometheus.http.endpoint`设置为 Prometheus 端点如`ozone.recon.prometheus.http.endpoint=http://prometheus:9090`来启用此集成。

## API 参考

[链接到完整的 API 参考]({{< ref path="interface/ReconApi.zh.md" >}})
   
## 持久化状态

 * [OM database]({{< ref "concept/OzoneManager.zh.md#持久化状态" >}})的本地副本
 * [SCM database]({{< ref "concept/StorageContainerManager.zh.md#持久化状态" >}})的本地副本
 * 以下数据在 Recon 中持久化在指定的 RocksDB 目录下： 
     * ContainerKey 表
         * 存储映射（容器，键） -> 计数
     * ContainerKeyCount 表
         * 存储容器 ID  -> 容器内的键数
 * 以下数据存储在已配置的 SQL 数据库中（默认为 Derby ）：
     * GlobalStats 表
         * 一个键 -> Value table 用于存储集群中出现的卷/桶/键的总数等聚合信息
     * FileCountBySize 表
         * 跟踪集群中文件大小范围内的文件数量
     * ReconTaskStatus 表
         * 跟踪在[Recon 任务框架](#task-framework)中已注册的 OM 和 SCM DB 任务的状态和最后运行时间戳
     * ContainerHistory 表
         * 存储容器副本 -> 具有最新已知时间戳记的数据节点映射。当一个容器被报告丢失时，它被用来确定最后已知的数据节点。
     * UnhealthyContainers 表
         * 随时跟踪集群中所有不健康的容器（MISSING、UNDER_REPLICATED、OVER_REPLICATED、MIS_REPLICATED）


## 需要关注的配置项

配置项 |默认值 | <div style="width:300px;">描述</div>
----|---------|------------
ozone.recon.http-address | 0.0.0.0:9888 | Recon web UI 监听的地址和基本端口。
ozone.recon.address | 0.0.0.0:9891 | Recon 的 RPC 地址。
ozone.recon.db.dir | none | Recon Server 存储其元数据的目录。
ozone.recon.om.db.dir | none | Recon Server 存储其 OM 快照 DB 的目录。
ozone.recon.om.snapshot<br>.task.interval.delay | 10m | Recon 以分钟间隔请求 OM DB 快照。
ozone.recon.task<br>.missingcontainer.interval | 300s | 定期检查集群中不健康容器的时间间隔。
ozone.recon.sql.db.jooq.dialect | DERBY | 请参考 [SQL 方言](https://www.jooq.org/javadoc/latest/org.jooq/org/jooq/SQLDialect.html) 来指定不同的方言。
ozone.recon.sql.db.jdbc.url | jdbc:derby:${ozone.recon.db.dir}<br>/ozone_recon_derby.db | Recon SQL database 的 jdbc url。
ozone.recon.sql.db.username | none | Recon SQL数据库的用户名。
ozone.recon.sql.db.password | none | Recon SQL数据库的密码。
ozone.recon.sql.db.driver | org.apache.derby.jdbc<br>.EmbeddedDriver | Recon SQL数据库的 jdbc driver。

