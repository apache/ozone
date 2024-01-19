---
title: "拓扑感知能力"
weight: 2
menu:
   main:
      parent: 特性
summary: 机架感知配置可以提高读/写性能
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

Ozone可以使用拓扑相关信息（例如机架位置）来优化读写管道。要获得完全的机架感知集群，Ozone需要三种不同的配置。

 1. 拓扑信息应由 Ozone 配置。
 2. 当Ozone为特定管道/容器选择3个不同的数据节点时，拓扑相关信息就会被使用.(写入)
 3. 当Ozone读取一个Key时，它应该优先从最近的节点读取。

<div class="alert alert-warning" role="alert">

Ozone 对OPEN容器（写）使用 RAFT 复制，对CLOSED的、不可变的容器（冷数据）使用异步复制。由于RAFT需要低延迟的网络，因此只有CLOSED容器才能使用拓扑感知布置。有关OPEN与CLOSED容器的更多信息，请参阅[容器]({{< ref "concept/Containers.zh.md">}}) 页面。

</div>

## 拓扑层次结构

拓扑层次结构可使用 net.topology.node.switch.mapping.impl 配置键进行配置。此配置应定义 org.apache.hadoop.net.CachedDNSToSwitchMapping 的实现。由于这是一个 Hadoop 类，因此该配置与 Hadoop 配置完全相同。

### 静态列表

静态列表可借助 ```TableMapping``` 进行配置：:

```XML
<property>
   <name>net.topology.node.switch.mapping.impl</name>
   <value>org.apache.hadoop.net.TableMapping</value>
</property>
<property>
   <name>net.topology.table.file.name</name>
   <value>/opt/hadoop/compose/ozone-topology/network-config</value>
</property>
```

第二个配置选项应指向一个文本文件。文件格式为两列文本文件，各列之间用空格隔开。第一列是 IP 地址，第二列指定地址映射的机架。如果找不到与集群中主机相对应的条目，则会使用 /default-rack。

### 动态列表 

机架信息可借助外部脚本识别：


```XML
<property>
   <name>net.topology.node.switch.mapping.impl</name>
   <value>org.apache.hadoop.net.ScriptBasedMapping</value>
</property>
<property>
   <name>net.topology.script.file.name</name>
   <value>/usr/local/bin/rack.sh</value>
</property>
```

如果使用外部脚本，则需要在配置文件中使用 net.topology.script.file.name 参数来指定。与 java 类不同，外部拓扑脚本不包含在 Ozone 发行版中，而是由管理员提供。Fork 拓扑脚本时，Ozone 会向 ARGV 发送多个 IP 地址。发送给拓扑脚本的 IP 地址数量由 net.topology.script.number.args 控制，默认为 100。如果将 net.topology.script.number.args 改为 1，则每个提交的 IP 地址都会Fork一个拓扑脚本。

## 写入路径

CLOSED容器放置可以通过 `ozone.scm.container.placement.impl` 配置键进行配置。 可用的容器放置策略可在 `org.apache.hdds.scm.container.placement` 包中找到。[包](https://github.com/apache/ozone/tree/master/hadoop-hdds/server-scm/src/main/java/org/apache/hadoop/hdds/scm/container/placement/algorithms).

默认情况下， CLOSED容器使用 `SCMContainerPlacementRandom` 放置策略，该策略不支持拓扑感知。为了启用拓扑感知，可配置 `SCMContainerPlacementRackAware` 作为CLOSED容器放置策略：

```XML
<property>
   <name>ozone.scm.container.placement.impl</name>
   <value>org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementRackAware</value>
</property>
```

这种放置策略符合 HDFS 中使用的算法。在默认的 3 个副本中，两个副本位于同一个机架上，第三个副本位于不同的机架上。
 
这种实现方式适用于"/机架/节点 "这样的网络拓扑结构。如果网络拓扑结构的层数较多，则不建议使用此方法。
 
## 读取路径

最后，读取路径也应配置为从最近的 pipeline 读取数据。

```XML
<property>
   <name>ozone.network.topology.aware.read</name>
   <value>true</value>
</property>
```

## 参考文献

 * 关于 `net.topology.node.switch.mapping.impl` 的 Hadoop 文档： https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/RackAwareness.html
 * [设计文档]({{< ref path="design/topology.md" lang="en">}})