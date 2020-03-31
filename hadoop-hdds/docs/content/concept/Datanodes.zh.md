---
title: "Datanodes"
date: "2017-09-14"
weight: 4
summary: Ozone支持亚马逊简单存储服务(S3)协议。实际上，你可以使用S3客户端和基于S3 SDK应用程序，不需要对Ozone进行任何修改。
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

Datanodes可以视作Ozone的工蜂。所有的数据都存储在数据节点上。
客户端根据数据块写入数据。Datanode把这些数据块聚合到一个存
储容器中。一个存储容器包含了由客户端写入的关于这个数据块的
数据流和元数据。

## Storage Containers

Storage Containers 是一个独立的超级数据块。它包含了一系列的 
Ozone块，以及包含实际数据流的磁盘文件。这是它的默认存储格式。
从Ozone角度来看，容器是一种协议规范，实际上他的存储布局并不
重要。换句话说，扩展或带来新的容器布局很简单。因此它被视为在
Ozone下容器的一种参考实现方式。

## 理解Ozone数据块和容器

当客户端想要从Ozone读取一个键时，客户端会把键的名称发送给
Ozone Manager。Ozone manager会返回由这个键组成的ozone的数据
块列表。

一个Ozone包含了容器ID和一个本地ID。下图显示了Ozone数据块的逻
辑布局。

![OzoneBlock](OzoneBlock.png)

容器ID使得客户端可以发现容器的位置。容器的所在的确切的信息存
储在Storage Container Manager(SCM)。在大多数情况下，容器的位
置被Ozone Manager缓存，并且将于Ozone块一起返回到客户端。 

一旦客户端能够找到容器，那么就知道哪些数据节点包含该容器，客
户端将连接到datanode并且根据_Container ID:Local ID_读取指定数
据流。换句话说，Local ID用作容器的索引，这个容器描述了我们要
从哪里读取数据流。

## 感知容器的位置

SCM如何知道容器的位置的呢？这与HDFS工作原理非常相似；数据节点
定期发送容器的报告就像数据块汇报一样。容器汇报比起块报告更加
轻量级。例如，Ozone部署在一个196TB的数据节点大概有40w个容器。
和HDFS百万个半块汇报相比较，块报告将近降低了40倍。

这就额外的间接极大帮助Ozone可以水平扩展，SCM需要处理的块数据
要少得多，而名称节点是另一种服务， 对于扩展Ozone至关重要。
