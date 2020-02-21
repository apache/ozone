---
title: "Ozone Manager"
date: "2017-09-14"
weight: 2
summary: Ozone Manager 是 Ozone 主要的命名空间服务，它管理了卷、桶和键的生命周期。
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

Ozone Manager（OM）管理 Ozone 的命名空间。

当向 Ozone 写入数据时，你需要向 OM 请求一个块，OM 会返回一个块并记录下相关信息。当你想要读取那个文件时，你也需要先通过 OM 获取那个块的地址。

OM 允许用户在卷和桶下管理键，卷和桶都是命名空间的一部分，也由 OM 管理。

每个卷都是 OM 下的一个独立命名空间的根，这一点和 HDFS 不同，HDFS 提供的是单个根目录的文件系统。

与 HDFS 中单根的树状结构相比，Ozone 的命名空间是卷的集合，或者可以看作是个森林，因此可以非常容易地部署多个 OM 来进行扩展。

## Ozone Manager 元数据

OM 维护了卷、桶和键的列表。它为每个用户维护卷的列表，为每个卷维护桶的列表，为每个桶维护键的列表。

OM 使用 Apache Ratis（Raft 协议的一种实现）来复制 OM 的状态，这为 Ozone 提供了高可用性保证。


## Ozone Manager 和 Storage Container Manager

为了方便理解 OM 和 SCM 之间的关系，我们来看看写入键和读取键的过程。

### 写入键

* 为了向 Ozone 中的某个卷下的某个桶的某个键写入数据，用户需要先向 OM 发起写请求，OM 会判断该用户是否有权限写入该键，如果权限许可，OM 分配一个块用于 Ozone 客户端数据写入。

* OM 通过 SCM 请求分配一个块（SCM 是数据节点的管理者），SCM 选择三个数据节点，分配新块并向 OM 返回块标识。

* OM 在自己的元数据中记录下块的信息，然后将块和块 token（带有向该块写数据的授权）返回给用户。

* 用户使用块 token 证明自己有权限向该块写入数据，并向对应的数据节点写入数据。

* 数据写入完成后，用户会更新该块在 OM 中的信息。


### 读取键

* 键读取相对比较简单，用户首先向 OM 请求该键的块列表。

* OM 返回块列表以及对应的块 token。

* 用户连接数据节点，出示块 token，然后读取键数据。