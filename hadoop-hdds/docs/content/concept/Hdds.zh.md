---
title: "Storage Container Manager"
date: "2017-09-14"
weight: 3
summary:  Storage Container Manager（SCM）是 Ozone 的核心元数据服务，它提供了 Ozone 的分布式数据块服务层。
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

SCM 为 Ozone 集群提供了多种重要功能，包括：集群管理、证书管理、块管理和副本管理等。

{{<card title="集群管理" icon="tasks">}}
SCM 负责创建一个 Ozone 集群，当通过 <kbd>init</kbd> 命令启动 SCM 时，SCM 会创建集群标识以及用于担任 CA 的根证书，SCM 负责集群中数据节点生命周期管理。
{{</card>}}

{{<card title="服务身份管理" icon="eye-open">}}
SCM 的 CA 负责向集群中的每个服务颁发身份证书，证书设施方便了网络层 mTLS 协议的启用，也为块 token 机制提供了支持。
{{</card>}}

{{<card title="块管理" icon="th">}}
SCM 管理 Ozone 中的块，它将块分配给数据节点，用户直接读写这些块。
{{</card>}}

{{<card title="副本管理" icon="link">}}
SCM 会跟踪所有块副本的状态，如果检测到数据节点宕机或磁盘异常，SCM 命令其它节点生成丢失块的新副本，以此保证高可用。
{{</card>}}