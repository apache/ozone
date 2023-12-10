---
title: 概览
date: "2017-10-10"
weight: 1
menu: 
  main:
     name: "ArchitectureOverview"
     title: "概览"
     parent: 概念
summary: 介绍 Ozone 的整体和各个组件。
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

Ozone 是一个分布式、多副本的对象存储系统，并针对大数据场景进行了专门的优化。Ozone 主要围绕可扩展性进行设计，目标是十亿数量级以上的对象存储。

Ozone 通过对命名空间与块空间的管理进行分离，大大增加了其可扩展性，其中命名空间由 [Ozone Manager ]({{< ref "OzoneManager.zh.md" >}})（OM）管理，块空间由 [Storage Container Manager]({{< ref "StorageContainerManager.zh.md" >}})（SCM）管理。


Ozone 的管理由卷、桶和键组成。卷类似于个人主目录，只有管理员可以创建。

卷用来存储桶，用户可以在一个卷中创建任意数量的桶，桶中包含键，在 Ozone 中通过键来存储数据。

Ozone 的命名空间由存储卷组成，同时存储卷也用作存储账户管理。

下面的框图展示了 Ozone 的核心组件：

![架构图](ozoneBlockDiagram.png)

Ozone Manager 管理命名空间，Storage Container Manager 管理底层的数据，而 Recon 是 Ozone 的管理接口。


## 多种角度看 Ozone

![FunctionalOzone](FunctionalOzone.png)

任何分布式系统都可以从不同角度去观察。一种观察角度是，把 Ozone 看作是在分布式块存储（HDDS）之上加了一个命名空间服务（OM）。

另一种角度是把 Ozone 看作由几个不同的功能层组成：由 OM 和 SCM 组成的元数据管理层；由数据节点以及 SCM 构成的数据存储层；由 Ratis 提供的副本层，用来复制 OM 和 SCM 的元数据以及在修改数据节点上数据时保持数据一致性；Recon 是管理服务器，它负责和其它 Ozone 组件通信，并提供统一的管理 API 和界面。

