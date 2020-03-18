---
title: 快速入门
name: Getting Started
identifier: Starting
menu: main
weight: 1
cards: "false"
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


{{<jumbotron title="安装 Ozone">}}
Ozone 的安装和运行有多种方式，支持从简单的本地节点 docker 部署，到大规模多节点的 Kubernetes 或物理集群部署。
{{</jumbotron>}}

<section class="row cardgroup">

<span class="label label-warning label-">入门级</span>

<h2>通过 Docker Hub 运行 Ozone</h2>

你可以通过 Docker Hub 来运行 Ozone，无需下载官方发行包，这让探索 Ozone 十分容易。
<br />
  {{<card title="在 Docker 中启动 ozone" link="start/StartFromDockerHub.zh.md" link-text="Ozone In Docker" image="start/docker.png">}}
  启动一个 ozone 集群来探索其功能的最简易的方式就是通过 docker 来启动 ozone。
  {{</card>}}

</section>

<section class="row cardgroup">

<span class="label label-success">推荐级</span>


<h2>通过官方发行包运行 Ozone</h2>

 官方发行包包括了源代码包和二进制代码包，二进制代码包无需编译就可以根据不同的配置运行在以下平台：
<br />
  {{<card title="在物理集群上运行 Ozone" link="start/OnPrem.zh.md" link-text="On-Prem Ozone Cluster" image="start/hadoop.png">}}
Ozone 能够以并存的方式部署到已有的 HDFS 集群，下面的文档介绍了 Ozone 的各个组件以及如何以灵活的配置进行部署。
  {{</card>}}

  {{<card title="在 K8s 上运行 Ozone" link="start/Kubernetes.zh.md" link-text="Kubernetes" image="start/k8s.png">}}
Ozone 也能够在 Kubernetes 上良好运行，下面的文档介绍了如何在 K8s 上部署 Ozone。Ozone 为 K8s 应用提供了一种多副本存储的解决方案。
  {{</card>}}

  {{<card title="使用 MiniKube 运行 Ozone" link="start/Minikube.zh.md" link-text="Minikube cluster" image="start/minikube.png">}}
Ozone 发行包中包括了一套标准的 K8s 资源，你可以在 MiniKube 上运行 Ozone 来进行基于 K8s 部署的实验。
  {{</card>}}

  {{<card title="在本地节点运行 Ozone 集群" link="start/RunningViaDocker.zh.md" link-text="docker-compose" image="start/docker.png">}}
 Ozone 发行包中包括了一系列的本地 docker 配置文件，它们可以不依赖 Docker Hub 运行。
  {{</card>}}

</section>

<section class="row cardgroup">

<span class="label label-danger">Hadoop 专家级</span>

<h2>从源码构建 Ozone</h2>

 关于从源码构建 Ozone 部署包。</br>

  {{<card title="从源码构建" link="start/FromSource.zh.md" link-text="Build ozone from source" image="start/hadoop.png">}}
如果你十分了解 Hadoop，并且熟悉 Apache 之道，那你应当知道 Apache 发行包的精髓在于源代码。不过即使是专家，下面的文档也会对你有帮助。
  {{</card>}}

</section>
