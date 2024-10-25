---
title: "安全化 Datanode"
date: "2019-04-03"
weight: 3
menu:
  main:
    parent: 安全
summary:  解释安全化 datanode 的不同模式，包括 Kerberos、证书的手动颁发和自动颁发等。
icon: th
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


Hadoop 中 datanode 的安全机制是通过给每个节点创建 Keytab 文件实现的。Ozone 的 datanode 安全机制不依赖 Kerberos，而是改用 datanode 证书。

但是我们也支持传统的基于 Kerberos 的认证来方便现有用户，用户只需要在 hdfs-site.xml 里配置下面参数即可：

参数名|描述
--------|--------------
dfs.datanode.kerberos.principal| datanode 的服务主体名 <br/> 比如：dn/_HOST@REALM.COM
dfs.datanode.kerberos.keytab.file| datanode 进程所使用的 keytab 文件
hdds.datanode.http.auth.kerberos.principal| datanode http 服务器的服务主体名
hdds.datanode.http.auth.kerberos.keytab| datanode http 服务器的服务主体登录所使用的 keytab 文件


## 如何安全化 datanode

在 Ozone 中，当 datanode 启动并发现 SCM 的地址之后，datanode 首先创建私钥并向 SCM 发送证书请求。

<h3>通过 Kerberos 颁发证书<span class="badge badge-secondary">当前模型</span></h3>
SCM 有一个内置的 CA 用来批准证书请求，如果 datanode 已经有一个 Kerberos keytab，SCM 会信任它并自动颁发一个证书。


<h3>手动颁发<span class="badge badge-primary">开发中</span></h3>
如果 datanode 是新加入的并且没有 keytab，那么证书请求需要等待管理员的批（手动批准功能尚未完全支持）。换句话说，信任关系链由集群管理员建立。

<h3>自动颁发 <span class="badge badge-secondary">开发中</span></h3>
如果你通过 Kubernetes 这样的容器编排软件运行 Ozone，Kubernetes 需要为 datanode 创建一次性 token，用于在启动阶段证明 datanode 容器的身份。（这个特性也正在开发中。）


证书颁发后，datanode 的安全就得到了保障，并且 OM 可以颁发块 token。如果 datanode 没有证书或者 SCM 的根证书，datanode 会自动进行注册，下载 SCM 的根证书，并获取自己的证书。
