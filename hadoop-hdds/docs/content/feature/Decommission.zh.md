---
title: "Decommissioning"
weight: 1
menu:
   main:
      parent: 特性
summary: Decommissioning of SCM, OM and Datanode.
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

# DataNode Decommission

DataNode Decommission是从Ozone集群中删除现有DataNode的过程中，同时确保新数据不会被写入正在Decommission的DataNode。当你启动DataNode Decommission的操作时候，Ozone会自动确保在Decommission完成之前，该数据节点上的所有Storage containers都在另一个DataNode上创建了额外的副本。因此，DataNode在Decommission完成后可以继续运行，并可用于读取，但不能用于写入，直到手动停止DataNode的服务。

当我们启动Decommission时，这个操作首先要检查节点的当前状态，理想情况下应该是 "IN_SERVICE"，然后将其状态更改为 "DECOMMISSIONING"，并启动Decommission的流程：

1. 首先它会触发一个事件，关闭节点上的所有Pipelines，同时关闭所有Containers。

2. 然后获取节点上的Container信息，并检查是否需要新的副本。如果需要，创建新的副本的任务就会被调度起来。 

3. 复制任务被调度后，节点仍处于待处理状态，直到复制任务完成。

4. 在此阶段，节点将完成Decommission的过程，然后节点状态将更改为 "DECOMMISSIONED"。

要检查DataNode的当前状态，可以执行以下命令,
```shell
ozone admin datanode list
```

要decommission某台datanode的时候，可以执行下面的命令,

```shell
ozone admin datanode decommission [-hV] [-id=<scmServiceId>]
       [--scm=<scm>] [<hosts>...]
```
您可以输入多个主机，以便一起Decommission多个DataNode。

查看 Decommission时datanode 的状态，可以执行下面的命令,

```shell
ozone admin datanode status decommission [-hV] [-id=<scmServiceId>] [--scm=<scm>] [--id=<uuid>] [--ip=<ipAddress>]
```
您可以指定一个 Datanode 的 IP address 或 UUID 以查看该 Datanode 相关的详细信息。


**Note:** 要Recommission某台DataNode的时候，可在命令行执行以下命令,
```shell
ozone admin datanode recommission [-hV] [-id=<scmServiceId>]
       [--scm=<scm>] [<hosts>...]
```

# OM Decommission

Ozone Manager（OM）Decommissioning是指从 OM HA Ring 中从容地(gracefully)移除一个 OM 的过程。

要Decommission OM 并将这个节点从 OM HA ring中移除，需要执行以下步骤。
1. 将要被Decommission的 OM 节点的 _OM NodeId_ 添加到所有其他 OM 的 _ozone-site.xml_ 中的 _ozone.om.decommissioned.nodes.<omServiceId>_ 属性中。
2. 运行以下命令Decommission这台 OM 节点.
```shell
ozone admin om decommission -id=<om-service-id> -nodeid=<decommissioning-om-node-id> -hostname=<decommissioning-om-node-address> [optional --force]
```
 _force选项将跳过检查 _ozone-site.xml_ 中的 OM 配置是否已更新，并将Decommission节点添加至 _**ozone.om.decommissioned.nodes**_ 配置中. <p>**Note -** 建议在Decommissioning一个 OM 节点之前bootstrap另一个 OM 节点，以保持OM的高可用性（HA）.</p>

# SCM Decommission

存储容器管理器 (SCM) Decommissioning 是允许您从容地(gracefully)将一个 SCM 从 SCM HA Ring 中移除的过程。

在Decommission一台SCM，并将其从SCM HA ring中移除时，需要执行以下步骤。
```shell
ozone admin scm decommission [-hV] [--service-id=<scmServiceId>] -nodeid=<nodeId>
```
执行以下命令可获得 "nodeId"： **"ozone admin scm roles "**

### Leader SCM
如果需要decommission **leader** SCM, 您必须先将leader的角色转移到另一个 scm，然后再Decommission这个节点。 

您可以使用以下的命令来转移leader的角色，
```shell
ozone admin scm transfer [--service-id=<scmServiceId>] -n=<nodeId>
```
在Leader的角色成功地转移之后，您可以继续decommission的操作。

### Primordial SCM
如果要decommission **primordial** scm，必须更改 _ozone.scm.primordial.node.id_ 的属性，使其指向不同的 SCM，然后再继续decommissioning。

### 注意
在运行SCM decommissioning的操作期间，应手动删除decommissioned SCM的私钥。私钥可在 _hdds.metadata.dir_ 中找到。

在支持证书吊销之前（HDDS-8399），需要手动删除decommissioned SCM上的证书。
