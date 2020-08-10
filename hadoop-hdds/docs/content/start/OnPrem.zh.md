---
title: 物理集群上 Ozone 的安装 
weight: 20

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

如果你想要有点挑战性，你可以在物理集群上安装 ozone。搭建一个 Ozone 集群需要了解它的各个组件，Ozone 既能和现有的 HDFS 集群并存运行，也可以独立运行。在这两种模式下，需要运行的 Ozone 组件是相同的。

## Ozone 组件 

1. Ozone Manager - 管理 Ozone 命名空间的服务，负责所有对卷、桶和键的操作。
2. Storage Container Manager - Ozone 中块的管理者，Ozone Manager 从 SCM 请求块，然后用户向块写入数据。
3. Datanodes - Ozone 的 Datanode 代码既可以运行在 HDFS 的 Datanode 内，也可以独立部署成单独的进程。

## 搭建一个独立 Ozone 集群

* 将 ozone-\<version\> 安装包解压到目标目录，因为 Ozone 的 jar 包需要部署到集群的所有机器上，所以你需要在所有机器上进行此操作。

* Ozone 依赖名为 ```ozone-site.xml``` 的配置文件， 运行下面的命令可以在指定目录生成名为 ```ozone-site.xml``` 的配置文件模板，然后你可以将参数替换为合适的值。

{{< highlight bash >}}
ozone genconf <path>
{{< /highlight >}}

我们来看看生成的文件（ozone-site.xml）中都有哪些参数，以及它们是如何影响 ozone 的。当各个参数都配置了合适的值之后，需要把该文件拷贝到 ```ozone directory/etc/hadoop```。

* **ozone.metadata.dirs** 管理员通过此参数指定元数据的存储位置，通常应该选择最快的磁盘（比如 SSD，如果节点上有的话），OM、SCM 和 Datanode 
会将元数据写入此路径。这是个必需的参数，如果不配置它，Ozone 会启动失败。
 
示例如下：

{{< highlight xml >}}
   <property>
      <name>ozone.metadata.dirs</name>
      <value>/data/disk1/meta</value>
   </property>
{{< /highlight >}}

*  **ozone.scm.names**  Storage container manager(SCM) 提供 ozone 使用的分布式块服务，Datanode 通过这个参数来连接 SCM 并向 SCM 发送心跳。Ozone
 目前尚未支持 SCM 的 HA，ozone.scm.names 只需配置单个 SCM 地址即可。
  
  示例如下：
  
  {{< highlight xml >}}
    <property>
        <name>ozone.scm.names</name>
      <value>scm.hadoop.apache.org</value>
      </property>
  {{< /highlight >}}
  
 * **ozone.scm.datanode.id.dir** 每个 Datanode 会生成一个唯一 ID，叫做 Datanode ID。Datanode ID 会被写入此参数所指定路径下名为 datanode.id
  的文件中，如果该路径不存在，Datanode 会自动创建。

示例如下：

{{< highlight xml >}}
   <property>
      <name>ozone.scm.datanode.id.dir</name>
      <value>/data/disk1/meta/node</value>
   </property>
{{< /highlight >}}

* **ozone.om.address** OM 服务地址，OzoneClient 和 Ozone 文件系统需要使用此地址。

示例如下：

{{< highlight xml >}}
    <property>
       <name>ozone.om.address</name>
       <value>ozonemanager.hadoop.apache.org</value>
    </property>
{{< /highlight >}}


## Ozone 参数汇总

| Setting                        | Value                        | Comment |
|--------------------------------|------------------------------|------------------------------------------------------------------|
| ozone.metadata.dirs            | 文件路径                | 元数据存储位置                    |
| ozone.scm.names                | SCM 服务地址            | SCM的主机名:端口，或者IP:端口  |
| ozone.scm.block.client.address | SCM 服务地址和端口 | Ozone 内部服务使用（如 OM）                                |
| ozone.scm.client.address       | SCM 服务地址和端口 | 客户端使用                                        |
| ozone.scm.datanode.address     | SCM 服务地址和端口 | Datanode 使用                            |
| ozone.om.address               | OM 服务地址           | Ozone handler 和 Ozone 文件系统使用             |


## 启动集群

在启动 Ozone 集群之前，需要依次初始化 SCM 和 OM。

{{< highlight bash >}}
ozone scm --init
{{< /highlight >}}

这条命令会使 SCM 创建集群 ID 并初始化它的状态。
```init``` 命令和 Namenode 的 ```format``` 命令类似，只需要执行一次，SCM 就可以在磁盘上准备好正常运行所需的数据结构。

{{< highlight bash >}}
ozone --daemon start scm
{{< /highlight >}}

SCM 启动之后，我们就可以创建对象存储空间，命令如下：

{{< highlight bash >}}
ozone om --init
{{< /highlight >}}


OM 初始化完成之后，就可以启动 OM 服务了：

{{< highlight bash >}}
ozone --daemon start om
{{< /highlight >}}

此时 Ozone 的命名服务 OM 和 块服务 SCM 都已运行。\
**注意**: 如果 SCM 未启动，```om --init``` 命令会失败，同样，如果磁盘上的元数据缺失，SCM 也无法启动，所以请确保 ```scm --init``` 和 ```om --init``` 两条命令都成功执行了。

接下来启动 Datanode，在每个 Datanode 上运行下面的命令：

{{< highlight bash >}}
ozone --daemon start datanode
{{< /highlight >}}

现在 SCM、OM 和所有的 Datanode 都已启动并运行。

***恭喜！你成功地搭建了一个完整的 ozone 集群。***

## 捷径

如果你想简化操作，可以直接运行：

{{< highlight bash >}}
ozone scm --init
ozone om --init
start-ozone.sh
{{< /highlight >}}

这么做的前提是，`workers` 文件已经正确编写，并且配置好了到各个 Datanode 的 ssh，这和 HDFS 的配置方式相同，具体方法请查看 HDFS 文档。
