---
title: Ozone 文件系统
date: 2017-09-14
weight: 2
menu:
   main:
      parent: "编程接口"
summary: Hadoop 文件系统兼容使得任何使用类 HDFS 接口的应用无需任何修改就可以在 Ozone 上工作，比如基于 Apache Spark、YARN 和 Hive 等框架的应用。
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

<div class="alert alert-warning">

注意：本页面翻译的信息可能滞后，最新的信息请参看英文版的相关页面。

</div>

Hadoop 的文件系统接口兼容可以让任意像 Ozone 这样的存储后端轻松地整合进 Hadoop 生态系统，Ozone 文件系统就是一个兼容 Hadoop 的文件系统。
目前ozone支持两种协议: o3fs和ofs。两者最大的区别是o3fs只支持在单个bucket上操作，而ofs则支持跨所有volume和bucket的操作。关于两者在操作上的具体区别请参考[OFS(英文页面)]({{< relref path="interface/ofs.md" lang="en" >}})中的"Differences from o3fs"。

## o3fs的配置及使用

要创建一个 ozone 文件系统，我们需要先为它选择一个用来存放数据的桶，这个桶会被用作 Ozone 文件系统的后端存储，所有的文件和目录都存储为这个桶中的键。

如果你还没有可用的卷和桶的话，请使用下面的命令创建：

{{< highlight bash >}}
ozone sh volume create /volume
ozone sh bucket create /volume/bucket
{{< /highlight >}}

创建之后，请使用 _list volume_ 或 _list bucket_ 命令来确认桶已存在。

请在 core-site.xml 中添加以下条目：

{{< highlight xml >}}
<property>
  <name>fs.AbstractFileSystem.o3fs.impl</name>
  <value>org.apache.hadoop.fs.ozone.OzFs</value>
</property>
<property>
  <name>fs.defaultFS</name>
  <value>o3fs://bucket.volume</value>
</property>
{{< /highlight >}}

这样会使指定的桶成为 HDFS 的 dfs 命令的默认文件系统，并且将其注册为了 o3fs 文件系统类型。

你还需要将 ozone-filesystem.jar 文件加入 classpath：

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/ozonefs/lib/ozone-filesystem-hadoop3-*.jar:$HADOOP_CLASSPATH
{{< /highlight >}}

(注意：当使用Hadoop 2.x时，应该在classpath上添加ozone-filesystem-hadoop2-*.jar)

当配置了默认的文件系统之后，用户可以运行 ls、put、mkdir 等命令，比如：

{{< highlight bash >}}
hdfs dfs -ls /
{{< /highlight >}}

或

{{< highlight bash >}}
hdfs dfs -mkdir /users
{{< /highlight >}}


或者 put 命令。换句话说，所有像 Hive、Spark 和 Distcp 的程序都会在这个文件系统上工作。
请注意，在这个桶里使用 Ozone 文件系统以外的方法来进行键的创建和删除时，最终都会体现为 Ozone 文件系统中的目录和文件的创建和删除。

注意：桶名和卷名不可以包含句点。
此外，文件系统的 URI 可以由桶名和卷名后跟着 OM 主机的 FQDN 和一个可选的端口组成，比如，你可以同时指定主机和端口：

{{< highlight bash>}}
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com:5678/key
{{< /highlight >}}

如果 URI 未指定端口，将会尝试从 `ozone.om.address` 配置中获取端口，如果 `ozone.om.address` 未配置，则使用默认端口 `9862`，比如，我们在 `ozone-site.xml
` 中配置 `ozone.om.address` 如下：

{{< highlight xml >}}
  <property>
    <name>ozone.om.address</name>
    <value>0.0.0.0:6789</value>
  </property>
{{< /highlight >}}

当我们运行下面的命令：

{{< highlight bash>}}
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com/key
{{< /highlight >}}

它其实等价于：

{{< highlight bash>}}
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com:6789/key
{{< /highlight >}}

注意：在这种情况下，`ozone.om.address` 配置中只有端口号会被用到，主机名是被忽略的。

## ofs的配置及使用
这只是一个通用的介绍。了解更详细的用法，请参考[OFS(英文页面)]({{< relref path="interface/ofs.md" lang="en" >}})。

请在 core-site.xml 中添加以下条目：

{{< highlight xml >}}
<property>
  <name>fs.ofs.impl</name>
  <value>org.apache.hadoop.fs.ozone.RootedOzoneFileSystem</value>
</property>
<property>
  <name>fs.defaultFS</name>
  <value>ofs://om-host.example.com/</value>
</property>
{{< /highlight >}}

这样会使该om的所有桶和卷成为 HDFS 的 dfs 命令的默认文件系统，并且将其注册为了 ofs 文件系统类型。

你还需要将 ozone-filesystem.jar 文件加入 classpath：

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/ozonefs/lib/ozone-filesystem-hadoop3-*.jar:$HADOOP_CLASSPATH
{{< /highlight >}}

(注意：当使用Hadoop 2.x时，应该在classpath上添加ozone-filesystem-hadoop2-*.jar)

当配置了默认的文件系统之后，用户可以运行 ls、put、mkdir 等命令，比如：

{{< highlight bash >}}
hdfs dfs -ls /
{{< /highlight >}}

需要注意的是ofs能够作用于所有的桶和卷之上，用户可以使用mkdir自行创建桶和卷，比如创建卷volume1和桶bucket1。

{{< highlight bash >}}
hdfs dfs -mkdir /volume1
hdfs dfs -mkdir /volume1/bucket1
{{< /highlight >}}


或者用 put 命令向对应的桶写入文件。

{{< highlight bash >}}
hdfs dfs -put /etc/hosts /volume1/bucket1/test
{{< /highlight >}}

更多用法可以参考: https://issues.apache.org/jira/secure/attachment/12987636/Design%20ofs%20v1.pdf


