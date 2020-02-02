---
title: Ozone 文件系统
date: 2017-09-14
weight: 2
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

Hadoop 的文件系统接口兼容可以让任意像 Ozone 这样的存储后端轻松地整合进 Hadoop 生态系统，Ozone 文件系统就是一个兼容 Hadoop 的文件系统。

## 搭建 Ozone 文件系统

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
  <name>fs.o3fs.impl</name>
  <value>org.apache.hadoop.fs.ozone.OzoneFileSystem</value>
</property>
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
export HADOOP_CLASSPATH=/opt/ozone/share/ozonefs/lib/hadoop-ozone-filesystem-lib-current*.jar:$HADOOP_CLASSPATH
{{< /highlight >}}

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


## 兼容旧版本 Hadoop（Legacy jar 和 BasicOzoneFilesystem）

Ozone 文件系统的 jar 包有两种类型，它们都包含了所有的依赖：

 * share/ozone/lib/hadoop-ozone-filesystem-lib-current-VERSION.jar
 * share/ozone/lib/hadoop-ozone-filesystem-lib-legacy-VERSION.jar

第一种 jar 包包含了在一个版本兼容的 hadoop（hadoop 3.2）中使用 Ozone 文件系统需要的所有依赖。

第二种 jar 包将所有依赖单独放在一个内部的目录，并且这个目录下的类会用一个特殊的类加载器来加载这些类。通过这种方法，旧版本的 hadoop 就可以使用 hadoop-ozone-filesystem-lib-legacy.jar（比如hadoop 3.1、hadoop 2.7 或者 spark+hadoop 2.7）。

和依赖的 jar 包类似， OzoneFileSystem 也有两种实现。

对于 Hadoop 3.0 之后的版本，你应当使用 `org.apache.hadoop.fs.ozone.OzoneFileSystem`，它是兼容 Hadoop 文件系统 API 的完整实现。

对于 Hadoop 2.x 的版本，你应该使用基础版本 `org.apache.hadoop.fs.ozone.BasicOzoneFileSystem`，两者实现基本相同，但是不包含在 Hadoop 3.0 中引入的特性和依赖（比如文件系统统计信息、加密桶等）。

### 总结

下表总结了各个版本 Hadoop 应当使用的 jar 包和文件系统实现：

Hadoop 版本 | 需要的 jar            | OzoneFileSystem 实现
---------------|-------------------------|----------------------------------------------------
3.2            | filesystem-lib-current  | org.apache.hadoop.fs.ozone.OzoneFileSystem
3.1            | filesystem-lib-legacy   | org.apache.hadoop.fs.ozone.OzoneFileSystem
2.9            | filesystem-lib-legacy   | org.apache.hadoop.fs.ozone.BasicOzoneFileSystem
2.7            | filesystem-lib-legacy   | org.apache.hadoop.fs.ozone.BasicOzoneFileSystem

由此可知，低版本的 Hadoop 可以使用 hadoop-ozone-filesystem-lib-legacy.jar（比如 hadoop 2.7 或者 spark+hadoop 2.7）。
