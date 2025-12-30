---
title: Ofs (兼容 Hadoop 的文件系统)
date: 2017-09-14
weight: 1
menu:
   main:
      parent: "编程接口"
summary: Hadoop Compatible file system allows any application that expects an HDFS like interface to work against Ozone with zero changes. Frameworks like Apache Spark, YARN and Hive work against Ozone without needing any change. **Global level view.**
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

兼容 Hadoop 的文件系统 (HCFS) 接口允许像 Ozone 这样的存储后端轻松集成到 Hadoop 生态系统中。Ozone 文件系统 (OFS) 是一个兼容 Hadoop 的文件系统。


<div class="alert alert-warning" role="alert">

目前，Ozone 支持两种协议：`o3fs://` 和 `ofs://`。
o3fs 与 ofs 之间最大的区别在于，`o3fs` 仅支持在 **单个桶** 上的操作, 而 `ofs` 支持跨所有卷和桶的操作，并提供所有卷/桶的完整视图。

</div>

## 基础知识

有效的 OFS 路径示例：

```
ofs://om1/
ofs://om3:9862/
ofs://omservice/
ofs://omservice/volume1/
ofs://omservice/volume1/bucket1/
ofs://omservice/volume1/bucket1/dir1
ofs://omservice/volume1/bucket1/dir1/key1

ofs://omservice/tmp/
ofs://omservice/tmp/key1
```

在 OFS 文件系统中，卷和挂载点位于根目录级别。卷的下一级是桶。每个桶下面是键和目录。

请注意，对于挂载点，目前仅支持临时挂载 /tmp。

## 配置

请在 `core-site.xml` 添加下列配置。

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

这将使所有的卷和桶成为默认的 Hadoop 兼容文件系统，并注册 ofs 文件系统类型。

您还需要将 ozone-filesystem-hadoop3.jar 文件添加到 classpath 中：

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/ozone/lib/ozone-filesystem-hadoop3-*.jar:$HADOOP_CLASSPATH
{{< /highlight >}}

(请注意: 在 Hadoop 2.x 中, 请使用 `ozone-filesystem-hadoop2-*.jar`)

当默认的文件系统被建立，用户可以运行命令例如ls，put，mkdir等。
例如：

{{< highlight bash >}}
hdfs dfs -ls /
{{< /highlight >}}

请注意，ofs 对所有桶和卷都有效。用户可以使用 mkdir 创建桶和卷，例如创建名为 volume1 的卷和名为 bucket1 的桶

{{< highlight bash >}}
hdfs dfs -mkdir /volume1
hdfs dfs -mkdir /volume1/bucket1
{{< /highlight >}}

或者使用 put 命令向桶中写入一个文件

{{< highlight bash >}}
hdfs dfs -put /etc/hosts /volume1/bucket1/test
{{< /highlight >}}

有关更多用法，请参见： https://issues.apache.org/jira/secure/attachment/12987636/Design%20ofs%20v1.pdf

## 与 [o3fs]({{< ref "interface/O3fs.md" >}}) 的区别

### 创建文件

OFS 不允许直接在根目录或卷下创建键（文件）。
当用户尝试这样做时，他们将收到一个错误消息：

```bash
$ ozone fs -touch /volume1/key1
touch: Cannot create file under root or volume.
```

### 简化 fs.defaultFS

使用 OFS 时，fs.defaultFS（在 core-site.xml 中）不再需要像 o3fs 那样在其路径中具有特定的卷和桶。
只需设置 OM 主机名或 service ID（在 HA 的情况下）：


```xml
<property>
  <name>fs.defaultFS</name>
  <value>ofs://omservice</value>
</property>
```

客户端将能够访问集群上的所有卷和桶，而无需指定主机名或 service ID。

```bash
$ ozone fs -mkdir -p /volume1/bucket1
```

### 通过 FileSystem shell 直接管理卷和桶

管理员可以通过 Hadoop FS shell 轻松创建和删除卷和桶。卷和桶被视为类似于目录，因此如果它们不存在，可以使用 `-p` 创建：

```bash
$ ozone fs -mkdir -p ofs://omservice/volume1/bucket1/dir1/
```
请注意，卷和桶名称字符集规则仍然适用。例如，桶和卷名称不接受下划线（`_`）：

```bash
$ ozone fs -mkdir -p /volume_1
mkdir: Bucket or Volume name has an unsupported character : _
```

## 挂载点和设置 /tmp

为了与使用 /tmp/ 的传统 Hadoop 应用程序兼容，我们在 FS 的根目录有一个特殊的临时目录挂载点。
这个功能将来可能会扩展，以支持自定义挂载路径。

目前 Ozone 支持两种 /tmp 的配置。第一种（默认）是每个用户的临时目录，
由一个挂载卷和一个用户特定的临时桶组成。第二种（通过 ozone-site.xml 配置）
是一个类似粘滞位的临时目录，对所有用户共用，由一个挂载卷和一个共用的临时桶组成。

重要提示：要使用它，首先，**管理员** 需要创建名为 tmp 的卷（卷名目前是硬编码的）并将其 ACL 设置为 world ALL 访问权限。

具体来说：

```bash
$ ozone sh volume create tmp
$ ozone sh volume setacl tmp -al world::a
```

每个集群中这些命令**仅需要执行一次**

### 对于每个用户的 /tmp 目录 (默认)

**每个用户** 都需要先创建并初始化他们自己的 temp 桶一次

```bash
$ ozone fs -mkdir /tmp
2020-06-04 00:00:00,050 [main] INFO rpc.RpcClient: Creating Bucket: tmp/0238 ...
```

在此之后用户可以向该目录写入，就和向其他常规目录写入一样。例如：

```bash
$ ozone fs -touch /tmp/key1
```

### 对于所有用户共享的 /tmp 目录

要启用类似粘滞位的共享 /tmp 目录，请在 ozone-site.xml 中更新以下配置：

```xml
<property>
  <name>ozone.om.enable.ofs.shared.tmp.dir</name>
  <value>true</value>
</property>
```

然后，在以**管理员**身份设置好 tmp 卷之后，还需要配置一个 tmp 桶，作为所有用户的共享 /tmp 目录，例如：

```bash
$ ozone sh bucket create /tmp/tmp
$ ozone sh volume setacl tmp -a user:anyuser:rwlc \
  user:adminuser:a,group:anyuser:rwlc,group:adminuser:a tmp/tmp
```

在这里，anyuser 是管理员希望授予访问权限的用户名，而 adminuser 是管理员的用户名。

然后用户可以访问 tmp 目录：

```bash
$ ozone fs -put ./NOTICE.txt ofs://om/tmp/key1
```

## 启用回收站的删除操作

为了在 Ozone 中启用回收站，请将这些配置添加到 core-site.xml：

{{< highlight xml >}}
<property>
  <name>fs.trash.interval</name>
  <value>10</value>
</property>
<property>
  <name>fs.trash.classname</name>
  <value>org.apache.hadoop.fs.ozone.OzoneTrashPolicy</value>
</property>
{{< /highlight >}}

当启用回收站功能后删除键时，这些键会被移动到每个桶下的一个回收站目录中，因为在 Ozone 中不允许将键在桶之间移动（重命名）。

```bash
$ ozone fs -rm /volume1/bucket1/key1
2020-06-04 00:00:00,100 [main] INFO fs.TrashPolicyDefault: Moved: 'ofs://id1/volume1/bucket1/key1' to trash at: ofs://id1/volume1/bucket1/.Trash/hadoop/Current/volume1/bucket1/key1
```

这与 HDFS encryption zone 处理回收站位置的方式非常相似。

**请注意**

1. 可以使用标志 `-skipTrash` 来永久删除文件，而不将其移动到回收站。
2. 启用回收站时，不允许在桶或卷级别进行删除操作。在这种情况下，必须使用 skipTrash。
即，不使用 skipTrash 的情况下，不允许使用 `ozone fs -rm -R ofs://vol1/bucket1` 或 `ozone fs -rm -R o3fs://bucket1.vol1` 进行操作。

## 递归地列出

OFS 支持递归地列出卷、桶和键。

例如，如果启用了 ACL 的话, 命令 `ozone fs -ls -R ofs://omservice/` 会递归地列出用户有 LIST 权限的所有卷、桶和键。
如果禁用了 ACL，这个命令会列出该集群上的所有内容。

这个功能不会降低服务器性能，因为循环操作是在客户端上进行的。可以将其视为客户端向服务器发出多个请求以获取所有信息的过程。
