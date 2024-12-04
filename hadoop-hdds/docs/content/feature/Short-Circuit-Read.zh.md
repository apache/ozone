---
title: "DataNode 本地短路读"
weight: 2
menu:
   main:
      parent: 特性
summary: Ozone DataNode 本地短路读功能介绍
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

当前在 Ozone 中，客户端使用 GRPC 通道从 DataNode 读取数据。当客户端向 DataNode 请求读取一个文件时，DataNode 将文件从本次磁盘读到内存，然后通过 GRPC 通道发回给客户端。

DataNode 本地短路读功能，当客户端和 DataNode 在同一个机器时，允许客户端绕过 DataNode，直接从本地磁盘读取文件内容。通过绕过 DataNode，去掉网络通信带来的开销，DataNode 本地短路读功能将帮助许多 Ozone 应用，提升读性能。

## 前提

DataNode 本地短路读功能基于 Unix domain socket 实现。 Unix domain socket 是一个特殊的文件系统路径，支持客户端和 DataNode 通过它交互传递信息。

DataNode 本地短路读功能需要用到 Hadoop 本地库 libhadoop.so。 libhadoop.so 提供了调用 Unix domain socket 的功能。该本地库的详细信息，详见 Native Libraries ("https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/NativeLibraries.html")。

在启用 DataNode 本地短路读功能前，确保获取一个可用的 libhadoop.so 文件，并将该文件放置在 Java System.loadLibrary() 调用能搜寻到路径下。

Java 环境变量 "java.library.path" 定义了 Java 的动态库搜寻路径。"java.library.path" 的默认值取决于操作系统和 JAVA 版本。例如，在 Linux 上 OpenJDK 8 的默认值是 `/usr/java/packages/lib/amd64:/usr/lib64:/lib64:/lib:/usr/lib`。

当放置好 libhadoop.so 后，可使用命令 "ozone checknative" 来查看 libhadoop.so 是否能被 Ozone的服务进程正确的搜寻和加载到。


## 配置

DataNode 本地短路读功能需要在客户端和 DataNode 端同时配置。 默认情况下，它是关闭的。

```XML
<property>
   <name>ozone.client.read.short-circuit</name>
   <value>false</value>
   <description>Disable or enable the short-circuit local read feature.</description>
</property>
```

DataNode 本地短路读基于 UNIX domain socket。以下变量将配置 domain socket 路径。 

```XML
<property>
   <name>ozone.domain.socket.path</name>
   <value>/var/lib/ozone_dn_socket</value>
   <description>The path used to create domain socket.</description>
</property>
```

DataNode 需要能创建该路径. 同时，除了启动 Ozone 服务的用户和 root 用户，其他用户不能创建该路径。 由于有这些限制，路径经常使用 /var/run 或者 /var/lib 下的子目录, 正如当前的默认值 "/var/lib/ozone_dn_socket" 一样。

如果修改了 "ozone.domain.socket.path" 的值，比如设置成 "/dir1/dir2/ozone_dn_socket"，请确保 dir1 和 dir2 是已存在的目录，并且 dir2 下还没有 ozone_dn_socket 文件。 ozone_dn_socket 将在 DataNode 启动的时候由 DataNode 创建。


### 安全考量 

为了确保数据的安全和完整性，Ozone 在 "ozone.domain.socket.path" 路径的权限上，将遵守和 Hadoop ("https://cwiki.apache.org/confluence/display/HADOOP2/SocketPathSecurity") 一样的规则。

如果 "ozone.domain.socket.path" 路径权限验证失败， 该功能将自动关闭。 

验证失败返回的信息包含修复问题的指引，例如

"The path component: '/etc/hadoop' in '/etc/hadoop/ozone_dn_socket' has permissions 0777 uid 0 and gid 0. It is not protected because it is world-writable. This might help: 'chmod o-w /etc/hadoop'. For more information: https://wiki.apache.org/hadoop/SocketPathSecurity"
