---
title: 与 HDFS 并存运行
linktitle: Runing with HDFS
weight: 1
summary: Ozone 能够与 HDFS 并存运行，本页介绍如何将 Ozone 部署到已有的 HDFS 集群上。
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

Ozone 支持与 HDFS 并存工作，所以用户可以轻易的在已有的 HDFS 集群上部署 Ozone。

Ozone 的容器管理组件可以在 HDFS 数据节点上以插件的形式或是独立运行，下文介绍插件运行的方法。

为了在 HDFS 数据节点上启用 Ozone 插件，你需要定义服务插件实现类。

<div class="alert alert-warning" role="alert">
<b>重要</b>：因为插件在 HDFS 数据节点启动过程中被激活，服务插件实现类的定义需要添加到 <b>hdfs-site.xml</b> 中。
</div>

{{< highlight xml >}}
<property>
   <name>dfs.datanode.plugins</name>
   <value>org.apache.hadoop.ozone.HddsDatanodeService</value>
</property>
{{< /highlight >}}

此外还需要将 /opt/ozone/share/ozone/lib/ 路径下的 jar 包添加到 Hadoop classpath 下： 

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/ozone/lib/*.jar
{{< /highlight >}}



让 Ozone 随 HDFS 一同启动的步骤为：

 1. HDFS Namenode（从 Hadoop 中启动）
 2. HDFS Datanode (从 Hadoop 中启动，需要按照如上配置插件和 classpath）
 3. Ozone Manager (从 Ozone 中启动）
 4. Storage Container Manager (从 Ozone 启动）

检查数据节点的日志以确认 HDDS/Ozone 插件是否启动，日志中应当包含以下内容：

```
2018-09-17 16:19:24 INFO  HddsDatanodeService:158 - Started plug-in org.apache.hadoop.ozone.HddsDatanodeService@6f94fb9d
```

<div class="alert alert-warning" role="alert">
<b>注意：</b> 上面的测试是基于 Hadoop 3.1 进行的。
</div>
