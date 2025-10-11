---
title: "在DataNode上合并Container的RocksDB"
weight: 2
menu:
   main:
      parent: 特性
summary: Ozone DataNode Container模式简介V3
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

在 Ozone 中，用户数据被分割成blocks并存储在 HDDS Container中。Container是 Ozone/HDDS 的基本复制单元。每个Container都有自己的元数据和数据, 数据以文件形式保存在磁盘上，元数据保存在RocksDB中。

之前，数据节点上每个Container都有一个RocksDB。随着用户数据的不断增长，一个DataNode上将会有成百上千个RocksDB实例。在一个JVM中管理如此多的RocksDB实例是一个巨大的挑战。

与以前的用法不同，"Merge Container RocksDB in DN"功能将为每个Volume只使用一个RocksDB，并在此RocksDB中保存所有Container的元数据。
  
## 配置

这主要是DataNode的功能，不需要太多配置。默认情况下，它是启用的。

如果更倾向于为每个Container使用一个RocksDB的模式，那么这下面的配置可以禁用上面所介绍的功能。请注意，一旦启用该功能，强烈建议以后不要再禁用。

```XML
<property>
   <name>hdds.datanode.container.schema.v3.enabled</name>
   <value>false</value>
   <description>Disable or enable this feature.</description>
</property>
```
 
无需任何特殊配置，单个RocksDB将会被创建在"hdds.datanode.dir"中所配置的数据卷下。

对于一些有高性能要求的高级集群管理员，他/她可以利用快速存储来保存RocksDB。在这种情况下，请配置下面这两个属性。

```XML
<property>
   <name>hdds.datanode.container.db.dir</name>
   <value/>
   <description>This setting is optional. Specify where the per-disk rocksdb instances will be stored.</description>
</property>
<property>
   <name>hdds.datanode.failed.db.volumes.tolerated</name>
   <value>-1</value>
   <description>The number of db volumes that are allowed to fail before a datanode stops offering service.
   Default -1 means unlimited, but we should have at least one good volume left.</description>
</property>
```

### 向后兼容性 

Existing containers each has one RocksDB for them will be still accessible after this feature is enabled. All container data will co-exist in an existing Ozone cluster.

## 参考文献

 * [设计文档]({{< ref path="design/dn-merge-rocksdb.md" lang="en">}})