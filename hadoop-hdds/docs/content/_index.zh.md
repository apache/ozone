---
title: 概述
menu: main
weight: -10
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

# Apache Ozone

{{<figure src="/ozone-usage.png" width="60%">}}

*_Ozone 是 Hadoop 的分布式对象存储系统，具有易扩展和冗余存储的特点。<p>
Ozone 不仅能存储数十亿个不同大小的对象，还支持在容器化环境（比如 Kubernetes）中运行。_* <p>

Apache Spark、Hive 和 YARN 等应用无需任何修改即可使用 Ozone。Ozone 提供了 [Java API]({{<
ref "JavaApi.zh.md" >}})、[S3 接口]({{< ref "S3.zh.md" >}})和命令行接口，极大地方便了 Ozone
 在不同应用场景下的使用。

Ozone 的管理由卷、桶和键组成：

* 卷的概念和用户账号类似，只有管理员可以创建和删除卷。
* 桶的概念和目录类似，用户可以在自己的卷下创建任意数量的桶，每个桶可以包含任意数量的键，但是不可以包含其它的桶。
* 键的概念和文件类似，用户通过键来读写数据。
