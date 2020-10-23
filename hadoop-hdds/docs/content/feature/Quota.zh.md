---
title: "Ozone中的配额"
date: "2020-October-22"
weight: 4
summary: Ozone中的配额
icon: user
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

 目前，我们知道Ozone允许用户创建Volume，Bucket和Key。一个Volume通常包含几个Bucket，每个Bucket也包含一定数量的Key。显然，它应该允许用户定义配额（例如，在一个Volume下可以创建多少个Bucket或一个Bucket可以使用多少空间），这是存储系统的常见要求。
## 目前支持的
1. Storage space级别配额

 管理员应该能够定义一个Volume或Bucket可以使用多少存储空间。

## 客户端用法
### Storage space级别配额
 Storage space级别配额允许使用 KB（k），MB（m），GB（g），TB（t）， PB（p）等单位。表示将使用多少个存储空间。
#### Volume Space quota用法
```shell
bin/ozone sh volume create --space-quota 5MB /volume1
```
 这意味着将volume1的存储空间设置为5MB

```shell
bin/ozone sh volume setquota --space-quota 10GB /volume1
```
 此行为将volume1的配额更改为10GB。

#### Bucket Space quota 用法
```shell
bin/ozone sh bucket create --space-quota 5MB /volume1/bucket1
```
 这意味着bucket1允许我们使用5MB的存储空间。

```shell
bin/ozone sh bucket setquota  --space-quota 10GB /volume1/bucket1 
```
 该行为将bucket1的配额更改为10GB

一个bucket配额 不应大于其Volume的配额。让我们看一个例子，如果我们有一个10MB的Volume，并在该Volume下创建5个Bucket，配额为5MB，则总配额为25MB。在这种情况下，创建存储桶将始终成功，我们会在数据真正写入时检查bucket和volume的quota。每次写入需要检查当前bucket的是否超上限，当前总的volume使用量是否超上限。

#### 清除Volume1的配额, Bucket清除命令与此类似
```shell
bin/ozone sh volume clrquota --space-quota /volume1
```
#### 查看volume和bucket的quota值以及usedBytes
```shell
bin/ozone sh volume info /volume1
bin/ozone sh bucket info /volume1/bucket1
```
我们能够在volume和bucket的info中查看quota及usedBytes的值
