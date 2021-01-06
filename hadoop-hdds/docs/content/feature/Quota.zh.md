---
title: "Ozone中的配额"
date: "2020-October-22"
weight: 4
summary: Ozone中的配额
icon: user
menu:
   main:
      parent: 特点
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

 管理员应该能够定义一个Volume或Bucket可以使用多少存储空间。目前支持以下storage space quota的设置:
 
 a. 默认情况下volume和bucket的quota不启用。
 
 b. 当volume quota启用时，bucket quota的总大小不能超过volume。
 
 c. 可以在不启用volume quota的情况下单独给bucket设置quota。此时bucket quota的大小是不受限制的。
 
 d. 目前不支持单独设置volume quota，只有在设置了bucket quota的情况下volume quota才会生效。因为ozone在写入key时只检查bucket的usedBytes。
 
 e. 如果集群从小于1.1.0的旧版本升级而来，则不建议在旧volume和bucket(可以通过查看volume或者bucket的info确认，如果quota值是-2，那么这个volume或者bucket就是旧的)上使用配额。由于旧的key没有计算到bucket的usedBytes中，所以此时配额设置是不准确的。
 
 f. 如果volume quota被启用，那么bucket quota将不能被清除。

2. 命名空间配额

 管理员应当能够定义一个Volume或Bucket可以使用多少命名空间。目前支持命名空间的配额设置为：

 a. 默认情况下volume和bucket的命名空间配额不启用(即无限配额)。

 b. 当volume命名空间配额启用时，该volume的bucket数目不能超过此配额。

 c. 当bucket的命名空间配额启用时，该bucket的key数目不能超过此配额。

 d. Linked bucket不消耗命名空间配额。

 e. 如果集群从小于1.1.0的旧版本升级而来，则不建议在旧volume和bucket(可以通过查看volume或者bucket的info确认，如果quota值是-2，那么这个volume或者bucket就是旧的)上使用配额。由于旧的key没有计算到bucket的命名空间配额中，所以此时配额设置是不准确的。

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

bucket的总配额 不应大于其Volume的配额。让我们看一个例子，如果我们有一个10MB的Volume，该volume下所有bucket的大小之和不能超过10MB，否则设置bucket quota将失败。

#### 清除volume和bucket的配额
```shell
bin/ozone sh volume clrquota --space-quota /volume1
bin/ozone sh bucket clrquota --space-quota /volume1/bucket1
```
#### 查看volume和bucket的quota值以及usedBytes
```shell
bin/ozone sh volume info /volume1
bin/ozone sh bucket info /volume1/bucket1
```
我们能够在volume和bucket的info中查看quota及usedBytes的值

### Namespace quota
命名空间配额是一个数字，其代表由多少个名字能够使用。这个数字不能超过Java long数据类型的最大值。

#### Volume Namespace quota
```shell
bin/ozone sh volume create --namespace-quota 100 /volume1
```
这意味着将volume1的命名空间配额设置为100。

```shell
bin/ozone sh volume setquota --namespace-quota 1000 /volume1
```
此行为将volume1的命名空间配额更改为1000。

#### Bucket Namespace quota
```shell
bin/ozone sh bucket create --namespace-quota 100 /volume1/bucket1
```
这意味着bucket1允许我们使用100的命名空间。

```shell
bin/ozone sh bucket setquota --namespace-quota 1000 /volume1/bucket1 
```
该行为将bucket1的命名空间配额更改为1000。

#### 清除volume和bucket的配额
```shell
bin/ozone sh volume clrquota --namespace-quota /volume1
bin/ozone sh bucket clrquota --namespace-quota /volume1/bucket1
```
#### 查看volume和bucket的quota值以及usedNamespace
```shell
bin/ozone sh volume info /volume1
bin/ozone sh bucket info /volume1/bucket1
```
我们能够在volume和bucket的info中查看quota及usedNamespace的值
