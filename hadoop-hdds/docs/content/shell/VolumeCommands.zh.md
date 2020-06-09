---
title: 卷命令
weight: 2
summary: 用卷命令管理卷的生命周期
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

卷命令通常需要管理员权限，ozone shell 支持以下卷命令：

  * [创建](#创建)
  * [删除](#删除)
  * [查看](#查看)
  * [列举](#列举)
  * [更新](#更新)

### 创建

管理员可以通过 `volume create` 命令创建一个卷并分配给一个用户。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
| -q, \-\-quota                    | 可选，指明该卷在 Ozone 集群所能使用的最大空间，即限额。         |
| -u, \-\-user                     |  必需，指明该卷的所有者，此用户可以在该卷中创建桶和键。    |
|  Uri                           | 卷名                                       |

{{< highlight bash >}}
ozone sh volume create -ssq=1TB --user=bilbo /hive
{{< /highlight >}}

上述命令会在 ozone 集群中创建名为 _hive_ 的卷，卷的限额为 1TB，所有者为 _bilbo_ 。

### 删除

管理员可以通过 `volume delete` 命令删除一个卷，如果卷不为空，此命令将失败。

***参数***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 卷名 |

{{< highlight bash >}}
ozone sh volume delete /hive
{{< /highlight >}}

如果 hive 卷中不包含任何桶，上述命令将删除 hive 卷。

### 查看

通过 `volume info` 命令可以获取卷的限额和所有者信息。

***参数：***

| 参数名                     |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 卷名     | 

{{< highlight bash >}}
ozone sh volume info /hive
{{< /highlight >}}

上述命令会打印出 hive 卷的相关信息。

### 列举

`volume list` 命令用来列举一个用户可以访问的所有卷。

{{< highlight bash >}}
ozone sh volume list --user hadoop
{{< /highlight >}}

若 ACL 已启用，上述命令会打印出 hadoop 用户有 LIST 权限的所有卷。
若 ACL 被禁用，上述命令会打印出 hadoop 用户拥有的所有卷。

### 更新

`volume update` 命令用来修改卷的所有者和限额。

***参数***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
| -q, \-\-quota                    | 可选，重新指定该卷在 Ozone 集群中的限额。  |
| -u, \-\-user                     | 可选，重新指定该卷的所有者 |
|  Uri                           | 卷名                                        |

{{< highlight bash >}}
ozone sh volume update -ssq=10TB /hive
{{< /highlight >}}

上述命令将 hive 卷的限额更新为 10TB。
