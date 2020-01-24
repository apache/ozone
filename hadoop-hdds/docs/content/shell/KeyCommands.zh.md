---
title: 键命令
summary: 用键命令管理键/对象的生命周期
weight: 4
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


Ozone shell 提供以下键命令：

  * [下载](#下载)
  * [上传](#上传)
  * [删除](#删除)
  * [查看](#查看)
  * [列举](#列举)
  * [重命名](#重命名)


### 下载

`key get` 命令从 Ozone 集群下载一个键到本地文件系统。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 键名，格式为 **/volume/bucket/key**
|  FileName                      | 下载到本地后的文件名


{{< highlight bash >}}
ozone sh key get /hive/jan/sales.orc sales.orc
{{< /highlight >}}

从 _/hive/jan_ 桶中下载 sales.orc 文件，写入到本地名为 sales.orc 的文件。

### 上传

`key put` 命令从本地文件系统上传一个文件到指定的桶。

***参数：***

| 参数名                      |  说明                                 |
|--------------------------------|-----------------------------------------|
|  Uri                           | 键名，格式为 **/volume/bucket/key**
|  FileName                      | 待上传的本地文件
| -r, \-\-replication              | 可选，上传后的副本数，合法值为 ONE 或者 THREE，如果不设置，将采用集群配置中的默认值。

{{< highlight bash >}}
ozone sh key put /hive/jan/corrected-sales.orc sales.orc
{{< /highlight >}}

上述命令将 sales.orc 文件作为新键上传到 _/hive/jan/corrected-sales.orc_ 。

### 删除

`key delete` 命令用来从桶中删除指定键。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 键名

{{< highlight bash >}}
ozone sh key delete /hive/jan/corrected-sales.orc
{{< /highlight >}}

上述命令会将 _/hive/jan/corrected-sales.orc_ 这个键删除。


### 查看

`key info` 命令返回指定键的信息。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 键名

{{< highlight bash >}}
ozone sh key info /hive/jan/sales.orc
{{< /highlight >}}

上述命令会打印出 _/hive/jan/sales.orc_ 键的相关信息。

### 列举

用户通过 `key list` 命令列出一个桶中的所有键。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
| -l, \-\-length                   | 返回结果的最大数量，默认值为 100
| -p, \-\-prefix                   | 可选，只有匹配指定前缀的键会被返回
| -s, \-\-start                    | 从指定键开始列举
|  Uri                           | 桶名

{{< highlight bash >}}
ozone sh key list /hive/jan
{{< /highlight >}}

此命令会列出 _/hive/jan_ 桶中的所有键。

### 重命名

`key rename` 命令用来修改指定桶中的已有键的键名。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 桶名，格式为 **/volume/bucket**
|  FromKey                       | 旧的键名
|  ToKey                         | 新的键名

{{< highlight bash >}}
ozone sh key rename /hive/jan sales.orc new_name.orc
{{< /highlight >}}

上述命令会将 _/hive/jan_ 桶中的 _sales.orc_ 重命名为 _new\_name.orc_ 。
