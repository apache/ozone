---
title: 桶命令
summary: 用桶命令管理桶的生命周期
weight: 3
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

Ozone shell 提供以下桶命令：

  * [创建](#创建)
  * [删除](#删除)
  * [查看](#查看)
  * [列举](#列举)

### 创建

用户使用 `bucket create` 命令来创建桶。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
| -g, \-\-enforcegdpr            | 可选，如果设置为 true 则创建符合 GDPR 规范的桶，设置为 false 或不指定则创建普通的桶|
| -k, \-\-bucketKey              | 可选，如果指定了 KMS 服务器中的桶加密密钥名，该桶中的文件都会被自动加密，KMS 的配置说明可以参考 Hadoop KMS 文档。  
|  Uri                           | 桶名，格式为 **/volume/bucket** |


{{< highlight bash >}}
ozone sh bucket create /hive/jan
{{< /highlight >}}

上述命令会在 _hive_ 卷中创建一个名为 _jan_ 的桶，因为没有指定 scheme，默认使用 O3（RPC）协议。

### 删除 

用户使用 `bucket delete` 命令来删除桶，如果桶不为空，此命令将失败。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 桶名 |

{{< highlight bash >}}
ozone sh bucket delete /hive/jan
{{< /highlight >}}

如果 _jan_ 桶不为空，上述命令会将其删除。

### 查看

`bucket info` 命令返回桶的信息。

***参数：***

| 参数名                      |  说明                                |
|--------------------------------|-----------------------------------------|
|  Uri                           | 桶名 | 

{{< highlight bash >}}
ozone sh bucket info /hive/jan
{{< /highlight >}}

上述命令会打印出 _jan_ 桶的有关信息。

### 列举

用户通过 `bucket list` 命令列举一个卷下的所有桶。

***参数：***

| 参数                      |  说明                                |
|--------------------------------|-----------------------------------------|
| -l, \-\-length                   | 返回结果的最大数量，默认为 100
| -p, \-\-prefix                   | 可选，只有匹配指定前缀的桶会被返回
| -s, \-\-start                    | 从指定键开始列举
|  Uri                           | 卷名

{{< highlight bash >}}
ozone sh bucket list /hive
{{< /highlight >}}

此命令会列出 _hive_ 卷中的所有桶。
