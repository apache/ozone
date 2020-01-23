---
title: Shell 概述
summary: shell 命令的语法介绍。
weight: 1
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

Ozone shell 的帮助命令既可以在 _对象_ 级别调用，也可以在 _操作_ 级别调用。
比如：

{{< highlight bash >}}
ozone sh volume --help
{{< /highlight >}}

此命令会列出所有对卷的可能操作。

你也可以通过它查看特定操作的帮助，比如：

{{< highlight bash >}}
ozone sh volume create --help
{{< /highlight >}}

这条命令会给出 create 命令的命令行选项。

</p>


### 通用命令格式

Ozone shell 命令都遵照以下格式：

> _ozone sh object action url_

**ozone** 脚本用来调用所有 Ozone 子命令，ozone shell 通过 ```sh``` 子命令调用。

对象可以是卷、桶或键，操作一般是各种动词，比如 create、list、delete 等等。


Ozone URL 可以指向卷、桶或键，格式如下：

_\[schema\]\[server:port\]/volume/bucket/key_


其中，

1. **Schema** - 可选，默认为 `o3`，表示使用原生 RPC 协议来访问 Ozone API。

2. **Server:Port** - OM 的地址，如果省略了端口， 则使用 ozone-site.xml 中的默认端口。

根据具体的命令不同，卷名、桶名和键名将用来构成 URL，卷、桶和键命令的文档有更多具体的说明。
