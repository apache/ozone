---
title: 从源码构建 Ozone
weight: 30
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

<div class="alert alert-warning">

注意：本页面翻译的信息可能滞后，最新的信息请参看英文版的相关页面。

</div>

{{< requirements >}}
 * Java 1.8
 * Maven
{{< /requirements >}}

<div class="alert alert-info" role="alert">本文档是关于从源码构建 Ozone 的指南，如果你<font
color="red">不</font>打算亲自这么做，你大可放心地跳过本页。</div>

如果你十分了解 Hadoop，并且熟悉 Apache 之道，那你应当知道 Apache 发行包的精髓在于源代码。

从源码构建 ozone 只需要解压源码压缩包然后运行构建命令即可，下面这条命令假设你的机器上拥有构建 Hadoop 所需的所有环境，如果你需要构建 Hadoop 的指南，请查看 Apache Hadoop 网站。

```bash
mvn clean package -DskipTests=true
```

命令执行完成后，`hadoop-ozone/dist/target` 目录下会生成一个 ozone-\<version\>.tar.gz 文件。

你可以拷贝和使用这个压缩包来替代官方发行的二进制包。

## 构建结果测试

为了确保从源码构建出的二进制包可用，你可以运行 hadoop-zone 目录下的验收测试集，测试方法请参照 `smoketest` 目录下的 **READMD.md** 说明。

```bash
cd smoketest
./test.sh
```

你也可以只执行最基本的验收测试：

```bash
cd smoketest
./test.sh --env ozone basic
```

验收测试会启动一个基于 docker-compose 的小型 ozone 集群，然后验证 ozone shell 和文件系统是否完全可用。