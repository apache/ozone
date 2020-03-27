---
title: "Docker 速查表"
date: 2017-08-10
summary: Docker Compose 速查表帮助你记住一些操作在 Docker 上运行的 Ozone 集群的常用命令。
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

Ozone 发行包中的 `compose` 目录包含了多种伪集群配置，可以用来以多种方式运行 Ozone（比如：安全集群，启用追踪功能，启用 prometheus 等）。

如果目录下没有额外的使用说明，默认的用法如下：

```bash
cd compose/ozone
docker-compose up -d
```

容器中的数据没有持久化，在集群关闭时会和 docker 卷一起被删除。
```bash
docker-compose down
```

## Docker 和 Ozone 实用命令

如果你对 Ozone 做了修改，最简单的测试方法是运行 freon 和单元测试。

下面是在基于 docker 的集群中运行 freon 的命令。

{{< highlight bash >}}
docker-compose exec datanode bash
{{< /highlight >}}

这会在数据节点的容器中打开一个 bash shell，接下来我们执行 freon 来生成负载。

{{< highlight bash >}}
ozone freon randomkeys --numOfVolumes=10 --numOfBuckets 10 --numOfKeys 10
{{< /highlight >}}

下面是一些与 docker 有关的实用命令。
检查各组件的状态：

{{< highlight bash >}}
docker-compose ps
{{< /highlight >}}

获取指定节点/服务中的日志：

{{< highlight bash >}}
docker-compose logs scm
{{< /highlight >}}


因为 WebUI 的端口已经被转发到外部机器，你可以查看 web UI：

* 对于 Storage Container Manager：http://localhost:9876
* 对于 Ozone Manager：http://localhost:9874
* 对于 数据节点：使用 `docker ps` 查看端口（因为可能会有多个数据节点，它们的端口被映射到一个临时的端口）

你也可以启动多个数据节点：

{{< highlight bash >}}
docker-compose scale datanode=3
{{< /highlight >}}

在一个容器中打开 bash shell 后，你也可以对 [Ozone 命令行接口]({{< ref "shell/_index.zh.md" >}})中的命令进行测试。

{{< highlight bash >}}
docker-compose exec datanode bash
{{< /highlight >}}
