---
title: 简易 Ozone
weight: 10

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

{{< requirements >}}
 * docker
 * AWS CLI（可选）
{{< /requirements >}}

# 所有 Ozone 服务在单个容器

启动一个 all-in-one 的 ozone 容器最简单的方法就是使用 Docker Hub 最新的 docker 镜像：

```bash
docker run -p 9878:9878 -p 9876:9876 apache/ozone
```
这个命令会从 Docker Hub 拉取 ozone 镜像并在一个容器中启动所有 ozone 服务，包括必要的元数据服务（Ozone Manager，Storage Container Manager）、一个数据节点和兼容 S3
 的 REST 服务（S3 网关）。

# Ozone 服务在多个独立的容器

如果你需要一个更类似生产环境的集群，使用 Ozone 发行包自带的 docker-compose 配置文件可以让 Ozone 服务组件在各自独立的容器中运行。

docker-compose 配置文件和一个 environment 文件已经包含在 Docker Hub 的镜像中。

下面的命令可以从镜像中获取到这两个文件：
```bash
docker run apache/ozone cat docker-compose.yaml > docker-compose.yaml
docker run apache/ozone cat docker-config > docker-config
```

现在你可以用 docker-compose 命令来启动集群：

```bash
docker-compose up -d
```

如果你需要多个数据节点，可以通过下面的命令增加：

```bash
 docker-compose scale datanode=3
 ```
# 运行 S3 客户端

集群启动就绪后，你可以连接 SCM 的 UI 来验证它的状态，地址为（[http://localhost:9876](http://localhost:9876)）。

S3 网关的端口为 9878，如果你正在使用 S3 作为存储方案，可以考虑 Ozone 的 S3 功能。


从命令行创建桶的命令为：

```bash
aws s3api --endpoint http://localhost:9878/ create-bucket --bucket=bucket1
```

唯一的区别在于你需要在运行 aws s3api 命令的时候用 --endpoint 选项指定 ozone S3 网关的地址。

下面我们来把一个简单的文件存入 Ozone 的 S3 桶中，首先创建一个用来上传的临时文件：
```bash
ls -1 > /tmp/testfile
 ```
 这个命令创建了一个用来上传到 Ozone 的临时文件，下面的命令用标准的 aws s3 命令行接口把这个文件上传到了 Ozone 的 S3 桶中：

```bash
aws s3 --endpoint http://localhost:9878 cp --storage-class REDUCED_REDUNDANCY  /tmp/testfile  s3://bucket1/testfile
```
<div class="alert alert-info" role="alert">
注意：对于单容器 ozone 来说，REDUCED_REDUNDANCY 参数是必需的，因为它只有一个数据节点。</div>
我们可以对桶运行 list 命令来验证文件是否上传成功：

```bash
aws s3 --endpoint http://localhost:9878 ls s3://bucket1/testfile
```

<div class="alert alert-info" role="alert"> 你也可以点击下面的链接，通过 Ozone S3 网关自带的浏览器去查看桶内的文件。
<br>
</div>
http://localhost:9878/bucket1?browser
