---
title: "Ozone 中的容器技术"
summary: Ozone 广泛地使用容器来进行测试，本页介绍 Ozone 中容器的使用及其最佳实践。
weight: 2
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

Ozone 的开发中大量地使用了 Docker，包括以下三种主要的应用场景：

* __开发__:
     * 我们使用 docker 来启动本地伪集群（docker 可以提供统一的环境，但是不需要创建镜像）。
* __测试__:
     * 我们从开发分支创建 docker 镜像，然后在 kubernetes 或其它容器编排系统上测试 ozone。
     * 我们为每个发行版提供了 _apache/ozone_ 镜像，以方便用户体验 Ozone。
     这些镜像 __不__ 应当在 __生产__ 中使用。

<div class="alert alert-warning" role="alert">
当在生产中使用容器方式部署 ozone 时，我们<b>强烈</b>建议你创建自己的镜像。请把所有自带的容器镜像和 k8s 资源文件当作示例指南，参考它们进行定制。
</div>

* __生产__:
     * 我们提供了如何为生产集群创建 docker 镜像的文档。

下面我们来详细地介绍一下各种应用场景：

## 开发

Ozone 安装包中包含了 docker-compose 的示例目录，用于方便地在本地机器启动 Ozone 集群。

使用官方提供的发行包：

```bash
cd compose/ozone
docker-compose up -d
```

本地构建方式：

```bash
cd  hadoop-ozone/dist/target/ozone-*/compose
docker-compose up -d
```

这些 compose 环境文件是重要的工具，可以用来随时启动各种类型的 Ozone 集群。

为了确保 compose 文件是最新的，我们提供了验收测试套件，套件会启动集群并检查其基本行为是否正常。

验收测试也包含在发行包中，你可以在 `smoketest` 目录下找到各个测试的定义。

你可以在任意 compose 目录进行测试，比如：

```bash
cd compose/ozone
./test.sh
```

### 实现细节

`compose` 测试都基于 apache/hadoop-runner 镜像，这个镜像本身并不包含任何 Ozone 的 jar 包或二进制文件，它只是提供其了启动 Ozone 的辅助脚本。

hadoop-runner 提供了一个随处运行 Ozone 的固定环境，Ozone 分发包通过目录挂载包含在其中。

(docker-compose 示例片段)

```
 scm:
      image: apache/hadoop-runner:jdk11
      volumes:
         - ../..:/opt/hadoop
      ports:
         - 9876:9876

```

容器应该通过环境变量来进行配置，由于每个容器都应当设置相同的环境变量，我们在单独的文件中维护了一个环境变量列表：

```
 scm:
      image: apache/hadoop-runner:jdk11
      #...
      env_file:
          - ./docker-config
```

docker-config 文件中包含了所需环境变量的列表：

```
OZONE-SITE.XML_ozone.om.address=om
OZONE-SITE.XML_ozone.om.http-address=om:9874
OZONE-SITE.XML_ozone.scm.names=scm
#...
```

你可以看到我们所使用的命名规范，根据这些环境变量的名字，`hadoop-runner` 基础镜像中的[脚本](https://github.com/apache/hadoop/tree/docker-hadoop-runner-latest/scripts) 会生成合适的 hadoop XML 配置文件（在我们这种情况下就是 `ozone-site.xml`）。

`hadoop-runner` 镜像的[入口点](https://github.com/apache/hadoop/blob/docker-hadoop-runner-latest/scripts/starter
.sh)包含了一个辅助脚本，这个辅助脚本可以根据环境变量触发上述的配置文件生成以及其它动作（比如初始化 SCM 和 OM 的存储、下载必要的 keytab 等）。

## 测试 

`docker-compose` 的方式应当只用于本地测试，不适用于多节点集群。要在多节点集群上使用容器，我们需要像 Kubernetes 这样的容器编排系统。

Kubernetes 示例文件在 `kubernetes` 文件夹中。

*请注意*：所有提供的镜像都使用 `hadoop-runner` 作为基础镜像，这个镜像中包含了所有测试环境所需的测试工具。对于生产环境，我们推荐用户使用自己的基础镜像创建可靠的镜像。

### 发行包测试

可以通过部署任意的示例集群来测试发行包：

```bash
cd kubernetes/examples/ozone
kubectl apply -f
```

注意，在这个例子中会从 Docker Hub 下载最新的镜像。

### 开发构建测试

为了测试开发中的构建，你需要创建自己的镜像并上传到自己的 docker 仓库中：


```bash
mvn clean install -DskipTests -Pdocker-build,docker-push -Ddocker.image=myregistry:9000/name/ozone
```

所有生成的 kubernetes 资源文件都会使用这个镜像 (`image:` keys are adjusted during the build)

```bash
cd kubernetes/examples/ozone
kubectl apply -f
```

## 生产

<div class="alert alert-danger" role="alert">
我们<b>强烈</b>推荐在生产集群使用自己的镜像，并根据实际的需求调整基础镜像、文件掩码、安全设置和用户设置。
</div>

你可以使用我们开发中所用的镜像作为示例：

 * [基础镜像] (https://github.com/apache/hadoop/blob/docker-hadoop-runner-jdk11/Dockerfile)
 * [完整镜像] (https://github.com/apache/hadoop/blob/trunk/hadoop-ozone/dist/src/main/docker/Dockerfile)

 Dockerfile 中大部分内容都是可选的辅助功能，但如果要使用我们提供的 kubernetes 示例资源文件，你可能需要[这里](https://github.com/apache/hadoop/tree/docker-hadoop-runner-jdk11/scripts)的脚本。

  * 两个 python 脚本将环境变量转化为实际的 hadoop XML 配置文件
  * start.sh 根据环境变量执行 python 脚本（以及其它初始化工作）

## 容器

Ozone 相关的容器镜像和 Dockerfile 位置：


<table class="table table-dark">
  <thead>
    <tr>
      <th scope="col">#</th>
      <th scope="col">容器</th>
      <th scope="col">仓库</th>
      <th scope="col">基础镜像</th>
      <th scope="col">分支</th>
      <th scope="col">标签</th>
      <th scope="col">说明</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th scope="row">1</th>
      <td>apache/ozone</td>
      <td>https://github.com/apache/hadoop-docker-ozone</td>
      <td>ozone-... </td>
      <td>hadoop-runner</td>
      <td>0.3.0,0.4.0,0.4.1</td>
      <td>每个 Ozone 发行版都对应一个新标签。</td>
    </tr>
    <tr>
      <th scope="row">2</th>
      <td>apache/hadoop-runner </td>
      <td>https://github.com/apache/hadoop</td>
      <td>docker-hadoop-runner</td>
      <td>centos</td>
      <td>jdk11,jdk8,latest</td>
      <td>这是用于测试 Hadoop Ozone 的基础镜像，包含了一系列可以让我们更加方便地运行 Ozone 的工具。
      </td>
    </tr>
  </tbody>
</table>
