---
title: "测试工具"
summary: Ozone 提供了负载生成、网络分片测试、验收测试等多种测试工具。
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

测试是开发分布式系统中最重要的部分，我们提供了以下类型的测试。

本页面给出了 Ozone 自带的测试工具。

注意：我们还进行了其它测试（比如通过 Spark 或 Hive 进行的 TCP-DS、TCP-H），但因为它们是外部工具，所以没有在此列出。

## 单元测试

和每个 java 项目一样，我们的每个项目都包含传统的单元测试。

## 集成测试（JUnit）

传统的单元测试只能测试一个单元，但我们也有更高层次的单元测试。它们使用 `MiniOzoneCluster` 辅助方法在单元测试中启动守护进程（SCM、OM、数据节点）。

从 maven 或 java 的角度来看，集成测试也只是普通的单元测试而已（使用了 JUnit 库），但为了解决一些依赖问题，我们将它们单独放在了 `hadoop-ozone/integration-test` 目录下。

## 冒烟测试

我们使用基于 docker-compose 的伪集群来运行不同配置的 Ozone，为了确保这些配置可用，我们在 https://robotframework.org/ 的帮助下实现了 _验收_ 测试。

冒烟测试包含在发行包中（`./smoketest`），但 robot 文件只定义了运行命令行然后检查输出的测试。

为了在不同环境（docker-compose、kubernetes）下运行冒烟测试，你需要定义如何启动容器，然后在正确的容器中执行正确的测试。

这部分的测试包含在 `compose` 目录中（查看 `./compose/*/test.sh` 或者 `./compose/test-all.sh`）。

例如，一种测试分发包的简单方法是：

```
cd compose/ozone
./test.sh
```

## Blockade

[Blockade](https://github.com/worstcase/blockade) 是一个测试网络故障和分片的工具（灵感来自于大名鼎鼎的[Jepsen 测试](https://jepsen.io/analyses)）。

Blockade 测试在其它测试的基础上实现，可以在分发包中的 `./blockade` 目录下进行测试。

```
cd blockade
pip install pytest==2.8.7,blockade
python -m pytest -s .
```

更多细节查看 blockade 目录下的 README。

## MiniChaosOzoneCluster

这是一种在你的机器上获得[混沌](https://en.wikipedia.org/wiki/Chaos_engineering)的方法。它可以直接从源码启动一个 MiniOzoneCluster
（会启动真实的守护进程），并随机杀死它。

## Freon

Freon 是 Ozone 发行包中包含的命令行应用，它是一个负载生成器，用于压力测试。

随机生成Key:

在randomkeys模式下，写入Ozone的数据是随机生成的。每个键的大小为10 KB。

volume/bucket/key的数量是可以配置的。副本type和factor(例如: 3个节点使用ratis控制副本)也可以配置。

更多信息，可使用如下命令查看:

bin/ozone freon --help

例如：

```
ozone freon randomkeys --num-of-volumes=10 --num-of-buckets 10 --num-of-keys 10  --replication-type=RATIS --factor=THREE
```

```
***************************************************
Status: Success
Git Base Revision: 48aae081e5afacbb3240657556b26c29e61830c3
Number of Volumes created: 10
Number of Buckets created: 100
Number of Keys added: 1000
Ratis replication factor: THREE
Ratis replication type: RATIS
Average Time spent in volume creation: 00:00:00,035
Average Time spent in bucket creation: 00:00:00,319
Average Time spent in key creation: 00:00:03,659
Average Time spent in key write: 00:00:10,894
Total bytes written: 10240000
Total Execution time: 00:00:16,898
***********************
```
