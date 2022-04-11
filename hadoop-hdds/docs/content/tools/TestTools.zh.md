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
cd blocakde
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

## Genesis

Genesis 是一个微型的基准测试工具，它也包含在发行包中（`ozone genesis`），但是它不需要一个真实的集群，而是采用一种隔离的方法测试不同部分的代码（比如，将数据存储到本地基于 RocksDB 的键值存储中）。

运行示例：

```
 ozone genesis -benchmark=BenchMarkRocksDbStore
# JMH version: 1.19
# VM version: JDK 11.0.1, VM 11.0.1+13-LTS
# VM invoker: /usr/lib/jvm/java-11-openjdk-11.0.1.13-3.el7_6.x86_64/bin/java
# VM options: -Dproc_genesis -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/var/log/hadoop -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/opt/hadoop -Dhadoop.id.str=hadoop -Dhadoop.root.logger=INFO,console -Dhadoop.policy.file=hadoop-policy.xml -Dhadoop.security.logger=INFO,NullAppender
# Warmup: 2 iterations, 1 s each
# Measurement: 20 iterations, 1 s each
# Timeout: 10 min per iteration
# Threads: 4 threads, will synchronize iterations
# Benchmark mode: Throughput, ops/time
# Benchmark: org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test
# Parameters: (backgroundThreads = 4, blockSize = 8, maxBackgroundFlushes = 4, maxBytesForLevelBase = 512, maxOpenFiles = 5000, maxWriteBufferNumber = 16, writeBufferSize = 64)

# Run progress: 0.00% complete, ETA 00:00:22
# Fork: 1 of 1
# Warmup Iteration   1: 213775.360 ops/s
# Warmup Iteration   2: 32041.633 ops/s
Iteration   1: 196342.348 ops/s
                 ?stack: <delayed till summary>

Iteration   2: 41926.816 ops/s
                 ?stack: <delayed till summary>

Iteration   3: 210433.231 ops/s
                 ?stack: <delayed till summary>

Iteration   4: 46941.951 ops/s
                 ?stack: <delayed till summary>

Iteration   5: 212825.884 ops/s
                 ?stack: <delayed till summary>

Iteration   6: 145914.351 ops/s
                 ?stack: <delayed till summary>

Iteration   7: 141838.469 ops/s
                 ?stack: <delayed till summary>

Iteration   8: 205334.438 ops/s
                 ?stack: <delayed till summary>

Iteration   9: 163709.519 ops/s
                 ?stack: <delayed till summary>

Iteration  10: 162494.608 ops/s
                 ?stack: <delayed till summary>

Iteration  11: 199155.793 ops/s
                 ?stack: <delayed till summary>

Iteration  12: 209679.298 ops/s
                 ?stack: <delayed till summary>

Iteration  13: 193787.574 ops/s
                 ?stack: <delayed till summary>

Iteration  14: 127004.147 ops/s
                 ?stack: <delayed till summary>

Iteration  15: 145511.080 ops/s
                 ?stack: <delayed till summary>

Iteration  16: 223433.864 ops/s
                 ?stack: <delayed till summary>

Iteration  17: 169752.665 ops/s
                 ?stack: <delayed till summary>

Iteration  18: 165217.191 ops/s
                 ?stack: <delayed till summary>

Iteration  19: 191038.476 ops/s
                 ?stack: <delayed till summary>

Iteration  20: 196335.579 ops/s
                 ?stack: <delayed till summary>



Result "org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test":
  167433.864 ?(99.9%) 43530.883 ops/s [Average]
  (min, avg, max) = (41926.816, 167433.864, 223433.864), stdev = 50130.230
  CI (99.9%): [123902.981, 210964.748] (assumes normal distribution)

Secondary result "org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test:?stack":
Stack profiler:

....[Thread state distributions]....................................................................
 78.9%         RUNNABLE
 20.0%         TIMED_WAITING
  1.1%         WAITING

....[Thread state: RUNNABLE]........................................................................
 59.8%  75.8% org.rocksdb.RocksDB.put
 16.5%  20.9% org.rocksdb.RocksDB.get
  0.7%   0.9% java.io.UnixFileSystem.delete0
  0.7%   0.9% org.rocksdb.RocksDB.disposeInternal
  0.3%   0.4% java.lang.Long.formatUnsignedLong0
  0.1%   0.2% org.apache.hadoop.ozone.genesis.BenchMarkRocksDbStore.test
  0.1%   0.1% java.lang.Long.toUnsignedString0
  0.1%   0.1% org.apache.hadoop.ozone.genesis.generated.BenchMarkRocksDbStore_test_jmhTest.test_thrpt_jmhStub
  0.0%   0.1% java.lang.Object.clone
  0.0%   0.0% java.lang.Thread.currentThread
  0.4%   0.5% <other>

....[Thread state: TIMED_WAITING]...................................................................
 20.0% 100.0% java.lang.Object.wait

....[Thread state: WAITING].........................................................................
  1.1% 100.0% jdk.internal.misc.Unsafe.park



# Run complete. Total time: 00:00:38

Benchmark                          (backgroundThreads)  (blockSize)  (maxBackgroundFlushes)  (maxBytesForLevelBase)  (maxOpenFiles)  (maxWriteBufferNumber)  (writeBufferSize)   Mode  Cnt       Score       Error  Units
BenchMarkRocksDbStore.test                           4            8                       4                     512            5000                      16                 64  thrpt   20  167433.864 ? 43530.883  ops/s
BenchMarkRocksDbStore.test:?stack                    4            8                       4                     512            5000                      16                 64  thrpt              NaN                ---
```
