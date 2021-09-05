---
title: "可观察性"
weight: 8
menu:
   main:
      parent: 特性
summary: 用于增强 Ozone 可观察性的不同工具
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

Ozone 为获取集群运行状态的详细信息提供了多种工具。

## Prometheus

Ozone 原生支持 Prometheus 集成。所有内部指标（通过 Hadoop 指标框架收集的指标）都发布在 `/prom` HTTP 端点下（比如 SCM 的指标发布在 http://localhost:9876/prom 下）。

Prometheus 端点默认开启，但可以通过 `hdds.prometheus.endpoint.enabled` 配置参数关闭。

在页面开启了 SPNEGO 认证的安全环境中，Prometheus 无法直接访问，需要配置特定的认证 token 才可以进行监控。

 `ozone-site.xml` 示例：	

```XML
<property>
   <name>hdds.prometheus.endpoint.token</name>
   <value>putyourtokenhere</value>
</property>
```

Prometheus 配置示例:

```YAML
scrape_configs:
  - job_name: ozone
    bearer_token: <putyourtokenhere>
    metrics_path: /prom
    static_configs:
     - targets:
         - "127.0.0.1:9876" 
```

## 分布式追踪

分布式追踪可以通过可视化端到端性能来辅助定位性能瓶颈。

Ozone 使用 [jaeger](https://jaegertracing.io) 追踪库来收集追踪数据并发送到任何兼容的后端（Zipkin等）。

追踪默认是关闭的，但可以通过 `ozone-site.xml` 中的 `hdds.tracing.enabled` 参数开启：

```XML
<property>
   <name>hdds.tracing.enabled</name>
   <value>true</value>
</property>
```

根据[此处文档](https://github.com/jaegertracing/jaeger-client-java/blob/master/jaeger-core/README.md)，Jaeger 客户端可以通过环境变量进行配置，例如：

```shell
JAEGER_SAMPLER_PARAM=0.01
JAEGER_SAMPLER_TYPE=probabilistic
JAEGER_AGENT_HOST=jaeger
```

此配置会记录 1% 的请求来限制性能开销。更多有关 jaeger 采样的信息[请查阅文档](https://www.jaegertracing.io/docs/1.18/sampling/#client-sampling-configuration)

## Ozone insight

Ozone insight 是查看 Ozone 集群当前状态的万能工具。它可以展示特定组件的日志、指标和配置。

使用 `ozone insight list` 命令查看可选组件：

```shell
> ozone insight list

Available insight points:

  scm.node-manager                     SCM Datanode management related information.
  scm.replica-manager                  SCM closed container replication manager
  scm.event-queue                      Information about the internal async event delivery
  scm.protocol.block-location          SCM Block location protocol endpoint
  scm.protocol.container-location      SCM Container location protocol endpoint
  scm.protocol.security                SCM Block location protocol endpoint
  om.key-manager                       OM Key Manager
  om.protocol.client                   Ozone Manager RPC endpoint
  datanode.pipeline                    More information about one ratis datanode ring.
```

### 配置

`ozone insight config` 命令可以展示特定组件的相关配置（只支持某些组件）。

```shell
> ozone insight config scm.replica-manager

Configuration for `scm.replica-manager` (SCM closed container replication manager)

>>> hdds.scm.replication.thread.interval
       default: 300s
       current: 300s

There is a replication monitor thread running inside SCM which takes care of replicating the containers in the cluster. This property is used to configure the interval in which that thread runs.


>>> hdds.scm.replication.event.timeout
       default: 30m
       current: 30m

Timeout for the container replication/deletion commands sent  to datanodes. After this timeout the command will be retried.

```

### 指标

`ozone insight metrics` 命令可以展示特定组件的相关指标（只支持某些组件）。


```shell
> ozone insight metrics scm.protocol.block-location
Metrics for `scm.protocol.block-location` (SCM Block location protocol endpoint)

RPC connections

  Open connections: 0
  Dropped connections: 0
  Received bytes: 1267
  Sent bytes: 2420


RPC queue

  RPC average queue time: 0.0
  RPC call queue length: 0


RPC performance

  RPC processing time average: 0.0
  Number of slow calls: 0


Message type counters

  Number of AllocateScmBlock: ???
  Number of DeleteScmKeyBlocks: ???
  Number of GetScmInfo: ???
  Number of SortDatanodes: ???
```

### 日志

`ozone insight logs` 命令可以连接必要的服务并展示特定组件的 DEBUG/TRACE 日志。比如展示 RPC 消息：

```shell
>ozone insight logs om.protocol.client

[OM] 2020-07-28 12:31:49,988 [DEBUG|org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB|OzoneProtocolMessageDispatcher] OzoneProtocol ServiceList request is received
[OM] 2020-07-28 12:31:50,095 [DEBUG|org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB|OzoneProtocolMessageDispatcher] OzoneProtocol CreateVolume request is received
```

使用 `-v` 标志位可以将 protobuf 消息的内容也展示出来(TRACE 级别日志）：

```shell
ozone insight logs -v om.protocol.client

[OM] 2020-07-28 12:33:28,463 [TRACE|org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB|OzoneProtocolMessageDispatcher] [service=OzoneProtocol] [type=CreateVolume] request is received:
cmdType: CreateVolume
traceID: ""
clientId: "client-A31DF5C6ECF2"
createVolumeRequest {
  volumeInfo {
    adminName: "hadoop"
    ownerName: "hadoop"
    volume: "vol1"
    quotaInBytes: 1152921504606846976
    volumeAcls {
      type: USER
      name: "hadoop"
      rights: "200"
      aclScope: ACCESS
    }
    volumeAcls {
      type: GROUP
      name: "users"
      rights: "200"
      aclScope: ACCESS
    }
    creationTime: 1595939608460
    objectID: 0
    updateID: 0
    modificationTime: 0
  }
}

[OM] 2020-07-28 12:33:28,474 [TRACE|org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB|OzoneProtocolMessageDispatcher] [service=OzoneProtocol] [type=CreateVolume] request is processed. Response:
cmdType: CreateVolume
traceID: ""
success: false
message: "Volume already exists"
status: VOLUME_ALREADY_EXISTS
```

<div class="alert alert-warning" role="alert">

`ozone insight` 命令在底层使用 HTTP 端点来获取所需的信息（`/conf`、`/prom` 和 `/logLevel` 等端点），所以在安全环境中暂不支持。

</div>
