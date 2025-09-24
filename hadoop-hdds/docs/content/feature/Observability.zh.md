---
title: "可观察性"
weight: 8
menu:
main:
parent: 特性
summary: Ozone 的不同工具来提高可观察性
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

Ozone 提供了多种工具来获取有关集群当前状态的更多信息。

## Prometheus
Ozone 原生支持 Prometheus 集成。所有内部指标（由 Hadoop 指标框架收集）都发布在 `/prom` 的 HTTP 端点下。（例如，在 SCM 的 http://localhost:9876/prom）。

Prometheus 端点默认是打开的，但可以通过`hdds.prometheus.endpoint.enabled`配置变量把它关闭。

在安全环境中，该页面是用 SPNEGO 认证来保护的，但 Prometheus 不支持这种认证。为了在安全环境中启用监控，可以配置一个特定的认证令牌。

`ozone-site.xml` 配置示例：

```XML
<property>
   <name>hdds.prometheus.endpoint.token</name>
   <value>putyourtokenhere</value>
</property>
```

prometheus 配置示例：
```YAML
scrape_configs:
  - job_name: ozone
    bearer_token: <putyourtokenhere>
    metrics_path: /prom
    static_configs:
     - targets:
         - "127.0.0.1:9876" 
```

## 分布式跟踪
分布式跟踪可以通过可视化端到端的性能来帮助了解性能瓶颈。

Ozone 使用 [OpenTelemetry](https://opentelemetry.io/) API 进行跟踪，并使用 Grpc 格式的 otlp 发送跟踪信息。
jaeger 跟踪库作为收集器可以通过默认端口 4317（默认）从 Ozone 收集跟踪信息。

默认情况下，跟踪功能是关闭的，可以通过 `ozon-site.xml` 的 `hdds.tracing.enabled` 配置变量打开。

```XML
<property>
   <name>hdds.tracing.enabled</name>
   <value>true</value>
</property>
```

以下是提供收集器端点和采样策略所需的配置。这些是需要为每个 Ozone 组件（OM、SCM、DataNode）和 Ozone 客户端设置的环境变量，以启用 Shell 等跟踪功能。

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_TRACES_SAMPLER_ARG=0.01
```

此配置将记录1%的请求，以限制性能开销。

## Ozone Insight
Ozone Insight 是一个用于检查 Ozone 集群当前状态的工具，它可以显示特定组件的日志记录、指标和配置。

请使用`ozone insight list`命令检查可用的组件：

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

## 配置

`ozone insight config` 可以显示与特定组件有关的配置（只支持选定的组件）。

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

## 指标
`ozone insight metrics` 可以显示与特定组件相关的指标（只支持选定的组件）。
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

## 日志

`ozone insights logs` 可以连接到所需的服务并显示与一个特定组件相关的DEBUG/TRACE日志。例如，显示RPC消息：

```shell
>ozone insight logs om.protocol.client

[OM] 2020-07-28 12:31:49,988 [DEBUG|org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB|OzoneProtocolMessageDispatcher] OzoneProtocol ServiceList request is received
[OM] 2020-07-28 12:31:50,095 [DEBUG|org.apache.hadoop.ozone.protocolPB.OzoneManagerProtocolServerSideTranslatorPB|OzoneProtocolMessageDispatcher] OzoneProtocol CreateVolume request is received
```

使用 `-v` 标志，也可以显示 protobuf 信息的内容（TRACE级别的日志）：

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

实际上 `ozone insight` 是通过 HTTP 端点来检索所需的信息（`/conf`、`/prom`和`/logLevel`端点），它在安全环境中还不被支持。

</div>
