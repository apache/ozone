---
title: "Ozone 中的 Recon"
weight: 7
menu:
   main:
      parent: GDPR
summary: Ozone 中的 Recon 是 Ozone 的网络产品界面设计（Web UI）和分析服務
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

Recon 是 Ozone 的网络产品界面设计（Web UI）和分析服务。它是一个可选组件，但强烈建议您使用，因为它可以增加可见性。

Recon 从 Ozone 集群中**收集**所有数据，并将它们存储在一个 SQL 数据库中为了进一步分析。

1.Ozone Manager data 是通过异步过程在后台下载的。 它会在 OM 端定期创建 RocksDB 快照，并将增量数据复制到 Recon 并进行处理。

2.Datanodes 不仅可以将心跳发送到 SCM，还可以发送到 Recon。Recon 可以成为一个心跳的只读侦听器，并根据收到的信息更新本地数据库。

一旦配置了 Recon，我们就可以准备启动服务。

{{< highlight bash >}}
ozone --daemon start recon
{{< /highlight >}}

## 值得注意的配置

配置项 | 默认值 | 描述
-------|-------|------
ozone.recon.http-address | 0.0.0.0:9888 | Recon web UI 侦听的地址和基本端口。
ozone.recon.address | 0.0.0.0:9891 | Recon 的 RPC 地址。
ozone.recon.db.dir | none | Recon Server 存储其元数据的目录。
ozone.recon.om.db.dir | none | Recon Server 存储其 OM snapshot DB 的目录。
ozone.recon.om.snapshot.task.interval.delay | 10m | Recon 以 MINUTES 间隔请求 OM DB Snapshot。



