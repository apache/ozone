---
title: "Recon"
weight: 7
menu:
   main:
      parent: 特点
summary: Recon 是 Ozone 中用于分析服务的网页用户界面（Web UI）
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

Recon 是 Ozone 中用于分析服务的网页用户界面（Web UI）。它是一个可选组件，但强烈建议您使用，因为它可以增加可视性。

Recon 从 Ozone 集群中**收集**所有数据，并将其存储在 SQL数据库中，以便进一步分析。

 1. Ozone Manager 的数据是通过异步过程在后台下载的。OM 会定期创建 RocksDB 快照，并将增量数据复制到 Recon 进行处理。

 2. 数据节点不仅可以将心跳发送到 SCM，也能发送到 Recon。Recon 可以成为心跳的唯读（Read-only）监听器，并根据收到的信息更新本地数据库。

当 Recon 配置完成时，我们便可以启动服务。

{{< highlight bash >}}
ozone --daemon start recon
{{< /highlight >}}

## 需要关注的配置项

配置项 | 默认值 | 描述
-------|--------|-----
ozone.recon.http-address | 0.0.0.0:9888 | Recon web UI 监听的地址和基本端口。
ozone.recon.address | 0.0.0.0:9891 | Recon 的 RPC 地址。
ozone.recon.db.dir | none | Recon Server 存储其元数据的目录。
ozone.recon.om.db.dir | none | Recon Server 存储其 OM 快照 DB 的目录。
ozone.recon.om.snapshot.task.interval.delay | 10m | Recon 以分钟间隔请求 OM DB 快照。

