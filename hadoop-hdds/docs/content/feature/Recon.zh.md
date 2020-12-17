---
title: "Recon 服务器"
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

Recon 作为 Ozone 的管理和监听控制台。它是一个可选组件，但强烈建议将其添加到集群中，因为 Recon 可以在关键时刻帮助您对集群进行故障排除。请参阅 [Recon 架构]({{< ref "concept/Recon.zh.md" >}}) 以获得详细的架构概述和 [Recon API]({{< ref path="interface/ReconApi.zh.md" >}}) 文档，以获得 HTTP API 参考。

Recon 是一个自带 HTTP 网页服务器的服务，可以通过以下命令启动。

{{< highlight bash >}}
ozone --daemon start recon
{{< /highlight >}}
