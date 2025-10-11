---
title: "Ozone Admin"
date: 2020-03-25
summary: Ozone Admin 命令可以用于所有与管理有关的任务。
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

Ozone 管理命令（`ozone admin`）是一套专为管理员使用的工具集合。

关于可用功能的简要概述：

 * `ozone admin safemode`: 您可以检查安全模式状态并强制退出/进入安全模式，`--verbose` 选项将打印评估安全模式状态的所有规则的验证状态。
 * `ozone admin container`: 容器是复制的单元。子命令可帮助调试当前容器的状态（list/get/create/...）。
 * `ozone admin pipeline`: 可帮助检查可用的管道（datanode 集合）。
 * `ozone admin datanode`: 提供有关 datanode 的信息。
 * `ozone admin printTopology`: 显示与机架感知相关的信息。
 * `ozone admin replicationmanager`: 可用于检查复制的状态（并在紧急情况下启动/停止复制）。
 * `ozone admin om`: 用于获取有关当前集群的信息的 Ozone Manager HA 相关工具。

如需更详细的使用说明，请查看 `--help` 的输出。
