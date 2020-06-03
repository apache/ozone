---
title: "工具"
date: "2017-10-10"
summary: Ozone 为开发者提供一系列方便的工具，本页给出了命令行工具的简要列表。
menu:
   main:
      weight: 8
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

Ozone 有一系列管理 Ozone 的命令行工具。

所有的命令都通过 ```ozone``` 脚本调用。

守护进程命令：

   * **scm** - 启动或停止 Storage Container Manager。
   * **om** -  启动或停止 Ozone Manager。
   * **datanode** - 启动或停止数据节点。
   * **s3g** - 启动或停止 s3 网关。

客户端命令：

   * **sh** -  操作 Ozone 中的卷、桶和键的主要命令。
   * **fs** - 运行一个文件系统命令（类似于 `hdfs dfs`）
   * **version** - 打印 Ozone 和 HDDS 的版本。


管理命令：

   * **admin** - 供管理员和开发者使用的 Ozone 管理员命令。
   * **classpath** - 打印包含 Hadoop jar 包和其它必要库的 CLASSPATH。
   * **dtutil**    - 进行和 token 有关的操作。
   * **envvars** - 列出 Hadoop 的环境变量。
   * **getconf** -  从 Ozone 配置中读取特定配置值。
   * **jmxget**  - 从 Ozone 服务中获取 JMX 导出的值。
   * **scmcli** -  SCM 的命令行接口，仅供开发者使用。
   Container Manager.
   * **genconf** -  生成 Ozone 所需的最小配置，并输出到 ozone-site.xml 中。

测试工具：

   * **freon** -  运行 Ozone 负载生成器。
   * **genesis**  - Ozone 的 benchmark 应用，仅供开发者使用。

更多信息请参见下面的子页面：