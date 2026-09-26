---
title: 最小化配置Ozone服务管理
summary: 仅分发最小配置，并在启动前下载所有剩余配置
date: 2019-05-25
jira: HDDS-1467
status: 已接收
author: Márton Elek, Anu Engineer
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

#  摘要

 配置键被划分为两部分：运行时设置和环境设置。

 环境设置（主机、端口）可以在启动时下载。
 
# 链接

  * https://issues.apache.org/jira/secure/attachment/12966992/configless.pdf
