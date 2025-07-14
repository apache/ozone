---
title: "Container Replica Debugger Tool"
date: 2025-06-09
jira: HDDS-12579
summary: 该工具处理来自 Ozone 数据节点的容器日志文件，以跟踪状态转换。它提供 CLI 命令，用于根据不同的属性查询容器。
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
# Container Replica Debugger Tool

## 背景
容器是 Ozone 最重要的组成部分。大多数 Ozone 操作都发生在容器上。Ozone 中的容器在其生命周期中会经历不同的状态。
过去，我们曾发现过各种与容器状态相关的问题。为了调试问题，我们通常采用手动步骤并识别与容器相关的问题，这非常耗时。为了优化可调试性，我们可以显示与容器相关的历史时间轴和其他信息。

## 解决方案
一个离线工具，可以分析 dn-container 日志文件并帮助我们找到与集群中数据节点上的容器相关的问题。

### 组件 1：解析器
该组件负责处理日志文件，提取相关日志条目，并批量存储插入到数据库中。

**命令如下：**

`ozone debug log container --db=<path to db> parse --path=<path to logs folder> --thread-count=<n>`

#### 日志文件解析和验证
- **目录遍历**: 它以递归方式扫描指定目录中的容器日志文件，仅考虑名称与模式 `dn-container-<roll>.log.<datanodeId>` 匹配的文件。使用多个线程同时解析日志文件以实现高效处理。
- **逐行处理**: 每个日志行使用管道分隔符进行拆分，并解析为键值组件。
- **字段提取**: 关键字段包括：
  - **Timestamp**
  - **logLevel**: INFO, WARN, ERROR.
  - **ID, BCSID, State, 和 Index**: 从键值对中提取。
- **错误信息捕获**: 日志行中任何剩余的非结构化部分都存储为 `errorMessage`，尤其与 WARN 或 ERROR 级别的日志相关。
- **副本索引过滤**: 仅处理索引 = 0 的日志条目。这将当前处理限制在 Ratis 复制的容器中。未来的改进可能会支持 EC 复制。

### 组件 2：数据库
该工具使用临时 SQLite 数据库来存储和管理从容器日志中提取的信息。这有助于组织数据，以便轻松分析数据节点上每个容器副本的完整历史记录和最新状态。

#### 该工具创建/维护了两个主要表：
1. **DatanodeContainerLogTable — 详细日志历史记录**  
   该表存储了每个数据节点上每个容器副本的完整状态变化历史记录。
  - Container ID
  - Datanode ID
  - State (例如 OPEN, CLOSED, 等等)
  - BCSID (Block Commit Sequence ID)
  - Timestamp
  - Log Level
  - Error Message (如果有的话)
  - Index Value

该表中的数据展示了状态转换和 BCSID 变化的完整时间线，有助于追踪每个容器副本随时间的变化情况。

2. **ContainerLogTable — 最新状态摘要**  
   这张表仅包含每个唯一的容器与数据节点对的最新状态和 BCSID。
  - Container ID
  - Datanode ID
  - Latest State
  - Latest BCSID

该表提供了所有容器副本当前 state 和 BCSID 的简化视图，有助于快速进行状态检查和摘要。

### **组件 3：CLI 命令**

#### **列出具有重复打开状态的容器**
这个 ozone debug CLI 命令通过跟踪前三个 “OPEN” 状态并标记任何后续的 “OPEN” 状态（如果它出现在相同的数据节点上或在初始事件之后出现在不同的数据节点上），帮助识别被打开次数超过所需数量（对于 Ratis 来说是 3 次）的容器。

该命令显示每个列出容器的 **Container ID** 以及其处于 'OPEN' 状态的副本数量。同时还提供具有重复 'OPEN' 状态条目的容器总数。

**命令如下：**

`ozone debug log container --db=<path to db> duplicate-open`

**示例输出：**

```
Container ID: 2187256 - OPEN state count: 4
.
.
.
Container ID: 12377064 - OPEN state count: 5
Container ID: 12377223 - OPEN state count: 5
Container ID: 12377631 - OPEN state count: 4
Container ID: 12377904 - OPEN state count: 5
Container ID: 12378161 - OPEN state count: 4
Container ID: 12378352 - OPEN state count: 5
Container ID: 12378789 - OPEN state count: 5
Container ID: 12379337 - OPEN state count: 5
Container ID: 12379489 - OPEN state count: 5
Container ID: 12380526 - OPEN state count: 5
Container ID: 12380898 - OPEN state count: 5
Container ID: 12642718 - OPEN state count: 4
Container ID: 12644806 - OPEN state count: 4
Total containers that might have duplicate OPEN state : 1579
```
<br>

#### **单个容器的详细信息及分析显示**

此 ozone debug CLI 命令提供了容器的每个副本（其 ID 通过命令提供）的完整状态转换历史记录。
该命令还提供对容器的分析，例如容器是否存在以下问题：

- Duplicate OPEN states
- Mismatched replication
- Under-replication 或者 over-replication
- Unhealthy replicas
- Open-unhealthy
- Quasi-closed stuck containers

该命令提供关键详细信息，例如：

- Datanode id
- Container id
- BCSID
- TimeStamp
- Index Value
- Message (如果有与该特定副本相关联的任何内容)

**命令如下：**

`ozone debug log container --db=<path to database> info <containerID>`

**示例输出：**

```
Timestamp               | Container ID | Datanode ID | Container State  | BCSID   | Message             | Index Value
----------------------------------------------------------------------------------------------------------------------
2024-06-04 15:07:55,390 | 700          | 100         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-06-04 14:50:18,177 | 700          | 150         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-04-04 10:32:29,026 | 700          | 250         | OPEN             | 0      | No error             | 0           
2024-06-04 14:44:28,126 | 700          | 250         | CLOSING          | 353807 | No error             | 0           
2024-06-04 14:47:59,893 | 700          | 250         | QUASI_CLOSED     | 353807 | Ratis group removed  | 0           
2024-06-04 14:50:17,038 | 700          | 250         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-06-04 14:50:18,184 | 700          | 250         | QUASI_CLOSED     | 353807 | No error             | 0           
2024-04-04 10:32:29,026 | 700          | 400         | OPEN             | 0      | No error             | 0           
Container 700 might be QUASI_CLOSED_STUCK.
```
<br>

#### **按健康状态列出容器**

此 ozone debug CLI 命令列出所有处于 UNDER-REPLICATED、OVER-REPLICATED、UNHEALTHY 或 QUASI_CLOSED stuck 状态的容器。

该命令显示每个列出容器的 **Container ID** 以及指定健康状态下副本的数量。

**使用的命令选项是：**

- 按健康类型列出容器：默认情况下，结果中仅提供 100 行

`ozone debug log container --db=<path to db> list --health=<type>`

- 按健康类型列出具有指定行限制的容器：

`ozone debug log container --db=<path to db> list --health=<type>  --length=<limit>`

- 按健康类型列出所有容器，覆盖行限制：

`ozone debug log container --db=<path to db> list --health=<type>  --all`

**示例输出：**

`ozone debug log container --db=path/to/db list --health=UNHEALTHY --all`

```
Container ID = 6002 - Count = 1
Container ID = 6201 - Count = 1
.
.
.
Container ID = 136662 - Count = 3
Container ID = 136837 - Count = 3
Container ID = 199954 - Count = 3
Container ID = 237747 - Count = 3
Container ID = 2579099 - Count = 1
Container ID = 2626888 - Count = 1
Container ID = 2627711 - Count = 1
Container ID = 2627751 - Count = 2
Number of containers listed: 25085
```
<br>

#### **根据最终状态列出容器**

此 ozone debug CLI 命令帮助列出所有容器的副本，这些容器的最新状态是通过 CLI 命令提供的状态。

该命令提供关键详细信息，例如：

- Datanode id
- Container id
- BCSID
- TimeStamp - 与容器副本状态关联的最新时间戳
- Index Value
- Message (如果有与该特定副本相关联的任何内容)

**使用的命令选项是：**

- 列出 容器 按 状态 默认情况下，结果中仅提供 100 行

`ozone debug log container --db=<path to db> list --lifecycle=<state>`
- 按状态列出具有指定行限制的容器：

`ozone debug log container --db=<path to db> list --lifecycle=<state> --length=<limit>`
- 按状态列出所有容器，覆盖行限制：

`ozone debug log container --db=<path to db> list --lifecycle=<state> --all`

**示例输出：**

`ozone debug log container --db=path/to/db list --lifecycle=CLOSED --all`

```
Timestamp                 | Datanode ID | Container ID | BCSID  | Message        | Index Value
---------------------------------------------------------------------------------------------------
2024-07-23 12:02:12,981   | 360         | 1            | 75654  | No error       | 0
2024-07-23 11:56:21,106   | 365         | 1            | 75654  | Volume failure | 0
2024-07-23 11:56:21,106   | 365         | 1            | 75654  | Volume failure | 0
2024-08-29 14:11:32,879   | 415         | 1            | 30     | No error       | 0
2024-08-29 14:11:17,533   | 430         | 1            | 30     | No error       | 0
2024-06-20 11:50:09,496   | 460         | 1            | 75654  | No error       | 0
2024-07-23 12:02:11,633   | 500         | 1            | 75654  | No error       | 0
2024-06-20 12:03:24,230   | 505         | 2            | 83751  | No error       | 0
2024-07-10 04:00:33,131   | 540         | 2            | 83751  | No error       | 0
2024-07-10 04:00:46,825   | 595         | 2            | 83751  | No error       | 0
```
<br>

### 笔记 :

- 此工具假定所有 dn-container 日志文件已从数据节点中提取并放入目录中。
- 对于解析命令，如果没有提供DB路径，则会在当前工作目录中创建一个新的数据库，默认文件名为 `container_datanode.db`。
- 对于所有其他命令，如果未提供数据库路径，它将在当前目录中查找默认数据库文件（`container_datanode.db`）。如果该文件不存在，它将抛出一个错误，要求提供有效的路径。

