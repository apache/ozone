---
title: "动态加载配置"
weight: 8
summary: 动态加载配置
menu:
   main:
      parent: 特性
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

Ozone支持在不重启服务的情况下动态加载某些配置。如果某个属性支持热加载，就可以先在配置文件（`ozone-site.xml`）中修改它然后调用命令使之刷新到内存。

命令：
```shell
ozone admin reconfig --service=[OM|SCM|DATANODE] --address=<ip:port> start|status|properties
```

命令项的含义:
- **--service**: --address 指定节点的Ozone服务类型
- **--address**: 一台服务所在的主机与客户端通信的RPC地址
- 提供3中操作:
    - **start**:      开始异步执行动态加载配置
    - **status**:     查看最近一次动态加载的状态
    - **properties**: 列出支持动态加载的配置项

## 获取可动态加载的属性列表
要获取 Ozone 中指定组件的可动态加载属性列表, 可以使用命令 `ozone admin reconfig --service=[OM|SCM|DATANODE] --address=<ip:port> properties`。
这个命令将会列出所有可以在运行时动态加载的属性。

> 例如, 获取 Ozone OM 可动态加载属性列表
>
>$ `ozone admin reconfig --service=OM --address=hadoop1:9862 properties`<br>
OM: Node [hadoop1:9862] Reconfigurable properties:<br>
ozone.administrators

## OM动态配置
>例如, 在`ozone-site.xml`文件中修改`ozone.administrators`的值并执行:
>
> $ `ozone admin reconfig --service=OM --address=hadoop1:9862 start`<br>
OM: Started reconfiguration task on node [hadoop1:9862].
>
>$ `ozone admin reconfig --service=OM --address=hadoop1:9862 status`<br>
OM: Reconfiguring status for node [hadoop1:9862]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.<br>
SUCCESS: Changed property ozone.administrators<br>
From: "hadoop"<br>
To: "hadoop,bigdata"
>
> $ `ozone admin reconfig --service=OM --address=hadoop1:9862 properties`<br>
OM: Node [hadoop1:9862] Reconfigurable properties:<br>
ozone.administrators

## SCM动态配置
>例如, 在`ozone-site.xml`文件中修改`ozone.administrators`的值并执行:
>
> $ `ozone admin reconfig --service=SCM --address=hadoop1:9860 start`<br>
SCM: Started reconfiguration task on node [hadoop1:9860].
>
>$ `ozone admin reconfig --service=SCM --address=hadoop1:9860 status`<br>
SCM: Reconfiguring status for node [hadoop1:9860]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.<br>
SUCCESS: Changed property ozone.administrators<br>
From: "hadoop"<br>
To: "hadoop,bigdata"
>
> $ `ozone admin reconfig --service=SCM --address=hadoop1:9860 properties`<br>
SCM: Node [hadoop1:9860] Reconfigurable properties:<br>
ozone.administrators


## Datanode 动态配置
>例如, 在`ozone-site.xml`文件中修改`hdds.datanode.block.deleting.limit.per.interval`的值并执行:
>
> $ `ozone admin reconfig --service=DATANODE --address=hadoop1:19864 start`<br>
Datanode: Started reconfiguration task on node [hadoop1:19864].
>
>$ `ozone admin reconfig --service=DATANODE --address=hadoop1:19864 status`<br>
Datanode: Reconfiguring status for node [hadoop1:19864]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.<br>
SUCCESS: Changed property hdds.datanode.block.deleting.limit.per.interval<br>
From: "old"<br>
To: "new"
>
> $ `ozone admin reconfig --service=DATANODE --address=hadoop1:19864 properties`<br>
Datanode: Node [hadoop1:19864] Reconfigurable properties:<br>
hdds.datanode.block.deleting.limit.per.interval


### 批量操作
如果要对 Datanode 执行批操作，你可以设置 `--in-service-datanodes` 标志.
这将向所有 operational state 为 “IN_SERVICE” 的可用 Datanode 发送 reconfig 请求。<br>
目前只有 Datanode 支持批量操作


>例如, 列出 Datanode 所有可配置的属性:<br>
> $ `ozone admin reconfig --service=DATANODE --in-service-datanodes properties`<br>
Datanode: Node [hadoop1:19864] Reconfigurable properties:<br>
hdds.datanode.block.deleting.limit.per.interval<br>
Datanode: Node [hadoop2:19864] Reconfigurable properties:<br>
hdds.datanode.block.deleting.limit.per.interval<br>
Datanode: Node [hadoop3:19864] Reconfigurable properties:<br>
hdds.datanode.block.deleting.limit.per.interval<br>
Reconfig successfully 3 nodes, failure 0 nodes.<br>