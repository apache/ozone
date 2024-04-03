---
title: "Reconfigurability"
weight: 11
menu:
   main:
      parent: Features
summary: Dynamic reloading configuration.
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

Ozone supports dynamic loading of certain properties without restarting the service. 
If a property is reconfigurable, you can modify it in the configuration file (`ozone-site.xml`) and then invoke the command to flush it to memory.

command:
```shell
ozone admin reconfig --service=[OM|SCM|DATANODE] --address=<ip:port> start|status|properties
```

The meaning of command options:
- **--service**: The node type of the server specified with --address
- **--address**: RPC address for one server
- Three operations are provided:
    - **start**:      Execute the reconfig operation asynchronously
    - **status**:     Check reconfig status
    - **properties**: List reconfigurable properties

## Retrieve the reconfigurable properties list
To retrieve all the reconfigurable properties list for a specific component in Ozone,
you can use the command: `ozone admin reconfig --service=[OM|SCM|DATANODE] --address=<ip:port> properties`.
This command will list all the properties that can be dynamically reconfigured at runtime for specific component.<br>

> For example, get the Ozone OM reconfigurable properties list.
>
>$ `ozone admin reconfig --service=OM --address=hadoop1:9862 properties`<br>
OM: Node [hadoop1:9862] Reconfigurable properties:<br>
ozone.administrators

## OM Reconfigurability
>For example, modify `ozone.administrators` in ozone-site.xml and execute:
>
> $ `ozone admin reconfig --service=OM --address=hadoop1:9862 start`<br>
OM: Started OM reconfiguration task on node [hadoop1:9862].
>
>$ `ozone admin reconfig --service=OM --address=hadoop1:9862 status`<br>
OM: Reconfiguring status for node [hadoop1:9862]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.<br>
SUCCESS: Changed property ozone.administrators<br>
From: "hadoop"<br>
To: "hadoop,bigdata"
>
> $ `ozone admin reconfig --service=OM -address=hadoop1:9862 properties`<br>
OM: Node [hadoop1:9862] Reconfigurable properties:<br>
ozone.administrators

## SCM Reconfigurability
>For example, modify `ozone.administrators` in ozone-site.xml and execute:
>
> $ `ozone admin reconfig --service=SCM --address=hadoop1:9860 start`<br>
SCM: Started OM reconfiguration task on node [hadoop1:9860].
>
>$ `ozone admin reconfig --service=SCM --address=hadoop1:9860 status`<br>
SCM: Reconfiguring status for node [hadoop1:9860]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.<br>
SUCCESS: Changed property ozone.administrators<br>
From: "hadoop"<br>
To: "hadoop,bigdata"
>
> $ `ozone admin reconfig --service=SCM -address=hadoop1:9860 properties`<br>
SCM: Node [hadoop1:9860] Reconfigurable properties:<br>
ozone.administrators

## Datanode Reconfigurability
>For example, modify `ozone.example.config` in ozone-site.xml and execute:
>
> $ `ozone admin reconfig --service=DATANODE --address=hadoop1:19864 start`<br>
Datanode: Started reconfiguration task on node [hadoop1:19864].
>
>$ `ozone admin reconfig --service=DATANODE --address=hadoop1:19864 status`<br>
Datanode: Reconfiguring status for node [hadoop1:19864]: started at Wed Dec 28 19:04:44 CST 2022 and finished at Wed Dec 28 19:04:44 CST 2022.<br>
SUCCESS: Changed property ozone.example.config<br>
From: "old"<br>
To: "new"
>
> $ `ozone admin reconfig --service=DATANODE -address=hadoop1:19864 properties`<br>
Datanode: Node [hadoop1:19864] Reconfigurable properties:<br>
ozone.example.config

### Batch operation
If you want to perform a batch operations on the Datanode, you can set the `--in-service-datanodes` flag.
This will send reconfiguration requests to all available DataNodes in the `IN_SERVICE`operational state.<br>
Currently, only Datanode supports batch operations<br>


>For example, to list the reconfigurable properties of all Datanodes:<br>
> $ `ozone admin reconfig --service=DATANODE --in-service-datanodes properties`<br>
Datanode: Node [hadoop1:19864] Reconfigurable properties:<br>
ozone.example.config<br>
Datanode: Node [hadoop2:19864] Reconfigurable properties:<br>
ozone.example.config<br>
Datanode: Node [hadoop3:19864] Reconfigurable properties:<br>
ozone.example.config<br>
Reconfig successfully 3 nodes, failure 0 nodes.<br>
