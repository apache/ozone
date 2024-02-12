---
title: "Container Balancer overview"
weight: 10
menu:
   main:
      parent: Features
summary: Container Balancer overview.
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

## Container Balancer overview

Container balancer is a service in Storage Container Manager that balances the utilization of datanodes in an Ozone cluster.

A cluster is considered balanced if for each datanode, the utilization of the datanode (used space to capacity ratio) differs from the utilization of the cluster (used space to capacity ratio of the entire cluster) no more than the threshold. This service balances the cluster by moving containers among over-utilized and under-utilized datanodes.

```XML
### Note
Container balancer has a command line interface for administrators. You can run the ```shell ozone admin containerbalancer -h``` help command for commands related to Container Balancer.
Container balancer supports only closed Ratis containers. Erasure Coded containers are not supported yet.
```
## Container balancer CLI commands({{< ref "feature/ContainerBalancer-cli.md" >}})
