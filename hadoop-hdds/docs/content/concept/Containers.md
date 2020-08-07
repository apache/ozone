---
title: Containers
weight: 5
menu: 
  main:
     parent: Architecture
summary: Description of the Containers, the replication unit of Ozone.
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

Containers are the fundamental replication unit of Ozone/HDDS, they are managed by the Storage Container Manager (SCM) service.

Containers are big binary units (5Gb by default) which can contain multiple blocks:

![Containers](Containers.png)

Blocks are local information and not managed by SCM. Therefore even if billions of small files are created in the system (which means billions of blocks are created), only of the status of the containers will be reported by the Datanodes and containers will be replicated.
 
When Ozone Manager requests a new Block allocation from the SCM, SCM will identify the suitable container and generate a block id which contains `ContainerId` + `LocalId`. Client will connect to the Datanode which stores the Container, and datanode can manage the separated block based on the `LocalId`.

## Open vs. Closed containers

When a container is created it starts in an OPEN state. When it's full (~5GB data is written), container will be closed and becomes a CLOSED container.

The fundamental differences between OPEN and CLOSED containers:

OPEN | CLOSED
-----------------------------------|-----------------------------------------
mutable | immutable
replicated with RAFT (Ratis) | Replicated with async container copy
Raft leader is used to READ / WRITE | All the nodes can be used to READ
