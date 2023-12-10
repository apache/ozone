---
title: "Ozone Admin"
date: 2020-03-25
summary: Ozone Admin command can be used for all the admin related tasks.
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

Ozone Admin command (`ozone admin`) is a collection of tools intended to be used only by admins.

And quick overview about the available functionalities:

 * `ozone admin safemode`: You can check the safe mode status and force to leave/enter from/to safemode,  `--verbose` option will print validation status of all rules that evaluate safemode status.
 * `ozone admin container`: Containers are the unit of the replication. The subcommands can help to debug the current state of the containers (list/get/create/...)
 * `ozone admin pipeline`: Can help to check the available pipelines (datanode sets)
 * `ozone admin datanode`: Provides information about the datanode
 * `ozone admin printTopology`: display the rack-awareness related information
 * `ozone admin replicationmanager`: Can be used to check the status of the replications (and start / stop replication in case of emergency).
 * `ozone admin om`: Ozone Manager HA related tool to get information about the current cluster.

For more detailed usage see the output of the `--help`.
