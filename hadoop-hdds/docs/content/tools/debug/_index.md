---
title: "Debug Tools"
date: "2025-07-28"
summary: Ozone Debug command can be used for all the debugging related tasks.
menu: tools
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

Ozone Debug command (`ozone debug`) is a collection of developer tools intended to help in debugging and get more information of various components of ozone.

It includes the following tools:

   * **ldb** - Tools to debug RocksDB related issues.
   * **om** - Debug commands related to OM.
   * **datanode** - Debug commands related to Datanode.
   * **replicas** - Debug commands for key replica related issues.
   * **ratis** - Debug commands related to Ratis.
   * **auditparser** - A tool to parse and query Ozone audit logs.
   * **log** - A tool to parse and provide insights on logs, currently supports only the datanode's container logs.
   * **checknative** - Checks if native libraries are loaded
   * **version** - Show internal version of Ozone components
     
For more information see the following subpages:
