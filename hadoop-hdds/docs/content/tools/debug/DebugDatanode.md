---
title: "Debug Datanode"
date: 2025-07-28
summary: Debug commands related to datanode.
menu: debug
weight: 3
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

Debug commands related to datanode. Currently, only container replica related commands are available.
Following is the usage and the subcommands available under the `ozone debug datanode container` command:

```bash
Usage: ozone debug datanode container [-hV] [--verbose] [COMMAND]
Container replica specific operations to be executed on datanodes only
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
Commands:
  list     Show container info of all container replicas on datanode
  info     Show container info of a container replica on datanode
  export   Export one container to a tarball
  inspect  Check the metadata of all container replicas on this datanode.
```
