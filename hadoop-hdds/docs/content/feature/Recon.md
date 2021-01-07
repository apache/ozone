---
title: "Recon Server"
weight: 7
menu:
   main:
      parent: Features
summary: Recon is the Web UI and analysis service for Ozone
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

Recon serves as a management and monitoring console for Ozone. 
It's an optional component, but it is strongly recommended to add it to the cluster
since Recon can help with troubleshooting the cluster at critical times. 
Refer to [Recon Architecture]({{< ref "concept/Recon.md" >}}) for detailed architecture overview and 
[Recon API]({{< ref "interface/ReconApi.md" >}}) documentation
for HTTP API reference.

Recon is a service that brings its own HTTP web server and can be started by
the following command.

{{< highlight bash >}}
ozone --daemon start recon
{{< /highlight >}}



