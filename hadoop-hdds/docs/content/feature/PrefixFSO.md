---
title: "Prefix based FileSystem Optimization"
weight: 2
menu:
   main:
      parent: Features
summary: Supports atomic rename and delete operation.
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

The prefix based FileSystem optimization feature supports atomic rename and
 delete of any directory at any level in the namespace. Also, it will perform
  rename and delete of any directory in a deterministic/constant time.


## Configuration
By default the feature is disabled. It can be enabled with the following
 settings in `ozone-site.xml`:

```XML
<property>
   <name>ozone.om.enable.filesystem.paths</name>
   <value>true</value>
</property>
<property>
   <name>ozone.om.metadata.layout</name>
   <value>prefix</value>
</property>
```