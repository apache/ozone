---
title: "Logs in Ozone"
date: 2023-01-30
summary: Logs in Ozone.
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

# AuditLog

AuditLogs configurations are set in "*-audit-log4j2.properties" files. We
can change the corresponding files to update the audit log policies for 
each component.

## Deletion of AuditLog

The default log appender is Rolling appender, the following configurations
can be added for deletion of out-of-date AuditLogs.

```
appender.rolling.strategy.type=DefaultRolloverStrategy
appender.rolling.strategy.delete.type=Delete
appender.rolling.strategy.delete.basePath=${sys:hadoop.log.dir}
appender.rolling.strategy.delete.maxDepth=1
appender.rolling.strategy.delete.ifFileName.type=IfFileName
appender.rolling.strategy.delete.ifFileName.glob=om-audit-*.log.gz
appender.rolling.strategy.delete.ifLastModified.type=IfLastModified
appender.rolling.strategy.delete.ifLastModified.age=30d
```

For more details, please check [Log4j2 Delete on Rollover](https://logging.apache.org/log4j/2.x/manual/appenders.html#CustomDeleteOnRollover).