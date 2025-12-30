---
title: "Configuring Audit Logs"
date: 2025-12-30
menu:
  main:
    parent: "Administrator Guide"
summary: "Configure and manage audit logs in Apache Ozone for security-sensitive operations."
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

Audit logs record security-sensitive operations, providing a trail of actions performed on the cluster. The following services produce audit logs:

*   Ozone Manager
*   Storage Container Manager
*   Datanode
*   S3 Gateway

Audit log configurations are set in `*-audit-log4j2.properties` files. You can change the corresponding files to update the audit log policies for each component.

## Sample Audit Log Entry

Here is an example of an audit log entry from the Ozone Manager:

```
INFO  | OMAudit | ? | user=hdfs | ip=127.0.0.1 | op=CREATE_VOLUME | params={volume=vol1, admin=hdfs, owner=hdfs} | result=SUCCESS
```

This entry shows that the user `hdfs` successfully created a volume named `vol1`.

## Deletion of Audit Logs

The default log appender is a rolling appender. The following configurations can be added for the deletion of out-of-date AuditLogs.

```
appender.rolling.strategy.type=DefaultRolloverStrategy
appender.rolling.strategy.max=3000
appender.rolling.strategy.delete.type=Delete
appender.rolling.strategy.delete.basePath=${sys:hadoop.log.dir}
appender.rolling.strategy.delete.maxDepth=1
appender.rolling.strategy.delete.ifFileName.type=IfFileName
appender.rolling.strategy.delete.ifFileName.glob=om-audit-*.log.gz
appender.rolling.strategy.delete.ifLastModified.type=IfLastModified
appender.rolling.strategy.delete.ifLastModified.age=30d
```

For more details, please check [Log4j2 Delete on Rollover](https://logging.apache.org/log4j/2.x/manual/appenders.html#CustomDeleteOnRollover).

