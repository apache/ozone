---
title: "Logs in Ozone"
date: 2023-01-30
summary: An overview of logging in Apache Ozone.
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

Apache Ozone produces different types of logs to help users monitor and troubleshoot the cluster. This document provides an overview of the available logs, their configuration, and how to use them for debugging.

## Service Logs

Each Ozone service (Ozone Manager, Storage Container Manager, Datanode, S3 Gateway, and Recon) generates its own log file. These logs contain detailed information about the service's operations, including errors and warnings.

By default, log files are stored in the `$OZONE_LOG_DIR` directory, which is usually set to the `logs` directory under the Ozone installation. The log file names are specific to each service, for example:

*   `ozone-om-....log` for Ozone Manager
*   `ozone-scm-....log` for Storage Container Manager
*   `ozone-datanode-....log` for Datanode

The logging behavior for each service is controlled by its `log4j.properties` file, located in the service's `$OZONE_CONF_DIR` directory, usually `etc/hadoop`. You can modify this file to change the log level, appenders, and other logging parameters.

## Audit Logs

Audit logs record security-sensitive operations, providing a trail of actions performed on the cluster. The following services produce audit logs:

*   Ozone Manager
*   Storage Container Manager
*   Datanode
*   S3 Gateway

Audit log configurations are set in `*-audit-log4j2.properties` files. You can change the corresponding files to update the audit log policies for each component.

### Sample Audit Log Entry

Here is an example of an audit log entry from the Ozone Manager:

```
INFO  | OMAudit | ? | user=hdfs | ip=127.0.0.1 | op=CREATE_VOLUME | params={volume=vol1, admin=hdfs, owner=hdfs} | result=SUCCESS
```

This entry shows that the user `hdfs` successfully created a volume named `vol1`.

### Deletion of Audit Logs

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

## Debugging

You can increase the log verbosity for debugging purposes for both services and CLI tools.

### Enabling Debug Logs for Services

To enable debug logging for a service, you need to modify its `log4j.properties` file. Change the log level for the desired logger from `INFO` to `DEBUG`. For example, to enable debug logging for the Ozone Manager, you would edit its `log4j.properties` and change the following line:

```
rootLogger.level = info
```

to:

```
rootLogger.level = debug
```

After saving the file and restarting the service, the service will start logging more detailed debug information.

### Enabling Debug Logs for CLI Tools

To enable debug logging for Ozone CLI tools (e.g., `ozone sh volume create`), you can set the `OZONE_ROOT_LOGGER` environment variable to `debug`:

```bash
export OZONE_ROOT_LOGGER=DEBUG,console
ozone sh volume create /vol1
```

Alternatively, you can use the `--loglevel` option with the `ozone` command:

```bash
ozone --loglevel debug sh volume create /vol1
```
