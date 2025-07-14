---
title: Ozone Environment Variables
weight: 10
menu:
  main:
    parent: Architecture
summary: Environment variables used by Ozone
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

Ozone relies on several environment variables to control its behavior. These variables are typically defined in `ozone-env.sh`.

| Variable | Description                                                                                                                                                                           | Default Value |
| --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --- |
| `JAVA_HOME` | The Java implementation to use. This is required on all platforms except OS X.                                                                                                        | |
| `OZONE_HOME` | Location of the Ozone installation.                                                                                                                                                   | Determined by the execution path. |
| `OZONE_CONF_DIR` | The directory containing Ozone configuration files.                                                                                                                                   | `${OZONE_HOME}/etc/hadoop` |
| `OZONE_HEAPSIZE_MAX` | The maximum heap size (Java -Xmx) for Ozone daemons.                                                                                                                                  | JVM default |
| `OZONE_HEAPSIZE` | Fallback for `OZONE_HEAPSIZE_MAX`.                                                                                                                                                    | JVM default |
| `OZONE_HEAPSIZE_MIN` | The minimum heap size (Java -Xms) for Ozone daemons.                                                                                                                                  | JVM default |
| `OZONE_OPTS` | Extra Java runtime options for all Ozone commands.                                                                                                                                    | `-Djava.net.preferIPv4Stack=true` |
| `OZONE_CLIENT_OPTS` | Extra Java runtime options for Ozone client commands (e.g., `ozone sh`), appended to `OZONE_OPTS`.                                                                  | |
| `OZONE_CLASSPATH` | An additional, custom CLASSPATH for Ozone.                                                                                                                                            | |
| `OZONE_USER_CLASSPATH_FIRST` | If set to "yes", the `OZONE_CLASSPATH` is prepended to the classpath.                                                                                                                 | `no` |
| `OZONE_USE_CLIENT_CLASSLOADER` | If set to "true", `OZONE_CLASSPATH` and `OZONE_USER_CLASSPATH_FIRST` are ignored.                                                                                                     | `false` |
| `OZONE_SSH_OPTS` | Options to pass to SSH for remote execution.                                                                                                                                          | `-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10s` |
| `OZONE_SSH_PARALLEL` | The number of simultaneous SSH connections to use.                                                                                                                                    | `10` |
| `OZONE_WORKERS` | The file containing the list of worker hosts.                                                                                                                                         | `${OZONE_CONF_DIR}/workers` |
| `OZONE_WORKER_NAMES` | An alternative to `OZONE_WORKERS` for specifying a comma-separated list of worker hosts.                                                                                              | |
| `OZONE_LOG_DIR` | The directory where log files are stored.                                                                                                                                             | `${OZONE_HOME}/logs` |
| `OZONE_IDENT_STRING` | A string to identify the Ozone instance.                                                                                                                                              | Current user |
| `OZONE_STOP_TIMEOUT` | The number of seconds to wait after stopping a daemon.                                                                                                                                | `5` |
| `OZONE_PID_DIR` | The directory where pid files are stored.                                                                                                                                             | `/tmp` |
| `OZONE_ROOT_LOGGER` | The default log4j setting for interactive commands.                                                                                                                                   | `INFO,console` |
| `OZONE_DAEMON_ROOT_LOGGER` | The default log4j setting for daemons started with the `--daemon` option.                                                                                                             | `INFO,RFA` |
| `OZONE_SECURITY_LOGGER` | The default log level and output for security-related messages.                                                                                                                       | `INFO,NullAppender` |
| `OZONE_NICENESS` | The process priority level for Ozone daemons.                                                                                                                                         | `0` |
| `OZONE_POLICYFILE` | The name of the service-level authorization file.                                                                                                                                     | `hadoop-policy.xml` |
| `JSVC_HOME` | The location of the `jsvc` implementation. Required to run secure daemons on privileged ports.                                                                                        | `/usr/bin` |
| `OZONE_SECURE_PID_DIR` | The directory for secure and privileged process pid files.                                                                                                                            | `${OZONE_PID_DIR}` |
| `OZONE_SECURE_LOG` | The directory for secure and privileged process log files.                                                                                                                            | `${OZONE_LOG_DIR}` |
| `OZONE_SECURE_IDENT_PRESERVE` | If set to "true", the `OZONE_IDENT_STRING` is not modified for secure daemons.                                                                                                        | `false` |
| `OZONE_OM_OPTS` | Extra JVM options for the Ozone Manager, appended to `OZONE_OPTS`.                                                                                                                    | |
| `OZONE_DATANODE_OPTS` | Extra JVM options for the Ozone DataNode, appended to `OZONE_OPTS`.                                                                                                                     | |
| `OZONE_SCM_OPTS` | Extra JVM options for the Storage Container Manager, appended to `OZONE_OPTS`.                                                                                                        | |
| `OZONE_DAEMON_JSVC_EXTRA_OPTS` | Extra options for the `jsvc` command.                                                                                                                                                 | |
| `OZONE_SHELL_SCRIPT_DEBUG` | If set to `true`, enables debug output for shell scripts.                                                                                                                             | `false` |
| `OZONE_SERVER_OPTS` | Extra options for the server.                                                                                                                                                         | |
| `OZONE_WORKER_SLEEP` | The number of seconds to sleep between spawning remote commands.                                                                                                                      | `0` |
| `OZONE_MANAGER_CLASSPATH` | The classpath for the Ozone Manager.                                                                                                                                                  | |
| `OZONE_DEPRECATION_WARNING` | A boolean to enable or disable deprecation warnings.                                                                                                                                  | `true` |
| `OZONE_OS_TYPE` | The operating system type.                                                                                                                                                            | Output of `uname -s` |
| `OZONE_ENABLE_BUILD_PATHS` | If set to "true", adds build paths to the classpath. For advanced users only.                                                                                                         | `false` |
| `OZONE_OM_USER` | If set, only this user is allowed to execute `ozone om` subcommands.                                                                                                                  |  |
| `OZONE_LIBEXEC_DIR` | The directory where Ozone shell scripts are located.                                                                                                                                  | `${OZONE_HOME}/libexec` |
