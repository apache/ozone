---
title: "Tools"
date: "2017-10-10"
summary: Ozone supports a set of tools that are handy for developers.Here is a quick list of command line tools.
menu:
   main:
      weight: 8
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

Ozone has a set of command line tools that can be used to manage ozone.

All these commands are invoked via the ```ozone``` script.

Daemon commands:

   * **scm** -  Storage Container Manager service, via daemon can be started
   or stopped.
   * **om** -   Ozone Manager, via daemon command can be started or stopped.
   * **datanode** - Via daemon command, the HDDS data nodes can be started or
   stopped.
   * **s3g** - Start the S3 compatible REST gateway
   * **recon** - The Web UI service of Ozone can be started with this command.
   
Client commands:

   * **sh** -  Primary command line interface for ozone to manage volumes/buckets/keys.
   * **fs** - Runs a command on ozone file system (similar to `hdfs dfs`)
   * **version** - Prints the version of Ozone and HDDS.


Admin commands:

   * **admin** -  Collects admin and developer related commands related to the 
   ozone components.
   * **insight** - Generic tool to display filtered log, metrics or configs to help debuging. See [the observability]({{< ref "feature/Observability.md" >}}) page for more information.
   * **classpath** - Prints the class path needed to get the hadoop jar and the
    required libraries.
   * **dtutil**    - Operations related to delegation tokens
   * **envvars** - Display computed Hadoop environment variables.
   * **getconf** -  Reads ozone config values from configuration.
   * **jmxget**  - Get JMX exported values from NameNode or DataNode.
   * **genconf** -  Generate minimally required ozone configs and output to
   ozone-site.xml.

Test tools:

   * **freon** -  Runs the ozone load generator.
   * **genesis**  - Developer Only, Ozone micro-benchmark application.

 For more information see the following subpages: