---
title: Third-Party Integrations
weight: 4
menu:
   main:
      parent: "Application Integrations"
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

Here is a non-exhaustive list of third-party tools and applications that can be integrated with Ozone:

These integrations are not maintained by the Apache Ozone community. If you have any questions or issues, please contact the respective tool or application maintainers.

## Open Source

* Apache NiFi

Apache NiFi adds support for Ozone in version 1.13.0.

Because Ozone file system is a Hadoop-compatible file system (HCFS), You can use the PutHDFS or the PutCDPObjectStore processor to move your data to Ozone.

For more information, see the [Pushing data to Ozone using Apache NiFi](https://docs.cloudera.com/cfm/4.0.0/nifi-ozone/topics/cfm-nifi-ozone-move-data.html) documentation.
* Apache Flink

Apache Flink can access Ozone as a Hadoop compatible file system.

Because Ozone does not yet support truncate() API, to use Ozone ofs as a DataStream connector, you must select OnCheckpointRollingPolicy. For example:

```java
  final StreamingFileSink<String> sink = StreamingFileSink
      .forRowFormat(new Path("ofs:///ozone1/vol1/bucket1/flink"), new SimpleStringEncoder<String>("UTF-8"))
      .withRollingPolicy(OnCheckpointRollingPolicy.build()) // Roll file on checkpoint
      .build();
```

For more information, see the [File Systems](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/overview/) documentation.

* PyArrow https://hackmd.io/@cxorm/cxorm
* Cloudera Hue can access Ozone through Ozone HTTPFS interface https://gethue.com/blog/discover-the-power-of-apache-ozone-using-the-hue-file-browser/
* Alluxio https://docs.alluxio.io/os/user/stable/en/ufs/Ozone.html

## Commercial
* Teradata https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20
* Starburst https://docs.starburst.io/latest/connector/starburst-ozone.html

## Via Ozone S3 Gateway
Various applications can access Ozone via the S3 Gateway, which opens up Ozone for more use cases.
For example, send Fluentd logs to Ozone S3 Gateway.