<!--
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

This helper project provides a reduced view to the Hadoop dependencies. Hadoop common by default depends on a lot of other projects
which are not required for the parts used in Ozone.

The exclude rules should be updated on each version bump. If Hadoop introduce new dependencies we need to exclude them (unless they are required).


For the reference here are the minimal jar files wich are required by Ozone:


```
mvn dependency:tree 

[INFO] Scanning for projects...
[INFO] 
[INFO] -------< org.apache.hadoop:hadoop-hdds-hadoop-dependency-client >-------
[INFO] Building Apache Hadoop HDDS Hadoop Client dependencies 1.0.0
[INFO] --------------------------------[ jar ]---------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:3.0.2:tree (default-cli) @ hadoop-hdds-hadoop-dependency-client ---
[INFO] org.apache.hadoop:hadoop-hdds-hadoop-dependency-client:jar:1.0.0
[INFO] +- org.apache.hadoop:hadoop-annotations:jar:3.2.1:compile
[INFO] |  \- jdk.tools:jdk.tools:jar:1.8:system
[INFO] +- org.apache.hadoop:hadoop-common:jar:3.2.1:compile
[INFO] |  +- org.apache.httpcomponents:httpclient:jar:4.5.2:compile
[INFO] |  |  \- org.apache.httpcomponents:httpcore:jar:4.4.4:compile
[INFO] |  +- org.apache.commons:commons-configuration2:jar:2.1.1:compile
[INFO] |  +- com.google.re2j:re2j:jar:1.1:compile
[INFO] |  +- com.google.protobuf:protobuf-java:jar:2.5.0:compile
[INFO] |  +- org.apache.hadoop:hadoop-auth:jar:3.2.1:compile
[INFO] |  +- com.google.code.findbugs:jsr305:jar:3.0.0:compile
[INFO] |  +- org.apache.htrace:htrace-core4:jar:4.1.0-incubating:compile
[INFO] |  +- org.codehaus.woodstox:stax2-api:jar:3.1.4:compile
[INFO] |  \- com.fasterxml.woodstox:woodstox-core:jar:5.0.3:compile
[INFO] +- org.apache.hadoop:hadoop-hdfs:jar:3.2.1:compile
[INFO] \- junit:junit:jar:4.11:test
[INFO]    \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  1.464 s
[INFO] Finished at: 2020-08-25T19:40:29+08:00
[INFO] ------------------------------------------------------------------------
```
