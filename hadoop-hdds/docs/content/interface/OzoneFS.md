---
title: Ozone File System
date: 2017-09-14
weight: 2
summary: Hadoop Compatible file system allows any application that expects an HDFS like interface to work against Ozone with zero changes. Frameworks like Apache Spark, YARN and Hive work against Ozone without needing any change.
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

The Hadoop compatible file system interface allows storage backends like Ozone
to be easily integrated into Hadoop eco-system.  Ozone file system is an
Hadoop compatible file system.

## Setting up the Ozone file system (o3fs)

To create an ozone file system, we have to choose a bucket where the file system would live. This bucket will be used as the backend store for OzoneFileSystem. All the files and directories will be stored as keys in this bucket.

Please run the following commands to create a volume and bucket, if you don't have them already.

{{< highlight bash >}}
ozone sh volume create /volume
ozone sh bucket create /volume/bucket
{{< /highlight >}}

Once this is created, please make sure that bucket exists via the _list volume_ or _list bucket_ commands.

Please add the following entry to the core-site.xml.

{{< highlight xml >}}
<property>
  <name>fs.AbstractFileSystem.o3fs.impl</name>
  <value>org.apache.hadoop.fs.ozone.OzFs</value>
</property>
<property>
  <name>fs.defaultFS</name>
  <value>o3fs://bucket.volume</value>
</property>
{{< /highlight >}}

This will make this bucket to be the default file system for HDFS dfs commands and register the o3fs file system type.

You also need to add the ozone-filesystem-hadoop3.jar file to the classpath:

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/ozonefs/lib/hadoop-ozone-filesystem-hadoop3-*.jar:$HADOOP_CLASSPATH
{{< /highlight >}}

(Note: with Hadoop 2.x, use the `hadoop-ozone-filesystem-hadoop2-*.jar`)

Once the default Filesystem has been setup, users can run commands like ls, put, mkdir, etc.
For example,

{{< highlight bash >}}
hdfs dfs -ls /
{{< /highlight >}}

or

{{< highlight bash >}}
hdfs dfs -mkdir /users
{{< /highlight >}}


Or put command etc. In other words, all programs like Hive, Spark, and Distcp will work against this file system.
Please note that any keys created/deleted in the bucket using methods apart from OzoneFileSystem will show up as directories and files in the Ozone File System.

Note: Bucket and volume names are not allowed to have a period in them.

Moreover, the filesystem URI can take a fully qualified form with the OM host and an optional port as a part of the path following the volume name.
For example, you can specify both host and port:

{{< highlight bash>}}
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com:5678/key
{{< /highlight >}}

When the port number is not specified, it will be retrieved from config key `ozone.om.address`
if defined; or it will fall back to the default port `9862`.
For example, we have `ozone.om.address` configured as following in `ozone-site.xml`:

{{< highlight xml >}}
  <property>
    <name>ozone.om.address</name>
    <value>0.0.0.0:6789</value>
  </property>
{{< /highlight >}}

When we run command:

{{< highlight bash>}}
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com/key
{{< /highlight >}}

The above command is essentially equivalent to:

{{< highlight bash>}}
hdfs dfs -ls o3fs://bucket.volume.om-host.example.com:6789/key
{{< /highlight >}}

Note: Only port number from the config is used in this case, 
whereas the host name in the config `ozone.om.address` is ignored.

