---
title: Ofs (Hadoop compatible)
date: 2017-09-14
weight: 1
menu:
   main:
      parent: "Client Interfaces"
summary: Hadoop Compatible file system allows any application that expects an HDFS like interface to work against Ozone with zero changes. Frameworks like Apache Spark, YARN and Hive work against Ozone without needing any change. **Global level view.**
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


<div class="alert alert-warning" role="alert">

Currently, Ozone supports two scheme: `o3fs://` and `ofs://`.
The biggest difference between the `o3fs` and `ofs`，is that `o3fs` supports operations 
only at a **single bucket**, while ofs supports operations across all volumes and buckets and 
provides a full view of all the volume/buckets.

</div>

## The Basics

Examples of valid OFS paths:

```
ofs://om1/
ofs://om3:9862/
ofs://omservice/
ofs://omservice/volume1/
ofs://omservice/volume1/bucket1/
ofs://omservice/volume1/bucket1/dir1
ofs://omservice/volume1/bucket1/dir1/key1

ofs://omservice/tmp/
ofs://omservice/tmp/key1
```

Volumes and mount(s) are located at the root level of an OFS Filesystem.
Buckets are listed naturally under volumes.
Keys and directories are under each buckets.

Note that for mounts, only temp mount `/tmp` is supported at the moment.

## Configuration


Please add the following entry to the core-site.xml.

{{< highlight xml >}}
<property>
  <name>fs.ofs.impl</name>
  <value>org.apache.hadoop.fs.ozone.RootedOzoneFileSystem</value>
</property>
<property>
  <name>fs.defaultFS</name>
  <value>ofs://om-host.example.com/</value>
</property>
{{< /highlight >}}

This will make all the volumes and buckets to be the default Hadoop compatible file system and register the ofs file system type.

You also need to add the ozone-filesystem-hadoop3.jar file to the classpath:

{{< highlight bash >}}
export HADOOP_CLASSPATH=/opt/ozone/share/ozonefs/lib/hadoop-ozone-filesystem-hadoop3-*.jar:$HADOOP_CLASSPATH
{{< /highlight >}}

(Note: with Hadoop 2.x, use the `hadoop-ozone-filesystem-hadoop2-*.jar`)

Once the default Filesystem has been setup, users can run commands like ls, put, mkdir, etc.
For example:

{{< highlight bash >}}
hdfs dfs -ls /
{{< /highlight >}}

Note that ofs works on all buckets and volumes. Users can create buckets and volumes using mkdir, such as create volume named volume1 and  bucket named bucket1:

{{< highlight bash >}}
hdfs dfs -mkdir /volume1
hdfs dfs -mkdir /volume1/bucket1
{{< /highlight >}}


Or use the put command to write a file to the bucket.

{{< highlight bash >}}
hdfs dfs -put /etc/hosts /volume1/bucket1/test
{{< /highlight >}}

For more usage, see: https://issues.apache.org/jira/secure/attachment/12987636/Design%20ofs%20v1.pdf

## Special note

Trash is disabled even if `fs.trash.interval` is set on purpose. (HDDS-3982)

## Differences from [o3fs]({{< ref "interface/O3fs.md" >}})

### Creating files

OFS doesn't allow creating keys(files) directly under root or volumes.
Users will receive an error message when they try to do that:

```
$ ozone fs -touch /volume1/key1
touch: Cannot create file under root or volume.
```

### Simplify fs.defaultFS

With OFS, fs.defaultFS (in core-site.xml) no longer needs to have a specific
volume and bucket in its path like o3fs did.
Simply put the OM host or service ID (in case of HA):

```
<property>
<name>fs.defaultFS</name>
<value>ofs://omservice</value>
</property>
```

The client would then be able to access every volume and bucket on the cluster
without specifying the hostname or service ID.

```
$ ozone fs -mkdir -p /volume1/bucket1
```

### Volume and bucket management directly from FileSystem shell

Admins can create and delete volumes and buckets easily with Hadoop FS shell.
Volumes and buckets are treated similar to directories so they will be created
if they don't exist with `-p`:

```
$ ozone fs -mkdir -p ofs://omservice/volume1/bucket1/dir1/
```

Note that the supported volume and bucket name character set rule still applies.
For instance, bucket and volume names don't take underscore(`_`):

```
$ ozone fs -mkdir -p /volume_1
mkdir: Bucket or Volume name has an unsupported character : _
```

## Mounts

In order to be compatible with legacy Hadoop applications that use /tmp/,
we have a special temp mount located at the root of the FS.
This feature may be expanded in the feature to support custom mount paths.

Important: To use it, first, an **admin** needs to create the volume tmp
(the volume name is hardcoded for now) and set its ACL to world ALL access.
Namely:

```
$ ozone sh volume create tmp
$ ozone sh volume setacl tmp -al world::a
```

These commands only needs to be done **once per cluster**.

Then, **each user** needs to mkdir first to initialize their own temp bucket
once.

```
$ ozone fs -mkdir /tmp
2020-06-04 00:00:00,050 [main] INFO rpc.RpcClient: Creating Bucket: tmp/0238 ...
```

After that they can write to it just like they would do to a regular
directory. e.g.:

```
$ ozone fs -touch /tmp/key1
```

## Delete with trash enabled

When keys are deleted with trash enabled, they are moved to a trash directory
under each bucket, because keys aren't allowed to be moved(renamed) between
buckets in Ozone.

```
$ ozone fs -rm /volume1/bucket1/key1
2020-06-04 00:00:00,100 [main] INFO fs.TrashPolicyDefault: Moved: 'ofs://id1/volume1/bucket1/key1' to trash at: ofs://id1/volume1/bucket1/.Trash/hadoop/Current/volume1/bucket1/key1
```

This is very similar to how the HDFS encryption zone handles trash location.

## Recursive listing

OFS supports recursive volume, bucket and key listing.

i.e. `ozone fs -ls -R ofs://omservice/`` will recursively list all volumes,
buckets and keys the user has LIST permission to if ACL is enabled.
If ACL is disabled, the command would just list literally everything on that
cluster.

This feature wouldn't degrade server performance as the loop is on the client.
Think it as a client is issuing multiple requests to the server to get all the
information.

## Special note

Trash is disabled even if `fs.trash.interval` is set on purpose. (HDDS-3982)
