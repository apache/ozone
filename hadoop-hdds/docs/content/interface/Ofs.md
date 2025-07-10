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
export HADOOP_CLASSPATH=/opt/ozone/share/ozone/lib/ozone-filesystem-hadoop3-*.jar:$HADOOP_CLASSPATH
{{< /highlight >}}

(Note: with Hadoop 2.x, use the `ozone-filesystem-hadoop2-*.jar`)

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

## Differences from [o3fs]({{< ref "interface/O3fs.md" >}})

### Creating files

OFS doesn't allow creating keys(files) directly under root or volumes.
Users will receive an error message when they try to do that:

```bash
$ ozone fs -touch /volume1/key1
touch: Cannot create file under root or volume.
```

### Simplify fs.defaultFS

With OFS, fs.defaultFS (in core-site.xml) no longer needs to have a specific
volume and bucket in its path like o3fs did.
Simply put the OM host or service ID (in case of HA):

```xml
<property>
  <name>fs.defaultFS</name>
  <value>ofs://omservice</value>
</property>
```

The client would then be able to access every volume and bucket on the cluster
without specifying the hostname or service ID.

```bash
$ ozone fs -mkdir -p /volume1/bucket1
```

### Volume and bucket management directly from FileSystem shell

Admins can create and delete volumes and buckets easily with Hadoop FS shell.
Volumes and buckets are treated similar to directories so they will be created
if they don't exist with `-p`:

```bash
$ ozone fs -mkdir -p ofs://omservice/volume1/bucket1/dir1/
```

Note that the supported volume and bucket name character set rule still applies.
For instance, bucket and volume names don't take underscore(`_`):

```bash
$ ozone fs -mkdir -p /volume_1
mkdir: Bucket or Volume name has an unsupported character : _
```

## Mounts and Configuring /tmp

In order to be compatible with legacy Hadoop applications that use /tmp/,
we have a special temp mount located at the root of the FS.
This feature may be expanded in the feature to support custom mount paths.

Currently Ozone supports two configurations for /tmp.  The first (default), 
is a tmp directory for each user comprised of a mount volume with a 
user specific temp bucket.  The second (configurable through ozone-site.xml), 
a sticky-bit like tmp directory common to all users comprised of a mount 
volume and a common temp bucket.

Important: To use it, first, an **admin** needs to create the volume tmp
(the volume name is hardcoded for now) and set its ACL to world ALL access.
Namely:

```bash
$ ozone sh volume create tmp
$ ozone sh volume setacl tmp -al world::a
```

These commands only need to be done **once per cluster**.

### For /tmp directory per user (default)

Then, **each user** needs to mkdir first to initialize their own temp bucket
once.

```bash
$ ozone fs -mkdir /tmp
2020-06-04 00:00:00,050 [main] INFO rpc.RpcClient: Creating Bucket: tmp/0238 ...
```

After that they can write to it just like they would do to a regular
directory. e.g.:

```bash
$ ozone fs -touch /tmp/key1
```

### For a sharable /tmp directory common to all users

To enable the sticky-bit common /tmp directory, update the ozone-site.xml with
the following property

```xml
<property>
  <name>ozone.om.enable.ofs.shared.tmp.dir</name>
  <value>true</value>
</property>
```
Then after setting up the volume tmp as **admin**, also configure a tmp bucket that 
serves as the common /tmp directory for all users, for example, 
```bash
$ ozone sh bucket create /tmp/tmp
$ ozone sh volume setacl tmp -a user:anyuser:rwlc \
  user:adminuser:a,group:anyuser:rwlc,group:adminuser:a tmp/tmp
```
where, anyuser is username(s) admin wants to grant access and,
adminuser is the admin username.

Users then access the tmp directory as,
```bash
$ ozone fs -put ./NOTICE.txt ofs://om/tmp/key1
```

## Delete with trash enabled

In order to enable trash in Ozone, Please add these configs to core-site.xml

{{< highlight xml >}}
<property>
  <name>fs.trash.interval</name>
  <value>10</value>
</property>
<property>
  <name>fs.trash.classname</name>
  <value>org.apache.hadoop.fs.ozone.OzoneTrashPolicy</value>
</property>
{{< /highlight >}}
                                           
 
When keys are deleted with trash enabled, they are moved to a trash directory
under each bucket, because keys aren't allowed to be moved(renamed) between
buckets in Ozone.

```bash
$ ozone fs -rm /volume1/bucket1/key1
2020-06-04 00:00:00,100 [main] INFO fs.TrashPolicyDefault: Moved: 'ofs://id1/volume1/bucket1/key1' to trash at: ofs://id1/volume1/bucket1/.Trash/hadoop/Current/volume1/bucket1/key1
```

This is very similar to how the HDFS encryption zone handles trash location.

**Note**
 
 1.The flag `-skipTrash` can be used to delete files permanently without being moved to trash.
 
 2.Deletes at bucket or volume level with trash enabled are not allowed. One must use skipTrash in such cases.
 i.e `ozone fs -rm -R ofs://vol1/bucket1` or  `ozone fs -rm -R o3fs://bucket1.vol1` are not allowed without skipTrash

## Recursive listing

OFS supports recursive volume, bucket and key listing.

i.e. `ozone fs -ls -R ofs://omservice/` will recursively list all volumes,
buckets and keys the user has LIST permission to if ACL is enabled.
If ACL is disabled, the command would just list literally everything on that
cluster.

This feature wouldn't degrade server performance as the loop is on the client.
Think it as a client is issuing multiple requests to the server to get all the
information.

## Operation Support

lists the support status of Ofs operations

### Supported

| Operation              | Description                               | Support        |
|------------------------|-------------------------------------------|----------------|
| `create`               | Creates a new file.                       | Supported      |
| `open`                 | Opens a file for reading.                 | Supported      |
| `rename`               | Renames a file or directory.              | Supported [1]  |
| `delete`               | Deletes a file or directory.              | Supported [2]  |
| `listStatus`           | Lists the status of files in a directory. | Supported [3]  |
| `mkdirs`               | Creates a directory and its parents.      | Supported      |
| `getFileStatus`        | Gets the status of a file.                | Supported      |
| `setTimes`             | Sets the modification and access times.   | Supported      |
| `getLinkTarget`        | Gets the target of a symbolic link.       | Supported [4]  |
| `getFileChecksum`      | Gets the checksum of a file.              | Supported      |
| `setSafeMode`          | Enters or leaves safe mode.               | Supported      |
| `recoverLease`         | Recovers a file lease.                    | Supported      |
| `isFileClosed`         | Checks if a file is closed.               | Supported      |
| `createSnapshot`       | Creates a snapshot.                       | Supported [5]  |
| `deleteSnapshot`       | Deletes a snapshot.                       | Supported [5]  |
| `renameSnapshot`       | Renames a snapshot.                       | Supported [5]  |
| `getSnapshotDiffReport`| Gets a snapshot diff report.              | Supported [5]  |
| `copyFromLocalFile`    | Copies a file from the local filesystem.  | Supported      |
| `exists`               | Checks if a path exists.                  | Supported      |
| `getContentSummary`    | Gets the content summary of a path.       | Supported      |
| `getDefaultBlockSize`  | Gets the default block size.              | Supported      |
| `getDefaultReplication`| Gets the default replication factor.      | Supported      |
| `getDelegationToken`   | Gets a delegation token.                  | Supported      |
| `getFileBlockLocations`| Gets file block locations.                | Supported      |
| `getHomeDirectory`     | Gets the user's home directory.           | Supported      |
| `getServerDefaults`    | Gets the server default values.           | Supported      |
| `getTrashRoot`         | Gets the trash root for a path.           | Supported      |
| `getTrashRoots`        | Gets all trash roots.                     | Supported      |
| `getWorkingDirectory`  | Gets the current working directory.       | Supported      |
| `globStatus`           | Finds files matching a pattern.           | Supported      |
| `hasPathCapability`    | Queries for a path capability.            | Supported      |
| `isDirectory`          | Checks if a path is a directory.          | Supported      |
| `isFile`               | Checks if a path is a file.               | Supported      |
| `listFiles`            | Returns a remote iterator for files.      | Supported      |
| `listLocatedStatus`    | Returns a remote iterator for located file statuses. | Supported |
| `listStatusIterator`   | Returns a remote iterator for file statuses. | Supported |
| `setWorkingDirectory`  | Sets the current working directory.       | Supported      |
| `supportsSymlinks`     | Checks if symbolic links are supported.   | Supported      |

### Unsupported

| Operation              | Description                               |
|------------------------|-------------------------------------------|
| `append`               | Appends to an existing file.              |
| `setPermission`        | Sets the permission of a file.            |
| `setOwner`             | Sets the owner of a file.                 |
| `setReplication`       | Sets the replication factor of a file.    |
| `createSymlink`        | Creates a symbolic link.                  |
| `resolveLink`          | Resolves a symbolic link.                 |
| `setXAttr`             | Sets an extended attribute.               |
| `getXAttr`             | Gets an extended attribute.               |
| `listXAttrs`           | Lists extended attributes.                |
| `removeXAttr`          | Removes an extended attribute.            |
| `setAcl`               | Sets an ACL.                              |
| `getAclStatus`         | Gets an ACL status.                       |
| `modifyAclEntries`     | Modifies ACL entries.                     |
| `removeAclEntries`     | Removes ACL entries.                      |
| `removeDefaultAcl`     | Removes the default ACL.                  |
| `removeAcl`            | Removes an ACL.                           |
| `truncate`             | Truncates a file.                         |
| `concat`               | Concatenates files.                       |

**Footnotes:**

[1] Renaming across buckets is not supported. For File System Optimized (FSO) buckets, rename is an atomic metadata operation. For legacy buckets, renaming a directory is non-atomic, as it is implemented by renaming each file and subdirectory individually.

[2] Deleting the root of the filesystem is not allowed. Recursive volume deletion is not supported.

[3] Recursive listing at the root or volume level is not supported.

[4] OFS supports 'linked buckets', where one bucket acts as a link to another. General-purpose symbolic links for files or directories are not supported.

[5] Snapshots are only supported at the bucket level.

### Unsupported HDFS-Specific Operations

The following operations are specific to HDFS and are not supported by Ofs.

| Operation                 | Description                                         | Support     |
|---------------------------|-----------------------------------------------------|-------------|
| `setStoragePolicy`        | Sets a storage policy on a file or directory.       | Unsupported |
| `getStoragePolicy`        | Gets the storage policy of a file or directory.     | Unsupported |
| `getErasureCodingPolicy`  | Gets the erasure coding policy of a file or directory.| Unsupported |
| `setErasureCodingPolicy`  | Sets an erasure coding policy on a directory.       | Unsupported |
| `unsetErasureCodingPolicy`| Unsets an erasure coding policy on a directory.     | Unsupported |
| `addCachePool`            | Adds a cache pool.                                  | Unsupported |
| `modifyCachePool`         | Modifies a cache pool.                              | Unsupported |
| `removeCachePool`         | Removes a cache pool.                               | Unsupported |
| `listCachePools`          | Lists all cache pools.                              | Unsupported |
| `addCacheDirective`       | Adds a cache directive.                             | Unsupported |
| `modifyCacheDirective`    | Modifies a cache directive.                         | Unsupported |
| `removeCacheDirective`    | Removes a cache directive.                          | Unsupported |
| `listCacheDirectives`     | Lists cache directives.                             | Unsupported |
| `allowSnapshot`           | Allows snapshots to be taken on a directory.        | Unsupported |
| `disallowSnapshot`        | Disallows snapshots to be taken on a directory.     | Unsupported |
| `addErasureCodingPolicies`| Adds erasure coding policies.                       | Unsupported |
| `getErasureCodingPolicies`| Gets the available erasure coding policies.         | Unsupported |
| `removeErasureCodingPolicy`| Removes an erasure coding policy.                  | Unsupported |
| `enableErasureCodingPolicy`| Enables an erasure coding policy.                   | Unsupported |
| `disableErasureCodingPolicy`| Disables an erasure coding policy.                  | Unsupported |
| `getEZForPath`            | Gets the encryption zone for a path.                | Unsupported |
| `listErasureCodingPolicies`| Lists all erasure coding policies.                  | Unsupported |
| `listErasureCodingCodecs` | Lists all erasure coding codecs.                    | Unsupported |
| `getQuotaUsage`           | Gets the quota usage for a path.                    | Unsupported |
| `setQuotaByStorageType`   | Sets the quota by storage type for a path.          | Unsupported |
| `getQuotaByStorageType`   | Gets the quota by storage type for a path.          | Unsupported |
| `msync`                   | Flushes out the data in client's user buffer.       | Unsupported |
| `satisfyStoragePolicy`    | Satisfies the storage policy of a file.             | Unsupported |


