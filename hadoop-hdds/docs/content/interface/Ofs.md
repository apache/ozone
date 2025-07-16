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

## Migrating from HDFS

This guide helps you migrate applications from HDFS to Ozone by detailing the API-level compatibility of the Ozone File System (Ofs).

To ensure a smooth transition, you should first verify that your existing applications will work with Ofs. You can check for potential incompatibilities by identifying the HDFS APIs your applications use.

*   **For Cluster Administrators**: Analyze the NameNode audit logs on your source HDFS cluster to identify the operations performed by your applications.
*   **For Application Developers**: Review your application's source code to identify which `FileSystem` or `DistributedFileSystem` APIs are being called.

Once you have a list of APIs, compare it against the tables below to identify any unsupported operations.

### Supported FileSystem APIs

The following standard `FileSystem` APIs are supported by Ofs.

| Operation               | NameNode audit log        | Description                                         | Support       |
|-------------------------|---------------------------|-----------------------------------------------------|---------------|
| `access`                | `checkAccess`             | Checks if the user can access a path.               | Supported     |
| `create`                | `create`                  | Creates a new file.                                 | Supported     |
| `open`                  | `open`                    | Opens a file for reading.                           | Supported     |
| `rename`                | `rename`                  | Renames a file or directory.                        | Supported [1] |
| `delete`                | `delete`                  | Deletes a file or directory.                        | Supported [2] |
| `listStatus`            | `listStatus`              | Lists the status of files in a directory.           | Supported [3] |
| `mkdirs`                | `mkdirs`                  | Creates a directory and its parents.                | Supported     |
| `getFileStatus`         | `getfileinfo`             | Gets the status of a file.                          | Supported     |
| `setTimes`              | `setTimes`                | Sets the modification and access times.             | Supported     |
| `getLinkTarget`         | `getfileinfo`             | Gets the target of a symbolic link.                 | Supported [4] |
| `getFileChecksum`       | `open`                    | Gets the checksum of a file.                        | Supported     |
| `setSafeMode`           | `safemode_leave`, `safemode_enter`, `safemode_get`, `safemode_force_exit` | Enters or leaves safe mode.                         | Supported     |
| `recoverLease`          | `recoverLease`            | Recovers a file lease.                              | Supported     |
| `isFileClosed`          | `isFileClosed`            | Checks if a file is closed.                         | Supported     |
| `createSnapshot`        | `createSnapshot`          | Creates a snapshot.                                 | Supported [5] |
| `deleteSnapshot`        | `deleteSnapshot`          | Deletes a snapshot.                                 | Supported [5] |
| `renameSnapshot`        | `renameSnapshot`          | Renames a snapshot.                                 | Supported [5] |
| `getSnapshotDiffReport` | `computeSnapshotDiff`     | Gets a snapshot diff report.                        | Supported [5] |
| `getContentSummary`     | `contentSummary`          | Gets the content summary of a path.                 | Supported     |
| `getDelegationToken`    | `getDelegationToken`      | Gets a delegation token.                            | Supported     |
| `globStatus`            | `listStatus`              | Finds files matching a pattern.                     | Supported     |
| `copyFromLocalFile`     | `getfileinfo`             | Copies a file from the local filesystem.            | Supported     |
| `exists`                | `getfileinfo`             | Checks if a path exists.                            | Supported     |
| `getFileBlockLocations` | `open`                    | Gets file block locations.                          | Supported     |
| `getTrashRoot`          | `listSnapshottableDirectory`, `getEZForPath` | Gets the trash root for a path.                     | Supported     |
| `getTrashRoots`         | `listStatus`, `listEncryptionZones` | Gets all trash roots.                     | Supported     |
| `isDirectory`           | `getfileinfo`             | Checks if a path is a directory.                    | Supported     |
| `isFile`                | `getfileinfo`             | Checks if a path is a file.                         | Supported     |
| `listFiles`             | `listStatus`              | Returns a remote iterator for files.                | Supported     |
| `listLocatedStatus`     | `listStatus`              | Returns a remote iterator for located file statuses.| Supported     |
| `listStatusIterator`    | `listStatus`              | Returns a remote iterator for file statuses.        | Supported     |
| `getDefaultBlockSize`   | N/A                       | Gets the default block size.                        | Supported     |
| `getDefaultReplication` | N/A                       | Gets the default replication factor.                | Supported     |
| `getHomeDirectory`      | N/A                       | Gets the user's home directory.                     | Supported     |
| `getServerDefaults`     | N/A                       | Gets the server default values.                     | Supported     |
| `getWorkingDirectory`   | N/A                       | Gets the current working directory.                 | Supported     |
| `hasPathCapability`     | N/A                       | Queries for a path capability.                      | Supported     |
| `setWorkingDirectory`   | N/A                       | Sets the current working directory.                 | Supported     |
| `supportsSymlinks`      | N/A                       | Checks if symbolic links are supported.             | Supported     |

*Note: An audit log of N/A means the API is client-side only and does not access the NameNode.*

### Unsupported FileSystem APIs

The following standard `FileSystem` APIs are not supported by Ofs.

| Operation          | NameNode audit log   | Description                    |
|--------------------|----------------------|--------------------------------|
| `append`           | `append`             | Appends to an existing file.   |
| `setPermission`    | `setPermission`      | Sets the permission of a file. |
| `setOwner`         | `setOwner`           | Sets the owner of a file.      |
| `setReplication`   | `setReplication`     | Sets the replication factor.   |
| `createSymlink`    | `createSymlink`      | Creates a symbolic link.       |
| `resolveLink`      | `getfileinfo`        | Resolves a symbolic link.      |
| `setXAttr`         | `setXAttr`           | Sets an extended attribute.    |
| `getXAttr`         | `getXAttrs`          | Gets an extended attribute.    |
| `getXAttrs`        | `getXAttrs`          | Gets extended attributes.      |
| `listXAttrs`       | `listXAttrs`         | Lists extended attributes.     |
| `removeXAttr`      | `removeXAttr`        | Removes an extended attribute. |
| `setAcl`           | `setAcl`             | Sets an ACL.                   |
| `getAclStatus`     | `getAclStatus`       | Gets an ACL status.            |
| `modifyAclEntries` | `modifyAclEntries`   | Modifies ACL entries.          |
| `removeAclEntries` | `removeAclEntries`   | Removes ACL entries.           |
| `removeDefaultAcl` | `removeDefaultAcl`   | Removes the default ACL.       |
| `removeAcl`        | `removeAcl`          | Removes an ACL.                |
| `truncate`         | `truncate`           | Truncates a file.              |
| `concat`           | `concat`             | Concatenates files.            |
| `listCorruptFileBlocks` | `listCorruptFileBlocks` | List corrupted file blocks |
| `msync`            | N/A                  | Does not have a corresponding HDFS audit log. |

### Unsupported HDFS-Specific APIs

The following APIs are specific to HDFS's `DistributedFileSystem` implementation and are not part of the generic `org.apache.hadoop.fs.FileSystem` API. Therefore, they are not supported by Ofs. However, see the footnotes below for equivalent APIs.

| Operation                      | NameNode audit log           | Description                                         | Support        |
|--------------------------------|------------------------------|-----------------------------------------------------|----------------|
| `getErasureCodingPolicy`       | `getErasureCodingPolicy`     | Gets the erasure coding policy of a file/dir.       | Unsupported [6]|
| `setErasureCodingPolicy`       | `setErasureCodingPolicy`     | Sets an erasure coding policy on a directory.       | Unsupported [6]|
| `unsetErasureCodingPolicy`     | `unsetErasureCodingPolicy`   | Unsets an erasure coding policy on a directory.     | Unsupported [6]|
| `addErasureCodingPolicies`     | `addErasureCodingPolicies`   | Adds erasure coding policies.                       | Unsupported [6]|
| `getErasureCodingPolicies`     | `getErasureCodingPolicies`   | Gets the available erasure coding policies.         | Unsupported [6]|
| `removeErasureCodingPolicy`    | `removeErasureCodingPolicy`  | Removes an erasure coding policy.                   | Unsupported [6]|
| `enableErasureCodingPolicy`    | `enableErasureCodingPolicy`  | Enables an erasure coding policy.                   | Unsupported [6]|
| `disableErasureCodingPolicy`   | `disableErasureCodingPolicy` | Disables an erasure coding policy.                  | Unsupported [6]|
| `getErasureCodingCodecs`       | `getErasureCodingCodecs`     | Lists all erasure coding codecs.                    | Unsupported [6]|
| `getECTopologyResultForPolicies` | `getECTopologyResultForPolicies` | Get erasure coding topology result for policies.    | Unsupported [6]|
| `getSnapshotListing`           | `ListSnapshot`               | List all snapshots of a snapshottable directory.    | Unsupported [6]|
| `allowSnapshot`                | `allowSnapshot`              | Allows snapshots to be taken on a directory.        | Unsupported [6]|
| `disallowSnapshot`             | `disallowSnapshot`           | Disallows snapshots to be taken on a directory.     | Unsupported [6]|
| `provisionSnapshotTrash`       | `getfileinfo`, `mkdirs`, `setPermission` | Provision trash for a snapshottable directory.      | Unsupported [6]|
| `createEncryptionZone`         | `createEncryptionZone`       | Creates an encryption zone.                         | Unsupported [6]|
| `getEZForPath`                 | `getEZForPath`               | Gets the encryption zone for a path.                | Unsupported [6]|
| `listEncryptionZones`          | `listEncryptionZones`        | Lists all encryption zones.                         | Unsupported [6]|
| `reencryptEncryptionZone`      | `reencryptEncryptionZone`    | Reencrypt an encryption zone.                       | Unsupported [6]|
| `listReencryptionStatus`       | `listReencryptionStatus`     | List reencryption status.                           | Unsupported [6]|
| `getFileEncryptionInfo`        | `getfileinfo`      | Get file encryption info.                           | Unsupported [6]|
| `provisionEZTrash`             | `getEZForPath`, `getfileinfo`, `mkdirs`, `setPermission` | Provision trash for an encryption zone.             | Unsupported [6]|
| `setQuota`                     | `clearQuota` or `clearSpaceQuota` or `setQuota` or `setSpaceQuota` | Sets the quota usage for a path.                    | Unsupported [6]|
| `getQuotaUsage`                | `quotaUsage`              | Gets the quota usage for a path.                    | Unsupported [6]|
| `setQuotaByStorageType`        | `setSpaceQuota`      | Sets quota by storage type for a path.              | Unsupported [6]|
| `unsetStoragePolicy`           | `unsetStoragePolicy`         | Unsets a storage policy on a file or directory.     | Unsupported    |
| `setStoragePolicy`             | `setStoragePolicy`           | Sets a storage policy on a file or directory.       | Unsupported    |
| `getStoragePolicy`             | `getStoragePolicy`           | Gets the storage policy of a file or directory.     | Unsupported    |
| `satisfyStoragePolicy`         | `satisfyStoragePolicy`       | Satisfies the storage policy of a file.             | Unsupported    |
| `addCachePool`                 | `addCachePool`               | Adds a cache pool.                                  | Unsupported    |
| `modifyCachePool`              | `modifyCachePool`            | Modifies a cache pool.                              | Unsupported    |
| `removeCachePool`              | `removeCachePool`            | Removes a cache pool.                               | Unsupported    |
| `listCachePools`               | `listCachePools`             | Lists all cache pools.                              | Unsupported    |
| `addCacheDirective`            | `addCacheDirective`          | Adds a cache directive.                             | Unsupported    |
| `modifyCacheDirective`         | `modifyCacheDirective`       | Modifies a cache directive.                         | Unsupported    |
| `removeCacheDirective`         | `removeCacheDirective`       | Removes a cache directive.                          | Unsupported    |
| `listCacheDirectives`          | `listCacheDirectives`        | Lists cache directives.                             | Unsupported    |
| `getSlowDatanodeStats`         | `datanodeReport`             | Get slow datanode stats.                            | Unsupported    |
| `saveNamespace`                | `saveNamespace`              | Save the namespace.                                 | Unsupported    |
| `restoreFailedStorage`         | `checkRestoreFailedStorage`, `enableRestoreFailedStorage`, `disableRestoreFailedStorage` | Restore failed storage. | Unsupported    |
| `refreshNodes`                 | `refreshNodes`               | Refresh nodes.                                      | Unsupported    |
| `setBalancerBandwidth`         | `setBalancerBandwidth`       | Set balancer bandwidth.                             | Unsupported    |
| `metaSave`                     | `metaSave`                   | Meta save.                                          | Unsupported    |
| `rollingUpgrade`               | `queryRollingUpgrade`, `startRollingUpgrade`, `finalizeRollingUpgrade` | Rolling upgrade. | Unsupported    |
| `finalizeUpgrade`              | `finalizeUpgrade`            | Finalize upgrade.                                   | Unsupported [7] |
| `listOpenFiles`                | `listOpenFiles`              | List open files.                                    | Unsupported [7] |

**Footnotes:**

[1] Renaming files or directories across different buckets is not supported. Within a File System Optimized (FSO) bucket, rename is an atomic metadata operation. For legacy buckets, renaming a directory is a non-atomic operation that renames each file and subdirectory individually.

[2] Deleting the filesystem root is not allowed. Recursively deleting a volume is also not supported.

[3] Recursive listing is not supported at the root or volume level.

[4] Ofs supports "linked buckets," where one bucket serves as a reference to another. However, general-purpose symbolic links for files or directories are not supported.

[5] Snapshots are supported at the bucket level only.

[6] For operations related to encryption zones, erasure coding, snapshots, and quotas, use the corresponding Ozone bucket-level APIs instead.

[7] Replace with `OzoneManagerProtocol.finalizeUpgrade()` and `OzoneManagerProtocol.listOpenFiles()`.

*The following audit logs are typically produced by HDFS internal services and are not relevant for application migration: `slowDataNodesReport`, `getDatanodeStorageReport`, `rollEditLog`, `renewDelegationToken`, `cancelDelegationToken`, `gcDeletedSnapshot`.*
*The following audit logs are produced by the HDFS output stream: `getAdditionalBlock`, `getAdditionalDatanode`, `abandonBlock`, `completeFile`, `fsync`.*
*The `getPreferredBlockSize` audit log is used in testing only.*
