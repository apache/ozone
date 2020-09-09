---
title: S3/Ozone Filesystem inter-op 
summary: How to support both S3 and HCFS and the same time
date: 2019-06-09
jira: HDDS-4097
status: draft
author: Marton Elek, 
---
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->


# Ozone S3 vs file-system semantics

Ozone is an object-store for Hadoop ecosystem which can be used from multiple interfaces: 

 1. From Hadoop Compatible File Systems (will be called as *HCFS* in the remaining of this document) (RPC)
 2. From S3 compatible applications (REST)
 3. From container orchestrator as mounted volume (CSI, alpha feature)

As Ozone is an object store it stores key and values in a flat hierarchy which is enough to support S3 (2). But to support Hadoop Compatible File System (and CSI), Ozone should simulated file system hierarchy.

There are multiple challenges when file system hierarchy is simulated by a flat namespace:

 1. Some key patterns couldn't be easily transformed to file system path (e.g. `/a/b/../c`, `/a/b//d`, or a real key with directory path like `/b/d/`)
 2. Directory entries (which may have own properties) require special handling as file system interface requires a dir entry even if it's not created explicitly (for example if key `/a/b/c` is created `/a/b` supposed to be a visible directory entry for file system interface) 
 3. Non-recursive listing of directories can be hard (Listing direct entries under `/a` should ignore all the `/a/b/...`, `/a/b/c/...` keys) 
 4. Similar to listing, rename can be a costly operation as it requires to rename many keys (renaming a first level directory means a rename of all the keys with the same prefix)

See also the [Hadoop S3A documentation](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Introducing_the_Hadoop_S3A_client) which describes some of these problem when AWS S3 is used. (*Warnings* section)

## Current status

As of today *Ozone Manager* has two different interfaces (both are defined in `OmClientProtocol.proto`): 

 1. object store related functions (like *CreateKey*, *LookupKey*, *DeleteKey*,...)  
 2. file system related functions (like *CreateFile*, *LookupFile*,...)

File system related functions uses the same flat hierarchy under the hood but includes additional functionality. For example the `createFile` call creates all the intermediate directories for a specific key (create file `/a/b/c` will create `/a/b` and `/a` entries in the key space)

Today, if a key is created from the S3 interface can cause exceptions if the intermediate directories are checked from HCFS:


```shell
$ aws s3api put-object --endpoint http://localhost:9878 --bucket bucket1 --key /a/b/c/d

$ ozone fs -ls o3fs://bucket1.s3v/a/
ls: `o3fs://bucket1.s3v/a/': No such file or directory
```

This problem is reported in [HDDS-3955](https://issues.apache.org/jira/browse/HDDS-3955), where a new configuration key is introduced (`ozone.om.enable.filesystem.paths`). If this is enabled, intermediate directories are created even if the object store interface is used.

This configuration is turned off by default, which means that S3 and HCFS couldn't be used together.

To solve the performance problems of the directory listing / rename, [HDDS-2939](https://issues.apache.org/jira/browse/HDDS-2939) is created, which propose to use a new prefix table to store the "directory" entries (=prefixes).

[HDDS-4097](https://issues.apache.org/jira/browse/HDDS-4097) is created to normalize the key names based on file-system semantics if `ozone.om.enable.filesystem.paths` is enabled. But please note that `ozone.om.enable.filesystem.paths` should always be turned on if S3 and HCFS are both used which means that S3 and HCFS couldn't be used together with normalization.

## Goals

 * Out of the box Ozone should support both S3 and HCFS interfaces without any settings. (It's possible only for the regular path)
 * As 100% compatibility couldn't be achieved on both side we need a configuration to set the expectations in case of incompatible key names
 * Default behavior of `o3fs` and `ofs` should be as close to `s3a` as possible

## Proposed solution

### S3/HCFS Interoperability

When somebody creates a new key like `/a/b/c/d`, the same key should be visible from HCFS (`o3fs//` or `o3://`). `/a`, `/a/b` and `/a/b/c` should be visible as directories from HCFS.

S3 should list only the `/a/b/c/d` keys, (`/a`, `/a/b`, `/a/b/c` keys **won't be visible**!)

This can be done with persisting an extra flag with the implicit directory entries. These entries can be modified if they are explicit created.

(This flag should be added only for the keys which are created by S3. `ofs://` and `of3fs://` can create explicit directories all the time)

### Handling of the incompatible paths

Creating intermediate directories might not be possible if path contains illegal characters or can't be parsed as a file system path. **These keys will be invisible from HCFS** by default. They will be ignored during the normal file list.

This behavior can be adjusted by a new configuration variable (eg. `ozone.keyspace.scheme`) based on the values:

 * `permissive` (default): any key name can be used from S3 interface but from HCFS only the valid key names (keys which can be transformed to a file system path) will be visible. The option provides the highest AWS s3 compatibility.
 * `strict`: This is the opposite: any non file-system compatible key name will be rejected by an exception. This is a safe choice to make everything visible from HCFS, but couldn't guarantee 100% AWS S3 compatibility as some key names couldn't be used. 
 * `normalize`: It's similar to the `strict` but -- instead of throwing an exception -- normalizes the key names to file-system compatible names. It breaks the AWS compatibility (some keys which are written will be readable from other path), but a safe choice when HCFS is heavliy used. 

### Using Ozone in object-store only mode

Creating intermediate directories can have some overhead (write amplification is increased if many keys are written with different prefixes as we need an entry for each prefixes). This write-amplification can be handled with the current implementation: based on the measurements RocksDB has no problems with billions of keys.

But it's possible to use a specific configuration key (like `ozone.om.enable.filesystem.paths`) to **disable** the default behavior of creating directory entries. But in this case all the file system calls (any HCFS call) **should be disabled and throw an exception** as consistency couldn't be guaranteed from there. (But Ozone can be used as a pure S3 replacement without using as a HCFS).

## Problematic cases

As described in the previous section there are some cases which couldn't be supported out-of-the-box due to the differences between the flat key-space and file-system hierarchy. These cases are collected here together with the information how existing tools (AWS console, AWS cli, AWS S3A Hadoop connector) behaves.

### Empty directory path

With a pure object store keys with empty directory names can be created.

```
aws s3api put-object --bucket ozonetest --key a/b//y --body README.md
```

Behavior:
 * *S3 web console*: empty dir is rendered as `____` to make it possible to navigate in
 * **aws s3 ls**: Prefix entry is visible as `PRE /`
 * S3A: Not visible
 
Proposed behavior:

 * `permissive`: `/c` is not accessible, `/a/b` directory doesn't contain this entry
 * `strict`: throwing exception
 * `normalize`: key stored as `/a/b/c`  
 
### Path with invalid characters (`..`,`.`) 

Path segments might include parts which has file system semantics:

```
aws s3api put-object --bucket ozonetest --key a/b/../e --body README.md
aws s3api put-object --bucket ozonetest --key a/b/./f --body README.md
```

Behavior:
 * *S3 web console*: `.` and `..` are rendered as directories to make it possible to navigate in
 * **aws s3 ls**: Prefix entry is visible as `PRE ../` and `PRE ./`
 * S3A: Entries are not visible
 
Proposed behavior:

 * `permissive`: `e` and `f` are not visible
 * `strict`: throwing exception
 * `normalize`: key stored as `/a/e` and `a/b/f`  

### Key and directory with the same name

It is possible to create directory and key with the same name in AWS:

```
aws s3api put-object --bucket ozonetest --key a/b/h --body README.md
aws s3api list-objects --bucket ozonetest --prefix=a/b/h/
```

Behavior:
 * *S3 web console*: both directory and file are rendered
 * **aws s3 ls**: prefix (`PRE h/`) and file (`h`) are both displayed
 * S3A: both entries are visible with the name `/a/b/h` but firt is a file (with size) second is a directory (with directory attributes)
 
Proposed behavior:

 * `permissive`: show both the file and the directory with the same name (similar to S3A)
 * `strict`: throwing exception when the second is created
 * `normalize`: throwing exception when the second one is created  

### Directory entry created with file content

In this case we create a directory (key which ends with `/`) but with real file content:

```
aws s3api put-object --bucket ozonetest --key a/b/i/ --body README.md
```

Behavior:
 * *S3 web console*: rendered as directory (couldn't be downloaded)
 * **aws s3 ls**: showed as a prefix (`aws s3 ls s3://ozonetest/a/b`), but when the full path is used showed as a file without name (`aws s3 ls s3://ozonetest/a/b/i/`)
 * S3A: `./bin/hdfs dfs -ls s3a://ozonetest/a/b/` shows a directory `h`, `./bin/hdfs dfs -ls s3a://ozonetest/a/b/i` shows a file `i`
 
Proposed behavior:

 * `permissive`: possible but `i/` is hidden from o3fs/ofs
 * `strict`: throwing exception when created
 * `normalize`: throwing exception when created  

### Create key and explicit create parent dir

```
aws s3api put-object --bucket ozonetest --key e/f/g/
aws s3api put-object --bucket ozonetest --key e/f/
```


Behavior:

 * S3 can support it without any problem
 
Proposed behavior:

After the first command `/e/f` and `/e` entries created in the key space (as they are required by `ofs`/`o3fs`) but **with a specific flag** (explicit=false). 

AWS S3 list-objects API should exclude those entries from the result (!).

Second command execution should modify the flag of `/e/f` key to (explicit=true).

Note: using `o3fs://` or `ofs://` all the intermediate directories can be (explicit=true)

### Create key and delete parent key

This is very similar to the previous one but the direction is opposite. If directory key entry is still required it should be modified to (explicit=false) and excluded from any listing from the object store interface.