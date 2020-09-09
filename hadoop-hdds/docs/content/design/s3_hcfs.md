---
title: S3/Ozone Filesystem inter-op 
summary: How to support both S3 and HCFS and the same time
date: 2020-09-09
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

# Current status

As of today *Ozone Manager* has two different interfaces (both are defined in `OmClientProtocol.proto`): 

 1. object store related functions (like *CreateKey*, *LookupKey*, *DeleteKey*,...)  
 2. file system related functions (like *CreateFile*, *LookupFile*,...)

File system related functions uses the same flat hierarchy under the hood but includes additional functionalities. For example the `createFile` call creates all the intermediate directories for a specific key (create file `/a/b/c` will create `/a/b` and `/a` entries in the key space)

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

# Goals

 * Out of the box Ozone should support both S3 and HCFS interfaces without any settings. (It's possible only for the regular, fs compatible key names)
 * As 100% compatibility couldn't be achieved on both side we need a configuration to set the expectations for incompatible key names
 * Default behavior of `o3fs` and `ofs` should be as close to `s3a` as possible (when s3 compatibilty is prefered)

# Possible cases to support

There are two main aspects of supporting both `ofs/o3fs` and `s3` together:

 1. `ofs/o3fs` require to create intermediate directory entries (for exapmle `/a/b` for the key `/b/c/c`)
 2. Special file-system incompatible key names require special attention

The second couldn't be done with compromise.

 1. We either support all key names (including non fs compatible key names), which means `ofs/o3fs` can provide only a partial view
 2. Or we can normalize the key names to be fs compatible (which makes it possible to create inconsistent S3 keys)

HDDS-3955 introduced `ozone.om.enable.filesystem.paths`, with this setting we will have two possible usage pattern:

| ozone.om.enable.filesystem.paths= | true | false
|-|-|-|
| create itermediate dirs | YES | NO |
| normalize key names from `ofs/o3fs` | YES | NO
| force to normalize key names of `s3` interface | YES (1) | NO 
| `s3` key `/a/b/c` available from `ofs/o3fs` | YES | NO
| `s3` key `/a/b//c` available from `ofs/o3fs` | YES | NO
| `s3` key `/a/b//c` available from `s3` | AWS S3 incompatibility | YES

(1): Under implementation

This proposal suggest to use a 3rd option where 100% AWS compatiblity is guaranteed in exchange of a limited `ofs/o3fs` view:

| ozone.om.intermediate.dir.generation= | true |
|-|-|-|
| create itermediate dirs | YES | 
| normalize key names from `ofs/o3fs` | YES |
| force to normalize key names of `s3` interface | **NO** |
| `s3` key `/a/b/c` available from `ofs/o3fs` | YES | 
| `s3` key `/a/b//c` available from `ofs/o3fs` | NO | 
| `s3` key `/a/b//c` available from `s3` | *YES* (100% AWS compatibility) |


# Proposed solution

In short: 

 **I propose to make it possible to configure **normalization** and **intermediate dir creation**, independent from each other**
 
It can be done in multiple ways. For the sake of simplicity, let's imagine two configuration option

| configuration | behavior | 
|-|-|
| `ozone.om.enable.filesystem.paths=true`  | Enable intermediate dir generation **AND** key name normalization 
| `ozone.om.enable.intermediate.dirs=true` | Enable only the intermediate dir generation

## S3/HCFS Interoperability

**In case of intermediate directory generation is enabled (with either of the configuraiton keys)**:

When somebody creates a new key like `/a/b/c/d`, the same key should be visible from HCFS (`o3fs//` or `o3://`). `/a`, `/a/b` and `/a/b/c` should be visible as directories from HCFS.

S3 should list only the `/a/b/c/d` keys, (`/a`, `/a/b`, `/a/b/c` keys, created to help HCFS, **won't be visible** if the key is created from S3)

This can be done with persisting an extra flag with the implicit directory entries. These entries can be modified if they are explicit created.

This flag should be added only for the keys which are created by S3. `ofs://` and `of3fs://`  create explicit directories all the time.

Advantages of this approach:

 1. HCFS and S3 can work together
 2. S3 behavior is closer to the original AWS s3 behavior (when `/a/b/c` key is created `/a/b` won't be visible)

## Handling of the incompatible paths

As it's defined above the intermediate directory generation and normalization are two independent settings. (It's possible to choose only to create the intermediate directories).

**If normalization is choosen**: (`ozone.om.enable.filesystem.paths=true`), all the key names will be normalized to fs-compatible name. It may cause a conflict (error) if the normalized key is already exists (or exists as a file instead of directory)

**Without normalization (`ozone.om.enable.intermediate.dirs=true`)**:

Creating intermediate directories might not be possible if path contains illegal characters or can't be parsed as a file system path. **These keys will be invisible from HCFS** by default. They will be ignored during the normal file list.

## Using Ozone in object-store only mode

Creating intermediate directories can have some overhead (write amplification is increased if many keys are written with different prefixes as we need an entry for each prefixes). This write-amplification can be handled with the current implementation: based on the measurements RocksDB has no problems with billions of keys.

If none of the mentioned configurations are enabled, the intermediate directories won't be created. But in this case, the consitent view of `ofs/o3fs` couldn't be guaranteed, so `ofs/o3fs` **should be disabled and throw an exception** (But Ozone can be used as a pure S3 replacement without using as a HCFS).

# Problematic cases

As described in the previous section there are some cases which couldn't be supported out-of-the-box due to the differences between the flat key-space and file-system hierarchy. These cases are collected here together with the information how existing tools (AWS console, AWS cli, AWS S3A Hadoop connector) behaves.

## Empty directory path

With a pure object store keys with empty directory names can be created.

```
aws s3api put-object --bucket ozonetest --key a/b//y --body README.md
```

Behavior:
 * *S3 web console*: empty dir is rendered as `____` to make it possible to navigate in
 * **aws s3 ls**: Prefix entry is visible as `PRE /`
 * S3A: Not visible
 
Proposed behavior:

 * `ozone.om.enable.intermediate.dirs=true`: `/y` is not accessible, `/a/b` directory doesn't contain this entry
 * `ozone.om.enable.filesystem.paths=true`: key stored as `/a/b/c`  
 
## Path with invalid characters (`..`,`.`) 

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

 * `ozone.om.enable.intermediate.dirs=true`: `e` and `f` are not visible
 * `ozone.om.enable.filesystem.paths=true`: key stored as `/a/e` and `a/b/f`  

## Key and directory with the same name

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
 * `ozone.om.enable.filesystem.paths=true`: throwing exception when the second one is created  

## Directory entry created with file content

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
 * `normalize`: key name is normalized to real key name

## Create key and explicit create parent dir

```
aws s3api put-object --bucket ozonetest --key e/f/g/
aws s3api put-object --bucket ozonetest --key e/f/
```

Behavior:

 * S3 can support it without any problem
 
Proposed behavior:

After the first command `/e/f/` and `/e/` entries created in the key space (as they are required by `ofs`/`o3fs`) but **with a specific flag** (explicit=false). 

AWS S3 list-objects API should exclude those entries from the result (!).

Second command execution should modify the flag of `/e/f/` key to (explicit=true).

## Create parent dir AND key with S3a

This is the problem which is reporeted by [HDDS-4209](https://issues.apache.org/jira/browse/HDDS-4209)

```
hdfs dfs -mkdir -p s3a://b12345/d11/d12 # -> Success

hdfs dfs -put /tmp/file1 s3a://b12345/d11/d12/file1 # -> fails with below error
```

Proposed behavior:

 * `ozone.om.enable.intermediate.dirs=true`: shold work without error
 * `ozone.om.enable.filesystem.paths=true`: should work without error.

This is an `ofs`/`o3fs` question not an S3. The directory created in the first step shouldn't block the creation of the file. This can be a **mandatory** normalization for `mkdir` directory creation. As it's an HCFS operation, s3 is not affected. Entries created from S3 can be visible from s3 without any problem.

## Create file and directory with S3

This problem is reported in HDDS-4209, thanks to @Bharat

```
hdfs dfs -mkdir -p s3a://b12345/d11/d12 -> Success

hdfs dfs -put /tmp/file1 s3a://b12345/d11/d12/file1 
```

In this case first a `d11/d12/` key is created. The intermediate key creation logic in the second step should use it as a directory instead of throwing an exception.