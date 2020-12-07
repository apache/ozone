---
title: S3/Ozone Filesystem inter-op 
summary: How to support both S3 and HCFS and the same time
date: 2020-09-09
jira: HDDS-4557
status: accepted
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

As Ozone is an object store it stores key and values in a flat hierarchy which is enough to support S3 (2). But to support Hadoop Compatible File System (and CSI), Ozone should simulate file system hierarchy.

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

[HDDS-4097](https://issues.apache.org/jira/browse/HDDS-4097) is created to normalize the key names based on file-system semantics if `ozone.om.enable.filesystem.paths` is enabled. But please note that `ozone.om.enable.filesystem.paths` should always be turned on if S3 and HCFS are both used. It means that if both S3 and HCFS are used, normalization is forced, and S3 interface is not fully AWS S3 compatible. There is no option to use HCFS and S3 but with full AWS compatibility (and reduced HCFS compatibility). 

# Goals

 * Out of the box Ozone should support both S3 and HCFS interfaces without any settings. (It's possible only for the regular, fs compatible key names)
 * As 100% compatibility couldn't be achieved on both side we need a configuration to set the expectations for incompatible key names (which means that un-preferred should use forced normalization, strict validation or limited view)
 * Default behavior of `o3fs` and `ofs` should be as close to `s3a` as possible (when s3 compatibility is preferred)

# Possible cases to support

There are two main aspects of supporting both `ofs/o3fs` and `s3` together:

 1. `ofs/o3fs` require to create intermediate directory entries (for example `/a/b` for the key `/a/b/c`)
 2. Special file-system incompatible key names require special attention

The second couldn't be done without compromise! For example if uploading a key with name `../../a.txt` can work with S3, but it's not possible by handled by `ofs/o3fs` as it would escape from the file system hierarchy.

At high level, we have two options:

 1. We either support all key names (including non fs compatible key names), which means `ofs/o3fs` can provide only a partial view. Some invalid key names couldn't be visible. (But all keys can be uploaded from S3)
 2. Or we can normalize the key names to always be HCFS compatible (which would break the S3 compatibility for some specific key names. For example a file (!) uploaded with name `a/b/` might be visible as `/a/b` after the creation after the normalization required by the `ofs/o3fs`).

As a result we can have three main options:

| option | prefix entries | normalization | S3 behavior | HCFS behavior | status |
|-|-|-|-|-|-|
| default | DISABLED | DISABLED | 100 % compatibility | FULLY UNSUPPORTED |  implemented, default |
| ozone.om.enable.filesystem.paths=true | ENABLED | ENABLED | limited compatibility | FULLY SUPPORTED | implemented |
| ozone.om.enable.intermediate.dirs=true | ENABLED | DISABLED | 100% compatibility | SUPPORTED but partial view. | NOT implemented |

Let's talk all of these option in more details

## Pure S3 Object store without Hadoop support

| function | status |
|-|-|
| normalization | DISABLED |
| prefix entries | DISABLED |
| S3 compatibility | 100% |
| HCFS compatibility | DISABLED |

The first version is the most simple. Only the S3 API is supported but full S3 compatibility can be exacted. (For example `a//b` key can be uploaded and downloaded).

With this option `o3fs/ofs` couldn't be supported as the prefix entries are not created for the keys / objects. 

**As of today this is the default behavior of Apache Ozone.**

## Hadoop Object store with limited S3 support

| function | status |
|-|-|
| normalization | ENABLED |
| prefix entries | ENABLED |
| S3 compatibility | 100% |
| HCFS compatibility | DISABLED |

This mode is introduced in HDDS-3955 with creating `ozone.om.enable.filesystem.paths`. This setting enabled both normalization and prefix entry creation. As normalization is forced, some of the behavior is S3 in-compatible. For example key uploaded with the name `/abs/d//f` can be read from the path `/abs/d/f`. Also some path which may be valid for S3 (like `../../a`) are disabled. 

This mode is supported and implemented in Ozone. The implementation details can be found under [HDDS-4097](https://issues.apache.org/jira/browse/HDDS-4097)

## S3 object store with limited Hadoop support

| function | status |
|-|-|
| normalization | ENABLED |
| prefix entries | DISABLED |
| S3 compatibility | 100% |
| HCFS compatibility | ENABLED but partial |

**This mode is not yet implemented**. Depends from the user feedback and requirements can be implemented as a hybrid mode.

This approach makes the normalization and prefix creation independent from each other. A new configuration can be introduced to turn on *only* the prefix creation.

| configuration | behavior |
|-|-|
| `ozone.om.enable.filesystem.paths=true`  | Enable intermediate dir generation **AND** key name normalization |
| `ozone.om.enable.intermediate.dirs=true` | Enable only the intermediate dir generation |

This mode can be 100% S3 compatibility (no forced normalization). But some of the path which are saved via S3 (like `a/b//////v`) couldn't be displayed via `ofs/o3fs`. This is the same behavior what S3A provides when incompatible path is found in S3.

With this approach `ofs/o3fs` can show only a partial view 

# Problematic cases

As described in the previous section there are some cases which couldn't be supported out-of-the-box due to the differences between the flat key-space and file-system hierarchy. These cases are collected here together with the information how existing tools (AWS console, AWS cli, AWS S3A Hadoop connector) behaves.


| name | configuration | status |
|-|-|-|
| S3 mode | (default) | implemented, default |
| Hadoop mode | `ozone.om.enable.filesystem.paths=true`  | implemented, can be configured |
| Hybrid mode | `ozone.om.enable.intermediate.dirs=true` | **not implemented** (!) |

Note: S3 functionality can be used together with Hadoop mode, but this is not 100% compatibility as some normalization are forced and validation is more strict.

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

 * Hybrid mode: `/y` is not accessible, `/a/b` directory doesn't contain this entry
 * Hadoop mode: key stored as `/a/b/c`  
 * S3 mode: key is used as is, but not available for `o3fs`/`ofs`.

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

 * Hybrid mode: `e` and `f` are not visible from `ofs` and `o3fs`, raw key path is used as defined (including dots)
 * Hadoop mode: key are stored as `/a/e` and `a/b/f`  
 * S3 mode: keys are stored as is (`a/b/../e`/` a/b/./f`)

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

 * Hybrid mode: show both the file and the directory with the same name (similar to S3A)
 * Hadoop mode: throwing exception when the second one is created  
 * S3 mode: both keys are saved, reading from `ofs`/`o3fs` is not supported 

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

 * Hybrid mode: possible but `i/` is hidden from o3fs/ofs
 * Hadoop mode: key name is normalized to real key name
 * Key is uploaded as is, without normalization. Using `ofs`/`o3fs` is not supported.

## Create key and explicit create parent dir

```
aws s3api put-object --bucket ozonetest --key e/f/g/
aws s3api put-object --bucket ozonetest --key e/f/
```

Behavior:

 * S3 can support it without any problem

Proposed behavior:

Hybrid mode: After the first command `/e/f/` and `/e/` entries created in the key space (as they are required by `ofs`/`o3fs`) but **with a specific flag** (explicit=false). 

AWS S3 list-objects API should exclude those entries from the result (!).

Second command execution should modify the flag of `/e/f/` key to (explicit=true).

## Create parent dir AND key with S3a

This is the problem which is reported by [HDDS-4209](https://issues.apache.org/jira/browse/HDDS-4209)

```
hdfs dfs -mkdir -p s3a://b12345/d11/d12 # -> Success

hdfs dfs -put /tmp/file1 s3a://b12345/d11/d12/file1 # -> fails with below error
```

Proposed behavior:

 * Hybrid mode: should work without error
 * Hadoop mode: should work without error.
 * S3 mode: should work but `ofs`/`o3fs` is not supported 
* 
This is an `ofs`/`o3fs` question not an S3. The directory created in the first step shouldn't block the creation of the file. This can be a **mandatory** normalization for `mkdir` directory creation. As it's an HCFS operation, s3 is not affected. Entries created from S3 can be visible from s3 without any problem.

## Create file and directory with S3

This problem is reported in HDDS-4209, thanks to @Bharat

```
hdfs dfs -mkdir -p s3a://b12345/d11/d12 -> Success

hdfs dfs -put /tmp/file1 s3a://b12345/d11/d12/file1 
```

* Hybrid and Hadoop mode: In this case first a `d11/d12/` key is created. The intermediate key creation logic in the second step should use it as a directory instead of throwing an exception.
* S3 mode: works in the same way but no intermediate directory is creaed.

# Challenges and implementation of hybrid mode

Implementation of hybrid mode depends from the feedback from the users. This section emphases some of the possible implementation problems and shows how is it possible to implement hybrid mode. 

When somebody creates a new key like `/a/b/c/d`, the same key should be visible from HCFS (`o3fs//` or `o3://`). `/a`, `/a/b` and `/a/b/c` should be visible as directories from HCFS.

S3 should list only the `/a/b/c/d` keys, (`/a`, `/a/b`, `/a/b/c` keys, created to help HCFS, **won't be visible** if the key is created from S3)

This can be done with persisting an extra flag with the implicit directory entries. These entries can be modified if they are explicit created.

This flag should be added only for the keys which are created by S3. `ofs://` and `of3fs://`  create explicit directories all the time.

## Handling of the incompatible paths

As it's defined above the intermediate directory generation and normalization are two independent settings. (It's possible to choose only to create the intermediate directories).

**If normalization is chosen**: (`ozone.om.enable.filesystem.paths=true`), all the key names will be normalized to fs-compatible name. It may cause a conflict (error) if the normalized key is already exists (or exists as a file instead of directory)

**Without normalization (`ozone.om.enable.intermediate.dirs=true`)**:

Creating intermediate directories might not be possible if path contains illegal characters or can't be parsed as a file system path. **These keys will be invisible from HCFS** by default. They will be ignored during the normal file list.