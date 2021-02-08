---
title: Command Line Interface
weight: 4
menu:
   main:
      parent: "Client Interfaces"
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


Ozone shell is the primary interface to interact with Ozone from the command line. Behind the scenes it uses the [Java API]({{< ref "interface/JavaApi.md">}}).
 
 There are some functionality which couldn't be accessed without using `ozone sh` commands. For example:
 
  1. Creating volumes with quota
  2. Managing internal ACLs
  3. Creating buckets with encryption key
  
All of these are one-time, administration tasks. Applications can use Ozone without this CLI using other interface like Hadoop Compatible File System (o3fs or ofs) or S3 interface.


Ozone shell help can be invoked at _object_ level or at _action_ level.

For example:

```bash
ozone sh volume --help
```

will show all possible actions for volumes.

Or it can be invoked to explain a specific action like:

```bash
ozone sh volume create --help
```

which will print the command line options of the `create` command for volumes.

## General Command Format

Ozone shell commands take the following form:

> _ozone sh object action url_

**ozone** script is used to invoke all Ozone sub-commands. The ozone shell is
invoked via ```sh``` command.

Object can be volume, bucket or key. Actions are various verbs like
create, list, delete etc.

Depending on the action, Ozone URL can point to a volume, bucket or key in the following format:

_\[schema\]\[server:port\]/volume/bucket/key_


Where,

1. **Schema** - This should be `o3` which is the native RPC protocol to access
  Ozone API. The usage of the schema is optional.

2. **Server:Port** - This is the address of the Ozone Manager. If the port is
omitted the default port from ozone-site.xml will be used.

Please see volume commands, bucket commands, and key commands section for more
detail.

## Volume operations

Volume is the top level element of the hierarchy, managed only by administrators. Optionally, quota and the owner user can be specified.

Example commands:

```shell
$ ozone sh volume create /vol1
```

```shell
$ ozone sh volume info /vol1
{
  "metadata" : { },
  "name" : "vol1",
  "admin" : "hadoop",
  "owner" : "hadoop",
  "creationTime" : "2020-07-28T12:31:50.112Z",
  "modificationTime" : "2020-07-28T12:31:50.112Z",
  "acls" : [ {
    "type" : "USER",
    "name" : "hadoop",
    "aclScope" : "ACCESS",
    "aclList" : [ "ALL" ]
  }, {
    "type" : "GROUP",
    "name" : "users",
    "aclScope" : "ACCESS",
    "aclList" : [ "ALL" ]
  } ],
  "quota" : 1152921504606846976
}
```

```shell
$ ozone sh volume list /
{
  "metadata" : { },
  "name" : "s3v",
  "admin" : "hadoop",
  "owner" : "hadoop",
  "creationTime" : "2020-07-27T11:32:22.314Z",
  "modificationTime" : "2020-07-27T11:32:22.314Z",
  "acls" : [ {
    "type" : "USER",
    "name" : "hadoop",
    "aclScope" : "ACCESS",
    "aclList" : [ "ALL" ]
  }, {
    "type" : "GROUP",
    "name" : "users",
    "aclScope" : "ACCESS",
    "aclList" : [ "ALL" ]
  } ],
  "quota" : 1152921504606846976
}
....
```
## Bucket operations

Bucket is the second level of the object hierarchy, and is similar to AWS S3 buckets. Users can create buckets in volumes, if they have the necessary permissions.

Command examples:

```shell
$ ozone sh bucket create /vol1/bucket1
```

```shell
$ ozone sh bucket info /vol1/bucket1
{
  "metadata" : { },
  "volumeName" : "vol1",
  "name" : "bucket1",
  "storageType" : "DISK",
  "versioning" : false,
  "creationTime" : "2020-07-28T13:14:45.091Z",
  "modificationTime" : "2020-07-28T13:14:45.091Z",
  "encryptionKeyName" : null,
  "sourceVolume" : null,
  "sourceBucket" : null
}
```

[Transparent Data Encryption]({{< ref "security/SecuringTDE.md" >}}) can be enabled at the bucket level.

## Key operations

Key is the object which can store the data.

```shell
$ ozone sh key put /vol1/bucket1/README.md README.md
```

<div class="alert alert-warning" role="alert">

In this case the standard `ozone sh <object_type> <action> <url>` scheme may be a bit confusing at first, as it results in the syntax `ozone sh key put <destination> <source>` instead of the arguably more natural order of `<source> <destination>`.
</div>



```shell
$ ozone sh key info /vol1/bucket1/README.md
{
  "volumeName" : "vol1",
  "bucketName" : "bucket1",
  "name" : "README.md",
  "dataSize" : 3841,
  "creationTime" : "2020-07-28T13:17:20.749Z",
  "modificationTime" : "2020-07-28T13:17:21.979Z",
  "replicationType" : "RATIS",
  "replicationFactor" : 1,
  "ozoneKeyLocations" : [ {
    "containerID" : 1,
    "localID" : 104591670688743424,
    "length" : 3841,
    "offset" : 0
  } ],
  "metadata" : { },
  "fileEncryptionInfo" : null
}
```

```shell
$ ozone sh key get /vol1/bucket1/README.md /tmp/
```
