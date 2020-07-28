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


 Ozone  shell is the primary interface to interact with Ozone. It provides a command shell interface to work against Ozone. It uses the [Java API]({{< ref "interface/JavaApi.md">}}).
 
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

This will show all possible actions for volumes.

or it can be invoked to explain a specific action like

```bash
ozone sh volume create --help
```

This command will give you command line options of the create command.

## General Command Format

The Ozone shell commands take the following format.

> _ozone sh object action url_

**ozone** script is used to invoke all Ozone sub-commands. The ozone shell is
invoked via ```sh``` command.

The object can be a volume, bucket or a key. The action is various verbs like
create, list, delete etc.

Ozone URL can point to a volume, bucket or keys in the following format:

_\[schema\]\[server:port\]/volume/bucket/key_


Where,

1. **Schema** - This should be `o3` which is the native RPC protocol to access
  Ozone API. The usage of the schema is optional.

2. **Server:Port** - This is the address of the Ozone Manager. If the port is
omitted the default port from ozone-site.xml will be used.

Depending on the call, the volume/bucket/key names will be part of the URL.
Please see volume commands, bucket commands, and key commands section for more
detail.

## Volume operation

Volume is the top level element of the hierarchy and supposed to managed only by the administrators. Quota and the owner user can be defined.

Example commands:

```shell
> ozone sh volume create /vol1
```

```shell
bash-4.2$ ozone sh volume info /vol1
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
ozone sh volume list /
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
## Bucket operation

Bucket is the second level hierarchy, similar to the AWS S3 buckets. Users can create buckets in volumes with permissions.

Command examples:

```shell
> ozone sh bucket create /vol1/bucket1
```shell

```shell
> ozone sh bucket info /vol1/bucket1
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

Bucket is the level of the [Transparent Data Encryption]({{< ref "security/SecuringTDE.md" >}})

## Key

Key is the object which can store the data.

```shell
>ozone sh key put /vol1/bucket1/README.md README.md
```

<div class="alert alert-warning" role="alert">

The standard `ozone sh _object_type _action_ _url_` scheme can cause some confusion here as the parameter order is `ozone sh key put <DESTINATION> <SOURCE>` instead of the opposite. 
</div>



```shell
>ozone sh key info /vol1/bucket1/README.md
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
>ozone sh key get /vol1/bucket1/README.md /tmp/
```