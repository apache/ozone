---
title: 命令行接口
weight: 4
menu:
   main:
      parent: "客户端接口"
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


Ozone shell 是从命令行与 Ozone 交互的主要接口。在后台，它调用 [Java API]({{< ref "interface/JavaApi.md">}}).

有些功能只能通过使用 `ozone sh` 命令才能访问。例如：

1. 创建带有配额的卷
2. 管理内部 ACL
3. 创建带有加密的键的桶

所有这些命令都是一次性的管理任务。应用程序也可以使用其他接口，如 Hadoop 兼容文件系统（o3fs 或 ofs）或 S3 接口来实现相同功能而无需使用 Ozone 命令行接口。


Ozone shell 的帮助菜单可以在 _对象_ 级别 或者 _动作_ 级别被调出.

示例命令:

```bash
ozone sh volume --help
```

这条命令展示了卷的所有可用的 _动作_ 命令

或者也可以用来解释具体的某个 _动作_ ，例如：

```bash
ozone sh volume create --help
```

这条命令输出卷的`create`动作的所有命令行选项

## 通用命令格式

Ozone shell 命令采取以下形式:

> _ozone sh object action url_

**ozone** 脚本用于调用所有 Ozone 子命令。通过 ```sh``` 命令调用 ozone shell 命令。

对象可以是卷、桶或键。动作可以是创建、列出、删除等。

根据动作，Ozone URL 可以指向以下格式的卷、桶或键:

_\[schema\]\[server:port\]/volume/bucket/key_


其中，

1. **Schema** - 应为 `o3`，这是访问 Ozone API 的原生 RPC 协议。是否指定 schema 是可选的。

2. **Server:Port** - 应为 Ozone Manager 的地址。如果不指定端口，则将使用 ozone-site.xml 中的默认端口。

请查看卷命令、桶命令和键命令部分了解更多详情。

## 卷操作

卷位于层次结构的顶层，仅由管理员管理。也可以指定所有者用户和配额。

示例命令：

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
[ {
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
}, {
  ....
} ]
```

如果卷为空，我们可以使用以下命令删除卷。

```shell
$ ozone sh volume delete /vol1
Volume vol1 is deleted
```
如果卷包含任意桶或键，我们可以递归地删除卷。这将删除卷中所有的桶和键，然后删除卷本身。在运行这个命令后，将无法恢复已删除的内容。

```shell
$ ozone sh volume delete -r /vol1
This command will delete volume recursively.
There is no recovery option after using this command, and no trash for FSO buckets.
Delay is expected running this command.
Enter 'yes' to proceed': yes
Volume vol1 is deleted
```

## 桶操作

桶是层次结构的第二层级，与 AWS S3 桶相似。如果用户有必要的权限，可以在卷中创建桶。

示例命令：

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

如果桶是空的，我们可以用以下命令来删除桶。

```shell
$ ozone sh bucket delete /vol1/bucket1
Bucket bucket1 is deleted
```

如果桶包含任意键，我们可以递归地删除桶。这将删除桶中的所有键，然后删除桶本身。在运行这个命令后，将无法恢复已删除的内容。

```shell
$ ozone sh bucket delete -r /vol1/bucket1
This command will delete bucket recursively.
There is no recovery option after using this command, and deleted keys won't move to trash.
Enter 'yes' to proceed': yes
Bucket bucket1 is deleted
```
[透明数据加密]({{< ref "security/SecuringTDE.md" >}}) 可以在桶层级被启用。

## 键操作

键是可以存储数据的对象。

```shell
$ ozone sh key put /vol1/bucket1/README.md README.md
```

<div class="alert alert-warning" role="alert">

在这个命令中，标准的命令顺序 `ozone sh <object_type> <action> <url>` 可能会让使用者困惑, 因为它的格式是 `ozone sh key put <destination> <source>`， 而不是我们习惯的自然的顺序 `<source> <destination>`.
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

```shell
$ ozone sh key delete /vol1/bucket1/key1
```


如果键是在 [FSO]({{< ref "feature/PrefixFSO.zh.md">}}) 桶中，当删除键时它会被移动到回收站，回收站的位置是:
```shell
$ /<volume>/<bucket>/.Trash/<user>
```
如果键是在OBS桶中，它将被永久删除。

## 查询命令行结果

Ozone命令行返回JSON响应。[jq](https://stedolan.github.io/jq/manual/) 是一个命令行JSON处理器，可以用来过滤CLI结果以获取所需信息.

示例命令:

* 列出不是链接的 FSO 桶。
```shell
$ ozone sh bucket list /s3v | jq '.[] | select(.link==false and .bucketLayout=="FILE_SYSTEM_OPTIMIZED")'
{
  "metadata": {},
  "volumeName": "s3v",
  "name": "fso-bucket",
  "storageType": "DISK",
  "versioning": false,
  "usedBytes": 0,
  "usedNamespace": 0,
  "creationTime": "2023-02-01T05:18:46.974Z",
  "modificationTime": "2023-02-01T05:18:46.974Z",
  "quotaInBytes": -1,
  "quotaInNamespace": -1,
  "bucketLayout": "FILE_SYSTEM_OPTIMIZED",
  "owner": "om",
  "link": false
}
```

* 列出 EC 桶以及它们的复制策略配置。
```shell
$ ozone sh bucket list /vol1 | jq -r '.[] | select(.replicationConfig.replicationType == "EC") | {"name": .name, "replicationConfig": .replicationConfig}'
{
  "name": "ec5",
  "replicationConfig": {
    "data": 3,
    "parity": 2,
    "ecChunkSize": 1048576,
    "codec": "RS",
    "replicationType": "EC",
    "requiredNodes": 5
  }
}
{
  "name": "ec9",
  "replicationConfig": {
    "data": 6,
    "parity": 3,
    "ecChunkSize": 1048576,
    "codec": "RS",
    "replicationType": "EC",
    "requiredNodes": 9
  }
}
```

* 以制表符分隔的格式列出加密桶的名字以及它们的加密的键名。
```shell

$ ozone sh bucket list /vol1 | jq -r '.[] | select(.encryptionKeyName != null) | [.name, .encryptionKeyName] | @tsv'
ec5     key1
encrypted-bucket        key1
```
