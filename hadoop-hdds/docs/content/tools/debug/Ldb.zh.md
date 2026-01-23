---
title: "Ozone Debug"
date: 2024-10-14
summary: Ozone Debug 命令可用于所有与调试相关的任务。
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

Ozone Debug 命令 (`ozone debug`) 是开发人员工具的集合，旨在帮助调试并获取 Ozone 各个组件的更多信息。

```bash
Usage: ozone debug [-hV] [--verbose] [-conf=<configurationPath>]
                   [-D=<String=String>]... [COMMAND]
Developer tools for Ozone Debug operations
      -conf=<configurationPath>
                  path to the ozone configuration file
  -D, --set=<String=String>
                  a map of (configuration_key,configuration_value) for any overrides
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
      --verbose   More verbose output. Show the stack trace of the errors.
```
子命令:
  chunkinfo                  返回指定文件/对象的块位置信息。
  print-log-dag, pld         在 OM 中创建当前压缩日志 DAG 的镜像。
  find-missing-padding, fmp  列出所有缺少填充的文件/对象，可以选择指定卷/存储桶/键 URI。
  recover                    恢复指定文件的租约。如果默认值不是 ofs:// ，请确保指定文件系统schema。
  prefix                     解析前缀内容。
  ldb                        解析 rocksdb 文件内容。
  read-replicas              读取给定路径文件/对象所有块的每个副本。
  container                  容器副本特定操作，仅在数据节点上执行。
  ratislogparser             解析Ratis Log 成用户可理解的文字形式。

有关更详细的用法，请参阅每个子命令的“--help”输出。


## ozone debug ldb

Ozone 大量使用 RocksDB 来存储元数据。该工具帮助解析各个Ozone Roles 的 RocksDB 数据内容。
支持的数据库：Ozone Manager (om.db)、StorageContainerManager (scm.db)、Datanode/Container (container.db)
下面是用法：

```bash
Usage: ozone debug ldb --db=<dbPath> [COMMAND]
Parse rocksdb file content
      --db=<dbPath>   Database File Path
Commands:
  scan                      Parse specified metadataTable
  list_column_families, ls  list all column families in db.
  value-schema              Schema of value in metadataTable
```

### list_column_families command

`list_column_families` 命令列出指定数据库中的所有列族。

```bash
$ ozone debug ldb --db=/path/to/scm.db ls
default
sequenceId
revokedCertsV2
pipelines
crls
crlSequenceId
meta
containers
validCerts
validSCMCerts
scmTransactionInfos
deletedBlocks
statefulServiceConfig
revokedCerts
move
```

### scan command

`scan` 命令解析提供的 rocksdb 的特定列族并打印记录。

```bash
Usage: ozone debug ldb scan [--compact] [--count] [--with-keys]
                            [--batch-size=<batchSize>] --cf=<tableName>
                            [--cid=<containerId>] [-d=<dnDBSchemaVersion>]
                            [-e=<endKey>] [--fields=<fieldsFilter>]
                            [--filter=<filter>] [-l=<limit>] [-o=<fileName>]
                            [-s=<startKey>] [--thread-count=<threadCount>]
Parse specified metadataTable
      --batch-size=<batchSize>
                          Batch size for processing DB data.
      --cf, --column_family, --column-family=<tableName>
                          Table name
      --cid, --container-id=<containerId>
                          Container ID. Applicable if datanode DB Schema is V3
      --compact           disable the pretty print the output
      --count, --show-count
                          Get estimated key count for the given DB column family
                            Default: false
  -d, --dnSchema, --dn-schema=<dnDBSchemaVersion>
                          Datanode DB Schema Version: V1/V2/V3
  -e, --ek, --endkey=<endKey>
                          Key at which iteration of the DB ends
      --fields=<fieldsFilter>
                          Comma-separated list of fields needed for each value.
                            eg.) "name,acls.type" for showing name and type
                            under acls.
      --filter=<filter>   Comma-separated list of "<field>:<operator>:<value>"
                            where <field> is any valid field of the record,
                            <operator> is [EQUALS,LESSER, GREATER or REGEX].
                            (EQUALS compares the exact string, REGEX compares
                            with a valid regular expression passed, and
                            LESSER/GREATER works with numeric values), <value>
                            is the value of the field.
                          eg.) "dataSize:equals:1000" for showing records
                            having the value 1000 for dataSize,
                               "keyName:regex:^key.*$" for showing records
                            having keyName that matches the given regex.
  -l, --limit, --length=<limit>
                          Maximum number of items to list.
  -o, --out=<fileName>    File to dump table scan data
  -s, --sk, --startkey=<startKey>
                          Key from which to iterate the DB
      --thread-count=<threadCount>
                          Thread count for concurrent processing.
      --with-keys         Print a JSON object of key->value pairs (default)
                            instead of a JSON array of only values.
```
默认情况下，内容打印在控制台上，但可以使用 `--out` 选项将其重定向到文件。 <br>
`--length` 可用于限制打印的记录数。 <br>
`--count` 不打印记录，它显示大概的，并不是完全精确的记录数。 <br>
`ozone debug ldb scan` 命令提供了许多过滤选项以使调试更容易，详细说明如下：<br>

<div class="alert alert-success" role="alert">
  多个过滤选项可以在单个命令中一起使用。
</div>

#### --startkey and --endkey
顾名思义，这些选项指定迭代需要发生的键。  <br>
`--startkey` 指定从哪个键开始迭代，包含该键。 `--endkey` 指定停止迭代的键，不包含该键。

```bash
$ ozone debug ldb --db=/path/to/om.db scan --cf=volumeTable --startkey=vol3 --endkey=vol5
```
```json
{ "/vol3": {
  "metadata" : { },
  "objectID" : -9999,
  "updateID" : 4000,
  "adminName" : "om",
  "ownerName" : "om",
  "volume" : "vol3",
  "creationTime" : 1707192335309,
  "modificationTime" : 1714057412205,
  "quotaInBytes" : 22854448694951936,
  "quotaInNamespace" : 100000000,
  "usedNamespace" : 1,
  "acls" : [ {
    "type" : "USER",
    "name" : "om",
    "aclScope" : "ACCESS"
  } ],
  "refCount" : 0
}
, "/vol4": {
    "metadata" : { },
    "objectID" : -888,
    "updateID" : 5000,
    "adminName" : "om",
    "ownerName" : "om",
    "volume" : "vol4",
    "creationTime" : 1696280979907,
    "modificationTime" : 1696280979907,
    "quotaInBytes" : 2251799813685250,
    "quotaInNamespace" : 100000000,
    "usedNamespace" : 2,
    "acls" : [ {
      "type" : "USER",
      "name" : "om",
      "aclScope" : "ACCESS"
    } ],
    "refCount" : 0
}
  }
```

#### --fields
每条记录中有多个字段。 `--fields` 选项允许我们选择要显示的特定字段。

```bash
$ ozone debug ldb --db=/path/to/om.db scan --cf=keyTable -l=1 --fields="volumeName,bucketName,keyName,keyLocationVersions.version,acls.name"
```
```json
{ "/vol1/ozone-legacy-bucket/10T-1-terasort-input/": {
  "keyLocationVersions" : [ {
    "version" : 0
  } ],
  "keyName" : "10T-1-terasort-input/",
  "bucketName" : "ozone-legacy-bucket",
  "acls" : [ {
    "name" : "om"
  }, {
    "name" : "scm"
  }, {
    "name" : "testuser"
  } ],
  "volumeName" : "vol1"
}
}
```

#### --filter
`--filter` 可用于选择值与给定条件匹配的记录。过滤器按以下格式给出：`<field>:<operator>:<value>`，
其中“<field>”是记录值中的任何有效字段，“<operator>”是 4 个支持的操作 `[equals, regex, lesser, greater]` 之一，“<value>”是使用的值用于比较。<br>
`Equals` 和 `regex` 适用于字符串、布尔值和数字字段，`lesser` 和 `greater` 仅适用于数字值。  <br>
也可以在一个命令中给出多个过滤器，它们需要用逗号分隔。<br>
使用 `equals` (等于) 运算符：
```bash
$ ozone debug ldb --db=/path/to/om.db scan --cf=volumeTable --filter="usedNamespace:equals:2"
```
```json
{
  "/vol4": {
    "metadata": {},
    "objectID": -888,
    "updateID": 5000,
    "adminName": "om",
    "ownerName": "om",
    "volume": "vol4",
    "creationTime": 1696280979907,
    "modificationTime": 1696280979907,
    "quotaInBytes": 2251799813685250,
    "quotaInNamespace": 100000000,
    "usedNamespace": 2,
    "acls": [
      {
        "type": "USER",
        "name": "om",
        "aclScope": "ACCESS"
      }
    ],
    "refCount": 0
  }
, "/vol5": {
  "metadata" : { },
  "objectID" : -956599,
  "updateID" : 45600,
  "adminName" : "om",
  "ownerName" : "om",
  "volume" : "vol5",
  "creationTime" : 1807192332309,
  "modificationTime" : 1914057410005,
  "quotaInBytes" : 7785494951936,
  "quotaInNamespace" : 100000000,
  "usedNamespace" : 2,
  "acls" : [ {
    "type" : "USER",
    "name" : "om",
    "aclScope" : "ACCESS"
  } ],
  "refCount" : 0
}
 }
```
使用 `lesser` (较小) 运算符（`greater`(较大) 运算符也可以以相同的方式使用）：
```bash
$ ozone debug ldb --db=/path/to/om.db scan --cf=volumeTable --filter="usedNamespace:lesser:2"
```
```json
{
  "/vol2": {
    "metadata": {},
    "objectID": -73548,
    "updateID": 2384,
    "adminName": "om",
    "ownerName": "om",
    "volume": "vol2",
    "creationTime": 11980979907,
    "modificationTime": 1296280979900,
    "quotaInBytes": 417913685250,
    "quotaInNamespace": 100000000,
    "usedNamespace": 1,
    "acls": [
      {
        "type": "USER",
        "name": "om",
        "aclScope": "ACCESS"
      }
    ],
    "refCount": 0
  }
 }
```
使用 `regex` 运算符：
```bash
$ ozone debug ldb --db=/path/to/om.db scan --cf=volumeTable --filter="volume:regex:^v.*2$"
```
```json
{
  "/vol2": {
    "metadata": {},
    "objectID": -73548,
    "updateID": 2384,
    "adminName": "om",
    "ownerName": "om",
    "volume": "vol2",
    "creationTime": 11980979907,
    "modificationTime": 1296280979900,
    "quotaInBytes": 417913685250,
    "quotaInNamespace": 100000000,
    "usedNamespace": 1,
    "acls": [
      {
        "type": "USER",
        "name": "om",
        "aclScope": "ACCESS"
      }
    ],
    "refCount": 0
  }
 }
```

使用多个过滤器：
```bash
$ ozone debug ldb --db=/path/to/om.db scan --cf=volumeTable --filter="usedNamespace:equals:2,volume:regex:^.*4$"
```
```json
{
  "/vol4": {
    "metadata": {},
    "objectID": -888,
    "updateID": 5000,
    "adminName": "om",
    "ownerName": "om",
    "volume": "vol4",
    "creationTime": 1696280979907,
    "modificationTime": 1696280979907,
    "quotaInBytes": 2251799813685250,
    "quotaInNamespace": 100000000,
    "usedNamespace": 2,
    "acls": [
      {
        "type": "USER",
        "name": "om",
        "aclScope": "ACCESS"
      }
    ],
    "refCount": 0
  }
 }
```

### value-schema command

“value-schema”命令显示存储在rocksdb的列族中的值的模式，即，它显示存储在值中的字段及其数据类型。
可以选择使用`--depth`来限制获取字段的级别。

```bash
$ ozone debug ldb --db=/data/metadata/om.db value-schema --cf=keyTable --depth=1
```
```json
{
  "OmKeyInfo" : {
    "bucketName" : "String", 
    "metadata" : "struct", 
    "fileName" : "String", 
    "creationTime" : "long",
    "isFile" : "boolean", 
    "acls" : "struct", 
    "keyName" : "String",
    "replicationConfig" : "struct", 
    "encInfo" : "struct", 
    "dataSize" : "long", 
    "tags" : "struct", 
    "keyLocationVersions" : "struct", 
    "updateID" : "long", 
    "ownerName" : "String", 
    "modificationTime" : "long", 
    "parentObjectID" : "long", 
    "volumeName" : "String", 
    "fileChecksum" : "struct", 
    "objectID" : "long"
}
 }
```
```bash
$ ozone debug ldb --db=/data/metadata/om.db value-schema --cf=keyTable
```
```json
{
  "OmKeyInfo" : {
    "bucketName" : "String",
    "metadata" : { },
    "fileName" : "String",
    "creationTime" : "long",
    "isFile" : "boolean",
    "acls" : {
      "toStringMethod" : { },
      "hashCodeMethod" : { },
      "name" : "String",
      "type" : {
        "name" : "String",
        "value" : "String",
        "ordinal" : "int"
      },
      "aclScope" : {
        "name" : "String",
        "ordinal" : "int"
      },
      "aclBits" : "int"
    },
    "keyName" : "String",
    "replicationConfig" : { },
    "encInfo" : {
      "ezKeyVersionName" : "String",
      "keyName" : "String",
      "edek" : { },
      "cipherSuite" : {
        "unknownValue" : {
          "value" : "int"
        },
        "name" : "String",
        "algoBlockSize" : "int",
        "ordinal" : "int"
      },
      "version" : {
        "unknownValue" : {
          "value" : "int"
        },
        "name" : "String",
        "description" : "String",
        "version" : "int",
        "ordinal" : "int"
      },
      "iv" : { }
    },
    "dataSize" : "long",
    "tags" : { },
    "keyLocationVersions" : {
      "isMultipartKey" : "boolean",
      "locationVersionMap" : { },
      "version" : "long"
    },
    "updateID" : "long",
    "ownerName" : "String",
    "modificationTime" : "long",
    "parentObjectID" : "long",
    "volumeName" : "String",
    "fileChecksum" : { },
    "objectID" : "long"
  }
}
```