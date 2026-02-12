---
title: "LDB Tool"
date: 2024-10-14
summary: Parsing rocksDB and debugging related issues.
menu: debug
weight: 1
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

Ozone heavily uses RocksDB for storing metadata. This tool helps parse the contents of RocksDB belonging to Ozone Roles.
Supported DB's : Ozone Manager (om.db) , StorageContainerManager (scm.db),  Datanode/Container (container.db)
Below is the usage:

```bash
Usage: ozone debug ldb --db=<dbPath> [COMMAND]
Parse rocksdb file content
      --db=<dbPath>   Database File Path
Commands:
  scan                      Parse specified metadataTable
  list_column_families, ls  list all column families in db.
  value-schema              Schema of value in metadataTable
  checkpoint                Create checkpoint for specified db
```

### list_column_families command

`list_column_families` command lists all the column families in the db provided.

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

`scan` command parses a particular column family of a rocksdb provided and prints the records.

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
By default, the contents are printed on the console, but it can be redirected to a file using the `--out` option. <br>
`--length` can be used to limit the number of records being printed. <br>
`--count` doesn't print the records, it shows the approximate number of records. This is not accurate. <br>
`ozone debug ldb scan` command provides many filtering options to make debugging easier, elaborated below: <br>

<div class="alert alert-success" role="alert">
  Multiple filtering options can be used together in a single command.
</div>

#### --startkey and --endkey
As the names suggest, these options specify the keys from/until which the iteration needs to happen.  <br>
`--startkey` specifies which key to start iterating from, it is inclusive. `--endkey` specifies which key to stop iterating at, it is exclusive.

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
There are multiple fields in each record. `--fields` option allows us to choose the specific fields to display. 

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
`--filter` can be used to select records whose value matches a given condition. The filter is given in this format: `<field>:<operator>:<value>`,
where `<field>` is any valid field from the value of the record, `<operator>` is one of the 4 supported operations `[equals, regex, lesser, greater]`, `<value>` is the value used for the comparison. <br>
'Equals' and 'regex' work with string, bool and numerical fields, 'lesser' and 'greater' work only with numerical values.  <br>
Multiple filters can also be given in one command, they need to be separated by commas.  <br>
Using `equals` operator: 
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
Using `lesser` operator (`greater` operator can also be used in the same way):
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
Using `regex` operator:
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

Using multiple filters:
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

`value-schema` command shows the schema of the value stored in a column-family of a rocksdb, i.e., it shows the fields stored in the value and it's datatype.
`--depth` can be used optionally to limit the level until which the fields are fetched.

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

### checkpoint command

`checkpoint` command takes a checkpoint of a rocksdb, at a provided path.

```bash
$ ozone debug ldb --db=/data/metadata/om.db checkpoint --output=/tmp/om-checkpoint
```
