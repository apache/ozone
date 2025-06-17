---
title: Recon API
weight: 4
menu:
   main:
      parent: "编程接口"
summary: Recon 服务器支持 HTTP 端点，以帮助故障排除和监听 Ozone 集群。
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

Recon API v1 是一组 HTTP 端点，可以帮助您了解 Ozone 集群的当前状态，并在需要时进行故障排除。

## 容器

### GET /api/v1/containers

**参数**
* prevKey (可选)

    只回传ID大于给定的 prevKey 的容器。
    示例：prevKey=1
* limit (可选)

    只回传有限数量的结果。默认限制是1000。

**回传**

回传所有 ContainerMetadata 对象。

```json
    {
      "data": {
        "totalCount": 3,
        "containers": [
          {
            "ContainerID": 1,
            "NumberOfKeys": 834
          },
          {
            "ContainerID": 2,
            "NumberOfKeys": 833
          },
          {
            "ContainerID": 3,
            "NumberOfKeys": 833
          }
        ]
      }
    }
```

### GET /api/v1/containers/:id/keys
    
**参数**

* prevKey (可选)
 
    只回传在给定的 prevKey 键前缀之后的键。
    示例：prevKey=/vol1/bucket1/key1
    
* limit (可选)

    只回传有限数量的结果。默认限制是1000。
    
**回传**

回传给定容器 ID 的所有 KeyMetadata 对象。
    
```json
    {
      "totalCount":7,
      "keys": [
        {
          "Volume":"vol-1-73141",
          "Bucket":"bucket-3-35816",
          "Key":"key-0-43637",
          "DataSize":1000,
          "Versions":[0],
          "Blocks": {
            "0": [
              {
                "containerID":1,
                "localID":105232659753992201
              }
            ]
          },
          "CreationTime":"2020-11-18T18:09:17.722Z",
          "ModificationTime":"2020-11-18T18:09:30.405Z"
        },
        ...
      ]
    }
```
### GET /api/v1/containers/missing

**参数**

没有参数。

**回传**

回传所有丢失容器的 MissingContainerMetadata 对象。
    
```json
    {
    	"totalCount": 26,
    	"containers": [{
    		"containerID": 1,
    		"missingSince": 1605731029145,
    		"keys": 7,
    		"pipelineID": "88646d32-a1aa-4e1a",
    		"replicas": [{
    			"containerId": 1,
    			"datanodeHost": "localhost-1",
    			"firstReportTimestamp": 1605724047057,
    			"lastReportTimestamp": 1605731201301
    		}, 
            ...
            ]
    	},
        ...
        ]
    }
```
### GET /api/v1/containers/:id/replicaHistory
    
**参数**

没有参数。

**回传**
回传给定容器 ID 的所有 ContainerHistory 对象。
    
```json
    [
      {
        "containerId": 1,
        "datanodeHost": "localhost-1",
        "firstReportTimestamp": 1605724047057,
        "lastReportTimestamp": 1605730421294
      },
      ...
    ]
```
### GET /api/v1/containers/unhealthy
     
**参数**

* batchNum (可选)
    回传结果的批号(如“页码”)。
    传递1，将回传记录1到limit。传递2，将回传limit + 1到2 * limit，依此类推。
    
* limit (可选)

    只回传有限数量的结果。默认限制是1000。
    
**回传**

回传所有不健康容器的 UnhealthyContainerMetadata 对象。

```json
     {
     	"missingCount": 2,
     	"underReplicatedCount": 0,
     	"overReplicatedCount": 0,
     	"misReplicatedCount": 0,
     	"containers": [{
     		"containerID": 1,
     		"containerState": "MISSING",
     		"unhealthySince": 1605731029145,
     		"expectedReplicaCount": 3,
     		"actualReplicaCount": 0,
     		"replicaDeltaCount": 3,
     		"reason": null,
     		"keys": 7,
     		"pipelineID": "88646d32-a1aa-4e1a",
     		"replicas": [{
     			"containerId": 1,
     			"datanodeHost": "localhost-1",
     			"firstReportTimestamp": 1605722960125,
     			"lastReportTimestamp": 1605731230509
     		}, 
            ...
            ]
     	},
        ...
        ]
     } 
```
     
### GET /api/v1/containers/unhealthy/:state
     
**参数**

* batchNum (可选)

    回传结果的批号(如“页码”)。
    传递1，将回传记录1到limit。传递2，将回传limit + 1到2 * limit，依此类推。
    
* limit (可选)

    只回传有限数量的结果。默认限制是1000。
    
**回传**

回传处于给定状态的容器的 UnhealthyContainerMetadata 对象。
不健康的容器状态可能为`MISSING`, `MIS_REPLICATED`, `UNDER_REPLICATED`,`OVER_REPLICATED`。
响应结构与`/containers/unhealthy`相同。


### GET /api/v1/containers/mismatch

**回传**

回传 OM 和 SCM 之间不匹配容器的列表。
* 容器存在于 OM 中，但不存在于 SCM 中。
* 容器存在于 SCM 中，但不存在于 OM 中。

```json
[
  {
    "containerId" : 1,
    "numberOfKeys" : 3,
    "pipelines" : [
      "pipelineId" : "1423ghjds832403232",
      "pipelineId" : "32vds94943fsdh4443",
      "pipelineId" : "32vds94943fsdhs443"
    ],
    "existsAt" : "OM"
  }
  ...
]
```

### GET /api/v1/containers/mismatch/deleted


**参数**

* prevKey (可选)

返回在SCM中，给定prevKey(容器ID) 后被标记为已删除状态，且在OM中存在的容器集合，
以便找出映射到这些已删除状态容器的键列表。例如：prevKey=5，跳过直到准确地定位到前一个容器ID。 

* limit (可选)

  仅返回有限数量的结果。默认限制为1000。

**回传**

返回在SCM中已删除但在OM中存在的容器集合，以找出映射到这些已删除状态容器的键列表。

```json
[
  {
    "containerId": 2,
    "numberOfKeys": 2,
    "pipelines": []
  }
  ...
]
```

### GET /api/v1/keys/open


**参数**

* prevKey (可选)

  返回给定 prevKey id 之后仍然处于打开状态且存在的键/文件集合。
  例如：prevKey=/vol1/bucket1/key1，这将跳过键，直到成功定位到给定的 prevKey。

* limit (可选)

  仅返回有限数量的结果。默认限制为1000。

**回传**

返回处于打开状态的键/文件集合。

```json
{
  "lastKey": "/vol1/fso-bucket/dir1/dir2/file2",
  "replicatedTotal": 13824,
  "unreplicatedTotal": 4608,
  "entities": [
    {
      "path": "/vol1/bucket1/key1",
      "keyState": "Open",
      "inStateSince": 1667564193026,
      "size": 1024,
      "replicatedSize": 3072,
      "unreplicatedSize": 1024,
      "replicationType": "RATIS",
      "replicationFactor": "THREE"
    },
    {
      "path": "/vol1/bucket1/key2",
      "keyState": "Open",
      "inStateSince": 1667564193026,
      "size": 512,
      "replicatedSize": 1536,
      "unreplicatedSize": 512,
      "replicationType": "RATIS",
      "replicationFactor": "THREE"
    },
    {
      "path": "/vol1/fso-bucket/dir1/file1",
      "keyState": "Open",
      "inStateSince": 1667564193026,
      "size": 1024,
      "replicatedSize": 3072,
      "unreplicatedSize": 1024,
      "replicationType": "RATIS",
      "replicationFactor": "THREE"
    },
    {
      "path": "/vol1/fso-bucket/dir1/dir2/file2",
      "keyState": "Open",
      "inStateSince": 1667564193026,
      "size": 2048,
      "replicatedSize": 6144,
      "unreplicatedSize": 2048,
      "replicationType": "RATIS",
      "replicationFactor": "THREE"
    }
  ]
}
```

### GET /api/v1/keys/deletePending


**参数**

* prevKey (可选)

  返回给定 prevKey id 之后处于待删除状态的键/文件集合。
  例如：prevKey=/vol1/bucket1/key1，这将跳过键，直到成功定位到给定的 prevKey。  

* limit (可选)

  仅返回有限数量的结果。默认限制为1000。

**回传**

返回处于待删除状态的键/文件集合。

```json
{
  "lastKey": "sampleVol/bucketOne/key_one",
  "replicatedTotal": -1530804718628866300,
  "unreplicatedTotal": -1530804718628866300,
  "deletedkeyinfo": [
    {
      "omKeyInfoList": [
        {
          "metadata": {},
          "objectID": 0,
          "updateID": 0,
          "parentObjectID": 0,
          "volumeName": "sampleVol",
          "bucketName": "bucketOne",
          "keyName": "key_one",
          "dataSize": -1530804718628866300,
          "keyLocationVersions": [],
          "creationTime": 0,
          "modificationTime": 0,
          "replicationConfig": {
            "replicationFactor": "ONE",
            "requiredNodes": 1,
            "replicationType": "STANDALONE"
          },
          "fileChecksum": null,
          "fileName": "key_one",
          "acls": [],
          "path": "0/key_one",
          "file": false,
          "latestVersionLocations": null,
          "replicatedSize": -1530804718628866300,
          "fileEncryptionInfo": null,
          "objectInfo": "OMKeyInfo{volume='sampleVol', bucket='bucketOne', key='key_one', dataSize='-1530804718628866186', creationTime='0', objectID='0', parentID='0', replication='STANDALONE/ONE', fileChecksum='null}",
          "updateIDset": false
        }
      ]
    }
  ],
  "status": "OK"
}
```

### GET /api/v1/keys/deletePending/dirs


**参数**

* prevKey (可选)

  返回给定 prevKey id 之后处于待删除状态的目录集合。
  例如：prevKey=/vol1/bucket1/bucket1/dir1，这将跳过目录，直到成功定位到给定的 prevKey。

* limit (可选)

  仅返回有限数量的结果。默认限制为1000。

**回传**

返回处于待删除状态的目录集合。

```json
{
  "lastKey": "vol1/bucket1/bucket1/dir1",
  "replicatedTotal": -1530804718628866300,
  "unreplicatedTotal": -1530804718628866300,
  "deletedkeyinfo": [
    {
      "omKeyInfoList": [
        {
          "metadata": {},
          "objectID": 0,
          "updateID": 0,
          "parentObjectID": 0,
          "volumeName": "sampleVol",
          "bucketName": "bucketOne",
          "keyName": "key_one",
          "dataSize": -1530804718628866300,
          "keyLocationVersions": [],
          "creationTime": 0,
          "modificationTime": 0,
          "replicationConfig": {
            "replicationFactor": "ONE",
            "requiredNodes": 1,
            "replicationType": "STANDALONE"
          },
          "fileChecksum": null,
          "fileName": "key_one",
          "acls": [],
          "path": "0/key_one",
          "file": false,
          "latestVersionLocations": null,
          "replicatedSize": -1530804718628866300,
          "fileEncryptionInfo": null,
          "objectInfo": "OMKeyInfo{volume='sampleVol', bucket='bucketOne', key='key_one', dataSize='-1530804718628866186', creationTime='0', objectID='0', parentID='0', replication='STANDALONE/ONE', fileChecksum='null}",
          "updateIDset": false
        }
      ]
    }
  ],
  "status": "OK"
}
```

## Blocks Metadata (admin only)
### GET /api/v1/blocks/deletePending


**参数**

* prevKey (可选)

  仅返回给定块ID（prevKey）之后处于待删除状态的块列表。
  例如：prevKey=4，这将跳过 deletedBlocks 表中的键以跳过 prevKey 之前的记录。  

* limit (可选)

  仅返回有限数量的结果。默认限制为1000。

**回传**

返回待删除的块列表。

```json
{
  "OPEN": [
    {
      "containerId": 100,
      "localIDList": [
        1,
        2,
        3,
        4
      ],
      "localIDCount": 4,
      "txID": 1
    }
  ]
}
```

## Namespace Metadata (仅 admin)

### GET /api/v1/namespace/summary

**参数**

* path

  字符串形式的路径请求，不包含任何协议前缀。

**回传**

返回路径的基本信息汇总，包括实体类型和路径下对象的聚合计数。

如果路径存在，则 `status` 为 `OK`，否则为 `PATH_NOT_FOUND`。

示例: /api/v1/namespace/summary?path=/
```json
    {
      "status": OK,
      "type": ROOT,
      "numVolume": 10,
      "numBucket": 100,
      "numDir": 1000,
      "numKey": 10000
    }
```

示例: /api/v1/namespace/summary?path=/volume1
```json
    {
      "status": OK,
      "type": VOLUME,
      "numVolume": -1,
      "numBucket": 10,
      "numDir": 100,
      "numKey": 1000
    }
```

示例: /api/v1/namespace/summary?path=/volume1/bucket1
```json
    {
      "status": OK,
      "type": BUCKET,
      "numVolume": -1,
      "numBucket": -1,
      "numDir": 50,
      "numKey": 500
    }
```

示例: /api/v1/namespace/summary?path=/volume1/bucket1/dir
```json
    {
      "status": OK,
      "type": DIRECTORY,
      "numVolume": -1,
      "numBucket": -1,
      "numDir": 10,
      "numKey": 100
    }
```

示例: /api/v1/namespace/summary?path=/volume1/bucket1/dir/nestedDir
```json
    {
      "status": OK,
      "type": DIRECTORY,
      "numVolume": -1,
      "numBucket": -1,
      "numDir": 5,
      "numKey": 50
    }
```

如果任何 `num` 字段为 `-1`，则该路径请求不适用于该实体类型。

### GET /api/v1/namespace/usage

**参数**

* path

  字符串形式的路径请求，不包含任何协议前缀。

* files (可选)

  一个布尔值，默认值为 `false`。如果设置为 `true`，则会计算路径下键的磁盘使用情况。

* replica (可选)

  一个布尔值，默认为 `false`。如果设置为 `true`，则会计算键的副本大小的磁盘使用情况。

**回传**

返回路径下所有子路径的磁盘使用情况。规范化 `path` 字段，返回路径下直接健的总大小作为
`sizeDirectKey`，并以字节为单位返回 `size/sizeWithReplica`。

如果路径存在，则 `status` 为 `OK`，否则为 `PATH_NOT_FOUND`。

示例: /api/v1/namespace/usage?path=/vol1/bucket1&files=true&replica=true
```json
    {
      "status": OK,
      "path": "/vol1/bucket1",
      "size": 100000,
      "sizeWithReplica": 300000,
      "subPathCount": 4,
      "subPaths": [
        {
          "path": "/vol1/bucket1/dir1-1",
          "size": 30000,
          "sizeWithReplica": 90000,
          "isKey": false
        },
        {
          "path": "/vol1/bucket1/dir1-2",
          "size": 30000,
          "sizeWithReplica": 90000,
          "isKey": false
        },
        {
          "path": "/vol1/bucket1/dir1-3",
          "size": 30000,
          "sizeWithReplica": 90000,
          "isKey": false
        },
        {
          "path": "/vol1/bucket1/key1-1",
          "size": 10000,
          "sizeWithReplica": 30000,
          "isKey": true
        }
      ],
      "sizeDirectKey": 10000
    }
```
如果 `files` 设置为 `false`，则子路径 `/vol1/bucket1/key1-1` 将被省略。
如果 `replica` 设置为 `false`，则 `sizeWithReplica` 返回 `-1`。
如果路径的实体类型无法具有直接键（例如根目录、卷），则 `sizeDirectKey` 返回 `-1`。

### GET /api/v1/namespace/quota

**参数**

* path

  路径请求为字符串，不包含任何协议前缀。

**回传**

返回路径下允许的配额和已使用的配额。
只有卷和存储桶具有配额。其他类型不适用于配额请求

如果请求有效，则 `status` 为 `OK`；如果路径不存在，则为 `PATH_NOT_FOUND`；
如果路径存在但路径的实体类型不适用于请求，则为 `TYPE_NOT_APPLICABLE`。

示例: /api/v1/namespace/quota?path=/vol
```json
    {
      "status": OK,
      "allowed": 200000,
      "used": 160000
    }
```

如果未设置配额，则 `allowed` 返回 `-1`。详情请参阅 [Ozone 中的配额]。
(https://ci-hadoop.apache.org/view/Hadoop%20Ozone/job/ozone-doc-master/lastSuccessfulBuild/artifact/hadoop-hdds/docs/public/feature/quota.html)


### GET /api/v1/namespace/dist

**参数**

* path

  路径请求为字符串，不包含任何协议前缀。

**回传**

返回路径下所有键的文件大小分布。

如果请求有效，则 `status` 为 `OK`；如果路径不存在，则为 `PATH_NOT_FOUND`；
如果路径存在，但该路径是一个键，键不具有文件大小分布，则为 `TYPE_NOT_APPLICABLE`。

示例: /api/v1/namespace/dist?path=/
```json
    {
      "status": OK,
      "dist": [
        0,
        0,
        10,
        20,
        0,
        30,
        0,
        100,
        ...
      ]
    }
```

Recon跟踪所有大小从`1 KB`到`1 PB`的键。对于小于`1 KB`的键，映射到第一个箱（索引）；
对于大于`1 PB`的键，映射到最后一个箱（索引）。

`dist` 的每个索引都映射到一个文件大小范围（例如 `1 MB` 到 `2 MB`）。

## 集群状态

### GET /api/v1/clusterState
     
**参数**

没有参数。

**回传**

返回 Ozone 集群当前状态的摘要。
    
```json
     {
     	"pipelines": 5,
     	"totalDatanodes": 4,
     	"healthyDatanodes": 4,
     	"storageReport": {
     		"capacity": 1081719668736,
     		"used": 1309212672,
     		"remaining": 597361258496
     	},
     	"containers": 26,
     	"volumes": 6,
     	"buckets": 26,
     	"keys": 25
     }
```

## Volumes (仅 admin)

### GET /api/v1/volumes

**参数**

* prevKey (可选)

  仅返回给定 prevKey 之后的卷。
  示例: prevKey=vol1

* limit (可选)

  仅返回有限数量的结果。默认限制为1000。

**回传**

返回集群中的所有卷。

```json
     {
     	"totalCount": 4,
     	"volumes": [{
          "volume": "vol1",
          "owner": "testuser",
          "admin": "ozone",
          "creationTime": 1665588176660 ,
          "modificationTime": 1665590397315,
          "quotaInNamespace": 2048,
          "quotaInBytes": 1073741824,
          "usedNamespace": 10,
          "acls": [
            {
              "type": "USER",
              "name": "testuser",
              "scope": "ACCESS",
              "aclList": [
                "WRITE",
                "READ",
                "DELETE"
              ]
            }
          ]
        },
        ...
        ]
     }
```

## Buckets (仅 admin)

### GET /api/v1/buckets

**参数**

* volume (可选)

  卷以字符串形式表示，不包含任何协议前缀。

* prevKey (可选)

  返回给定 prevKey 之后的存储桶。 如果未指定卷，则忽略 prevKey。
  示例: prevKey=bucket1

* limit (可选)

  仅返回有限数量的结果。默认限制为1000。


**回传**

如果未指定卷或指定的卷是一个空字符串，则返回集群中的所有存储桶。
如果指定了 `volume`，则仅返回 `volume` 下的存储桶。

```json
     {
     	"totalCount": 5,
     	"buckets": [{
          "volumeName": "vol1",
          "bucketName": "buck1",
          "versioning": false,
          "storageType": "DISK",
          "creationTime": 1665588176616,
          "modificationTime": 1665590392293,
          "usedBytes": 943718400,
          "usedNamespace": 40000,
          "quotaInBytes": 1073741824,
          "quotaInNamespace": 50000,
          "owner": "testuser",
          "bucketLayout": "OBJECT_STORE",
          "acls": [
            {
              "type": "USER",
              "name": "testuser",
              "scope": "ACCESS",
              "aclList": [
                "WRITE",
                "READ",
                "DELETE"
              ]
            }
          ]
        },
        ...
        ]
     }
```
     
## 数据节点

### GET /api/v1/datanodes
    
**参数**

没有参数。

**回传**

回传集群中的所有数据节点。
    
```json
    {
     	"totalCount": 4,
     	"datanodes": [{
     		"uuid": "f8f8cb45-3ab2-4123",
     		"hostname": "localhost-1",
     		"state": "HEALTHY",
     		"lastHeartbeat": 1605738400544,
     		"storageReport": {
     			"capacity": 270429917184,
     			"used": 358805504,
     			"remaining": 119648149504
     		},
     		"pipelines": [{
     			"pipelineID": "b9415b20-b9bd-4225",
     			"replicationType": "RATIS",
     			"replicationFactor": 3,
     			"leaderNode": "localhost-2"
     		}, {
     			"pipelineID": "3bf4a9e9-69cc-4d20",
     			"replicationType": "RATIS",
     			"replicationFactor": 1,
     			"leaderNode": "localhost-1"
     		}],
     		"containers": 17,
     		"leaderCount": 1
     	},
        ...
        ]
     }
```
     
## 管道

### GET /api/v1/pipelines

**参数**

没有参数

**回传**

回传在集群中的所有管道。
    
```json
     {
     	"totalCount": 5,
     	"pipelines": [{
     		"pipelineId": "b9415b20-b9bd-4225",
     		"status": "OPEN",
     		"leaderNode": "localhost-1",
     		"datanodes": ["localhost-1", "localhost-2", "localhost-3"],
     		"lastLeaderElection": 0,
     		"duration": 23166128,
     		"leaderElections": 0,
     		"replicationType": "RATIS",
     		"replicationFactor": 3,
     		"containers": 0
     	},
        ...
        ]
     }
```  

## 任务

### GET /api/v1/task/status
    
**参数**

没有参数

**回传**

回传所有 Recon 任务的状态。
  
```json
     [
       {
     	"taskName": "OmDeltaRequest",
     	"lastUpdatedTimestamp": 1605724099147,
     	"lastUpdatedSeqNumber": 186
       },
       ...
     ]
```
    
## 使用率

### GET /api/v1/utilization/fileCount
    
**参数**

* volume (可选)

    根据给定的卷名过滤结果。
    
* bucket (可选)

    根据给定的桶名过滤结果。
    
* fileSize (可选)
    根据给定的文件大小筛选结果。
    
**回传**

回传不同文件范围内的文件计数，其中响应对象中的`fileSize`是文件大小范围的上限。
    
```json
     [{
     	"volume": "vol-2-04168",
     	"bucket": "bucket-0-11685",
     	"fileSize": 1024,
     	"count": 1
     }, {
     	"volume": "vol-2-04168",
     	"bucket": "bucket-1-41795",
     	"fileSize": 1024,
     	"count": 1
     }, {
     	"volume": "vol-2-04168",
     	"bucket": "bucket-2-93377",
     	"fileSize": 1024,
     	"count": 1
     }, {
     	"volume": "vol-2-04168",
     	"bucket": "bucket-3-50336",
     	"fileSize": 1024,
     	"count": 2
     }]
```
    
## 指标

### GET /api/v1/metrics/:api
    
**参数**

请参阅 [Prometheus HTTP API 参考](https://prometheus.io/docs/prometheus/latest/querying/api/) 以获取完整的查询文档。

**回传**

这是 Prometheus 的代理端点，并回传与 Prometheus 端点相同的响应。
示例：/api/v1/metrics/query?query=ratis_leader_election_electionCount
    
```json
     {
       "status": "success",
       "data": {
         "resultType": "vector",
         "result": [
           {
             "metric": {
               "__name__": "ratis_leader_election_electionCount",
               "exported_instance": "33a5ac1d-8c65-4c74-a0b8-9314dfcccb42",
               "group": "group-03CA9397D54B",
               "instance": "ozone_datanode_1:9882",
               "job": "ozone"
             },
             "value": [
               1599159384.455,
               "5"
             ]
           }
         ]
       }
     }
```


