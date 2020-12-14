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

### HTTP 端点

#### 容器

* **/containers**

    **URL 结构**
    ```
    GET /api/v1/containers
    ```

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

* **/containers/:id/keys**

    **URL 结构**
    ```
    GET /api/v1/containers/:id/keys
    ```
    
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
* **/containers/missing**
    
    **URL 结构**
    ```
    GET /api/v1/containers/missing
    ```
    
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
* **/containers/:id/replicaHistory**

    **URL 结构**
    ```
    GET /api/v1/containers/:id/replicaHistory
    ```
    
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
* **/containers/unhealthy**

    **URL 结构**
     ```
     GET /api/v1/containers/unhealthy
     ```
     
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
     
* **/containers/unhealthy/:state**

    **URL 结构**
    ```
    GET /api/v1/containers/unhealthy/:state
    ```
     
    **参数**
    
    * batchNum (可选)
    
        回传结果的批号(如“页码”)。
        传递1，将回传记录1到limit。传递2，将回传limit + 1到2 * limit，依此类推。
        
    * limit (可选)
    
        只回传有限数量的结果。默认限制是1000。
        
    **回传**
    
    回传处于给定状态的容器的 UnhealthyContainerMetadata 对象。
    不健康的容器状态可能为`MISSING`, `MIS_REPLICATED`, `UNDER_REPLICATED`, `OVER_REPLICATED`。
    响应结构与`/containers/unhealthy`相同。
    
#### 集群状态

* **/clusterState**

    **URL 结构**
    ```
    GET /api/v1/clusterState
    ```
     
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
     
#### 数据节点

* **/datanodes**

    **URL 结构**
    ```
    GET /api/v1/datanodes
    ```
    
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
     
#### 管道

* **/pipelines**

    **URL 结构**
    ```
    GET /api/v1/pipelines
    ```
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

#### 任务

* **/task/status**

    **URL 结构**
    ```
    GET /api/v1/task/status
    ```
    
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
    
#### 使用率

* **/utilization/fileCount**

    **URL 结构**
    ```
    GET /api/v1/utilization/fileCount
    ```
    
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
    
#### <a name="metrics"></a> 指标

* **/metrics/:api**

    **URL 结构**
    ```
    GET /api/v1/metrics/:api
    ```
    
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


