---
title: Recon API
weight: 4
menu:
   main:
      parent: "Client Interfaces"
summary: Recon server supports HTTP endpoints to help troubleshoot and monitor Ozone cluster.
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

The Recon API v1 is a set of HTTP endpoints that help you understand the current 
state of an Ozone cluster and to troubleshoot if needed. 

## Containers 

### GET /api/v1/containers

**Parameters**

* prevKey (optional)

    Only returns the containers with ID greater than the given prevKey.
    Example: prevKey=1
    
* limit (optional)

    Only returns the limited number of results. The default limit is 1000.

**Returns**

Returns all the ContainerMetadata objects.

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

**Parameters**

* prevKey (optional)

    Only returns the keys that are present after the given prevKey key prefix.
    Example: prevKey=/vol1/bucket1/key1
    
* limit (optional)

    Only returns the limited number of results. The default limit is 1000.  

**Returns**

Returns all the KeyMetadata objects for the given ContainerID.
    
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

**Parameters**

No parameters.  

**Returns**

Returns the MissingContainerMetadata objects for all the missing containers.
    
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

**Parameters**

No parameters.  

**Returns**

Returns all the ContainerHistory objects for the given ContainerID.
    
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

 
**Parameters**

* batchNum (optional)

    The batch number (like "page number") of results to return.
    Passing 1, will return records 1 to limit. 2 will return
    limit + 1 to 2 * limit, etc.
    
* limit (optional)

    Only returns the limited number of results. The default limit is 1000.  

**Returns**

Returns the UnhealthyContainerMetadata objects for all the unhealthycontainers.
     
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
 
**Parameters**

* batchNum (optional)
     
   The batch number (like "page number") of results to return.
   Passing 1, will return records 1 to limit. 2 will return
   limit + 1 to 2 * limit, etc.
 
* limit (optional)

   Only returns the limited number of results. The default limit is 1000.  

**Returns**

Returns the UnhealthyContainerMetadata objects for the containers in the given state.
Possible unhealthy container states are `MISSING`, `MIS_REPLICATED`,`UNDER_REPLICATED`, `OVER_REPLICATED`.
The response structure is same as `/containers/unhealthy`.

## Namespace Metadata

### GET /api/v1/namespace/summary

**Parameters**

* path

   The path request in string without any protocol prefix.

**Returns**

Returns a basic summary of the path, including entity type and aggregate count 
of objects under the path.

`status` is `OK` if path exists, `PATH_NOT_FOUND` otherwise.

Example: /api/v1/namespace/summary?path=/
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

Example: /api/v1/namespace/summary?path=/volume1
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

Example: /api/v1/namespace/summary?path=/volume1/bucket1
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

Example: /api/v1/namespace/summary?path=/volume1/bucket1/dir
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

Example: /api/v1/namespace/summary?path=/volume1/bucket1/dir/nestedDir
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

If any `num` field is `-1`, the path request is not applicable to such an entity type.

### GET /api/v1/namespace/du

**Parameters**

* path

  The path request in string without any protocol prefix.

* files (optional)

  A boolean with a default value of `false`. If set to `true`, computes disk usage for keys 
  under the path.

* replica (optional)

  A boolean with a default value of `false`. If set to `true`, computes disk usage with replicated
size of keys.

**Returns**

Returns disk usage of all sub-paths under the path. Normalizes `path` fields, returns
total size of keys directly under the path as `sizeDirectKey`, and returns 
`size`/`sizeWithReplica` in number of bytes. 

`status` is `OK` if path exists, `PATH_NOT_FOUND` otherwise.

Example: /api/v1/namespace/du?path=/vol1/bucket1&files=true&replica=true
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
If `files` is set to `false`, sub-path `/vol1/bucket1/key1-1` is omitted.
If `replica` is set to `false`, `sizeWithReplica` returns `-1`. If the path's entity type
cannot have direct keys (Root, Volume), `sizeDirectKey` returns `-1`.

### GET /api/v1/namespace/quota

**Parameters**

* path

  The path request in string without any protocol prefix.

**Returns**

Returns the quota allowed and used under the path. Only volumes and buckets
have quota. Other types are not applicable to the quota request.

`status` is `OK` if the request is valid, `PATH_NOT_FOUND` if path doesn't exist,
`TYPE_NOT_APPLICABLE` if path exists, but the path's entity type is not applicable
to the request.

Example: /api/v1/namespace/quota?path=/vol
```json
    {
      "status": OK,
      "allowed": 200000,
      "used": 160000
    }
```

If quota is not set, `allowed` returns `-1`. More on [Quota in Ozone]
(https://ci-hadoop.apache.org/view/Hadoop%20Ozone/job/ozone-doc-master/lastSuccessfulBuild/artifact/hadoop-hdds/docs/public/feature/quota.html)


### GET /api/v1/namespace/dist

**Parameters**

* path

  The path request in string without any protocol prefix.

**Returns**

Returns the file size distribution of all keys under the path.

`status` is `OK` if the request is valid, `PATH_NOT_FOUND` if path doesn't exist,
`TYPE_NOT_APPLICABLE` if path exists, but the path is a key, which does not have
a file size distribution.

Example: /api/v1/namespace/dist?path=/
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

Recon keeps track of all keys with size from `1 KB` to `1 PB`. For keys smaller than `1 KB`,
map to the first bin (index); for keys larger than `1 PB`, map to the last bin (index).

Each index of `dist` is mapped to a file size range (e.g. `1 MB` - `2 MB`).

## ClusterState

### GET /api/v1/clusterState
 
**Parameters**

No parameters.  

**Returns**

Returns a summary of the current state of the Ozone cluster.

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

## Datanodes

### GET /api/v1/datanodes
 
**Parameters**

No parameters.  

**Returns**

Returns all the datanodes in the cluster.

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
  
## Pipelines

### GET /api/v1/pipelines
 
**Parameters**

No parameters.  

**Returns**

Returns all the pipelines in the cluster.

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
  
## Tasks

### GET /api/v1/task/status
 
**Parameters**

No parameters.  

**Returns**

Returns the status of all the Recon tasks.

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

## Utilization

### GET /api/v1/utilization/fileCount
 
**Parameters**

* volume (optional)
     
   Filters the results based on the given volume name.
 
* bucket (optional)

   Filters the results based on the given bucket name.
   
* fileSize (optional)

   Filters the results based on the given fileSize.

**Returns**

Returns the file counts within different file ranges with `fileSize` in the
response object being the upper cap for file size range. 

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
  
## Metrics

### GET /api/v1/metrics/:api
 
**Parameters**

Refer to [Prometheus HTTP API Reference](https://prometheus.io/docs/prometheus/latest/querying/api/) 
for complete documentation on querying. 

**Returns**

This is a proxy endpoint for Prometheus and returns the same response as 
the prometheus endpoint. 
Example: /api/v1/metrics/query?query=ratis_leader_election_electionCount

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
  