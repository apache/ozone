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

The Recon API v1 offers a collection of HTTP endpoints designed to provide insights into the current state of an Ozone cluster, 
facilitating monitoring, management, and troubleshooting. These endpoints allow administrators to access critical cluster 
metadata, container status, key management, and more.

Endpoints that are marked as *admin only* can only be accessed by Kerberos users
specified in the **ozone.administrators** or **ozone.recon.administrators**
configurations of a secure cluster. See [Securing Ozone]({{< relref "../security/SecureOzone.md" >}}) for more information.
To restrict access to these endpoints, set the following configurations:

Property|Value
----------------------|---------
ozone.security.enabled| *true*
ozone.security.http.kerberos.enabled| *true*
ozone.acl.enabled| *true*

Access an interactive version of the API, complete with detailed descriptions and example requests, powered by Swagger [here]({{< relref "./SwaggerReconApi.md" >}})

## Containers (admin only)

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

Returns all the KeyMetadata objects for the given ContainerID. `lastKey` is the final key seen in
this page: pass it back as `prevKey` to continue paginating.

```json
    {
      "totalCount": 7,
      "lastKey": "/vol-1-73141/bucket-3-35816/key-0-43637",
      "keys": [
        {
          "Volume": "vol-1-73141",
          "Bucket": "bucket-3-35816",
          "Key": "key-0-43637",
          "CompletePath": "/vol-1-73141/bucket-3-35816/dir1/dir2/key-0-43637",
          "DataSize": 1000,
          "Versions": [0],
          "Blocks": {
            "0": [
              {
                "containerID": 1,
                "localID": 105232659753992201
              }
            ]
          },
          "CreationTime": "2020-11-18T18:09:17.722Z",
          "ModificationTime": "2020-11-18T18:09:30.405Z"
        }
      ]
    }
```

### GET /api/v1/containers/missing

> **Deprecated.** Use `/api/v1/containers/unhealthy/MISSING` instead.

**Parameters**

* limit (optional)

  Only returns the limited number of results. The default limit is 1000.


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
  
### GET /api/v1/containers/quasiClosed

**Parameters**

* limit (optional)

   Maximum number of containers to return. Default is 1000.

* minContainerId (optional)

   Cursor. Returns containers with ID greater than this value, in ascending order. Pass the
   previous response's `lastKey` to fetch the next page. Default is 0.

**Returns**

Returns containers currently in the `QUASI_CLOSED` lifecycle state. `quasiClosedCount` is the
cluster-wide total (not just the current page). When the page is empty, both `firstKey` and
`lastKey` echo back the `minContainerId` cursor.

```json
{
  "quasiClosedCount": 42,
  "firstKey": 100,
  "lastKey": 199,
  "containers": [
    {
      "containerID": 100,
      "pipelineID": "88646d32-a1aa-4e1a-a8d5-aa1e7dd3f5cc",
      "keys": 17,
      "stateEnterTime": 1718640123456,
      "expectedReplicaCount": 3,
      "actualReplicaCount": 2,
      "replicas": [
        {
          "containerID": 100,
          "datanodeUuid": "841be80f-0454-47df-b676",
          "datanodeHost": "localhost-1",
          "firstSeenTime": 1605724047057,
          "lastSeenTime": 1605731201301,
          "lastBcsId": 123,
          "state": "QUASI_CLOSED"
        }
      ]
    }
  ]
}
```

Responses:

* `400 Bad Request`: `limit` or `minContainerId` is negative.

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

* limit (optional)

    Only returns the limited number of results. The default limit is 1000.

* maxContainerId (optional)

    Upper bound for container IDs (exclusive). When specified, returns containers with IDs less
    than this value in descending order. Use it for backward pagination.

* minContainerId (optional)

    Lower bound for container IDs (exclusive). When `maxContainerId` is not specified, returns
    containers with IDs greater than this value in ascending order. Use it for forward pagination.

**Returns**

Returns the UnhealthyContainerMetadata objects for all the unhealthy containers.
     
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

* limit (optional)

   Only returns the limited number of results. The default limit is 1000.

* maxContainerId (optional)

   Upper bound for container IDs (exclusive). When specified, returns containers with IDs less
   than this value in descending order. Use it for backward pagination.

* minContainerId (optional)

   Lower bound for container IDs (exclusive). When `maxContainerId` is not specified, returns
   containers with IDs greater than this value in ascending order. Use it for forward pagination.

**Returns**

Returns the UnhealthyContainerMetadata objects for the containers in the given state.
Possible unhealthy container states are `MISSING`, `MIS_REPLICATED`, `UNDER_REPLICATED`, `OVER_REPLICATED`.
The response structure is same as `/containers/unhealthy`.


### GET /api/v1/containers/unhealthy/export

**Returns**

Lists every unhealthy-container export job currently tracked by Recon, in any status.
Items are `ExportJob` objects (see schema below).

```json
[
  {
    "jobId": "4f7a8b9c-1234-5678-9abc-def012345678",
    "state": "MISSING",
    "status": "RUNNING",
    "submittedAt": 1718640123456,
    "startedAt": 1718640124000,
    "completedAt": 0,
    "totalRecords": 250,
    "estimatedTotal": 1000,
    "fileName": "",
    "errorMessage": null,
    "progressPercent": 25,
    "queuePosition": 0,
    "downloadCount": 0,
    "downloadsRemaining": 3
  }
]
```

### POST /api/v1/containers/unhealthy/export

**Parameters**

* state (required)

    One of `MISSING`, `MIS_REPLICATED`, `UNDER_REPLICATED`, `OVER_REPLICATED`.

**Returns**

Submits a new CSV export job and returns the `ExportJob` with the assigned `jobId`.
The job initially has `status: QUEUED`.

* `400 Bad Request`: `state` is missing or not a valid unhealthy state.
* `429 Too Many Requests`: the export queue is full; retry later. Body: `{ "error": "Too Many Requests", "message": "<reason>" }`.

### GET /api/v1/containers/unhealthy/export/:jobId

**Returns**

Returns the current `ExportJob` for the given `jobId`. `404 Not Found` if no job has that id.

### GET /api/v1/containers/unhealthy/export/:jobId/download

**Returns**

Streams the TAR archive produced by the export job. Response `Content-Type` is `application/x-tar` with
a `Content-Disposition: attachment` header carrying the export filename.

* `404 Not Found`: `jobId` is unknown or the on-disk file was removed.
* `409 Conflict`: the job has not reached `COMPLETED` status yet.
* `429 Too Many Requests`: the per-job download limit has been reached. Body: `{ "error": "Download limit reached", "message": "<reason>" }` (schema `DownloadLimitReachedError`).

### DELETE /api/v1/containers/unhealthy/export/:jobId

**Returns**

Cancels the export job. `200 OK` with empty body on success. `404 Not Found` if the job cannot be
cancelled (for example, it has already reached a terminal state).


### GET /api/v1/containers/mismatch

**Returns**

Returns the list of mis-matched containers between OM and SCM
* Containers are present in OM, but not in SCM.
* Containers are present in SCM, but not in OM.

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


**Parameters**

* prevKey (optional)

   Returns the set of deleted containers in SCM which are present in OM to find out
   list of keys mapped to such DELETED state containers after the given prevKey (ContainerId).
   Example: prevKey=5, skip containers till it seeks correctly to the previous containerId.

* limit (optional)

   Only returns the limited number of results. The default limit is 1000.

**Returns**

Returns the set of deleted containers in SCM which are present in OM to find out
list of keys mapped to such DELETED state containers.

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

### GET /api/v1/containers/deleted

**Parameters**

* limit (optional)

  Maximum number of DELETED containers to return. Default 1000.

* prevKey (optional)

  Previous container ID to skip. Use the last returned `containerId` to fetch the next page.
  Default 0.

**Returns**

Returns all DELETED containers in SCM along with their pipeline and replication info.

```json
[
  {
    "containerId": 12,
    "pipelineID": { "id": "1202e6bb-b7c1-4a85-8067-61374b069adb" },
    "containerState": "DELETED",
    "stateEnterTime": 1716123456789,
    "lastUsed": 1716123456789,
    "replicationConfig": {
      "replicationType": "RATIS",
      "replicationFactor": "THREE",
      "replicationNodes": 3
    },
    "replicationFactor": "THREE"
  }
]
```

### GET /api/v1/keys/open


**Parameters**

* prevKey (optional)

    Returns the set of keys/files which are open and present after the given prevKey id.
    Example: prevKey=/vol1/bucket1/key1, this will skip keys till it seeks correctly to the given prevKey.

* limit (optional)

    Only returns the limited number of results. The default limit is 1000.

* startPrefix (optional)

    Restricts the listing to keys matching this prefix. Must be at bucket level or deeper
    (e.g. `/vol1/bucket1` or `/vol1/bucket1/dir1`); shallower prefixes return `400 Bad Request`.

* includeFso (optional)

    Boolean, default `false`. Include keys/files from FSO buckets in the result.

* includeNonFso (optional)

    Boolean, default `false`. Include keys/files from non-FSO (OBS / LEGACY) buckets.

If neither `includeFso` nor `includeNonFso` is `true`, the response will be empty.

**Returns**

Returns set of keys/files which are open. FSO and non-FSO keys are reported in separate arrays.

```json
{
  "lastKey": "/vol1/fso-bucket/dir1/dir2/file2",
  "replicatedDataSize": 13824,
  "unreplicatedDataSize": 4608,
  "status": "OK",
  "fso": [
    {
      "key": "/-9223372036854775552/-9223372036854774016/file1",
      "path": "/vol1/fso-bucket/dir1/file1",
      "inStateSince": 1667564193026,
      "size": 1024,
      "replicatedSize": 3072,
      "replicationInfo": {
        "replicationFactor": "THREE",
        "requiredNodes": 3,
        "replicationType": "RATIS"
      },
      "creationTime": 1667564000000,
      "modificationTime": 1667564193026,
      "isKey": true
    }
  ],
  "nonFSO": [
    {
      "key": "/vol1/bucket1/key1",
      "path": "/vol1/bucket1/key1",
      "inStateSince": 1667564193026,
      "size": 1024,
      "replicatedSize": 3072,
      "replicationInfo": {
        "replicationFactor": "THREE",
        "requiredNodes": 3,
        "replicationType": "RATIS"
      },
      "creationTime": 1667564000000,
      "modificationTime": 1667564193026,
      "isKey": true
    }
  ]
}
```

### GET /api/v1/keys/open/summary

**Returns**

Returns a flat summary of all currently-open keys across the cluster.

```json
{
  "totalOpenKeys": 8,
  "totalReplicatedDataSize": 90000,
  "totalUnreplicatedDataSize": 30000
}
```

### GET /api/v1/keys/open/mpu/summary

**Returns**

Returns a flat summary of all currently-open multipart-upload keys across the cluster. Note that
the unreplicated total is reported as `totalDataSize` (not `totalUnreplicatedDataSize`): the
naming differs from `/keys/open/summary`.

```json
{
  "totalOpenMPUKeys": 2,
  "totalReplicatedDataSize": 90000,
  "totalDataSize": 30000
}
```

### GET /api/v1/keys/deletePending


**Parameters**

* prevKey (optional)

  Returns the set of keys/files pending for deletion that are present after the given prevKey id.
  Example: prevKey=/vol1/bucket1/key1, this will skip keys till it seeks correctly to the given prevKey.

* limit (optional)

  Only returns the limited number of results. The default limit is 1000.

* startPrefix (optional)

  Restricts the listing to keys matching this prefix. Must be at bucket level or deeper
  (e.g. `/vol1/bucket1` or `/vol1/bucket1/dir1`); shallower prefixes return `400 Bad Request`.

**Returns**

Returns the set of keys/files pending deletion, paired with aggregated size totals. Each item in
`deletedKeyInfo` is a `RepeatedOmKeyInfo` (a wrapper around one or more `OmKeyInfo` entries).

```json
{
  "lastKey": "sampleVol/bucketOne/key_one",
  "replicatedDataSize": 1800000,
  "unreplicatedDataSize": 600000,
  "deletedKeyInfo": [
    {
      "omKeyInfoList": [
        {
          "volumeName": "sampleVol",
          "bucketName": "bucketOne",
          "keyName": "key_one",
          "dataSize": 200000,
          "replicatedSize": 600000,
          "replicationConfig": {
            "replicationFactor": "THREE",
            "requiredNodes": 3,
            "replicationType": "RATIS"
          },
          "creationTime": 1717000000000,
          "modificationTime": 1717100000000
        }
      ]
    },
    ...
  ],
  "status": "OK"
}
```

### GET /api/v1/keys/deletePending/dirs


**Parameters**

* prevKey (optional)

   Returns the set of directories pending for deletion that are present after the given prevKey id.
   Example: prevKey=/vol1/bucket1/bucket1/dir1, this will skip directories till it seeks correctly to the given prevKey.

* limit (optional)

   Only returns the limited number of results. The default limit is 1000.

**Returns**

Returns the set of directories pending for deletion. Each entry in `deletedDirInfo` is a
`KeyEntityInfo` describing one pending-delete directory (not a `RepeatedOmKeyInfo` like
`/keys/deletePending`).

```json
{
  "lastKey": "/vol1/bucket1/dir1",
  "replicatedDataSize": 13824,
  "unreplicatedDataSize": 4608,
  "deletedDirInfo": [
    {
      "key": "/-9223372036854775552/-9223372036854774016/dir1",
      "path": "/vol1/bucket1/dir1",
      "inStateSince": 1717000000000,
      "size": 4608,
      "replicatedSize": 13824,
      "replicationInfo": {
        "replicationFactor": "THREE",
        "requiredNodes": 3,
        "replicationType": "RATIS"
      },
      "creationTime": 1716900000000,
      "modificationTime": 1716999999999,
      "isKey": false
    }
  ],
  "status": "OK"
}
```

### GET /api/v1/keys/deletePending/summary

**Returns**

Returns a flat summary of all keys pending deletion across the cluster.

```json
{
  "totalDeletedKeys": 8,
  "totalReplicatedDataSize": 90000,
  "totalUnreplicatedDataSize": 30000
}
```

### GET /api/v1/keys/deletePending/dirs/summary

**Returns**

Returns the total count of directories pending deletion.

```json
{
  "totalDeletedDirectories": 5
}
```

### GET /api/v1/keys/listKeys

**Parameters**

* startPrefix (optional, but effectively required)

  Bucket-level or deeper prefix (e.g. `/vol1/bucket1` or `/vol1/bucket1/dir1`). HTTP-level the
  parameter is optional (defaults to `/`), but the handler rejects anything shallower than
  bucket level with `400 Bad Request`, so in practice callers must supply one.

* replicationType (optional)

  Filter by replication type (e.g. `RATIS`, `EC`).

* creationDate (optional)

  Filter by creation date; only keys created on or after this date are returned.

* keySize (optional)

  Filter to keys with data size at least this many bytes. Default 0.

* prevKey (optional)

  Pagination cursor. Pass back the `lastKey` from the previous response to continue iteration.

* limit (optional)

  Maximum number of keys to return. Default 1000.

**Returns**

Returns committed keys (and files in FSO buckets) under the given prefix.

* `200 OK` with a `ListKeysResponse` body.
* `204 No Content` when no keys matched the given filters.
* `400 Bad Request` when `startPrefix` is missing or shallower than bucket level.
* `503 Service Unavailable` while Recon is still bootstrapping OM DB; response body status is `INITIALIZING`.

```json
{
  "status": "OK",
  "path": "/vol1/bucket1",
  "replicatedDataSize": 600000,
  "unReplicatedDataSize": 200000,
  "lastKey": "/vol1/bucket1/dir1/file42",
  "keys": [
    {
      "key": "/vol1/bucket1/dir1/file42",
      "path": "/vol1/bucket1/dir1/file42",
      "size": 1048576,
      "replicatedSize": 3145728,
      "replicationInfo": {
        "replicationFactor": "THREE",
        "requiredNodes": 3,
        "replicationType": "RATIS"
      },
      "creationTime": 1717000000000,
      "modificationTime": 1717100000000,
      "isKey": true
    }
  ]
}
```

## Blocks Metadata (admin only)
### GET /api/v1/blocks/deletePending


**Parameters**

* prevKey (optional)

  Only returns the list of blocks pending for deletion, that are present after the given block id (prevKey).
  Example: prevKey=4, this will skip deletedBlocks table key to skip records before prevKey.

* limit (optional)

  Only returns the limited number of results. The default limit is 1000.

**Returns**

Returns list of blocks pending for deletion.

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

## Namespace Metadata (admin only)

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

### GET /api/v1/namespace/usage

**Parameters**

* path

  The path request in string without any protocol prefix.

* files (optional)

  A boolean with a default value of `false`. If set to `true`, computes namespace usage for keys 
  under the path.

* replica (optional)

  A boolean with a default value of `false`. If set to `true`, computes namespace usage with replicated
size of keys.

**Returns**

Returns the namespace usage of all sub-paths under the path. Normalizes `path` fields, returns
total size of keys directly under the path as `sizeDirectKey`, and returns 
`size`/`sizeWithReplica` in number of bytes. 

`status` is `OK` if path exists, `PATH_NOT_FOUND` otherwise.

Example: /api/v1/namespace/usage?path=/vol1/bucket1&files=true&replica=true
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
    "remaining": 597361258496,
    "committed": 27007111,
    "reserved": 31457280,
    "minimumFreeSpace": 20480,
    "filesystemCapacity": 1081730000000,
    "filesystemUsed": 1310000000,
    "filesystemAvailable": 597361258496
  },
  "containers": 26,
  "missingContainers": 0,
  "openContainers": 5,
  "deletedContainers": 1,
  "volumes": 6,
  "buckets": 26,
  "keys": 25,
  "keysPendingDeletion": 0,
  "deletedDirs": 0,
  "scmServiceId": "scmservice",
  "omServiceId": "omservice"
}
```

## Volumes (admin only)

### GET /api/v1/volumes

**Parameters**

* prevKey (optional)

  Only returns the volume after the given prevKey.
  Example: prevKey=vol1

* limit (optional)

  Only returns the limited number of results. The default limit is 1000.

**Returns**

Returns all the volumes in the cluster.

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

## Buckets (admin only)

### GET /api/v1/buckets

**Parameters**

* volume (optional)

  The volume in string without any protocol prefix.

* prevKey (optional)

  Only returns the bucket after the given prevKey. prevKey is ignored if volume is not specified.
  Example: prevKey=bucket1

* limit (optional)

  Only returns the limited number of results. The default limit is 1000.
  

**Returns**

Returns all the buckets in the cluster if volume is not specified or it is an empty string. 
If `volume` is specified, it returns only the buckets under `volume`.

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

## Datanodes

### GET /api/v1/datanodes
 
**Parameters**

No parameters.  

**Returns**

Returns all the datanodes in the cluster.

```json
{
  "totalCount": 4,
  "datanodes": [
    {
      "uuid": "f8f8cb45-3ab2-4123",
      "hostname": "localhost-1",
      "state": "HEALTHY",
      "opState": "IN_SERVICE",
      "lastHeartbeat": 1605738400544,
      "storageReport": {
        "capacity": 270429917184,
        "used": 358805504,
        "remaining": 270071111680,
        "committed": 27007111,
        "reserved": 31457280,
        "minimumFreeSpace": 20480,
        "filesystemCapacity": 270461374464,
        "filesystemUsed": 390262784,
        "filesystemAvailable": 270071111680
      },
      "pipelines": [
        { "pipelineID": "b9415b20-b9bd-4225", "replicationType": "RATIS", "replicationFactor": 3, "leaderNode": "localhost-2" },
        { "pipelineID": "3bf4a9e9-69cc-4d20", "replicationType": "RATIS", "replicationFactor": 1, "leaderNode": "localhost-1" }
      ],
      "containers": 17,
      "openContainers": 4,
      "leaderCount": 1,
      "version": "2.0.0",
      "setupTime": 1605700000000,
      "revision": "abcdef1",
      "layoutVersion": 6,
      "networkLocation": "/default-rack"
    },
    ...
  ]
}
```

### PUT /api/v1/datanodes/remove

**Parameters**

* uuids (List of node uuids in JSON array format).

```json
[
  "50ca4c95-2ef3-4430-b944-97d2442c3daf"
]
```

**Returns**

Returns a `datanodesResponseMap` keyed by the outcome category. Each value is a `DatanodesResponse`
(same shape as `GET /api/v1/datanodes`). Categories that have no entries for a given request are
omitted (not present as empty arrays).

* `removedDatanodes`: successfully removed.
* `failedDatanodes`: pre-checks failed (e.g. node is not DEAD, or still has open containers/pipelines). Includes `totalCount` and a per-uuid `errors` map describing the failure reason; `datanodes` is empty.
* `notFoundDatanodes`: uuid did not match any known datanode.

```json
{
  "datanodesResponseMap": {
    "removedDatanodes": {
      "totalCount": 1,
      "datanodes": [
        {
          "uuid": "50ca4c95-2ef3-4430-b944-97d2442c3daf",
          "hostname": "ozone-datanode-4.ozone_default",
          "state": "DEAD"
        }
      ]
    },
    "failedDatanodes": {
      "totalCount": 1,
      "datanodes": [],
      "errors": {
        "60ca4c95-...": "Open Containers/Pipelines"
      }
    }
  }
}
```

### GET /api/v1/datanodes/decommission/info

**Parameters**

No parameters.

**Returns**

Returns info for every datanode currently in the `DECOMMISSIONING` state. Each entry wraps the
datanode details, the per-state container list, and decommission metrics from the SCM JMX bean
`Hadoop:service=StorageContainerManager,name=NodeDecommissionMetrics`.

```json
{
  "DatanodesDecommissionInfo": [
    {
      "datanodeDetails": {
        "uuid": "f8f8cb45-3ab2-4123",
        "hostName": "ozone-datanode-3",
        "ipAddress": "10.0.0.13",
        "persistedOpState": "DECOMMISSIONING"
      },
      "metrics": {
        "decommissionStartTime": "2024-05-01T10:00:00Z",
        "numOfUnclosedContainers": 2,
        "numOfUnclosedPipelines": 0,
        "numOfUnderReplicatedContainers": 1
      },
      "containers": {
        "OPEN": ["#1234"],
        "CLOSED": ["#1235", "#1236"]
      }
    }
  ]
}
```

### GET /api/v1/datanodes/decommission/info/datanode

Returns info for a single decommissioning datanode. Provide either `uuid` or `ipAddress`. If both
are passed, `uuid` wins. Omitting both returns an error.

**Parameters**

* uuid (optional)

   UUID of the decommissioning datanode.

* ipAddress (optional)

   IP address of the decommissioning datanode. Used when `uuid` is not provided.

**Returns**

Same shape as `/api/v1/datanodes/decommission/info`, but the array contains at most one entry.

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

### GET /api/v1/utilization/containerCount

**Parameters**

* containerSize (optional)

  Filters the results based on the given container size. The smallest container size being tracked for count is 512 MB (512000000 bytes).

**Returns**

Returns the container counts within different container size ranges, with `containerSize` representing the size range and `count` representing the number of containers within that range.

```json
    [{
      "containerSize": 2147483648,
      "count": 9
    }, {
      "containerSize": 1073741824,
      "count": 3
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

## Storage Distribution (admin only)

### GET /api/v1/storageDistribution

**Parameters**

No parameters.

**Returns**

Aggregated storage capacity distribution across the cluster, including the global storage hierarchy
(filesystem capacity, Ozone capacity, used/free/reserved/committed space), namespace totals, a
breakdown of used space (open vs finalized), and per-datanode storage reports.

`500 Internal Server Error` (text/plain body) is returned if the report cannot be produced.

```json
{
  "globalStorage": {
    "totalFileSystemCapacity": 270461374464,
    "totalReservedSpace": 31457280,
    "totalOzoneCapacity": 270429917184,
    "totalOzoneUsedSpace": 358805504,
    "totalOzoneFreeSpace": 270071111680,
    "totalOzoneCommittedSpace": 27007111,
    "totalMinimumFreeSpace": 20480
  },
  "globalNamespace": {
    "totalUsedSpace": 500000000,
    "totalKeys": 10000
  },
  "usedSpaceBreakdown": {
    "openKeyBytes": {
      "openKeyAndFileBytes": 13824,
      "multipartOpenKeyBytes": 4096,
      "totalOpenKeyBytes": 17920
    },
    "finalizedKeyBytes": 450000000
  },
  "dataNodeUsage": [
    {
      "datanodeUuid": "841be80f-0454-47df-b676",
      "hostName": "ozone-datanode-1",
      "capacity": 270429917184,
      "used": 358805504,
      "remaining": 270071111680,
      "committed": 27007111,
      "minimumFreeSpace": 20480,
      "reserved": 31457280,
      "filesystemCapacity": 270461374464,
      "filesystemUsed": 390262784,
      "filesystemAvailable": 270071111680
    }
  ]
}
```

### GET /api/v1/storageDistribution/download

**Parameters**

No parameters.

**Returns**

Triggers or polls a background per-datanode metrics collection. The response varies by collection
state:

* `200 OK` (`text/csv`) when collection is FINISHED. The CSV columns are HostName, Datanode UUID,
  Filesystem Capacity, Filesystem Used Space, Filesystem Remaining Space, Ozone Capacity, Ozone Used
  Space, Ozone Remaining Space, PreAllocated Container Space, Reserved Space, Minimum Free Space,
  Pending Block Size. A `Content-Disposition: attachment` header carries the file name.
* `202 Accepted` (`application/json`, body matches `DataNodeMetricsServiceResponse`) when collection
  is NOT_STARTED or IN_PROGRESS. Poll the endpoint again until status is FINISHED.
* `500 Internal Server Error` (`text/plain`) if collection is marked FINISHED but the metrics data
  is missing.

## Pending Deletion (admin only)

### GET /api/v1/pendingDeletion

Returns pending-deletion statistics for one of the three Ozone components.

**Parameters**

* component (required)

  One of `scm`, `om`, `dn`. Selects the source whose pending-deletion data should be returned.

* limit (optional)

  Maximum number of per-datanode entries to return. Only applies when `component=dn`. Must be at
  least 1.

**Returns**

The response body depends on `component`:

* `component=scm`
  * `200 OK` with a `ScmPendingDeletion` object (`totalBlocksize`, `totalReplicatedBlockSize`,
    `totalBlocksCount`).
  * `204 No Content` if SCM has no pending-deletion summary yet.
* `component=om`
  * `200 OK` with a map keyed by category (typical keys: `pendingDirectorySize`,
    `pendingKeySize`). Values are byte counts.
* `component=dn`
  * `200 OK` with a `DataNodeMetricsServiceResponse` body when the background metrics collection
    has FINISHED.
  * `202 Accepted` with the same shape while collection is NOT_STARTED or IN_PROGRESS; poll until
    `status` becomes `FINISHED`.

`400 Bad Request` (text/plain) is returned when `component` is missing/invalid, or when
`component=dn` and `limit < 1`.

```json
{
  "totalBlocksize": 10485760,
  "totalReplicatedBlockSize": 31457280,
  "totalBlocksCount": 500
}
```

## Heat Map (admin only)

Read-access heatmap data is feature-gated. If the HeatMap feature is listed by
`/api/v1/features/disabledFeatures`, `/api/v1/heatmap/readaccess` returns `404 Not Found`.

### GET /api/v1/heatmap/readaccess

**Parameters**

* startDate (optional)

  Look-back window for access aggregation. Default `24H`.

* entityType (optional)

  Entity granularity. Default `key`.

* path (optional)

  Restrict the heatmap to this path prefix.

**Returns**

A nested `EntityReadAccessHeatMap` tree. The root represents `/`; children represent volumes, then
buckets, then directories, then keys. Each node carries `size`, `accessCount`,
`minAccessCount`/`maxAccessCount`, and a normalized `color` value.

```json
{
  "label": "root",
  "path": "/",
  "size": 12345678,
  "accessCount": 1000,
  "minAccessCount": 0,
  "maxAccessCount": 250,
  "color": 0.5,
  "children": [
    {
      "label": "vol1",
      "path": "/vol1",
      "size": 8345678,
      "accessCount": 750,
      "color": 0.75,
      "children": []
    }
  ]
}
```

### GET /api/v1/heatmap/healthCheck

**Returns**

Health-check response from the configured HeatMap provider. The body shape depends on the provider
implementation.

## Features (admin only)

### GET /api/v1/features/disabledFeatures

**Returns**

JSON array of feature enum names that are currently disabled. The only feature name in use today
is `HEATMAP`. Useful for the UI to decide whether to show or grey out feature-gated controls.

```json
["HEATMAP"]
```

## Admin Utilities (admin only)

### GET /api/v1/triggerdbsync/om

**Returns**

Requests Recon to start an immediate sync from the Ozone Manager DB. Returns a boolean indicating
whether the sync request was accepted by the OM service provider.

```json
true
```

### POST /api/v1/triggerdbsync/scm/snapshot

**Returns**

Starts a one-shot SCM DB snapshot sync in the background. Idempotent. The response always carries
the current `ScmDbSnapshotSyncStatus` so callers can distinguish "accepted and started" from
"rejected because another sync is already in progress".

* `202 Accepted`: sync accepted and started. Body has `accepted: true`.
* `409 Conflict`: another SCM DB sync is already running. Body has `accepted: false`.

```json
{
  "accepted": true,
  "status": "IN_PROGRESS",
  "message": "SCM DB snapshot sync started."
}
```

### GET /api/v1/triggerdbsync/scm/snapshot/status

**Returns**

Current status of the triggered SCM DB snapshot sync. Always returns 200, even when no sync has
ever run (status will be `IDLE`, phase `NONE`, `startedAt`/`finishedAt` zero).

* `status`: one of `IDLE`, `IN_PROGRESS`, `SUCCESS`, `FAILED`, `CANCELLED`.
* `phase`: one of `NONE`, `DOWNLOADING_CHECKPOINT`, `INITIALIZING_DB`, `SWAPPING_DB`,
  `COMPLETED`, `FAILED`, `CANCELLED`.
* `cancelAllowed`: true only while in `DOWNLOADING_CHECKPOINT`. Once the phase advances to
  `INITIALIZING_DB`, cancellation is no longer honored.
* `durationMs`: elapsed time in millis; for a running sync, computed against `now()`.

```json
{
  "status": "IN_PROGRESS",
  "phase": "DOWNLOADING_CHECKPOINT",
  "startedAt": 1718640123456,
  "finishedAt": 0,
  "durationMs": 12345,
  "cancelAllowed": true,
  "lastError": null
}
```

### POST /api/v1/triggerdbsync/scm/snapshot/cancel

**Returns**

Cancels an in-progress SCM DB snapshot sync. Only honored while `status == IN_PROGRESS` and
`cancelAllowed == true` (see `/triggerdbsync/scm/snapshot/status`).

* `200 OK`: cancellation accepted and the sync has been cancelled. Body has `cancelled: true`.
* `409 Conflict`: no sync is running, or the sync has passed the cancellable phase. Body has
  `cancelled: false` and `message` explains which.

```json
{
  "cancelled": true,
  "status": "CANCELLED",
  "phase": "CANCELLED",
  "message": "SCM DB snapshot sync cancelled."
}
```
