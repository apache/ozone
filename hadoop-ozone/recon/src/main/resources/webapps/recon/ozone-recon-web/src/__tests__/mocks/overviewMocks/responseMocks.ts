export const ClusterState = {
  "deletedDirs": 0,
  "pipelines": 7,
  "totalDatanodes": 5,
  "healthyDatanodes": 3,
  "storageReport": {
    "capacity": 1352149585920,
    "used": 822805801,
    "remaining": 1068824879104
  },
  "containers": 20,
  "missingContainers": 2,
  "openContainers": 8,
  "deletedContainers": 10,
  "volumes": 2,
  "buckets": 24,
  "keys": 1424,
  "keysPendingDeletion": 2
}

export const TaskStatus = [
  {
    "taskName": "ContainerKeyMapperTask",
    "lastUpdatedTimestamp": 1701160650019,
    "lastUpdatedSeqNumber": 0
  },
  {
    "taskName": "FileSizeCountTask",
    "lastUpdatedTimestamp": 1701160650019,
    "lastUpdatedSeqNumber": 0
  },
  {
    "taskName": "OmTableInsightTask",
    "lastUpdatedTimestamp": 1701160650017,
    "lastUpdatedSeqNumber": 0
  },
  {
    "taskName": "NSSummaryTask",
    "lastUpdatedTimestamp": 1701160650018,
    "lastUpdatedSeqNumber": 0
  },
  {
    "taskName": "OmDeltaRequest",
    "lastUpdatedTimestamp": 1701160649982,
    "lastUpdatedSeqNumber": 299434
  },
  {
    "taskName": "OmSnapshotRequest",
    "lastUpdatedTimestamp": 1700001974734,
    "lastUpdatedSeqNumber": 3
  },
  {
    "taskName": "PipelineSyncTask",
    "lastUpdatedTimestamp": 1701160699829,
    "lastUpdatedSeqNumber": 0
  },
  {
    "taskName": "ContainerHealthTask",
    "lastUpdatedTimestamp": 1701160619972,
    "lastUpdatedSeqNumber": 0
  },
  {
    "taskName": "ContainerSizeCountTask",
    "lastUpdatedTimestamp": 0,
    "lastUpdatedSeqNumber": 0
  }
]

export const OpenKeys = {
  "keysSummary": {
    "totalUnreplicatedDataSize": 1024,
    "totalReplicatedDataSize": 4096,
    "totalOpenKeys": 12
  },
  "lastKey": "",
  "replicatedDataSize": 0,
  "unreplicatedDataSize": 0,
  "status": "OK"
}

export const DeletePendingSummary = {
  "keysSummary": {
    "totalUnreplicatedDataSize": 2048,
    "totalReplicatedDataSize": 4096,
    "totalDeletedKeys": 10
  },
  "lastKey": "",
  "replicatedDataSize": 0,
  "unreplicatedDataSize": 0,
  "status": "OK"
}