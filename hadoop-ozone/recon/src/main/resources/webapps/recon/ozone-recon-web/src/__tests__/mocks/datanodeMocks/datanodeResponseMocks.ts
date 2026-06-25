/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

export const DatanodeResponse = {
  "totalCount": 5,
  "datanodes": [
      {
          "uuid": "1",
          "hostname": "ozone-datanode-1.ozone_default",
          "state": "HEALTHY",
          "opState": "IN_SERVICE",
          "lastHeartbeat": 1728280581608,
          "storageReport": {
              "capacity": 125645656770,
              "used": 4096,
              "remaining": 114225606656,
              "committed": 0,
              "filesystemCapacity": 150000000000,
              "filesystemUsed": 30000000000,
              "filesystemAvailable": 120000000000
          },
          "pipelines": [
              {
                  "pipelineID": "0f9f7bc0-505e-4428-b148-dd7eac2e8ac2",
                  "replicationType": "RATIS",
                  "replicationFactor": "THREE",
                  "leaderNode": "ozone-datanode-3.ozone_default"
              },
              {
                  "pipelineID": "2c23e76e-3f18-4b86-9541-e48bdc152fda",
                  "replicationType": "RATIS",
                  "replicationFactor": "ONE",
                  "leaderNode": "ozone-datanode-1.ozone_default"
              }
          ],
          "leaderCount": 1,
          "version": "2.0.0-SNAPSHOT",
          "setupTime": 1728280539733,
          "revision": "3f9953c0fbbd2175ee83e8f0b4927e45e9c10ac1",
          "buildDate": "2024-10-06T16:41Z",
          "layoutVersion": 8,
          "networkLocation": "/default-rack"
      },
      {
          "uuid": "3",
          "hostname": "ozone-datanode-3.ozone_default",
          "state": "DEAD",
          "opState": "IN_SERVICE",
          "lastHeartbeat": 1728280582060,
          "storageReport": {
              "capacity": 125645656770,
              "used": 4096,
              "remaining": 114225623040,
              "committed": 0,
              "filesystemCapacity": 150000000000,
              "filesystemUsed": 30000000000,
              "filesystemAvailable": 120000000000
          },
          "pipelines": [
              {
                  "pipelineID": "9c5bbf5e-62da-4d4a-a6ad-cb63d9f6aa6f",
                  "replicationType": "RATIS",
                  "replicationFactor": "ONE",
                  "leaderNode": "ozone-datanode-3.ozone_default"
              },
              {
                  "pipelineID": "0f9f7bc0-505e-4428-b148-dd7eac2e8ac2",
                  "replicationType": "RATIS",
                  "replicationFactor": "THREE",
                  "leaderNode": "ozone-datanode-3.ozone_default"
              }
          ],
          "leaderCount": 2,
          "version": "1.5.0-SNAPSHOT",
          "setupTime": 1728280539726,
          "revision": "3f9953c0fbbd2175ee83e8f0b4927e45e9c10ac1",
          "buildDate": "2024-10-06T16:41Z",
          "layoutVersion": 8,
          "networkLocation": "/default-rack"
      },
      {
          "uuid": "4",
          "hostname": "ozone-datanode-4.ozone_default",
          "state": "HEALTHY",
          "opState": "DECOMMISSIONING",
          "lastHeartbeat": 1728280581614,
          "storageReport": {
              "capacity": 125645656770,
              "used": 4096,
              "remaining": 114225541120,
              "committed": 0,
              "filesystemCapacity": 150000000000,
              "filesystemUsed": 30000000000,
              "filesystemAvailable": 120000000000
          },
          "pipelines": [
              {
                  "pipelineID": "4092a584-5c2f-40c6-98e5-ce9a9246e65d",
                  "replicationType": "RATIS",
                  "replicationFactor": "ONE",
                  "leaderNode": "ozone-datanode-4.ozone_default"
              }
          ],
          "leaderCount": 1,
          "version": "2.0.0-SNAPSHOT",
          "setupTime": 1728280540325,
          "revision": "3f9953c0fbbd2175ee83e8f0b4927e45e9c10ac1",
          "buildDate": "2024-10-06T16:41Z",
          "layoutVersion": 8,
          "networkLocation": "/default-rack"
      },
      {
          "uuid": "2",
          "hostname": "ozone-datanode-2.ozone_default",
          "state": "STALE",
          "opState": "IN_SERVICE",
          "lastHeartbeat": 1728280581594,
          "storageReport": {
              "capacity": 125645656770,
              "used": 4096,
              "remaining": 114225573888,
              "committed": 0,
              "filesystemCapacity": 150000000000,
              "filesystemUsed": 30000000000,
              "filesystemAvailable": 120000000000
          },
          "pipelines": [
              {
                  "pipelineID": "20a874e4-790b-4312-8fc2-ca53846dba0f",
                  "replicationType": "RATIS",
                  "replicationFactor": "ONE",
                  "leaderNode": "ozone-datanode-2.ozone_default"
              }
          ],
          "leaderCount": 1,
          "version": "2.0.0-SNAPSHOT",
          "setupTime": 1728280539745,
          "revision": "3f9953c0fbbd2175ee83e8f0b4927e45e9c10ac1",
          "buildDate": "2024-10-06T16:41Z",
          "layoutVersion": 8,
          "networkLocation": "/default-rack"
      },
      {
          "uuid": "5",
          "hostname": "ozone-DataNode-5.ozone_default",
          "state": "DEAD",
          "opState": "DECOMMISSIONED",
          "lastHeartbeat": 1728280582055,
          "storageReport": {
              "capacity": 125645656770,
              "used": 4096,
              "remaining": 114225614848,
              "committed": 0,
              "filesystemCapacity": 150000000000,
              "filesystemUsed": 30000000000,
              "filesystemAvailable": 120000000000
          },
          "pipelines": [
              {
                  "pipelineID": "0f9f7bc0-505e-4428-b148-dd7eac2e8ac2",
                  "replicationType": "RATIS",
                  "replicationFactor": "THREE",
                  "leaderNode": "ozone-datanode-3.ozone_default"
              },
              {
                  "pipelineID": "67c973a0-722a-403a-8893-b8a5faaed7f9",
                  "replicationType": "RATIS",
                  "replicationFactor": "ONE",
                  "leaderNode": "ozone-datanode-5.ozone_default"
              }
          ],
          "leaderCount": 1,
          "version": "2.0.0-SNAPSHOT",
          "setupTime": 1728280539866,
          "revision": "3f9953c0fbbd2175ee83e8f0b4927e45e9c10ac1",
          "buildDate": "2024-10-06T16:41Z",
          "layoutVersion": 8,
          "networkLocation": "/default-rack"
      }
  ]
}

export const NullDatanodeResponse = {
  "totalCount": null,
  "datanodes": [
      {
          "uuid": null,
          "hostname": null,
          "state": null,
          "opState": null,
          "lastHeartbeat": null,
          "storageReport": null,
          "pipelines": null,
          "leaderCount": null,
          "version": null,
          "setupTime": null,
          "revision": null,
          "buildDate": null,
          "layoutVersion": null,
          "networkLocation": null
      }
  ]
}

export const NullDatanodes = {
  "totalCount": null,
  "datanodes": null
}

export const DecommissionInfo = {
  "DatanodesDecommissionInfo": []
}