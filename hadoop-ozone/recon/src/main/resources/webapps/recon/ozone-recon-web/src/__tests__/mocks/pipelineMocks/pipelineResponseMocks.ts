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

export const PipelinesResponse = {
  "totalCount": 6,
  "pipelines": [
      {
          "pipelineId": "pipeline-1",
          "status": "OPEN",
          "leaderNode": "ozone-datanode-1.ozone_default",
          "datanodes": [
              {
                  "level": 3,
                  "parent": null,
                  "cost": 0,
                  "uuid": "a5421a9b-ac50-4242-8d0e-a528e40da8c1",
                  "uuidString": "a5421a9b-ac50-4242-8d0e-a528e40da8c1",
                  "ipAddress": "0.0.0.0",
                  "hostName": "ozone-datanode-1.ozone_default",
                  "ports": [
                      {
                          "name": "HTTP",
                          "value": 9882
                      },
                      {
                          "name": "CLIENT_RPC",
                          "value": 19864
                      },
                      {
                          "name": "REPLICATION",
                          "value": 9886
                      },
                      {
                          "name": "RATIS",
                          "value": 9858
                      },
                      {
                          "name": "RATIS_ADMIN",
                          "value": 9857
                      },
                      {
                          "name": "RATIS_SERVER",
                          "value": 9856
                      },
                      {
                          "name": "RATIS_DATASTREAM",
                          "value": 9855
                      },
                      {
                          "name": "STANDALONE",
                          "value": 9859
                      }
                  ],
                  "certSerialId": null,
                  "version": null,
                  "setupTime": 0,
                  "revision": null,
                  "persistedOpState": "IN_SERVICE",
                  "persistedOpStateExpiryEpochSec": 0,
                  "initialVersion": 0,
                  "currentVersion": 2,
                  "ratisPort": {
                      "name": "RATIS",
                      "value": 9858
                  },
                  "decommissioned": false,
                  "maintenance": false,
                  "ipAddressAsByteString": {
                      "string": "0.0.0.0",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "hostNameAsByteString": {
                      "string": "ozone-datanode-1.ozone_default",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "restPort": null,
                  "standalonePort": {
                      "name": "STANDALONE",
                      "value": 9859
                  },
                  "networkName": "mock-network-1",
                  "networkLocation": "/default-rack",
                  "networkLocationAsByteString": {
                      "string": "/default-rack",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "networkNameAsByteString": {
                      "string": "mock-network-1",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "networkFullPath": "/default-rack/mock-network-1",
                  "numOfLeaves": 1
              }
          ],
          "lastLeaderElection": 0,
          "duration": 2700000,
          "leaderElections": 0,
          "replicationType": "RATIS",
          "replicationFactor": "THREE",
          "containers": 0
      },
      {
          "pipelineId": "pipeline-2",
          "status": "CLOSED",
          "leaderNode": "ozone-datanode-2.ozone_default",
          "datanodes": [
              {
                  "level": 3,
                  "parent": null,
                  "cost": 0,
                  "uuid": "c8254ecb-6c24-4c54-9834-76f3e74fd37a",
                  "uuidString": "c8254ecb-6c24-4c54-9834-76f3e74fd37a",
                  "ipAddress": "0.0.0.1",
                  "hostName": "ozone-datanode-2.ozone_default",
                  "ports": [
                      {
                          "name": "HTTP",
                          "value": 9882
                      },
                      {
                          "name": "CLIENT_RPC",
                          "value": 19864
                      },
                      {
                          "name": "REPLICATION",
                          "value": 9886
                      },
                      {
                          "name": "RATIS",
                          "value": 9858
                      },
                      {
                          "name": "RATIS_ADMIN",
                          "value": 9857
                      },
                      {
                          "name": "RATIS_SERVER",
                          "value": 9856
                      },
                      {
                          "name": "RATIS_DATASTREAM",
                          "value": 9855
                      },
                      {
                          "name": "STANDALONE",
                          "value": 9859
                      }
                  ],
                  "certSerialId": null,
                  "version": null,
                  "setupTime": 0,
                  "revision": null,
                  "persistedOpState": "IN_SERVICE",
                  "persistedOpStateExpiryEpochSec": 0,
                  "initialVersion": 0,
                  "currentVersion": 2,
                  "ratisPort": {
                      "name": "RATIS",
                      "value": 9858
                  },
                  "decommissioned": false,
                  "maintenance": false,
                  "ipAddressAsByteString": {
                      "string": "0.0.0.1",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "hostNameAsByteString": {
                      "string": "ozone-datanode-2.ozone_default",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "restPort": null,
                  "standalonePort": {
                      "name": "STANDALONE",
                      "value": 9859
                  },
                  "networkName": "mock-network-2",
                  "networkLocation": "/default-rack",
                  "networkLocationAsByteString": {
                      "string": "/default-rack",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "networkNameAsByteString": {
                      "string": "mock-network-2",
                      "bytes": {
                          "validUtf8": true,
                          "empty": false
                      }
                  },
                  "networkFullPath": "/default-rack/mock-network-2",
                  "numOfLeaves": 1
              }
          ],
          "lastLeaderElection": 0,
          "duration": 2700000,
          "leaderElections": 0,
          "replicationType": "RATIS",
          "replicationFactor": "ONE",
          "containers": 1
      },
      {
        "pipelineId": "pipeline-3",
        "status": "OPEN",
        "leaderNode": "ozone-datanode-3.ozone_default",
        "datanodes": [
            {
                "level": 3,
                "parent": null,
                "cost": 0,
                "uuid": "b557edd1-cce3-4320-834b-2e6f13ba98f1",
                "uuidString": "b557edd1-cce3-4320-834b-2e6f13ba98f1",
                "ipAddress": "0.0.0.2",
                "hostName": "ozone-datanode-3.ozone_default",
                "ports": [
                    {
                        "name": "HTTP",
                        "value": 9882
                    },
                    {
                        "name": "CLIENT_RPC",
                        "value": 19864
                    },
                    {
                        "name": "REPLICATION",
                        "value": 9886
                    },
                    {
                        "name": "RATIS",
                        "value": 9858
                    },
                    {
                        "name": "RATIS_ADMIN",
                        "value": 9857
                    },
                    {
                        "name": "RATIS_SERVER",
                        "value": 9856
                    },
                    {
                        "name": "RATIS_DATASTREAM",
                        "value": 9855
                    },
                    {
                        "name": "STANDALONE",
                        "value": 9859
                    }
                ],
                "certSerialId": null,
                "version": null,
                "setupTime": 0,
                "revision": null,
                "persistedOpState": "IN_SERVICE",
                "persistedOpStateExpiryEpochSec": 0,
                "initialVersion": 0,
                "currentVersion": 2,
                "ratisPort": {
                    "name": "RATIS",
                    "value": 9858
                },
                "decommissioned": false,
                "maintenance": false,
                "ipAddressAsByteString": {
                    "string": "0.0.0.2",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "hostNameAsByteString": {
                    "string": "ozone-datanode-3.ozone_default",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "restPort": null,
                "standalonePort": {
                    "name": "STANDALONE",
                    "value": 9859
                },
                "networkName": "mock-network-3",
                "networkLocation": "/default-rack",
                "networkLocationAsByteString": {
                    "string": "/default-rack",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "networkNameAsByteString": {
                    "string": "mock-network-3",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "networkFullPath": "/default-rack/mock-network-3",
                "numOfLeaves": 1
            },
            {
                "level": 3,
                "parent": null,
                "cost": 0,
                "uuid": "c8254ecb-6c24-4c54-9834-76f3e74fd37a",
                "uuidString": "c8254ecb-6c24-4c54-9834-76f3e74fd37a",
                "ipAddress": "0.0.0.1",
                "hostName": "ozone-datanode-2.ozone_default",
                "ports": [
                    {
                        "name": "HTTP",
                        "value": 9882
                    },
                    {
                        "name": "CLIENT_RPC",
                        "value": 19864
                    },
                    {
                        "name": "REPLICATION",
                        "value": 9886
                    },
                    {
                        "name": "RATIS",
                        "value": 9858
                    },
                    {
                        "name": "RATIS_ADMIN",
                        "value": 9857
                    },
                    {
                        "name": "RATIS_SERVER",
                        "value": 9856
                    },
                    {
                        "name": "RATIS_DATASTREAM",
                        "value": 9855
                    },
                    {
                        "name": "STANDALONE",
                        "value": 9859
                    }
                ],
                "certSerialId": null,
                "version": null,
                "setupTime": 0,
                "revision": null,
                "persistedOpState": "IN_SERVICE",
                "persistedOpStateExpiryEpochSec": 0,
                "initialVersion": 0,
                "currentVersion": 2,
                "ratisPort": {
                    "name": "RATIS",
                    "value": 9858
                },
                "decommissioned": false,
                "maintenance": false,
                "ipAddressAsByteString": {
                    "string": "0.0.0.4",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "hostNameAsByteString": {
                    "string": "ozone-datanode-2.ozone_default",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "restPort": null,
                "standalonePort": {
                    "name": "STANDALONE",
                    "value": 9859
                },
                "networkName": "mock-network-2",
                "networkLocation": "/default-rack",
                "networkLocationAsByteString": {
                    "string": "/default-rack",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "networkNameAsByteString": {
                    "string": "mock-network-2",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "networkFullPath": "/default-rack/mock-network-2",
                "numOfLeaves": 1
            },
            {
                "level": 3,
                "parent": null,
                "cost": 0,
                "uuid": "fc26783d-0ecf-43fe-9915-a76d6e276e39",
                "uuidString": "fc26783d-0ecf-43fe-9915-a76d6e276e39",
                "ipAddress": "0.0.0.3",
                "hostName": "ozone-datanode-4.ozone_default",
                "ports": [
                    {
                        "name": "HTTP",
                        "value": 9882
                    },
                    {
                        "name": "CLIENT_RPC",
                        "value": 19864
                    },
                    {
                        "name": "REPLICATION",
                        "value": 9886
                    },
                    {
                        "name": "RATIS",
                        "value": 9858
                    },
                    {
                        "name": "RATIS_ADMIN",
                        "value": 9857
                    },
                    {
                        "name": "RATIS_SERVER",
                        "value": 9856
                    },
                    {
                        "name": "RATIS_DATASTREAM",
                        "value": 9855
                    },
                    {
                        "name": "STANDALONE",
                        "value": 9859
                    }
                ],
                "certSerialId": null,
                "version": null,
                "setupTime": 0,
                "revision": null,
                "persistedOpState": "IN_SERVICE",
                "persistedOpStateExpiryEpochSec": 0,
                "initialVersion": 0,
                "currentVersion": 2,
                "ratisPort": {
                    "name": "RATIS",
                    "value": 9858
                },
                "decommissioned": false,
                "maintenance": false,
                "ipAddressAsByteString": {
                    "string": "0.0.0.3",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "hostNameAsByteString": {
                    "string": "ozone-datanode-4.ozone_default",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "restPort": null,
                "standalonePort": {
                    "name": "STANDALONE",
                    "value": 9859
                },
                "networkName": "mock-network-4",
                "networkLocation": "/default-rack",
                "networkLocationAsByteString": {
                    "string": "/default-rack",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "networkNameAsByteString": {
                    "string": "mock-network-4",
                    "bytes": {
                        "validUtf8": true,
                        "empty": false
                    }
                },
                "networkFullPath": "/default-rack/mock-network-4",
                "numOfLeaves": 1
            }
        ],
        "lastLeaderElection": 0,
        "duration": 3600000,
        "leaderElections": 0,
        "replicationType": "RATIS",
        "replicationFactor": "THREE",
        "containers": 0
    }
  ]
}
