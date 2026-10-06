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

function missingContainer(containerID: number) {
  return {
    containerID,
    containerState: 'MISSING',
    unhealthySince: 1665590446222,
    expectedReplicaCount: 3,
    actualReplicaCount: 0,
    replicaDeltaCount: 3,
    reason: '',
    keys: 4,
    pipelineID: 'a10ffab6-8ed5-414a-aaf5-79890ff3e8a1',
    replicas: [],
  };
}

export const MissingContainersResponse = {
  missingCount: 25,
  underReplicatedCount: 15,
  overReplicatedCount: 12,
  misReplicatedCount: 13,
  replicaMismatchCount: 11,
  firstKey: 101,
  lastKey: 103,
  containers: [missingContainer(101), missingContainer(102), missingContainer(103)],
};

export const EmptyUnhealthyResponse = {
  missingCount: 0,
  underReplicatedCount: 0,
  overReplicatedCount: 0,
  misReplicatedCount: 0,
  replicaMismatchCount: 0,
  firstKey: 0,
  lastKey: 0,
  containers: [],
};

export const UnderReplicatedContainersResponse = {
  missingCount: 25,
  underReplicatedCount: 15,
  overReplicatedCount: 12,
  misReplicatedCount: 13,
  replicaMismatchCount: 11,
  firstKey: 201,
  lastKey: 201,
  containers: [{
    containerID: 201,
    containerState: 'UNDER_REPLICATED',
    unhealthySince: 1665591000000,
    expectedReplicaCount: 3,
    actualReplicaCount: 2,
    replicaDeltaCount: -1,
    reason: '',
    keys: 8,
    pipelineID: 'b20ggbc7-9fe6-525b-bbg6-80901gg4fb12',
    replicas: [],
  }],
};

export const OverReplicatedContainersResponse = {
  missingCount: 25,
  underReplicatedCount: 15,
  overReplicatedCount: 12,
  misReplicatedCount: 13,
  replicaMismatchCount: 11,
  firstKey: 301,
  lastKey: 301,
  containers: [{
    containerID: 301,
    containerState: 'OVER_REPLICATED',
    unhealthySince: 1665591000000,
    expectedReplicaCount: 3,
    actualReplicaCount: 4,
    replicaDeltaCount: 1,
    reason: '',
    keys: 1,
    pipelineID: 'c30hhcd8-0gf7-636c-cch7-91012hh5gc23',
    replicas: [],
  }],
};

export const QuasiClosedResponse = {
  quasiClosedCount: 2,
  firstKey: 401,
  lastKey: 401,
  containers: [{
    containerID: 401,
    pipelineID: 'a10ffab6-8ed5-414a-aaf5-79890ff3e8a1',
    keys: 3,
    stateEnterTime: 1665590446222,
    expectedReplicaCount: 3,
    actualReplicaCount: 3,
    replicas: [],
  }],
};

export const ExportJobsResponse: [] = [];
