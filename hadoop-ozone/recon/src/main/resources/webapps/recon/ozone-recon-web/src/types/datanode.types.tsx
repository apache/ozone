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

// Corresponds to HddsProtos.NodeState
export const DatanodeStateList = ['HEALTHY', 'STALE', 'DEAD'] as const;
type DatanodeStateType = typeof DatanodeStateList;
export type DatanodeState = DatanodeStateType[number];

// Corresponds to HddsProtos.NodeOperationalState
export const DatanodeOpStateList = ['IN_SERVICE', 'DECOMMISSIONING', 'DECOMMISSIONED', 'ENTERING_MAINTENANCE', 'IN_MAINTENANCE'] as const;
type DatanodeOpStateType = typeof DatanodeOpStateList;
export type DatanodeOpState = DatanodeOpStateType[number];

export interface IStorageReport {
  capacity: number;
  used: number;
  remaining: number;
}
