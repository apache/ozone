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

export const totalCapacityDesc = 'The space configured for Ozone to use in the cluster. The actual disk space may be larger than what is allocated to Ozone.';

export const otherUsedSpaceDesc = 'This is the space occupied by other Ozone related files but not actual data stored by Ozone. This may include things like logs, configuration files, Rocks DB files etc.';

export const ozoneUsedSpaceDesc = 'These could also include potential missing space or extra occupied space due to situations like under-replication, over-replication, mismatched replicas, etc.';

export const datanodesPendingDeletionDesc = 'This is the unreplicated size and a cumulative value of all the blocks across all the datanodes in the cluster.';

export const nodeSelectorMessage = "This contains the list of the top 15 DNs by pending deletion size. The information on all the DNs is available as a CSV download."
