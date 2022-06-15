/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container.replication;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

import java.util.List;
import java.util.Set;

/**
 * Interface used by ReplicationManager to check if containers are healthy or
 * not.
 */
public interface ContainerHealthCheck {

  ContainerHealthResult checkHealth(
      ContainerInfo container, Set<ContainerReplica> replicas,
      List<Pair<Integer, DatanodeDetails>> indexesPendingAdd,
      List<Pair<Integer, DatanodeDetails>> indexesPendingDelete,
      int remainingRedundancyForMaintenance);
}