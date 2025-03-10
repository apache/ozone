/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.container.replication;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;

/**
 * Class to correct over replicated QuasiClosed Stuck Ratis containers.
 */
public class QuasiClosedStuckOverReplicationHandler implements UnhealthyReplicationHandler {


  public QuasiClosedStuckOverReplicationHandler(final PlacementPolicy placementPolicy,
      final ConfigurationSource conf, final ReplicationManager replicationManager) {
  }

  @Override
  public int processAndSendCommands(Set<ContainerReplica> replicas, List<ContainerReplicaOp> pendingOps,
      ContainerHealthResult result, int remainingMaintenanceRedundancy)
      throws IOException {
    return 0;
  }
}
