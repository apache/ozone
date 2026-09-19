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

package org.apache.hadoop.hdds.scm.container.replication.health;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.scm.container.ContainerHealthState.DATA_CHECKSUM_MISMATCH;
import static org.apache.hadoop.hdds.scm.container.ContainerReplicaChecksumMismatch.hasMismatch;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects replicas with the same BCSID and different data checksums.
 */
public class DataChecksumMismatchCheckHandler extends AbstractCheck {

  private static final Logger LOG =
      LoggerFactory.getLogger(DataChecksumMismatchCheckHandler.class);

  private final Set<ContainerID> warnedMismatches = new HashSet<>();

  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo container = request.getContainerInfo();
    Set<ContainerReplica> replicas = request.getContainerReplicas();
    boolean mismatch = container.getState() == CLOSED &&
        container.getReplicationType() == RATIS &&
        hasMismatch(replicas, ContainerReplica::getSequenceId,
            ContainerReplica::getDataChecksum);
    ContainerID containerID = container.containerID();
    if (!mismatch) {
      if (!request.isReadOnly()) {
        warnedMismatches.remove(containerID);
      }
      return false;
    }

    request.getReport().incrementAndSampleAdditionalState(
        DATA_CHECKSUM_MISMATCH, containerID);
    if (!request.isReadOnly() && warnedMismatches.add(containerID)) {
      LOG.warn(
          "Container {} has replicas with the same BCSID but different data checksums: {}",
          containerID, formatChecksumDetails(replicas));
    }
    return false;
  }

  private static String formatChecksumDetails(Set<ContainerReplica> replicas) {
    return replicas.stream()
        .sorted()
        .map(replica -> replica.getDatanodeDetails().getUuidString() +
            "(BCSID=" + replica.getSequenceId() +
            ", dataChecksum=" +
            HddsUtils.checksumToString(replica.getDataChecksum()) + ")")
        .collect(Collectors.joining(", "));
  }
}
