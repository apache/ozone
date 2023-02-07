/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.ec.reconstruction;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex;

import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.IntStream;

import static java.util.Collections.unmodifiableSortedMap;
import static java.util.stream.Collectors.toMap;

/**
 * This class is to keep the required EC reconstruction info.
 */
public class ECReconstructionCommandInfo {
  private final SortedMap<Integer, DatanodeDetails> sourceNodeMap;
  private final SortedMap<Integer, DatanodeDetails> targetNodeMap;
  private final long containerID;
  private final ECReplicationConfig ecReplicationConfig;
  private final byte[] missingContainerIndexes;
  private final long deadlineMsSinceEpoch;
  private final long term;

  public ECReconstructionCommandInfo(ReconstructECContainersCommand cmd) {
    this.containerID = cmd.getContainerID();
    this.ecReplicationConfig = cmd.getEcReplicationConfig();
    this.missingContainerIndexes =
        Arrays.copyOf(cmd.getMissingContainerIndexes(),
            cmd.getMissingContainerIndexes().length);
    this.deadlineMsSinceEpoch = cmd.getDeadline();
    this.term = cmd.getTerm();

    sourceNodeMap = cmd.getSources().stream()
        .collect(toMap(
            DatanodeDetailsAndReplicaIndex::getReplicaIndex,
            DatanodeDetailsAndReplicaIndex::getDnDetails,
            (v1, v2) -> v1, TreeMap::new));
    targetNodeMap = IntStream.range(0, cmd.getTargetDatanodes().size())
        .boxed()
        .collect(toMap(
            i -> (int) missingContainerIndexes[i],
            i -> cmd.getTargetDatanodes().get(i),
            (v1, v2) -> v1, TreeMap::new));
  }

  public long getDeadline() {
    return deadlineMsSinceEpoch;
  }

  public long getContainerID() {
    return containerID;
  }

  public ECReplicationConfig getEcReplicationConfig() {
    return ecReplicationConfig;
  }

  SortedMap<Integer, DatanodeDetails> getSourceNodeMap() {
    return unmodifiableSortedMap(sourceNodeMap);
  }

  SortedMap<Integer, DatanodeDetails> getTargetNodeMap() {
    return unmodifiableSortedMap(targetNodeMap);
  }

  @Override
  public String toString() {
    return "ECReconstructionCommand{"
        + "containerID=" + containerID
        + ", replication=" + ecReplicationConfig.getReplication()
        + ", missingIndexes=" + Arrays.toString(missingContainerIndexes)
        + ", sources=" + sourceNodeMap
        + ", targets=" + targetNodeMap + "}";
  }

  public long getTerm() {
    return term;
  }

}
