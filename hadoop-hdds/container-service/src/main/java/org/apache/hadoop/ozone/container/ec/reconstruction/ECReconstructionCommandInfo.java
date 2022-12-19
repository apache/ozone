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
import java.util.List;

/**
 * This class is to keep the required EC reconstruction info.
 */
public class ECReconstructionCommandInfo {
  private long containerID;
  private ECReplicationConfig ecReplicationConfig;
  private byte[] missingContainerIndexes;
  private List<ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex>
      sources;
  private List<DatanodeDetails> targetDatanodes;
  private long deadlineMsSinceEpoch = 0;
  private final long term;

  public ECReconstructionCommandInfo(ReconstructECContainersCommand cmd) {
    this.containerID = cmd.getContainerID();
    this.ecReplicationConfig = cmd.getEcReplicationConfig();
    this.missingContainerIndexes =
        Arrays.copyOf(cmd.getMissingContainerIndexes(),
            cmd.getMissingContainerIndexes().length);
    this.sources = cmd.getSources();
    this.targetDatanodes = cmd.getTargetDatanodes();
    this.deadlineMsSinceEpoch = cmd.getDeadline();
    this.term = cmd.getTerm();
  }

  public long getDeadline() {
    return deadlineMsSinceEpoch;
  }

  public long getContainerID() {
    return containerID;
  }

  public byte[] getMissingContainerIndexes() {
    return Arrays
        .copyOf(missingContainerIndexes, missingContainerIndexes.length);
  }

  public ECReplicationConfig getEcReplicationConfig() {
    return ecReplicationConfig;
  }

  public List<DatanodeDetailsAndReplicaIndex> getSources() {
    return sources;
  }

  public List<DatanodeDetails> getTargetDatanodes() {
    return targetDatanodes;
  }

  @Override
  public String toString() {
    return "ECReconstructionCommandInfo{"
        + "containerID=" + containerID
        + ", ecReplicationConfig=" + ecReplicationConfig
        + ", missingContainerIndexes=" + Arrays
        .toString(missingContainerIndexes)
        + ", sources=" + sources
        + ", targetDatanodes=" + targetDatanodes + '}';
  }

  public long getTerm() {
    return term;
  }
}
