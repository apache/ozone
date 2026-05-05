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

package org.apache.hadoop.ozone.container.ec.reconstruction;

import static java.util.Collections.unmodifiableSortedMap;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reconstructECContainersCommand;

import com.google.protobuf.ByteString;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand;
import org.apache.hadoop.ozone.protocol.commands.ReconstructECContainersCommand.DatanodeDetailsAndReplicaIndex;

/**
 * This class is to keep the required EC reconstruction info.
 */
public class ECReconstructionCommandInfo {
  private final SortedMap<Integer, DatanodeDetails> sourceNodeMap;
  private final SortedMap<Integer, DatanodeDetails> targetNodeMap;
  private final long containerID;
  private final ECReplicationConfig ecReplicationConfig;
  private final ByteString missingContainerIndexes;
  private final long deadlineMsSinceEpoch;
  private final long term;

  public ECReconstructionCommandInfo(ReconstructECContainersCommand cmd) {
    this.containerID = cmd.getContainerID();
    this.ecReplicationConfig = cmd.getEcReplicationConfig();
    this.missingContainerIndexes = cmd.getMissingContainerIndexes();
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
            i -> (int) missingContainerIndexes.byteAt(i),
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
    return reconstructECContainersCommand
        + ": containerID=" + containerID
        + ", replication=" + ecReplicationConfig.getReplication()
        + ", missingIndexes=" + StringUtils.bytes2String(missingContainerIndexes.asReadOnlyByteBuffer())
        + ", sources=" + sourceNodeMap
        + ", targets=" + targetNodeMap;
  }

  public long getTerm() {
    return term;
  }

}
