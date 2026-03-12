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

package org.apache.hadoop.ozone.protocol.commands;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.HddsIdFactory;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto.Builder;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

/**
 * SCM command to request reconstruction of EC containers.
 */
public class ReconstructECContainersCommand
    extends SCMCommand<ReconstructECContainersCommandProto> {
  private final long containerID;
  private final List<DatanodeDetailsAndReplicaIndex> sources;
  private final List<DatanodeDetails> targetDatanodes;
  private final ByteString missingContainerIndexes;
  private final ECReplicationConfig ecReplicationConfig;

  public ReconstructECContainersCommand(long containerID,
      List<DatanodeDetailsAndReplicaIndex> sources,
      List<DatanodeDetails> targetDatanodes, ByteString missingContainerIndexes,
      ECReplicationConfig ecReplicationConfig) {
    this(containerID, sources, targetDatanodes, missingContainerIndexes,
        ecReplicationConfig, HddsIdFactory.getLongId());
  }

  public ReconstructECContainersCommand(long containerID,
      List<DatanodeDetailsAndReplicaIndex> sourceDatanodes,
      List<DatanodeDetails> targetDatanodes, ByteString missingContainerIndexes,
      ECReplicationConfig ecReplicationConfig, long id) {
    super(id);
    this.containerID = containerID;
    this.sources = sourceDatanodes;
    this.targetDatanodes = targetDatanodes;
    this.missingContainerIndexes = missingContainerIndexes;
    this.ecReplicationConfig = ecReplicationConfig;
    if (targetDatanodes.size() != missingContainerIndexes.size()) {
      throw new IllegalArgumentException("Number of target datanodes and " +
          "container indexes should be same");
    }
  }

  @Override
  public Type getType() {
    return Type.reconstructECContainersCommand;
  }

  @Override
  public ReconstructECContainersCommandProto getProto() {
    Builder builder =
        ReconstructECContainersCommandProto.newBuilder().setCmdId(getId())
            .setContainerID(containerID);
    for (DatanodeDetailsAndReplicaIndex dd : sources) {
      builder.addSources(dd.toProto());
    }
    for (DatanodeDetails dd : targetDatanodes) {
      builder.addTargets(dd.getProtoBufMessage());
    }
    builder.setMissingContainerIndexes(missingContainerIndexes);
    builder.setEcReplicationConfig(ecReplicationConfig.toProto());
    return builder.build();
  }

  public static ReconstructECContainersCommand getFromProtobuf(
      ReconstructECContainersCommandProto protoMessage) {
    Objects.requireNonNull(protoMessage, "protoMessage == null");

    List<DatanodeDetailsAndReplicaIndex> srcDatanodeDetails =
        protoMessage.getSourcesList().stream()
            .map(a -> DatanodeDetailsAndReplicaIndex.fromProto(a))
            .collect(Collectors.toList());
    List<DatanodeDetails> targetDatanodeDetails =
        protoMessage.getTargetsList().stream()
            .map(DatanodeDetails::getFromProtoBuf).collect(Collectors.toList());

    return new ReconstructECContainersCommand(protoMessage.getContainerID(),
        srcDatanodeDetails, targetDatanodeDetails,
        protoMessage.getMissingContainerIndexes(),
        new ECReplicationConfig(protoMessage.getEcReplicationConfig()),
        protoMessage.getCmdId());
  }

  public long getContainerID() {
    return containerID;
  }

  public List<DatanodeDetailsAndReplicaIndex> getSources() {
    return sources;
  }

  public List<DatanodeDetails> getTargetDatanodes() {
    return targetDatanodes;
  }

  public ByteString getMissingContainerIndexes() {
    return missingContainerIndexes;
  }

  public ECReplicationConfig getEcReplicationConfig() {
    return ecReplicationConfig;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", containerID: ").append(containerID)
        .append(", replicationConfig: ").append(ecReplicationConfig)
        .append(", sources: [").append(getSources().stream()
            .map(a -> a.dnDetails
                + " replicaIndex: " + a.getReplicaIndex())
            .collect(Collectors.joining(", "))).append(']')
        .append(", targets: ").append(getTargetDatanodes())
        .append(", missingIndexes: ").append(
            Arrays.toString(missingContainerIndexes.toByteArray()));
    return sb.toString();
  }

  /**
   * To store the datanode details with replica index.
   */
  public static class DatanodeDetailsAndReplicaIndex {
    private DatanodeDetails dnDetails;
    private int replicaIndex;

    public DatanodeDetailsAndReplicaIndex(DatanodeDetails dnDetails,
        int replicaIndex) {
      this.dnDetails = dnDetails;
      this.replicaIndex = replicaIndex;
    }

    public DatanodeDetails getDnDetails() {
      return dnDetails;
    }

    public int getReplicaIndex() {
      return replicaIndex;
    }

    public StorageContainerDatanodeProtocolProtos
        .DatanodeDetailsAndReplicaIndexProto toProto() {
      return StorageContainerDatanodeProtocolProtos
          .DatanodeDetailsAndReplicaIndexProto.newBuilder()
          .setDatanodeDetails(dnDetails.getProtoBufMessage())
          .setReplicaIndex(replicaIndex).build();
    }

    public static DatanodeDetailsAndReplicaIndex fromProto(
        StorageContainerDatanodeProtocolProtos
            .DatanodeDetailsAndReplicaIndexProto proto) {
      return new DatanodeDetailsAndReplicaIndex(
          DatanodeDetails.getFromProtoBuf(proto.getDatanodeDetails()),
          proto.getReplicaIndex());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DatanodeDetailsAndReplicaIndex that = (DatanodeDetailsAndReplicaIndex) o;
      return replicaIndex == that.replicaIndex && Objects
          .equals(dnDetails, that.dnDetails);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dnDetails, replicaIndex);
    }
  }
}
