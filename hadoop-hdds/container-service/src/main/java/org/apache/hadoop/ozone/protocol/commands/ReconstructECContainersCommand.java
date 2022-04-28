/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.protocol.commands;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ReconstructECContainersCommandProto
    .Builder;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;

import com.google.common.base.Preconditions;

/**
 * SCM command to request reconstruction of EC containers.
 */
public class ReconstructECContainersCommand
    extends SCMCommand<ReconstructECContainersCommandProto> {

  private final long containerID;
  private final List<DatanodeDetails> sourceDatanodes;
  private final List<DatanodeDetails> targetDatanodes;
  private final List<Long> srcNodesIndexes;
  private final byte[] missingContainerIndexes;
  private final ECReplicationConfig ecReplicationConfig;

  public ReconstructECContainersCommand(long containerID,
      List<DatanodeDetails> sourceDatanodes,
      List<DatanodeDetails> targetDatanodes,
      List<Long> srcNodesIndexes,
      byte[] missingContainerIndexes,
      ECReplicationConfig ecReplicationConfig) {
    super();
    this.containerID = containerID;
    this.sourceDatanodes = sourceDatanodes;
    this.targetDatanodes = targetDatanodes;
    this.srcNodesIndexes = srcNodesIndexes;
    this.missingContainerIndexes =
        Arrays.copyOf(missingContainerIndexes, missingContainerIndexes.length);
    this.ecReplicationConfig = ecReplicationConfig;
  }

  // Should be called only for protobuf conversion
  public ReconstructECContainersCommand(long containerID,
      List<DatanodeDetails> sourceDatanodes,
      List<DatanodeDetails> targetDatanodes,
      List<Long> srcNodesIndexes,
      byte[] missingContainerIndexes,
      ECReplicationConfig ecReplicationConfig, long id) {
    super(id);
    this.containerID = containerID;
    this.sourceDatanodes = sourceDatanodes;
    this.targetDatanodes = targetDatanodes;
    this.srcNodesIndexes = srcNodesIndexes;
    this.missingContainerIndexes =
        Arrays.copyOf(missingContainerIndexes, missingContainerIndexes.length);
    this.ecReplicationConfig = ecReplicationConfig;
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
    for (DatanodeDetails dd : sourceDatanodes) {
      builder.addSources(dd.getProtoBufMessage());
    }
    for (DatanodeDetails dd : targetDatanodes) {
      builder.addTargets(dd.getProtoBufMessage());
    }
    builder.setMissingContainerIndexes(getByteString(missingContainerIndexes));
    builder.setEcReplicationConfig(ecReplicationConfig.toProto());
    return builder.build();
  }

  public static ByteString getByteString(byte[] bytes) {
    return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
  }

  public static ReconstructECContainersCommand getFromProtobuf(
      ReconstructECContainersCommandProto protoMessage) {
    Preconditions.checkNotNull(protoMessage);

    List<DatanodeDetails> srcDatanodeDetails =
        protoMessage.getSourcesList().stream()
            .map(DatanodeDetails::getFromProtoBuf).collect(Collectors.toList());
    List<DatanodeDetails> targetDatanodeDetails =
        protoMessage.getTargetsList().stream()
            .map(DatanodeDetails::getFromProtoBuf).collect(Collectors.toList());

    return new ReconstructECContainersCommand(protoMessage.getContainerID(),
        srcDatanodeDetails, targetDatanodeDetails,
        protoMessage.getSrcNodesIndexesList(),
        protoMessage.getMissingContainerIndexes().toByteArray(),
        new ECReplicationConfig(protoMessage.getEcReplicationConfig()),
        protoMessage.getCmdId());
  }

  public long getContainerID() {
    return containerID;
  }

  public List<DatanodeDetails> getSourceDatanodes() {
    return sourceDatanodes;
  }

  public List<DatanodeDetails> getTargetDatanodes() {
    return targetDatanodes;
  }

  public List<Long> getSrcNodesIndexes() {
    return srcNodesIndexes;
  }

  public byte[] getMissingContainerIndexes() {
    return Arrays
        .copyOf(missingContainerIndexes, missingContainerIndexes.length);
  }

  public ECReplicationConfig getEcReplicationConfig() {
    return ecReplicationConfig;
  }
}
