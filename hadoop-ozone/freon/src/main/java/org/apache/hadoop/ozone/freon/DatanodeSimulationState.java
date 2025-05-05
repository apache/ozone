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

package org.apache.hadoop.ozone.freon;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatRequestProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMHeartbeatResponseProto;
import org.apache.hadoop.ozone.container.common.impl.StorageLocationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates states of a simulated datanode instance.
 */
class DatanodeSimulationState {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      DatanodeSimulationState.class);
  public static final long CONTAINER_SIZE = (long) StorageUnit.GB.toBytes(5);
  private int targetContainersCount;

  private DatanodeDetails datanodeDetails;
  private boolean isRegistered = false;
  private long fullContainerReportDurationMs;
  private Map<InetSocketAddress, EndpointState> endpointStates =
      new HashMap<>();

  private Set<String> pipelines = new HashSet<>();
  private Map<Long, ContainerReplicaProto.State> containers =
      new HashMap<>();

  // indicate if this node is in read-only mode, no pipeline should be created.
  private volatile boolean readOnly = false;

  DatanodeSimulationState(DatanodeDetails datanodeDetails,
                          long fullContainerReportDurationMs,
                          List<InetSocketAddress> allEndpoints,
                          int targetContainersCount) {
    this();
    this.datanodeDetails = datanodeDetails;
    this.fullContainerReportDurationMs = fullContainerReportDurationMs;
    initEndpointsState(allEndpoints);
    this.targetContainersCount = targetContainersCount;
  }

  DatanodeSimulationState() {
  }

  void initEndpointsState(List<InetSocketAddress> allEndpoints) {
    for (InetSocketAddress endpoint : allEndpoints) {
      endpointStates.put(endpoint, new EndpointState());
    }
  }

  public synchronized void ackHeartbeatResponse(
      SCMHeartbeatResponseProto response) {
    for (SCMCommandProto command : response.getCommandsList()) {
      switch (command.getCommandType()) {
      case createPipelineCommand:
        StorageContainerDatanodeProtocolProtos.CreatePipelineCommandProto
            pipelineCmd =
            command.getCreatePipelineCommandProto();
        if (!readOnly) {
          pipelines.add(pipelineCmd.getPipelineID().getId());
        } else {
          LOGGER.info("Ignored pipeline creation for {}-{}",
              pipelineCmd.getType(), pipelineCmd.getFactor());
        }
        break;
      case closePipelineCommand:
        pipelines.remove(
            command.getClosePipelineCommandProto()
                .getPipelineID().getId());
        break;
      case closeContainerCommand:
        StorageContainerDatanodeProtocolProtos.CloseContainerCommandProto
            closeContainerCmd = command.getCloseContainerCommandProto();
        closeContainer(closeContainerCmd.getContainerID());
        break;
      default:
        LOGGER.debug("Ignored command: {}",
            command.getCommandType());
      }
    }
  }

  public synchronized SCMHeartbeatRequestProto heartbeatRequest(
      InetSocketAddress endpoint, LayoutVersionProto layoutInfo)
      throws IOException {
    SCMHeartbeatRequestProto.Builder builder =
        SCMHeartbeatRequestProto.newBuilder()
            .setDatanodeDetails(datanodeDetails.getProtoBufMessage())
            .setDataNodeLayoutVersion(layoutInfo)
            .setNodeReport(createNodeReport())
            .setPipelineReports(createPipelineReport());

    addContainerReport(endpoint, builder);

    return builder.build();
  }

  private void addContainerReport(InetSocketAddress endpoint,
                                  SCMHeartbeatRequestProto.Builder builder) {
    EndpointState state = endpointStates.get(endpoint);
    if (state.nextFullContainerReport.compareTo(Instant.now()) <= 0) {
      builder.setContainerReport(createFullContainerReport());

      // Every datanode will start with a full report, then the next full
      // repport should be schedule randomly between 0 and the next true cycle
      // to avoid peaks.
      if (state.nextFullContainerReport == Instant.MIN) {
        state.nextFullContainerReport = Instant.now().plusMillis(
            RandomUtils.secure().randomLong(1, fullContainerReportDurationMs));
      } else {
        state.nextFullContainerReport = Instant.now()
            .plusMillis(fullContainerReportDurationMs);
      }
      state.icr.clear();
    } else {
      if (state.icr.getReportCount() > 0) {
        builder.addIncrementalContainerReport(state.icr.build());
        state.icr.clear();
      }
    }
  }

  ContainerReportsProto createFullContainerReport() {
    ContainerReportsProto.Builder builder = ContainerReportsProto.newBuilder();
    for (Map.Entry<Long, ContainerReplicaProto.State> entry :
        containers.entrySet()) {
      ContainerReplicaProto container =
          ContainerReplicaProto.newBuilder()
              .setContainerID(entry.getKey())
              .setReadCount(10_000)
              .setWriteCount(10_000)
              .setReadBytes(10_000_000L)
              .setWriteBytes(5_000_000_000L)
              .setKeyCount(10_000)
              .setUsed(5_000_000_000L)
              .setState(entry.getValue())
              .setBlockCommitSequenceId(1000)
              .setOriginNodeId(datanodeDetails.getUuidString())
              .setReplicaIndex(0)
              .build();
      builder.addReports(container);
    }
    return builder.build();
  }

  PipelineReportsProto createPipelineReport() {
    PipelineReportsProto.Builder builder = PipelineReportsProto.newBuilder();
    for (String pipelineId : pipelines) {
      builder.addPipelineReport(
          StorageContainerDatanodeProtocolProtos.PipelineReport.newBuilder()
              .setPipelineID(HddsProtos.PipelineID
                  .newBuilder().setId(pipelineId).build())
              .setIsLeader(true).build());
    }
    return builder.build();
  }

  StorageContainerDatanodeProtocolProtos.NodeReportProto createNodeReport()
      throws IOException {
    long capacity = targetContainersCount * CONTAINER_SIZE;
    long used = readOnly ? capacity :
        CONTAINER_SIZE * containers.size();

    StorageLocationReport storageLocationReport = StorageLocationReport
        .newBuilder()
        .setStorageLocation("/tmp/unreal_storage")
        .setId("simulated-storage-volume")
        .setCapacity(capacity)
        .setScmUsed(used)
        .setRemaining(capacity - used)
        .setStorageType(StorageType.DEFAULT)
        .build();

    StorageLocationReport metaLocationReport = StorageLocationReport
        .newBuilder()
        .setStorageLocation("/tmp/unreal_metadata")
        .setId("simulated-storage-volume")
        .setCapacity((long) StorageUnit.GB.toBytes(100))
        .setScmUsed((long) StorageUnit.GB.toBytes(50))
        .setRemaining((long) StorageUnit.GB.toBytes(50))
        .setStorageType(StorageType.DEFAULT)
        .build();

    return StorageContainerDatanodeProtocolProtos.NodeReportProto.newBuilder()
        .addStorageReport(storageLocationReport.getProtoBufMessage())
        .addMetadataStorageReport(
            metaLocationReport.getMetadataProtoBufMessage())
        .build();
  }

  public synchronized void newContainer(long containerId) {
    containers.put(containerId, ContainerReplicaProto.State.OPEN);
    for (EndpointState state : endpointStates.values()) {
      state.icr.addReport(
          ContainerReplicaProto.newBuilder()
              .setContainerID(containerId)
              .setReadCount(10_000)
              .setWriteCount(10_000)
              .setReadBytes(10_000_000L)
              .setWriteBytes(5_000_000_000L)
              .setKeyCount(10_000)
              .setUsed(CONTAINER_SIZE)
              .setState(ContainerReplicaProto.State.OPEN)
              .setBlockCommitSequenceId(1000)
              .setOriginNodeId(datanodeDetails.getUuidString())
              .setReplicaIndex(0)
              .build()
      );
    }
  }

  public synchronized void closeContainer(Long containerID) {
    if (containers.containsKey(containerID)) {
      containers.put(containerID, ContainerReplicaProto.State.CLOSED);
      for (EndpointState state : endpointStates.values()) {
        state.icr.addReport(
            ContainerReplicaProto.newBuilder()
                .setContainerID(containerID)
                .setReadCount(10_000)
                .setWriteCount(10_000)
                .setReadBytes(10_000_000L)
                .setWriteBytes(5_000_000_000L)
                .setKeyCount(10_000)
                .setUsed(5_000_000_000L)
                .setState(ContainerReplicaProto.State.CLOSED)
                .setBlockCommitSequenceId(1000)
                .setOriginNodeId(datanodeDetails.getUuidString())
                .setReplicaIndex(0)
                .build()
        );
      }
    } else {
      LOGGER.error("Unrecognized closeContainerCommand");
    }
  }

  @JsonSerialize(using = DatanodeDetailsSerializer.class)
  @JsonDeserialize(using = DatanodeDeserializer.class)
  public synchronized DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  public synchronized void setDatanodeDetails(
      DatanodeDetails datanodeDetails) {
    this.datanodeDetails = datanodeDetails;
  }

  public synchronized Set<String> getPipelines() {
    return pipelines;
  }

  public synchronized void setPipelines(Set<String> pipelines) {
    this.pipelines = pipelines;
  }

  public synchronized boolean isRegistered() {
    return isRegistered;
  }

  public synchronized void setRegistered(boolean registered) {
    isRegistered = registered;
  }

  public synchronized Map<Long, ContainerReplicaProto.State> getContainers() {
    return containers;
  }

  public synchronized void setContainers(
      Map<Long, ContainerReplicaProto.State> containers) {
    this.containers = containers;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public long getFullContainerReportDurationMs() {
    return fullContainerReportDurationMs;
  }

  public void setFullContainerReportDurationMs(
      long fullContainerReportDurationMs) {
    this.fullContainerReportDurationMs = fullContainerReportDurationMs;
  }

  public int getTargetContainersCount() {
    return targetContainersCount;
  }

  public void setTargetContainersCount(int targetContainersCount) {
    this.targetContainersCount = targetContainersCount;
  }

  private static class DatanodeDetailsSerializer
      extends StdSerializer<DatanodeDetails> {
    protected DatanodeDetailsSerializer() {
      super(DatanodeDetails.class);
    }

    @Override
    public void serialize(DatanodeDetails value, JsonGenerator gen,
                          SerializerProvider provider) throws IOException {
      gen.writeBinary(value.getProtoBufMessage().toByteArray());
    }
  }

  private static class DatanodeDeserializer
      extends StdDeserializer<DatanodeDetails> {
    protected DatanodeDeserializer() {
      super(DatanodeDetails.class);
    }

    @Override
    public DatanodeDetails deserialize(JsonParser p,
                                       DeserializationContext ctxt)
        throws IOException {
      byte[] binaryValue = p.getBinaryValue();
      return DatanodeDetails.getFromProtoBuf(
          HddsProtos.DatanodeDetailsProto.parseFrom(binaryValue));
    }
  }

  private static class EndpointState {
    private final IncrementalContainerReportProto.Builder icr =
        IncrementalContainerReportProto.newBuilder();

    private Instant nextFullContainerReport = Instant.MIN;
  }
}
