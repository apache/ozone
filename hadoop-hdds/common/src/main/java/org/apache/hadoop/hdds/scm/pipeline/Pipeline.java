/**
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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Represents a group of datanodes which store a container.
 */
public final class Pipeline {

  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private final PipelineID id;
  private final ReplicationConfig replicationConfig;

  private PipelineState state;
  private Map<DatanodeDetails, Long> nodeStatus;
  // nodes with ordered distance to client
  private ThreadLocal<List<DatanodeDetails>> nodesInOrder = new ThreadLocal<>();
  // Current reported Leader for the pipeline
  private UUID leaderId;
  // Timestamp for pipeline upon creation
  private Instant creationTimestamp;
  // suggested leader id with high priority
  private final UUID suggestedLeaderId;

  /**
   * The immutable properties of pipeline object is used in
   * ContainerStateManager#getMatchingContainerByPipeline to take a lock on
   * the container allocations for a particular pipeline.
   */
  private Pipeline(PipelineID id,
      ReplicationConfig replicationConfig, PipelineState state,
      Map<DatanodeDetails, Long> nodeStatus, UUID suggestedLeaderId) {
    this.id = id;
    this.replicationConfig = replicationConfig;
    this.state = state;
    this.nodeStatus = nodeStatus;
    this.creationTimestamp = Instant.now();
    this.suggestedLeaderId = suggestedLeaderId;
  }

  /**
   * Returns the ID of this pipeline.
   *
   * @return PipelineID
   */
  public PipelineID getId() {
    return id;
  }

  /**
   * Returns the type.
   *
   * @return type - Simple or Ratis.
   */
  public ReplicationType getType() {
    return replicationConfig.getReplicationType();
  }

  /**
   * Returns the State of the pipeline.
   *
   * @return - LifeCycleStates.
   */
  public PipelineState getPipelineState() {
    return state;
  }

  /**
   * Return the creation time of pipeline.
   *
   * @return Creation Timestamp
   */
  public Instant getCreationTimestamp() {
    return creationTimestamp;
  }

  /**
   * Return the suggested leaderId which has a high priority among DNs of the
   * pipeline.
   *
   * @return Suggested LeaderId
   */
  public UUID getSuggestedLeaderId() {
    return suggestedLeaderId;
  }

  /**
   * Set the creation timestamp. Only for protobuf now.
   */
  public void setCreationTimestamp(Instant creationTimestamp) {
    this.creationTimestamp = creationTimestamp;
  }

  /**
   * Return the pipeline leader's UUID.
   *
   * @return DatanodeDetails.UUID.
   */
  public UUID getLeaderId() {
    return leaderId;
  }

  /**
   * Pipeline object, outside of letting leader id to be set, is immutable.
   */
  void setLeaderId(UUID leaderId) {
    this.leaderId = leaderId;
  }

  /**
   * Returns the list of nodes which form this pipeline.
   *
   * @return List of DatanodeDetails
   */
  public List<DatanodeDetails> getNodes() {
    return new ArrayList<>(nodeStatus.keySet());
  }

  /**
   * Return an immutable set of nodes which form this pipeline.
   *
   * @return Set of DatanodeDetails
   */
  public Set<DatanodeDetails> getNodeSet() {
    return Collections.unmodifiableSet(nodeStatus.keySet());
  }

  /**
   * Check if the input pipeline share the same set of datanodes.
   *
   * @return true if the input pipeline shares the same set of datanodes.
   */
  public boolean sameDatanodes(Pipeline pipeline) {
    return getNodeSet().equals(pipeline.getNodeSet());
  }


  /**
   * Returns the leader if found else defaults to closest node.
   *
   * @return {@link DatanodeDetails}
   */
  public DatanodeDetails getLeaderNode() throws IOException {
    if (nodeStatus.isEmpty()) {
      throw new IOException(String.format("Pipeline=%s is empty", id));
    }
    Optional<DatanodeDetails> datanodeDetails =
        nodeStatus.keySet().stream().filter(d ->
            d.getUuid().equals(leaderId)).findFirst();
    if (datanodeDetails.isPresent()) {
      return datanodeDetails.get();
    } else {
      return getClosestNode();
    }
  }

  public DatanodeDetails getFirstNode() throws IOException {
    if (nodeStatus.isEmpty()) {
      throw new IOException(String.format("Pipeline=%s is empty", id));
    }
    return nodeStatus.keySet().iterator().next();
  }

  public DatanodeDetails getClosestNode() throws IOException {
    if (nodesInOrder.get() == null || nodesInOrder.get().isEmpty()) {
      LOG.debug("Nodes in order is empty, delegate to getFirstNode");
      return getFirstNode();
    }
    return nodesInOrder.get().get(0);
  }

  public boolean isClosed() {
    return state == PipelineState.CLOSED;
  }

  public boolean isOpen() {
    return state == PipelineState.OPEN;
  }

  public boolean isAllocationTimeout() {
    //TODO: define a system property to control the timeout value
    return false;
  }

  public void setNodesInOrder(List<DatanodeDetails> nodes) {
    nodesInOrder.set(nodes);
  }

  public List<DatanodeDetails> getNodesInOrder() {
    if (nodesInOrder.get() == null || nodesInOrder.get().isEmpty()) {
      LOG.debug("Nodes in order is empty, delegate to getNodes");
      return getNodes();
    }
    return nodesInOrder.get();
  }

  void reportDatanode(DatanodeDetails dn) throws IOException {
    if (nodeStatus.get(dn) == null) {
      throw new IOException(
          String.format("Datanode=%s not part of pipeline=%s", dn, id));
    }
    nodeStatus.put(dn, System.currentTimeMillis());
  }

  public boolean isHealthy() {
    for (Long reportedTime : nodeStatus.values()) {
      if (reportedTime < 0) {
        return false;
      }
    }
    return leaderId != null;
  }

  public boolean isEmpty() {
    return nodeStatus.isEmpty();
  }

  public ReplicationConfig getReplicationConfig() {
    return replicationConfig;
  }

  public HddsProtos.Pipeline getProtobufMessage(int clientVersion)
      throws UnknownPipelineStateException {
    List<HddsProtos.DatanodeDetailsProto> members = new ArrayList<>();
    for (DatanodeDetails dn : nodeStatus.keySet()) {
      members.add(dn.toProto(clientVersion));
    }

    HddsProtos.Pipeline.Builder builder = HddsProtos.Pipeline.newBuilder()
        .setId(id.getProtobuf())
        .setType(replicationConfig.getReplicationType())
        .setFactor(ReplicationConfig.getLegacyFactor(replicationConfig))
        .setState(PipelineState.getProtobuf(state))
        .setLeaderID(leaderId != null ? leaderId.toString() : "")
        .setCreationTimeStamp(creationTimestamp.toEpochMilli())
        .addAllMembers(members);

    if (leaderId != null) {
      HddsProtos.UUID uuid128 = HddsProtos.UUID.newBuilder()
          .setMostSigBits(leaderId.getMostSignificantBits())
          .setLeastSigBits(leaderId.getLeastSignificantBits())
          .build();
      builder.setLeaderID128(uuid128);
    }

    if (suggestedLeaderId != null) {
      HddsProtos.UUID uuid128 = HddsProtos.UUID.newBuilder()
          .setMostSigBits(suggestedLeaderId.getMostSignificantBits())
          .setLeastSigBits(suggestedLeaderId.getLeastSignificantBits())
          .build();
      builder.setSuggestedLeaderID(uuid128);
    }

    // To save the message size on wire, only transfer the node order based on
    // network topology
    List<DatanodeDetails> nodes = nodesInOrder.get();
    if (nodes != null && !nodes.isEmpty()) {
      for (int i = 0; i < nodes.size(); i++) {
        Iterator<DatanodeDetails> it = nodeStatus.keySet().iterator();
        for (int j = 0; j < nodeStatus.keySet().size(); j++) {
          if (it.next().equals(nodes.get(i))) {
            builder.addMemberOrders(j);
            break;
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Serialize pipeline {} with nodesInOrder {}", id, nodes);
      }
    }
    return builder.build();
  }

  public static Pipeline getFromProtobuf(HddsProtos.Pipeline pipeline)
      throws UnknownPipelineStateException {
    Preconditions.checkNotNull(pipeline, "Pipeline is null");

    List<DatanodeDetails> nodes = new ArrayList<>();
    for (DatanodeDetailsProto member : pipeline.getMembersList()) {
      nodes.add(DatanodeDetails.getFromProtoBuf(member));
    }
    UUID leaderId = null;
    if (pipeline.hasLeaderID128()) {
      HddsProtos.UUID uuid = pipeline.getLeaderID128();
      leaderId = new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
    } else if (pipeline.hasLeaderID() &&
        StringUtils.isNotEmpty(pipeline.getLeaderID())) {
      leaderId = UUID.fromString(pipeline.getLeaderID());
    }

    UUID suggestedLeaderId = null;
    if (pipeline.hasSuggestedLeaderID()) {
      HddsProtos.UUID uuid = pipeline.getSuggestedLeaderID();
      suggestedLeaderId =
          new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits());
    }

    final ReplicationConfig config = ReplicationConfig
        .fromProto(pipeline.getType(), pipeline.getFactor());
    return new Builder().setId(PipelineID.getFromProtobuf(pipeline.getId()))
        .setReplicationConfig(config)
        .setState(PipelineState.fromProtobuf(pipeline.getState()))
        .setNodes(nodes)
        .setLeaderId(leaderId)
        .setSuggestedLeaderId(suggestedLeaderId)
        .setNodesInOrder(pipeline.getMemberOrdersList())
        .setCreateTimestamp(pipeline.getCreationTimeStamp())
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Pipeline that = (Pipeline) o;

    return new EqualsBuilder()
        .append(id, that.id)
        .append(replicationConfig, that.replicationConfig)
        .append(getNodes(), that.getNodes())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(id)
        .append(replicationConfig.getReplicationType())
        .append(nodeStatus)
        .toHashCode();
  }

  @Override
  public String toString() {
    final StringBuilder b =
        new StringBuilder(getClass().getSimpleName()).append("[");
    b.append(" Id: ").append(id.getId());
    b.append(", Nodes: ");
    nodeStatus.keySet().forEach(b::append);
    b.append(", ReplicationConfig: ").append(replicationConfig);
    b.append(", State:").append(getPipelineState());
    b.append(", leaderId:").append(leaderId != null ? leaderId.toString() : "");
    b.append(", CreationTimestamp").append(getCreationTimestamp());
    b.append("]");
    return b.toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(Pipeline pipeline) {
    return new Builder(pipeline);
  }

  /**
   * Builder class for Pipeline.
   */
  public static class Builder {
    private PipelineID id = null;
    private ReplicationConfig replicationConfig = null;
    private PipelineState state = null;
    private Map<DatanodeDetails, Long> nodeStatus = null;
    private List<Integer> nodeOrder = null;
    private List<DatanodeDetails> nodesInOrder = null;
    private UUID leaderId = null;
    private Instant creationTimestamp = null;
    private UUID suggestedLeaderId = null;

    public Builder() {}

    public Builder(Pipeline pipeline) {
      this.id = pipeline.id;
      this.replicationConfig = pipeline.replicationConfig;
      this.state = pipeline.state;
      this.nodeStatus = pipeline.nodeStatus;
      this.nodesInOrder = pipeline.nodesInOrder.get();
      this.leaderId = pipeline.getLeaderId();
      this.creationTimestamp = pipeline.getCreationTimestamp();
      this.suggestedLeaderId = pipeline.getSuggestedLeaderId();
    }

    public Builder setId(PipelineID id1) {
      this.id = id1;
      return this;
    }

    public Builder setReplicationConfig(ReplicationConfig replicationConf) {
      this.replicationConfig = replicationConf;
      return this;
    }

    public Builder setState(PipelineState state1) {
      this.state = state1;
      return this;
    }

    public Builder setLeaderId(UUID leaderId1) {
      this.leaderId = leaderId1;
      return this;
    }

    public Builder setNodes(List<DatanodeDetails> nodes) {
      this.nodeStatus = new LinkedHashMap<>();
      nodes.forEach(node -> nodeStatus.put(node, -1L));
      return this;
    }

    public Builder setNodesInOrder(List<Integer> orders) {
      this.nodeOrder = orders;
      return this;
    }

    public Builder setCreateTimestamp(long createTimestamp) {
      this.creationTimestamp = Instant.ofEpochMilli(createTimestamp);
      return this;
    }

    public Builder setSuggestedLeaderId(UUID uuid) {
      this.suggestedLeaderId = uuid;
      return this;
    }

    public Pipeline build() {
      Preconditions.checkNotNull(id);
      Preconditions.checkNotNull(replicationConfig);
      Preconditions.checkNotNull(state);
      Preconditions.checkNotNull(nodeStatus);
      Pipeline pipeline =
          new Pipeline(id, replicationConfig, state, nodeStatus,
              suggestedLeaderId);
      pipeline.setLeaderId(leaderId);
      // overwrite with original creationTimestamp
      if (creationTimestamp != null) {
        pipeline.setCreationTimestamp(creationTimestamp);
      }

      if (nodeOrder != null && !nodeOrder.isEmpty()) {
        // This branch is for build from ProtoBuf
        List<DatanodeDetails> nodesWithOrder = new ArrayList<>();
        for(int i = 0; i < nodeOrder.size(); i++) {
          int nodeIndex = nodeOrder.get(i);
          Iterator<DatanodeDetails> it = nodeStatus.keySet().iterator();
          while(it.hasNext() && nodeIndex >= 0) {
            DatanodeDetails node = it.next();
            if (nodeIndex == 0) {
              nodesWithOrder.add(node);
              break;
            }
            nodeIndex--;
          }
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Deserialize nodesInOrder {} in pipeline {}",
              nodesWithOrder, id);
        }
        pipeline.setNodesInOrder(nodesWithOrder);
      } else if (nodesInOrder != null){
        // This branch is for pipeline clone
        pipeline.setNodesInOrder(nodesInOrder);
      }

      return pipeline;
    }
  }

  /**
   * Possible Pipeline states in SCM.
   */
  public enum PipelineState {
    ALLOCATED, OPEN, DORMANT, CLOSED;

    public static PipelineState fromProtobuf(HddsProtos.PipelineState state)
        throws UnknownPipelineStateException {
      Preconditions.checkNotNull(state, "Pipeline state is null");
      switch (state) {
      case PIPELINE_ALLOCATED: return ALLOCATED;
      case PIPELINE_OPEN: return OPEN;
      case PIPELINE_DORMANT: return DORMANT;
      case PIPELINE_CLOSED: return CLOSED;
      default:
        throw new UnknownPipelineStateException(
            "Pipeline state: " + state + " is not recognized.");
      }
    }

    public static HddsProtos.PipelineState getProtobuf(PipelineState state)
        throws UnknownPipelineStateException {
      Preconditions.checkNotNull(state, "Pipeline state is null");
      switch (state) {
      case ALLOCATED: return HddsProtos.PipelineState.PIPELINE_ALLOCATED;
      case OPEN: return HddsProtos.PipelineState.PIPELINE_OPEN;
      case DORMANT: return HddsProtos.PipelineState.PIPELINE_DORMANT;
      case CLOSED: return HddsProtos.PipelineState.PIPELINE_CLOSED;
      default:
        throw new UnknownPipelineStateException(
            "Pipeline state: " + state + " is not recognized.");
      }
    }
  }
}
