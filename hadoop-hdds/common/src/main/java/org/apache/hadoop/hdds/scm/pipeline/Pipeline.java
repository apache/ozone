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

package org.apache.hadoop.hdds.scm.pipeline;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicatedReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a group of datanodes which store a container.
 */
public final class Pipeline {
  /**
   * This codec is inconsistent since
   * deserialize(serialize(original)) does not equal to original
   * -- the creation time may change.
   */
  private static final Codec<Pipeline> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(HddsProtos.Pipeline.getDefaultInstance()),
      Pipeline::getFromProtobufSetCreationTimestamp,
      p -> p.getProtobufMessage(ClientVersion.CURRENT_VERSION),
      Pipeline.class,
      DelegatedCodec.CopyType.UNSUPPORTED);

  private static final Logger LOG = LoggerFactory.getLogger(Pipeline.class);
  private final PipelineID id;
  private final ReplicationConfig replicationConfig;

  private final PipelineState state;
  private final Map<DatanodeDetails, Long> nodeStatus;
  private final Map<DatanodeDetails, Integer> replicaIndexes;
  // nodes with ordered distance to client
  private final ImmutableList<DatanodeDetails> nodesInOrder;
  // Current reported Leader for the pipeline
  private DatanodeID leaderId;
  // Timestamp for pipeline upon creation
  private Instant creationTimestamp;
  // suggested leader id with high priority
  private final DatanodeID suggestedLeaderId;

  private final Instant stateEnterTime;

  /**
   * The immutable properties of pipeline object is used in
   * ContainerStateManager#getMatchingContainerByPipeline to take a lock on
   * the container allocations for a particular pipeline.
   * <br><br>
   * Since the Pipeline class is immutable, if we want to change the state of
   * the Pipeline we should create a new Pipeline object with the new state.
   * Make sure that you set the value of <i>creationTimestamp</i> properly while
   * creating the new Pipeline object.
   * <br><br>
   * There is no need to worry about the value of <i>stateEnterTime</i> as it's
   * set to <i>Instant.now</i> when you crate the Pipeline object as part of
   * state change.
   */
  private Pipeline(Builder b) {
    id = b.id;
    replicationConfig = b.replicationConfig;
    state = b.state;
    leaderId = b.leaderId;
    suggestedLeaderId = b.suggestedLeaderId;
    nodeStatus = b.nodeStatus;
    nodesInOrder = b.nodesInOrder != null ? ImmutableList.copyOf(b.nodesInOrder) : ImmutableList.of();
    replicaIndexes = b.replicaIndexes;
    creationTimestamp = b.creationTimestamp != null ? b.creationTimestamp : Instant.now();
    stateEnterTime = Instant.now();
  }

  public static Codec<Pipeline> getCodec() {
    return CODEC;
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

  public Instant getStateEnterTime() {
    return stateEnterTime;
  }

  /**
   * Return the suggested leaderId which has a high priority among DNs of the
   * pipeline.
   *
   * @return Suggested LeaderId
   */
  public DatanodeID getSuggestedLeaderId() {
    return suggestedLeaderId;
  }

  /**
   * Set the creation timestamp. Only for protobuf now.
   */
  public void setCreationTimestamp(Instant creationTimestamp) {
    this.creationTimestamp = creationTimestamp;
  }

  /**
   * Return the pipeline leader's DatanodeID.
   *
   * @return DatanodeDetails.DatanodeID.
   */
  public DatanodeID getLeaderId() {
    return leaderId;
  }

  /**
   * Pipeline object, outside of letting leader id to be set, is immutable.
   */
  void setLeaderId(DatanodeID leaderId) {
    this.leaderId = leaderId;
  }

  /** @return the number of datanodes in this pipeline. */
  public int size() {
    return nodeStatus.size();
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
  @JsonIgnore
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
   * Return the replica index of the specific datanode in the datanode set.
   * <p>
   * For non-EC case all the replication should be exactly the same,
   * therefore the replication index can always be zero. In case of EC
   * different Datanodes can have different data for one specific block
   * (parity and/or data parts) therefore the replicaIndex should be
   * different for each of the pipeline members.
   *
   * @param dn datanode details
   */
  public int getReplicaIndex(DatanodeDetails dn) {
    return replicaIndexes.getOrDefault(dn, 0);
  }

  /**
   * Get the replicaIndex Map.
   */
  public Map<DatanodeDetails, Integer> getReplicaIndexes() {
    return this.getNodes().stream().collect(Collectors.toMap(Function.identity(), this::getReplicaIndex));
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
            d.getID().equals(leaderId)).findFirst();
    if (datanodeDetails.isPresent()) {
      return datanodeDetails.get();
    } else {
      return getClosestNode();
    }
  }

  public DatanodeDetails getFirstNode() throws IOException {
    return getFirstNode(null);
  }

  public DatanodeDetails getFirstNode(Set<DatanodeDetails> excluded)
      throws IOException {
    if (excluded == null) {
      excluded = Collections.emptySet();
    }
    if (nodeStatus.isEmpty()) {
      throw new IOException(String.format("Pipeline=%s is empty", id));
    }
    for (DatanodeDetails d : nodeStatus.keySet()) {
      if (!excluded.contains(d)) {
        return d;
      }
    }
    throw new IOException(String.format(
        "All nodes are excluded: Pipeline=%s, excluded=%s", id, excluded));
  }

  public DatanodeDetails getClosestNode() throws IOException {
    return getClosestNode(null);
  }

  public DatanodeDetails getClosestNode(Set<DatanodeDetails> excluded)
      throws IOException {
    if (excluded == null) {
      excluded = Collections.emptySet();
    }
    if (nodesInOrder.isEmpty()) {
      LOG.debug("Nodes in order is empty, delegate to getFirstNode");
      return getFirstNode(excluded);
    }
    for (DatanodeDetails d : nodesInOrder) {
      if (!excluded.contains(d)) {
        return d;
      }
    }
    throw new IOException(String.format(
        "All nodes are excluded: Pipeline=%s, excluded=%s", id, excluded));
  }

  @JsonIgnore
  public boolean isClosed() {
    return state == PipelineState.CLOSED;
  }

  @JsonIgnore
  public boolean isOpen() {
    return state == PipelineState.OPEN;
  }

  public List<DatanodeDetails> getNodesInOrder() {
    if (nodesInOrder.isEmpty()) {
      LOG.debug("Nodes in order is empty, delegate to getNodes");
      return getNodes();
    }
    return nodesInOrder;
  }

  void reportDatanode(DatanodeDetails dn) throws IOException {
    //This is a workaround for the case a datanode restarted with reinitializing it's dnId but it still reports the
    // same set of pipelines it was part of. The pipeline report should be accepted for this anomalous condition.
    //  We rely on StaleNodeHandler in closing this pipeline eventually.
    if (dn == null || (nodeStatus.get(dn) == null
        && nodeStatus.keySet().stream().noneMatch(node -> node.compareNodeValues(dn)))) {
      throw new IOException(
          String.format("Datanode=%s not part of pipeline=%s", dn, id));
    }
    nodeStatus.put(dn, System.currentTimeMillis());
  }

  public boolean isHealthy() {
    // EC pipelines are not reported by the DN and do not have a leader. If a
    // node goes stale or dead, EC pipelines will be closed like RATIS pipelines
    // but at the current time there are not other health metrics for EC.
    if (replicationConfig.getReplicationType() == ReplicationType.EC) {
      return true;
    }
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

  public HddsProtos.Pipeline getProtobufMessage(int clientVersion) {
    return getProtobufMessage(clientVersion, Collections.emptySet());
  }

  public HddsProtos.Pipeline getProtobufMessage(int clientVersion, Set<DatanodeDetails.Port.Name> filterPorts) {
    List<HddsProtos.DatanodeDetailsProto> members = new ArrayList<>();
    List<Integer> memberReplicaIndexes = new ArrayList<>();

    for (DatanodeDetails dn : nodeStatus.keySet()) {
      members.add(dn.toProto(clientVersion, filterPorts));
      memberReplicaIndexes.add(replicaIndexes.getOrDefault(dn, 0));
    }

    HddsProtos.Pipeline.Builder builder = HddsProtos.Pipeline.newBuilder()
        .setId(id.getProtobuf())
        .setType(replicationConfig.getReplicationType())
        .setState(PipelineState.getProtobuf(state))
        .setLeaderID(leaderId != null ? leaderId.toString() : "")
        .setCreationTimeStamp(creationTimestamp.toEpochMilli())
        .addAllMembers(members)
        .addAllMemberReplicaIndexes(memberReplicaIndexes);

    if (replicationConfig instanceof ECReplicationConfig) {
      builder.setEcReplicationConfig(((ECReplicationConfig) replicationConfig)
          .toProto());
    } else {
      builder.setFactor(ReplicationConfig.getLegacyFactor(replicationConfig));
    }
    if (leaderId != null) {
      builder.setLeaderDatanodeID(leaderId.toProto());
    }

    if (suggestedLeaderId != null) {
      builder.setSuggestedLeaderDatanodeID(suggestedLeaderId.toProto());
    }

    // To save the message size on wire, only transfer the node order based on
    // network topology
    if (!nodesInOrder.isEmpty()) {
      for (DatanodeDetails datanodeDetails : nodesInOrder) {
        Iterator<DatanodeDetails> it = nodeStatus.keySet().iterator();
        for (int j = 0; j < nodeStatus.size(); j++) {
          if (it.next().equals(datanodeDetails)) {
            builder.addMemberOrders(j);
            break;
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Serialize pipeline {} with nodesInOrder {}", id, nodesInOrder);
      }
    }
    return builder.build();
  }

  private static Pipeline getFromProtobufSetCreationTimestamp(HddsProtos.Pipeline proto) {
    return toBuilder(proto)
        .setCreateTimestamp(Instant.now())
        .build();
  }

  public Pipeline copyWithNodesInOrder(List<? extends DatanodeDetails> nodes) {
    return toBuilder().setNodesInOrder(nodes).build();
  }

  public Pipeline copyForRead() {
    if (replicationConfig.getReplicationType() == ReplicationType.STAND_ALONE) {
      return this;
    }

    HddsProtos.ReplicationFactor factor = replicationConfig instanceof ReplicatedReplicationConfig
        ? ((ReplicatedReplicationConfig) replicationConfig).getReplicationFactor()
        : HddsProtos.ReplicationFactor.ONE;

    return toBuilder()
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(factor))
        .build();
  }

  public Pipeline copyForReadFromNode(DatanodeDetails node) {
    Preconditions.assertTrue(nodeStatus.containsKey(node), () -> node + " is not part of the pipeline " + id.getId());

    return toBuilder()
        .setNodes(Collections.singletonList(node))
        .setReplicaIndexes(Collections.singletonMap(node, getReplicaIndex(node)))
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE))
        .build();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  public static Builder toBuilder(HddsProtos.Pipeline pipeline) {
    Objects.requireNonNull(pipeline, "pipeline == null");

    Map<DatanodeDetails, Integer> nodes = new LinkedHashMap<>();
    int index = 0;
    int repIndexListLength = pipeline.getMemberReplicaIndexesCount();
    for (DatanodeDetailsProto member : pipeline.getMembersList()) {
      int repIndex = 0;
      if (index < repIndexListLength) {
        repIndex = pipeline.getMemberReplicaIndexes(index);
      }
      nodes.put(DatanodeDetails.getFromProtoBuf(member), repIndex);
      index++;
    }
    DatanodeID leaderId = null;
    if (pipeline.hasLeaderDatanodeID()) {
      leaderId = DatanodeID.of(pipeline.getLeaderDatanodeID().getUuid());
    } else if (pipeline.hasLeaderID128()) {
      HddsProtos.UUID uuid = pipeline.getLeaderID128();
      leaderId = DatanodeID.of(uuid);
    } else if (pipeline.hasLeaderID() &&
        StringUtils.isNotEmpty(pipeline.getLeaderID())) {
      leaderId = DatanodeID.fromUuidString(pipeline.getLeaderID());
    }

    DatanodeID suggestedLeaderId = null;
    if (pipeline.hasSuggestedLeaderDatanodeID()) {
      suggestedLeaderId = DatanodeID.of(pipeline.getSuggestedLeaderDatanodeID().getUuid());
    } else if (pipeline.hasSuggestedLeaderID()) {
      HddsProtos.UUID uuid = pipeline.getSuggestedLeaderID();
      suggestedLeaderId = DatanodeID.of(uuid);
    }

    final ReplicationConfig config = ReplicationConfig
        .fromProto(pipeline.getType(), pipeline.getFactor(),
            pipeline.getEcReplicationConfig());
    return newBuilder()
        .setId(PipelineID.getFromProtobuf(pipeline.getId()))
        .setReplicationConfig(config)
        .setState(PipelineState.fromProtobuf(pipeline.getState()))
        .setNodes(new ArrayList<>(nodes.keySet()))
        .setReplicaIndexes(nodes)
        .setLeaderId(leaderId)
        .setSuggestedLeaderId(suggestedLeaderId)
        .setNodeOrder(pipeline.getMemberOrdersList())
        .setCreateTimestamp(pipeline.getCreationTimeStamp());
  }

  public static Pipeline getFromProtobuf(HddsProtos.Pipeline pipeline) {
    return toBuilder(pipeline).build();
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
        .append(nodeStatus.keySet(), that.nodeStatus.keySet())
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
        new StringBuilder(getClass().getSimpleName()).append('{');
    b.append(" Id: ").append(id.getId());
    b.append(", Nodes: [");
    for (DatanodeDetails datanodeDetails : nodeStatus.keySet()) {
      b.append(" {").append(datanodeDetails);
      b.append(", ReplicaIndex: ").append(this.getReplicaIndex(datanodeDetails)).append("},");
    }
    b.append(']');
    b.append(", ReplicationConfig: ").append(replicationConfig);
    b.append(", State:").append(getPipelineState());
    b.append(", leaderId:").append(leaderId != null ? leaderId.toString() : "");
    b.append(", CreationTimestamp").append(getCreationTimestamp()
        .atZone(ZoneId.systemDefault()));
    b.append('}');
    return b.toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for Pipeline.
   */
  public static final class Builder {
    private PipelineID id = null;
    private ReplicationConfig replicationConfig = null;
    private PipelineState state = null;
    private Map<DatanodeDetails, Long> nodeStatus = null;
    private List<Integer> nodeOrder = null;
    private List<DatanodeDetails> nodesInOrder = null;
    private DatanodeID leaderId = null;
    private Instant creationTimestamp = null;
    private DatanodeID suggestedLeaderId = null;
    private Map<DatanodeDetails, Integer> replicaIndexes = ImmutableMap.of();

    private Builder() { }

    private Builder(Pipeline pipeline) {
      this.id = pipeline.id;
      this.replicationConfig = pipeline.replicationConfig;
      this.state = pipeline.state;
      this.nodeStatus = pipeline.nodeStatus;
      this.nodesInOrder = pipeline.nodesInOrder;
      this.leaderId = pipeline.getLeaderId();
      this.creationTimestamp = pipeline.getCreationTimestamp();
      this.suggestedLeaderId = pipeline.getSuggestedLeaderId();
      if (nodeStatus != null) {
        final ImmutableMap.Builder<DatanodeDetails, Integer> b = ImmutableMap.builder();
        for (DatanodeDetails dn : nodeStatus.keySet()) {
          int index = pipeline.getReplicaIndex(dn);
          if (index > 0) {
            b.put(dn, index);
          }
        }
        replicaIndexes = b.build();
      }
    }

    public Builder setId(DatanodeID datanodeID) {
      this.id = datanodeID.toPipelineID();
      return this;
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

    public Builder setLeaderId(DatanodeID leaderId1) {
      this.leaderId = leaderId1;
      return this;
    }

    public Builder setNodes(List<DatanodeDetails> nodes) {
      Map<DatanodeDetails, Long> newNodeStatus = new LinkedHashMap<>();
      nodes.forEach(node -> newNodeStatus.put(node, -1L));

      // replace pipeline ID if nodes are not the same
      if (nodeStatus != null && !nodeStatus.keySet().equals(newNodeStatus.keySet())) {
        if (nodes.size() == 1) {
          setId(nodes.iterator().next().getID());
        } else {
          setId(PipelineID.randomId());
        }
      }

      nodeStatus = newNodeStatus;

      if (nodesInOrder != null) {
        // nodesInOrder may belong to another pipeline, avoid overwriting it
        nodesInOrder = new LinkedList<>(nodesInOrder);
        // drop nodes no longer part of the pipeline
        nodesInOrder.retainAll(nodes);
      }
      return this;
    }

    public Builder setNodeOrder(List<Integer> orders) {
      // for build from ProtoBuf
      this.nodeOrder = Collections.unmodifiableList(orders);
      return this;
    }

    public Builder setNodesInOrder(List<? extends DatanodeDetails> nodes) {
      this.nodesInOrder = new LinkedList<>(nodes);
      return this;
    }

    public Builder setCreateTimestamp(Instant instant) {
      this.creationTimestamp = instant;
      return this;
    }

    public Builder setCreateTimestamp(long createTimestamp) {
      this.creationTimestamp = Instant.ofEpochMilli(createTimestamp);
      return this;
    }

    public Builder setSuggestedLeaderId(DatanodeID dnId) {
      this.suggestedLeaderId = dnId;
      return this;
    }

    public Builder setReplicaIndexes(Map<DatanodeDetails, Integer> indexes) {
      this.replicaIndexes = indexes == null ? ImmutableMap.of() : ImmutableMap.copyOf(indexes);
      return this;
    }

    public Pipeline build() {
      Objects.requireNonNull(id, "id == null");
      Objects.requireNonNull(replicationConfig, "replicationConfig == null");
      Objects.requireNonNull(state, "state == null");
      Objects.requireNonNull(nodeStatus, "nodeStatus == null");

      if (nodeOrder != null && !nodeOrder.isEmpty()) {
        List<DatanodeDetails> nodesWithOrder = new ArrayList<>();
        for (int nodeIndex : nodeOrder) {
          Iterator<DatanodeDetails> it = nodeStatus.keySet().iterator();
          while (it.hasNext() && nodeIndex >= 0) {
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
        nodesInOrder = nodesWithOrder;
      }

      return new Pipeline(this);
    }
  }

  /**
   * Possible Pipeline states in SCM.
   */
  public enum PipelineState {
    ALLOCATED, OPEN, DORMANT, CLOSED;

    public static PipelineState fromProtobuf(HddsProtos.PipelineState state) {
      Objects.requireNonNull(state, "state == null");
      switch (state) {
      case PIPELINE_ALLOCATED: return ALLOCATED;
      case PIPELINE_OPEN: return OPEN;
      case PIPELINE_DORMANT: return DORMANT;
      case PIPELINE_CLOSED: return CLOSED;
      default:
        throw new IllegalArgumentException("Unexpected value " + state
            + " from " + state.getClass());
      }
    }

    public static HddsProtos.PipelineState getProtobuf(PipelineState state) {
      Objects.requireNonNull(state, "state == null");
      switch (state) {
      case ALLOCATED: return HddsProtos.PipelineState.PIPELINE_ALLOCATED;
      case OPEN: return HddsProtos.PipelineState.PIPELINE_OPEN;
      case DORMANT: return HddsProtos.PipelineState.PIPELINE_DORMANT;
      case CLOSED: return HddsProtos.PipelineState.PIPELINE_CLOSED;
      default:
        throw new IllegalArgumentException("Unexpected value " + state
            + " from " + state.getClass());
      }
    }
  }
}
