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

package org.apache.hadoop.ozone.recon.scm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;

import com.google.common.collect.ImmutableSet;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reregisterCommand;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon SCM's Node manager that includes persistence.
 */
public class ReconNodeManager extends SCMNodeManager {

  public static final Logger LOG = LoggerFactory
      .getLogger(ReconNodeManager.class);

  private Table<UUID, DatanodeDetails> nodeDB;
  private static final Set<Type> ALLOWED_COMMANDS =
      ImmutableSet.of(reregisterCommand);

  /**
   * Map that contains mapping between datanodes
   * and their last heartbeat time.
   */
  private Map<UUID, Long> datanodeHeartbeatMap = new HashMap<>();
  private Map<UUID, DatanodeDetails> inMemDatanodeDetails = new HashMap<>();

  private long reconDatanodeOutdatedTime;
  private static int reconStaleDatanodeMultiplier = 3;

  private static final DatanodeDetails EMPTY_DATANODE_DETAILS =
      DatanodeDetails.newBuilder().setUuid(UUID.randomUUID()).build();

  public ReconNodeManager(OzoneConfiguration conf,
                          SCMStorageConfig scmStorageConfig,
                          EventPublisher eventPublisher,
                          NetworkTopology networkTopology,
                          Table<UUID, DatanodeDetails> nodeDB,
                          HDDSLayoutVersionManager scmLayoutVersionManager) {
    super(conf, scmStorageConfig, eventPublisher, networkTopology,
        SCMContext.emptyContext(), scmLayoutVersionManager);
    this.reconDatanodeOutdatedTime = reconStaleDatanodeMultiplier *
        HddsServerUtil.getReconHeartbeatInterval(conf);
    this.nodeDB = nodeDB;
    loadExistingNodes();
  }

  private void loadExistingNodes() {
    try (TableIterator<UUID, ? extends Table.KeyValue<UUID, DatanodeDetails>>
             iterator = nodeDB.iterator()) {
      int nodeCount = 0;
      while (iterator.hasNext()) {
        DatanodeDetails datanodeDetails = iterator.next().getValue();
        register(datanodeDetails, null, null,
            LayoutVersionProto.newBuilder()
                .setMetadataLayoutVersion(
                    HDDSLayoutVersionManager.maxLayoutVersion())
                .setSoftwareLayoutVersion(
                    HDDSLayoutVersionManager.maxLayoutVersion())
                .build());
        nodeCount++;
      }
      LOG.info("Loaded {} nodes from node DB.", nodeCount);
    } catch (IOException ioEx) {
      LOG.error("Exception while loading existing nodes.", ioEx);
    }
  }

  @Override
  public VersionResponse getVersion(SCMVersionRequestProto versionRequest) {
    return VersionResponse.newBuilder()
        .setVersion(0)
        .build();
  }

  /**
   * Add a new new node to the NodeDB. Must be called after register.
   * @param datanodeDetails Datanode details.
   */
  public void addNodeToDB(DatanodeDetails datanodeDetails) throws IOException {
    nodeDB.put(datanodeDetails.getUuid(), datanodeDetails);
    LOG.info("Adding new node {} to Node DB.", datanodeDetails.getUuid());
  }

  /**
   * Returns the last heartbeat time of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return last heartbeat time
   */
  public long getLastHeartbeat(DatanodeDetails datanodeDetails) {
    return datanodeHeartbeatMap.getOrDefault(datanodeDetails.getUuid(), 0L);
  }

  /**
   * Returns the hostname of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return hostname
   */
  public String getHostName(DatanodeDetails datanodeDetails) {
    return inMemDatanodeDetails.getOrDefault(datanodeDetails.getUuid(),
        EMPTY_DATANODE_DETAILS).getHostName();
  }

  /**
   * Returns the version of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return setTime
   */
  public String getVersion(DatanodeDetails datanodeDetails) {
    return inMemDatanodeDetails.getOrDefault(datanodeDetails.getUuid(),
        EMPTY_DATANODE_DETAILS).getVersion();
  }

  /**
   * Returns the setupTime of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return setupTime
   */
  public long getSetupTime(DatanodeDetails datanodeDetails) {
    return inMemDatanodeDetails.getOrDefault(datanodeDetails.getUuid(),
        EMPTY_DATANODE_DETAILS).getSetupTime();
  }

  /**
   * Returns the revision of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return revision
   */
  public String getRevision(DatanodeDetails datanodeDetails) {
    return inMemDatanodeDetails.getOrDefault(datanodeDetails.getUuid(),
        EMPTY_DATANODE_DETAILS).getRevision();
  }

  /**
   * Returns the build date of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return buildDate
   */
  public String getBuildDate(DatanodeDetails datanodeDetails) {
    return inMemDatanodeDetails.getOrDefault(datanodeDetails.getUuid(),
        EMPTY_DATANODE_DETAILS).getBuildDate();
  }

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
                        EventPublisher ignored) {
    final Type cmdType = commandForDatanode.getCommand().getType();
    if (ALLOWED_COMMANDS.contains(cmdType)) {
      super.onMessage(commandForDatanode, ignored);
    } else {
      LOG.debug("Ignoring unsupported command {} for Datanode {}.",
          commandForDatanode.getCommand().getType(),
          commandForDatanode.getDatanodeId());
    }
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @param layoutInfo - Layout Version Proto
   * @return SCMheartbeat response.
   */
  @Override
  public List<SCMCommand> processHeartbeat(DatanodeDetails datanodeDetails,
                                           LayoutVersionProto layoutInfo) {
    List<SCMCommand> cmds = new ArrayList<>();
    long currentTime = Time.now();
    if (needUpdate(datanodeDetails, currentTime)) {
      cmds.add(new ReregisterCommand());
      LOG.info("Sending ReregisterCommand() for " +
          datanodeDetails.getHostName());
      datanodeHeartbeatMap.put(datanodeDetails.getUuid(), Time.now());
      return cmds;
    }
    // Update heartbeat map with current time
    datanodeHeartbeatMap.put(datanodeDetails.getUuid(), Time.now());
    cmds.addAll(super.processHeartbeat(datanodeDetails, layoutInfo));
    return cmds.stream()
        .filter(c -> ALLOWED_COMMANDS.contains(c.getType()))
        .collect(toList());
  }

  @Override
  protected void updateDatanodeOpState(DatanodeDetails reportedDn)
      throws NodeNotFoundException {
    super.updateDatanodeOpState(reportedDn);
    // Update NodeOperationalState in NodeStatus to keep it consistent for Recon
    super.getNodeStateManager().setNodeOperationalState(reportedDn,
        reportedDn.getPersistedOpState(),
        reportedDn.getPersistedOpStateExpiryEpochSec());
  }

  /**
   * send refresh command to all the healthy datanodes to refresh
   * volume usage info immediately.
   */
  @Override
  public void refreshAllHealthyDnUsageInfo() {
    //no op
  }

  @Override
  public RegisteredCommand register(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport,
      PipelineReportsProto pipelineReportsProto,
      LayoutVersionProto layoutInfo) {
    inMemDatanodeDetails.put(datanodeDetails.getUuid(), datanodeDetails);
    if (isNodeRegistered(datanodeDetails)) {
      try {
        nodeDB.put(datanodeDetails.getUuid(), datanodeDetails);
        LOG.info("Updating nodeDB for " + datanodeDetails.getHostName());
      } catch (IOException e) {
        LOG.error("Can not update node {} to Node DB.",
            datanodeDetails.getUuid());
      }
    }
    return super.register(datanodeDetails, nodeReport, pipelineReportsProto,
        layoutInfo);
  }

  public void updateNodeOperationalStateFromScm(HddsProtos.Node scmNode,
                                                DatanodeDetails dnDetails)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = getNodeStatus(dnDetails);
    HddsProtos.NodeOperationalState nodeOperationalStateFromScm =
        scmNode.getNodeOperationalStates(0);
    if (nodeOperationalStateFromScm != nodeStatus.getOperationalState()) {
      LOG.info("Updating Node operational state for {}, in SCM = {}, in " +
              "Recon = {}", dnDetails.getHostName(),
          nodeOperationalStateFromScm,
          nodeStatus.getOperationalState());

      setNodeOperationalState(dnDetails, nodeOperationalStateFromScm);
      DatanodeDetails scmDnd = getNodeByUuid(dnDetails.getUuidString());
      scmDnd.setPersistedOpState(nodeOperationalStateFromScm);
    }
  }

  private boolean needUpdate(DatanodeDetails datanodeDetails,
      long currentTime) {
    return currentTime - getLastHeartbeat(datanodeDetails) >=
        reconDatanodeOutdatedTime;
  }

  public void reinitialize(Table<UUID, DatanodeDetails> nodeTable) {
    this.nodeDB = nodeTable;
    loadExistingNodes();
  }

  @VisibleForTesting
  public long getNodeDBKeyCount() throws IOException {
    long nodeCount = 0;
    try (TableIterator<UUID, ? extends Table.KeyValue<UUID, DatanodeDetails>>
        iterator = nodeDB.iterator()) {
      while (iterator.hasNext()) {
        iterator.next();
        nodeCount++;
      }
      return nodeCount;
    }
  }
}
