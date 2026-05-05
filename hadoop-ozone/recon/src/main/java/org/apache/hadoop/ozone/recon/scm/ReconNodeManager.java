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

package org.apache.hadoop.ozone.recon.scm;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type.reregisterCommand;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandQueueReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.LayoutVersionProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto.Type;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.scm.net.NetworkTopology.InvalidTopologyException;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.SCMNodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutVersionManager;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recon SCM's Node manager that includes persistence.
 */
public class ReconNodeManager extends SCMNodeManager {

  private static final Logger LOG = LoggerFactory
      .getLogger(ReconNodeManager.class);

  private Table<DatanodeID, DatanodeDetails> nodeDB;
  private ReconContext reconContext;
  private static final Set<Type> ALLOWED_COMMANDS =
      ImmutableSet.of(reregisterCommand);

  /**
   * Map that contains mapping between datanodes
   * and their last heartbeat time.
   */
  private Map<DatanodeID, Long> datanodeHeartbeatMap = new HashMap<>();

  private final long reconDatanodeOutdatedTime;

  public ReconNodeManager(OzoneConfiguration conf,
                          SCMStorageConfig scmStorageConfig,
                          EventPublisher eventPublisher,
                          NetworkTopology networkTopology,
                          Table<DatanodeID, DatanodeDetails> nodeDB,
                          HDDSLayoutVersionManager scmLayoutVersionManager) {
    super(conf, scmStorageConfig, eventPublisher, networkTopology,
        SCMContext.emptyContext(), scmLayoutVersionManager);
    final int reconStaleDatanodeMultiplier = 3;
    this.reconDatanodeOutdatedTime = reconStaleDatanodeMultiplier *
        HddsServerUtil.getReconHeartbeatInterval(conf);
    this.nodeDB = nodeDB;
  }

  public ReconNodeManager(OzoneConfiguration conf, SCMStorageConfig scmStorageConfig, EventQueue eventQueue,
                          NetworkTopology clusterMap, Table<DatanodeID, DatanodeDetails> table,
                          HDDSLayoutVersionManager scmLayoutVersionManager, ReconContext reconContext) {
    this(conf, scmStorageConfig, eventQueue, clusterMap, table, scmLayoutVersionManager);
    this.reconContext = reconContext;
    loadExistingNodes();
  }

  private void loadExistingNodes() {
    try (TableIterator<DatanodeID, ? extends Table.KeyValue<DatanodeID, DatanodeDetails>>
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
    nodeDB.put(datanodeDetails.getID(), datanodeDetails);
    LOG.info("Added a new node to db: {}", datanodeDetails);
  }

  /**
   * Returns the last heartbeat time of the given node.
   *
   * @param datanodeDetails DatanodeDetails
   * @return last heartbeat time
   */
  @Override
  public long getLastHeartbeat(DatanodeDetails datanodeDetails) {
    return datanodeHeartbeatMap.getOrDefault(datanodeDetails.getID(), 0L);
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
          commandForDatanode);
    }
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @return SCMheartbeat response.
   */
  @Override
  public List<SCMCommand<?>> processHeartbeat(DatanodeDetails datanodeDetails,
      CommandQueueReportProto queueReport) {
    long currentTime = Time.now();
    final Long lastHeartbeat = datanodeHeartbeatMap.put(datanodeDetails.getID(), currentTime);
    final boolean needUpdate = lastHeartbeat == null
        || currentTime - lastHeartbeat >= reconDatanodeOutdatedTime;
    if (needUpdate) {
      LOG.info("Sending ReregisterCommand() for {}", datanodeDetails);
      return Collections.singletonList(new ReregisterCommand());
    }
    return super.processHeartbeat(datanodeDetails, queueReport).stream()
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
    if (isNodeRegistered(datanodeDetails)) {
      try {
        nodeDB.put(datanodeDetails.getID(), datanodeDetails);
        LOG.info("Updated {} db table for {}", nodeDB.getName(), datanodeDetails);
      } catch (IOException e) {
        LOG.error("Failed to update {} db table for {}", nodeDB.getName(), datanodeDetails, e);
      }
    }
    try {
      RegisteredCommand registeredCommand = super.register(datanodeDetails, nodeReport, pipelineReportsProto,
          layoutInfo);
      reconContext.updateHealthStatus(new AtomicBoolean(true));
      reconContext.getErrors().remove(ReconContext.ErrorCode.INVALID_NETWORK_TOPOLOGY);
      return registeredCommand;
    } catch (InvalidTopologyException invalidTopologyException) {
      LOG.error("InvalidTopologyException error occurred : {}", invalidTopologyException.getMessage());
      reconContext.updateHealthStatus(new AtomicBoolean(false));
      reconContext.getErrors().add(ReconContext.ErrorCode.INVALID_NETWORK_TOPOLOGY);
      return RegisteredCommand.newBuilder()
          .setErrorCode(
              StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto.ErrorCode.errorNodeNotPermitted)
          .setDatanode(datanodeDetails)
          .setClusterID(reconContext.getClusterId())
          .build();
    }
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
      DatanodeDetails scmDnd = getNode(dnDetails.getID());
      scmDnd.setPersistedOpState(nodeOperationalStateFromScm);
    }
  }

  public void reinitialize(Table<DatanodeID, DatanodeDetails> nodeTable) {
    this.nodeDB = nodeTable;
    loadExistingNodes();
  }

  @VisibleForTesting
  public long getNodeDBKeyCount() throws IOException {
    long nodeCount = 0;
    try (TableIterator<DatanodeID, ? extends Table.KeyValue<DatanodeID, DatanodeDetails>>
        iterator = nodeDB.iterator()) {
      while (iterator.hasNext()) {
        iterator.next();
        nodeCount++;
      }
      return nodeCount;
    }
  }

  /**
   * Remove an existing node from the NodeDB. Explicit removal from admin user.
   * First this API call removes the node info from NodeManager memory and
   * if successful, then remove the node finally from NODES table as well.
   *
   * @param datanodeDetails Datanode details.
   */
  @Override
  public void removeNode(DatanodeDetails datanodeDetails) throws NodeNotFoundException, IOException {
    try {
      super.removeNode(datanodeDetails);
      nodeDB.delete(datanodeDetails.getID());
    } catch (IOException e) {
      throw new IOException("Failed to delete from nodeDB for " + datanodeDetails, e);
    }
    datanodeHeartbeatMap.remove(datanodeDetails.getID());
    LOG.info("Removed existing node {} from Node DB and NodeManager data structures in memory ",
        datanodeDetails);
  }

  @Override
  protected void sendFinalizeToDatanodeIfNeeded(DatanodeDetails datanodeDetails,
      LayoutVersionProto layoutVersionReport) {
    // Recon should do nothing here.
    int scmSlv = getLayoutVersionManager().getSoftwareLayoutVersion();
    int scmMlv = getLayoutVersionManager().getMetadataLayoutVersion();
    int dnSlv = layoutVersionReport.getSoftwareLayoutVersion();
    int dnMlv = layoutVersionReport.getMetadataLayoutVersion();

    if (dnSlv > scmSlv) {
      LOG.error("Invalid data node reporting to Recon : {}. " +
              "DataNode SoftwareLayoutVersion = {}, Recon/SCM " +
              "SoftwareLayoutVersion = {}",
          datanodeDetails.getHostName(), dnSlv, scmSlv);
    }

    if (scmMlv == scmSlv) {
      // Recon metadata is finalised.
      if (dnMlv < scmMlv) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Data node {} reports a lower MLV than Recon "
                  + "DataNode MetadataLayoutVersion = {}, Recon/SCM "
                  + "MetadataLayoutVersion = {}. SCM needs to finalize this DN",
              datanodeDetails.getHostName(), dnMlv, scmMlv);
        }
      }
    }

  }
}
