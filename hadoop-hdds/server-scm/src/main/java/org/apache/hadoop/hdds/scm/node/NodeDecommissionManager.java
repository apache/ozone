/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class used to manage datanodes scheduled for maintenance or decommission.
 */
public class NodeDecommissionManager {

  private ScheduledExecutorService executor;
  private DatanodeAdminMonitor monitor;

  private NodeManager nodeManager;
  //private ContainerManager containerManager;
  private SCMContext scmContext;
  private EventPublisher eventQueue;
  private ReplicationManager replicationManager;
  private OzoneConfiguration conf;
  private boolean useHostnames;
  private long monitorInterval;

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeDecommissionManager.class);

  public NodeDecommissionManager(OzoneConfiguration config, NodeManager nm,
             ContainerManager containerManager, SCMContext scmContext,
             EventPublisher eventQueue, ReplicationManager rm) {
    this.nodeManager = nm;
    conf = config;
    //this.containerManager = containerManager;
    this.scmContext = scmContext;
    this.eventQueue = eventQueue;
    this.replicationManager = rm;

    executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("DatanodeAdminManager-%d")
            .setDaemon(true).build());

    useHostnames = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);

    monitorInterval = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT,
        TimeUnit.SECONDS);
    if (monitorInterval <= 0) {
      LOG.warn("{} must be greater than zero, defaulting to {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT);
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT);
      monitorInterval = conf.getTimeDuration(
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT,
          TimeUnit.SECONDS);
    }

    monitor = new DatanodeAdminMonitorImpl(conf, eventQueue, nodeManager,
        replicationManager);

    executor.scheduleAtFixedRate(monitor, monitorInterval, monitorInterval,
        TimeUnit.SECONDS);
  }

  @VisibleForTesting
  public DatanodeAdminMonitor getMonitor() {
    return monitor;
  }

  public synchronized List<DatanodeAdminError> decommissionNodes(
      List<String> nodes) throws InvalidHostStringException {
    List<DatanodeDetails> dns = NodeUtils.mapHostnamesToDatanodes(nodeManager,
        nodes, useHostnames);
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        startDecommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to " +
            "decommission it", dn.getHostName());
        errors.add(new DatanodeAdminError(dn.getHostName(),
            "The host was not found in SCM"));
      } catch (InvalidNodeStateException e) {
        errors.add(new DatanodeAdminError(dn.getHostName(), e.getMessage()));
      }
    }
    return errors;
  }

  /**
   * If a SCM is restarted, then upon re-registration the datanode will already
   * be in DECOMMISSIONING or ENTERING_MAINTENANCE state. In that case, it
   * needs to be added back into the monitor to track its progress.
   * @param dn Datanode to add back to tracking.
   * @throws NodeNotFoundException
   */
  public synchronized void continueAdminForNode(DatanodeDetails dn)
      throws NodeNotFoundException {
    if (!scmContext.isLeader()) {
      LOG.info("follower SCM ignored continue admin for datanode {}", dn);
      return;
    }
    NodeOperationalState opState = getNodeStatus(dn).getOperationalState();
    if (opState == NodeOperationalState.DECOMMISSIONING
        || opState == NodeOperationalState.ENTERING_MAINTENANCE
        || opState == NodeOperationalState.IN_MAINTENANCE) {
      LOG.info("Continue admin for datanode {}", dn);
      monitor.startMonitoring(dn);
    }
  }

  public synchronized void startDecommission(DatanodeDetails dn)
      throws NodeNotFoundException, InvalidNodeStateException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    if (opState == NodeOperationalState.IN_SERVICE) {
      LOG.info("Starting Decommission for node {}", dn);
      nodeManager.setNodeOperationalState(
          dn, NodeOperationalState.DECOMMISSIONING);
      monitor.startMonitoring(dn);
    } else if (nodeStatus.isDecommission()) {
      LOG.info("Start Decommission called on node {} in state {}. Nothing to " +
          "do.", dn, opState);
    } else {
      LOG.error("Cannot decommission node {} in state {}", dn, opState);
      throw new InvalidNodeStateException("Cannot decommission node " +
          dn + " in state " + opState);
    }
  }

  public synchronized List<DatanodeAdminError> recommissionNodes(
      List<String> nodes) throws InvalidHostStringException {
    List<DatanodeDetails> dns = NodeUtils.mapHostnamesToDatanodes(nodeManager,
        nodes, useHostnames);
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        recommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("Host {} was not found in SCM. Ignoring the request to " +
            "recommission it.", dn.getHostName());
        errors.add(new DatanodeAdminError(dn.getHostName(),
            "The host was not found in SCM"));
      }
    }
    return errors;
  }

  public synchronized void recommission(DatanodeDetails dn)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    if (opState != NodeOperationalState.IN_SERVICE) {
      // The node will be set back to IN_SERVICE when it is processed by the
      // monitor
      monitor.stopMonitoring(dn);
      LOG.info("Queued node {} for recommission", dn);
    } else {
      LOG.info("Recommission called on node {} with state {}. " +
          "Nothing to do.", dn, opState);
    }
  }

  public synchronized List<DatanodeAdminError> startMaintenanceNodes(
      List<String> nodes, int endInHours) throws InvalidHostStringException {
    List<DatanodeDetails> dns = NodeUtils.mapHostnamesToDatanodes(nodeManager,
        nodes, useHostnames);
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        startMaintenance(dn, endInHours);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to " +
            "start maintenance on it", dn.getHostName());
      } catch (InvalidNodeStateException e) {
        errors.add(new DatanodeAdminError(dn.getHostName(), e.getMessage()));
      }
    }
    return errors;
  }

  // TODO - If startMaintenance is called on a host already in maintenance,
  //        then we should update the end time?
  public synchronized void startMaintenance(DatanodeDetails dn, int endInHours)
      throws NodeNotFoundException, InvalidNodeStateException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();

    long maintenanceEnd = 0;
    if (endInHours != 0) {
      maintenanceEnd =
          (System.currentTimeMillis() / 1000L) + (endInHours * 60L * 60L);
    }
    if (opState == NodeOperationalState.IN_SERVICE) {
      nodeManager.setNodeOperationalState(
          dn, NodeOperationalState.ENTERING_MAINTENANCE, maintenanceEnd);
      monitor.startMonitoring(dn);
      LOG.info("Starting Maintenance for node {}", dn);
    } else if (nodeStatus.isMaintenance()) {
      LOG.info("Starting Maintenance called on node {} with state {}. " +
          "Nothing to do.", dn, opState);
    } else {
      LOG.error("Cannot start maintenance on node {} in state {}", dn, opState);
      throw new InvalidNodeStateException("Cannot start maintenance on node " +
          dn + " in state " + opState);
    }
  }

  /**
   *  Stops the decommission monitor from running when SCM is shutdown.
   */
  public void stop() {
    if (executor != null) {
      executor.shutdown();
    }
  }

  private NodeStatus getNodeStatus(DatanodeDetails dn)
      throws NodeNotFoundException {
    return nodeManager.getNodeStatus(dn);
  }

  /**
   * Called in SCMStateMachine#notifyLeaderChanged when current SCM becomes
   *  leader.
   */
  public void onBecomeLeader() {
    nodeManager.getAllNodes().forEach(datanodeDetails -> {
      try {
        continueAdminForNode(datanodeDetails);
      } catch (NodeNotFoundException e) {
        // Should not happen, as the node has just registered to call this event
        // handler.
        LOG.warn("NodeNotFound when adding the node to the decommissionManager",
            e);
      }
    });
  }
}
