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

package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerHealthState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.replication.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.NodeDecommissionMetrics.ContainerStateInWorkflow;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitor thread which watches for nodes to be decommissioned, recommissioned
 * or placed into maintenance. Newly added nodes are queued in pendingNodes
 * and recommissoned nodes are queued in cancelled nodes. On each monitor
 * 'tick', the cancelled nodes are processed and removed from the monitor.
 * Then any pending nodes are added to the trackedNodes set, where they stay
 * until decommission or maintenance has ended.
 * <p>
 * Once an node is placed into tracked nodes, it goes through a workflow where
 * the following happens:
 * <p>
 * 1. First an event is fired to close any pipelines on the node, which will
 * also close any containers.
 * 2. Next the containers on the node are obtained and checked to see if new
 * replicas are needed. If so, the new replicas are scheduled.
 * 3. After scheduling replication, the node remains pending until replication
 * has completed.
 * 4. At this stage the node will complete decommission or enter maintenance.
 * 5. Maintenance nodes will remain tracked by this monitor until maintenance
 * is manually ended, or the maintenance window expires.
 */
public class DatanodeAdminMonitorImpl implements DatanodeAdminMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(DatanodeAdminMonitorImpl.class);

  private EventPublisher eventQueue;
  private NodeManager nodeManager;
  private ReplicationManager replicationManager;
  private Queue<TrackedNode> pendingNodes = new ArrayDeque();
  private Queue<TrackedNode> cancelledNodes = new ArrayDeque();
  private Set<TrackedNode> trackedNodes = ConcurrentHashMap.newKeySet();
  private NodeDecommissionMetrics metrics;
  private long pipelinesWaitingToClose = 0;
  private long sufficientlyReplicatedContainers = 0;
  private long trackedDecomMaintenance = 0;
  private long trackedRecommission = 0;
  private long unClosedContainers = 0;
  private long underReplicatedContainers = 0;

  private Map<String, ContainerStateInWorkflow> containerStateByHost;

  // The number of containers for each of under replicated and unhealthy
  // that will be logged in detail each time a node is checked.
  private final int containerDetailsLoggingLimit;

  /**
   * Inner class for snapshot of Datanode ContainerState in
   * Decommissioning and Maintenance mode workflow.
   */
  public static final class TrackedNode {

    private DatanodeDetails datanodeDetails;
    private long startTime = 0L;
    private Map<String, List<ContainerID>> containersReplicatedOnNode = new ConcurrentHashMap<>();

    public TrackedNode(DatanodeDetails datanodeDetails, long startTime) {
      this.datanodeDetails = datanodeDetails;
      this.startTime = startTime;
    }

    @Override
    public int hashCode() {
      return datanodeDetails.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TrackedNode &&
          datanodeDetails.equals(((TrackedNode) obj).datanodeDetails);
    }

    public DatanodeDetails getDatanodeDetails() {
      return datanodeDetails;
    }

    public long getStartTime() {
      return startTime;
    }

    public Map<String, List<ContainerID>> getContainersReplicatedOnNode() {
      return containersReplicatedOnNode;
    }

    public void setContainersReplicatedOnNode(List<ContainerID> underReplicated, List<ContainerID> unClosed) {
      this.containersReplicatedOnNode.put("UnderReplicated", Collections.unmodifiableList(underReplicated));
      this.containersReplicatedOnNode.put("UnClosed", Collections.unmodifiableList(unClosed));
    }
  }

  public DatanodeAdminMonitorImpl(
      OzoneConfiguration conf,
      EventPublisher eventQueue,
      NodeManager nodeManager,
      ReplicationManager replicationManager) {
    this.eventQueue = eventQueue;
    this.nodeManager = nodeManager;
    this.replicationManager = replicationManager;
    containerDetailsLoggingLimit = conf.getInt(
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_LOGGING_LIMIT,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_LOGGING_LIMIT_DEFAULT);
    containerStateByHost = new HashMap<>();
  }

  /**
   * Add a node to the decommission or maintenance workflow. The node will be
   * queued and added to the workflow after a defined interval.
   *
   * @param dn         The datanode to move into an admin state
   */
  @Override
  public synchronized void startMonitoring(DatanodeDetails dn) {
    TrackedNode tn = new TrackedNode(dn, System.currentTimeMillis());
    cancelledNodes.remove(tn);
    pendingNodes.add(tn);
  }

  /**
   * Remove a node from the decommission or maintenance workflow, and return it
   * to service. The node will be queued and removed from decommission or
   * maintenance after a defined interval.
   *
   * @param dn The datanode for which to stop decommission or maintenance.
   */
  @Override
  public synchronized void stopMonitoring(DatanodeDetails dn) {
    TrackedNode tn = new TrackedNode(dn, 0L);
    pendingNodes.remove(tn);
    cancelledNodes.add(tn);
  }

  @Override
  public synchronized void setMetrics(NodeDecommissionMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * Get the set of nodes which are currently tracked in the decommissioned
   * and maintenance workflow.
   *
   * @return An unmodifiable set of the tracked nodes.
   */
  @Override
  public synchronized Set<TrackedNode> getTrackedNodes() {
    return Collections.unmodifiableSet(trackedNodes);
  }

  /**
   * Get a node which is currently tracked in the decommissioned
   * or maintenance workflow.
   *
   * @return tracked node with given IpAddress.
   */
  @VisibleForTesting
  public synchronized TrackedNode getSingleTrackedNode(String ip) {
    Iterator<TrackedNode> iterator = trackedNodes.iterator();
    while (iterator.hasNext()) {
      TrackedNode trackedNode = iterator.next();
      if (trackedNode.getDatanodeDetails().getIpAddress().equals(ip)) {
        return trackedNode;
      }
    }
    return null;
  }

  /**
   * Run an iteration of the monitor. This is the main run loop, and performs
   * the following checks:
   * <p>
   * 1. Check for any cancelled nodes and process them
   * 2. Check for any newly added nodes and add them to the workflow
   * 3. Perform checks on the transitioning nodes and move them through the
   * workflow until they have completed decommission or maintenance
   */
  @Override
  public void run() {
    try {
      containerStateByHost.clear();
      synchronized (this) {
        trackedRecommission = getCancelledCount();
        processCancelledNodes();
        processPendingNodes();
        trackedDecomMaintenance = getTrackedNodeCount();
      }
      processTransitioningNodes();
      if (!trackedNodes.isEmpty() || !pendingNodes.isEmpty()) {
        LOG.info("There are {} nodes tracked for decommission and " +
            "maintenance.  {} pending nodes.",
            trackedNodes.size(), pendingNodes.size());
      }
      setMetricsToGauge();
    } catch (Exception e) {
      LOG.error("Caught an error in the DatanodeAdminMonitor", e);
      // Intentionally do not re-throw, as if we do the monitor thread
      // will not get rescheduled.
    }
  }

  public int getPendingCount() {
    return pendingNodes.size();
  }

  public int getCancelledCount() {
    return cancelledNodes.size();
  }

  public int getTrackedNodeCount() {
    return trackedNodes.size();
  }

  synchronized void setMetricsToGauge() {
    synchronized (metrics) {
      metrics.setContainersUnClosedTotal(unClosedContainers);
      metrics.setRecommissionNodesTotal(trackedRecommission);
      metrics.setDecommissioningMaintenanceNodesTotal(
          trackedDecomMaintenance);
      metrics.setContainersUnderReplicatedTotal(
          underReplicatedContainers);
      metrics.setContainersSufficientlyReplicatedTotal(
          sufficientlyReplicatedContainers);
      metrics.setPipelinesWaitingToCloseTotal(pipelinesWaitingToClose);
      metrics.metricRecordOfContainerStateByHost(containerStateByHost);
    }
  }

  void resetContainerMetrics() {
    pipelinesWaitingToClose = 0;
    sufficientlyReplicatedContainers = 0;
    unClosedContainers = 0;
    underReplicatedContainers = 0;
  }

  private void processCancelledNodes() {
    while (!cancelledNodes.isEmpty()) {
      TrackedNode dn = cancelledNodes.poll();
      try {
        stopTrackingNode(dn.getDatanodeDetails());
        putNodeBackInService(dn.getDatanodeDetails());
        LOG.info("Recommissioned node {}", dn);
      } catch (NodeNotFoundException e) {
        LOG.warn("Failed processing the cancel admin request for {}", dn, e);
      }
    }
  }

  private void processPendingNodes() {
    while (!pendingNodes.isEmpty()) {
      startTrackingNode(pendingNodes.poll());
    }
  }

  private void processTransitioningNodes() {
    resetContainerMetrics();
    Iterator<TrackedNode> iterator = trackedNodes.iterator();

    while (iterator.hasNext()) {
      TrackedNode trackedNode = iterator.next();
      DatanodeDetails dn = trackedNode.getDatanodeDetails();
      try {
        NodeStatus status = getNodeStatus(dn);

        if (!shouldContinueWorkflow(dn, status)) {
          abortWorkflow(dn);
          iterator.remove();
          continue;
        }

        if (status.isMaintenance()) {
          if (status.operationalStateExpired()) {
            completeMaintenance(dn);
            iterator.remove();
            continue;
          }
        }

        if (status.isDecommissioning() || status.isEnteringMaintenance()) {
          if (checkPipelinesClosedOnNode(trackedNode)
              // Ensure the DN has received and persisted the current maint
              // state.
              && status.getOperationalState()
                  == dn.getPersistedOpState()
              && checkContainersReplicatedOnNode(trackedNode)) {
            // CheckContainersReplicatedOnNode may take a short time to run
            // so after it completes, re-get the nodestatus to check the health
            // and ensure the state is still good to continue
            status = getNodeStatus(dn);
            if (status.isDead()) {
              LOG.warn("Datanode {} is dead and the admin workflow cannot " +
                  "continue. The node will be put back to IN_SERVICE and " +
                  "handled as a dead node", dn);
              putNodeBackInService(dn);
              iterator.remove();
            } else if (status.isDecommissioning()) {
              completeDecommission(dn);
              iterator.remove();
            } else if (status.isEnteringMaintenance()) {
              putIntoMaintenance(dn);
            }
          }
        }

      } catch (NodeNotFoundException e) {
        LOG.error("An unexpected error occurred processing datanode {}. " +
            "Aborting the admin workflow", dn, e);
        abortWorkflow(dn);
        iterator.remove();
      }
    }
  }

  /**
   * Checks if a node is in an unexpected state or has gone dead while
   * decommissioning or entering maintenance. If the node is not in a valid
   * state to continue the admin workflow, return false, otherwise return true.
   *
   * @param dn         The Datanode for which to check the current state
   * @param nodeStatus The current NodeStatus for the datanode
   * @return True if admin can continue, false otherwise
   */
  private boolean shouldContinueWorkflow(DatanodeDetails dn,
      NodeStatus nodeStatus) {
    if (!nodeStatus.isDecommission() && !nodeStatus.isMaintenance()) {
      LOG.warn("Datanode {} has an operational state of {} when it should " +
          "be undergoing decommission or maintenance. Aborting admin for " +
          "this node.", dn, nodeStatus.getOperationalState());
      return false;
    }
    if (nodeStatus.isDead() && !nodeStatus.isInMaintenance()) {
      LOG.error("Datanode {} is dead but is not IN_MAINTENANCE. Aborting the " +
          "admin workflow for this node", dn);
      return false;
    }
    return true;
  }

  private boolean checkPipelinesClosedOnNode(TrackedNode dn)
      throws NodeNotFoundException {
    Set<PipelineID> pipelines = nodeManager.getPipelines(dn
        .getDatanodeDetails());
    NodeStatus status = nodeManager.getNodeStatus(dn.getDatanodeDetails());
    if (pipelines == null || pipelines.isEmpty()
        || status.operationalStateExpired()) {
      return true;
    } else {
      LOG.info("Waiting for pipelines to close for {}. There are {} " +
          "pipelines", dn, pipelines.size());
      containerStateByHost.put(dn.getDatanodeDetails().getHostName(),
        new ContainerStateInWorkflow(dn.getDatanodeDetails().getHostName(),
            0L, 0L, 0L, pipelines.size(), dn.getStartTime()));
      pipelinesWaitingToClose += pipelines.size();
      return false;
    }
  }

  private boolean checkContainersReplicatedOnNode(TrackedNode dn)
      throws NodeNotFoundException {
    int sufficientlyReplicated = 0;
    int deleting = 0;
    int underReplicated = 0;
    int unclosed = 0;
    List<ContainerID> underReplicatedIDs = new ArrayList<>();
    List<ContainerID> unClosedIDs = new ArrayList<>();
    Set<ContainerID> containers =
        nodeManager.getContainers(dn.getDatanodeDetails());
    for (ContainerID cid : containers) {
      try {
        ContainerReplicaCount replicaSet =
            replicationManager.getContainerReplicaCount(cid);

        // If a container is deleted or deleting, and we have a replica on this
        // datanode, just ignore it. It should not block decommission.
        HddsProtos.LifeCycleState containerState
            = replicaSet.getContainer().getState();
        if (containerState == HddsProtos.LifeCycleState.DELETED
            || containerState == HddsProtos.LifeCycleState.DELETING) {
          deleting++;
          continue;
        }

        boolean isHealthy = replicaSet.isHealthyEnoughForOffline();
        if (!isHealthy) {
          unClosedIDs.add(cid);
          if (unclosed < containerDetailsLoggingLimit
              || LOG.isDebugEnabled()) {
            LOG.info("Unclosed Container {} {}; {}", cid, replicaSet, replicaDetails(replicaSet.getReplicas()));
          }
          unclosed++;
          continue;
        }

        ReplicationManagerReport report = new ReplicationManagerReport(
            replicationManager.getConfig().getContainerSampleLimit());
        replicationManager.checkContainerStatus(replicaSet.getContainer(), report);
        boolean replicatedOK = report.getStat(ContainerHealthState.UNDER_REPLICATED) == 0;

        if (replicatedOK) {
          sufficientlyReplicated++;
        } else {
          underReplicatedIDs.add(cid);
          if (underReplicated < containerDetailsLoggingLimit || LOG.isDebugEnabled()) {
            LOG.info("Under Replicated Container {} {}; {}", cid, replicaSet, replicaDetails(replicaSet.getReplicas()));
          }
          underReplicated++;
        }
      } catch (ContainerNotFoundException e) {
        LOG.warn("ContainerID {} present in node list for {} but not found in containerManager", cid,
            dn.getDatanodeDetails());
      }
    }
    LOG.info("{} has {} sufficientlyReplicated, {} deleting, {} " +
            "underReplicated and {} unclosed containers",
        dn, sufficientlyReplicated, deleting, underReplicated, unclosed);
    containerStateByHost.put(dn.getDatanodeDetails().getHostName(),
        new ContainerStateInWorkflow(dn.getDatanodeDetails().getHostName(),
            sufficientlyReplicated,
            underReplicated,
            unclosed,
            0L, dn.getStartTime()));
    sufficientlyReplicatedContainers += sufficientlyReplicated;
    underReplicatedContainers += underReplicated;
    unClosedContainers += unclosed;
    if (LOG.isDebugEnabled() && underReplicatedIDs.size() < 10000 &&
        unClosedIDs.size() < 10000) {
      LOG.debug("{} has {} underReplicated [{}] and {} unclosed [{}] " +
              "containers", dn, underReplicated,
          underReplicatedIDs.stream().map(
              Object::toString).collect(Collectors.joining(", ")),
          unclosed, unClosedIDs.stream().map(
              Object::toString).collect(Collectors.joining(", ")));
    }
    dn.setContainersReplicatedOnNode(underReplicatedIDs, unClosedIDs);
    return underReplicated == 0 && unclosed == 0;
  }

  @Override
  public Map<String, List<ContainerID>> getContainersPendingReplication(DatanodeDetails dn) {
    Iterator<TrackedNode> iterator = trackedNodes.iterator();
    while (iterator.hasNext()) {
      TrackedNode trackedNode = iterator.next();
      if (trackedNode.equals(new TrackedNode(dn, 0L))) {
        return trackedNode.getContainersReplicatedOnNode();
      }
    }
    return new HashMap<>();
  }

  private String replicaDetails(Collection<ContainerReplica> replicas) {
    StringBuilder sb = new StringBuilder();
    sb.append("Replicas{");
    sb.append(replicas.stream()
        .map(Object::toString)
        .collect(Collectors.joining(",")));
    sb.append('}');
    return sb.toString();
  }

  private void completeDecommission(DatanodeDetails dn)
      throws NodeNotFoundException {
    setNodeOpState(dn, NodeOperationalState.DECOMMISSIONED);
    LOG.info("Datanode {} has completed the admin workflow. The operational " +
            "state has been set to {}", dn,
        NodeOperationalState.DECOMMISSIONED);
  }

  private void putIntoMaintenance(DatanodeDetails dn)
      throws NodeNotFoundException {
    LOG.info("Datanode {} has entered maintenance", dn);
    setNodeOpState(dn, NodeOperationalState.IN_MAINTENANCE);
  }

  private void completeMaintenance(DatanodeDetails dn)
      throws NodeNotFoundException {
    // The end state of Maintenance is to put the node back IN_SERVICE, whether
    // it is dead or not.
    LOG.info("Datanode {} has ended maintenance automatically", dn);
    putNodeBackInService(dn);
  }

  private void startTrackingNode(TrackedNode dn) {
    eventQueue.fireEvent(SCMEvents.START_ADMIN_ON_NODE,
        dn.getDatanodeDetails());
    trackedNodes.add(dn);
  }

  private void stopTrackingNode(DatanodeDetails dn) {
    trackedNodes.remove(new TrackedNode(dn, 0L));
  }

  /**
   * If we encounter an unexpected condition in maintenance, we must abort the
   * workflow by setting the node operationalState back to IN_SERVICE and then
   * remove the node from tracking.
   *
   * @param dn The datanode for which to abort tracking
   */
  private void abortWorkflow(DatanodeDetails dn) {
    try {
      putNodeBackInService(dn);
    } catch (NodeNotFoundException e) {
      LOG.error("Unable to set the node OperationalState for {} while " +
          "aborting the datanode admin workflow", dn);
    }
  }

  private void putNodeBackInService(DatanodeDetails dn)
      throws NodeNotFoundException {
    setNodeOpState(dn, NodeOperationalState.IN_SERVICE);
  }

  private void setNodeOpState(DatanodeDetails dn,
      HddsProtos.NodeOperationalState state) throws NodeNotFoundException {
    long expiry = 0;
    if ((state == NodeOperationalState.IN_MAINTENANCE)
        || (state == NodeOperationalState.ENTERING_MAINTENANCE)) {
      NodeStatus status = nodeManager.getNodeStatus(dn);
      expiry = status.getOpStateExpiryEpochSeconds();
    }
    nodeManager.setNodeOperationalState(dn, state, expiry);
  }

  private NodeStatus getNodeStatus(DatanodeDetails dnd)
      throws NodeNotFoundException {
    return nodeManager.getNodeStatus(dnd);
  }

}
