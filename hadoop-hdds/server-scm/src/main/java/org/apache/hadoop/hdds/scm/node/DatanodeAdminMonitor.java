/**
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
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaCount;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Monitor thread which watches for nodes to be decommissioned, recommissioned
 * or placed into maintenance. Newly added nodes are queued in pendingNodes
 * and recommissoned nodes are queued in cancelled nodes. On each monitor
 * 'tick', the cancelled nodes are processed and removed from the monitor.
 * Then any pending nodes are added to the trackedNodes set, where they stay
 * until decommission or maintenance has ended.
 *
 * Once an node is placed into tracked nodes, it goes through a workflow where
 * the following happens:
 *
 * 1. First an event is fired to close any pipelines on the node, which will
 *    also close any containers.
 * 2. Next the containers on the node are obtained and checked to see if new
 *    replicas are needed. If so, the new replicas are scheduled.
 * 3. After scheduling replication, the node remains pending until replication
 *    has completed.
 * 4. At this stage the node will complete decommission or enter maintenance.
 * 5. Maintenance nodes will remain tracked by this monitor until maintenance
 *    is manually ended, or the maintenance window expires.
 */
public class DatanodeAdminMonitor implements DatanodeAdminMonitorInterface {

  private OzoneConfiguration conf;
  private EventPublisher eventQueue;
  private NodeManager nodeManager;
  private PipelineManager pipelineManager;
  private ReplicationManager replicationManager;
  private Queue<DatanodeAdminNodeDetails> pendingNodes = new ArrayDeque();
  private Queue<DatanodeAdminNodeDetails> cancelledNodes = new ArrayDeque();
  private Set<DatanodeAdminNodeDetails> trackedNodes = new HashSet<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminMonitor.class);

  public DatanodeAdminMonitor(OzoneConfiguration config) {
    conf = config;
  }

  @Override
  public void setConf(OzoneConfiguration config) {
    conf = config;
  }

  @Override
  public void setEventQueue(EventPublisher eventQueue) {
    this.eventQueue = eventQueue;
  }

  @Override
  public void setNodeManager(NodeManager nm) {
    nodeManager = nm;
  }

  @Override
  public void setPipelineManager(PipelineManager pm) {
    pipelineManager = pm;
  }

  @Override
  public void setReplicationManager(ReplicationManager rm) {
    replicationManager = rm;
  }

  /**
   * Add a node to the decommission or maintenance workflow. The node will be
   * queued and added to the workflow after a defined interval.
   *
   * @param dn The datanode to move into an admin state
   * @param endInHours For nodes going into maintenance, the number of hours
   *                   from now for maintenance to automatically end. Ignored
   *                   for decommissioning nodes.
   */
  @Override
  public synchronized void startMonitoring(DatanodeDetails dn, int endInHours) {
    DatanodeAdminNodeDetails nodeDetails =
        new DatanodeAdminNodeDetails(dn, endInHours);
    cancelledNodes.remove(nodeDetails);
    pendingNodes.add(nodeDetails);
  }

  /**
   * Remove a node from the decommission or maintenance workflow, and return it
   * to service. The node will be queued and removed from decommission or
   * maintenance after a defined interval.
   * @param dn The datanode for which to stop decommission or maintenance.
   */
  @Override
  public synchronized void stopMonitoring(DatanodeDetails dn) {
    DatanodeAdminNodeDetails nodeDetails = new DatanodeAdminNodeDetails(dn, 0);
    pendingNodes.remove(nodeDetails);
    cancelledNodes.add(nodeDetails);
  }

  /**
   * Run an iteration of the monitor. This is the main run loop, and performs
   * the following checks:
   *
   * 1. Check for any cancelled nodes and process them
   * 2. Check for any newly added nodes and add them to the workflow
   * 3. Perform checks on the transitioning nodes and move them through the
   *    workflow until they have completed decommission or maintenance
   */
  @Override
  public void run() {
    try {
      synchronized (this) {
        processCancelledNodes();
        processPendingNodes();
      }
      processTransitioningNodes();
      if (trackedNodes.size() > 0 || pendingNodes.size() > 0) {
        LOG.info("There are {} nodes tracked for decommission and "+
            "maintenance. {} pending nodes.",
            trackedNodes.size(), pendingNodes.size());
      }
    } catch (Exception e) {
      LOG.error("Caught an error in the DatanodeAdminMonitor", e);
      // Intentionally do not re-throw, as if we do the monitor thread
      // will not get rescheduled.
    }
  }

  @Override
  public int getPendingCount() {
    return pendingNodes.size();
  }

  @Override
  public int getCancelledCount() {
    return cancelledNodes.size();
  }

  @Override
  public int getTrackedNodeCount() {
    return trackedNodes.size();
  }

  @VisibleForTesting
  public Set<DatanodeAdminNodeDetails> getTrackedNodes() {
    return trackedNodes;
  }

  private void processCancelledNodes() {
    while(!cancelledNodes.isEmpty()) {
      stopTrackingNode(cancelledNodes.poll());
      // TODO - fire event to bring node back into service?
    }
  }

  private void processPendingNodes() {
    while(!pendingNodes.isEmpty()) {
      startTrackingNode(pendingNodes.poll());
    }
  }

  private void processTransitioningNodes() {
    Iterator<DatanodeAdminNodeDetails> iterator = trackedNodes.iterator();
    while (iterator.hasNext()) {
      DatanodeAdminNodeDetails dn = iterator.next();
      try {
        NodeStatus status = getNodeStatus(dn.getDatanodeDetails());

        if (!shouldContinueWorkflow(dn, status)) {
          abortWorkflow(dn);
          iterator.remove();
          continue;
        }

        if (status.isMaintenance()) {
          if (dn.shouldMaintenanceEnd()) {
            completeMaintenance(dn);
            iterator.remove();
            continue;
          }
        }

        if (status.isDecommissioning() || status.isEnteringMaintenance()) {
          if (checkPipelinesClosedOnNode(dn)
              && checkContainersReplicatedOnNode(dn)) {
            if (status.isDecommissioning()) {
              completeDecommission(dn);
              iterator.remove();
            } else {
              putIntoMaintenance(dn);
            }
          }
        }

      } catch (NodeNotFoundException e) {
        LOG.error("An unexpected error occurred processing datanode {}. " +
            "Aborting the admin workflow", dn.getDatanodeDetails(), e);
        abortWorkflow(dn);
        iterator.remove();
      }
    }
  }

  /**
   * Checks if a node is in an unexpected state or has gone dead while
   * decommissioning or entering maintenance. If the node is not in a valid
   * state to continue the admin workflow, return false, otherwise return true.
   * @param dn The Datanode for which to check the current state
   * @param nodeStatus The current NodeStatus for the datanode
   * @return True if admin can continue, false otherwise
   */
  private boolean shouldContinueWorkflow(DatanodeAdminNodeDetails dn,
      NodeStatus nodeStatus) {
    if (!nodeStatus.isDecommission() && !nodeStatus.isMaintenance()) {
      LOG.warn("Datanode {} has an operational state of {} when it should "+
              "be undergoing decommission or maintenance. Aborting admin for "+
              "this node.",
          dn.getDatanodeDetails(), nodeStatus.getOperationalState());
      return false;
    }
    if (nodeStatus.isDead() && !nodeStatus.isInMaintenance()) {
      LOG.error("Datanode {} is dead but is not IN_MAINTENANCE. Aborting the "+
          "admin workflow for this node", dn.getDatanodeDetails());
      return false;
    }
    return true;
  }

  private boolean checkPipelinesClosedOnNode(DatanodeAdminNodeDetails dn) {
    DatanodeDetails dnd = dn.getDatanodeDetails();
    Set<PipelineID> pipelines = nodeManager.getPipelines(dnd);
    if (pipelines == null || pipelines.size() == 0
        || dn.shouldMaintenanceEnd()) {
      return true;
    } else {
      LOG.info("Waiting for pipelines to close for {}. There are {} "+
          "pipelines", dnd, pipelines.size());
      return false;
    }
  }

  private boolean checkContainersReplicatedOnNode(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException {
    int sufficientlyReplicated = 0;
    int underReplicated = 0;
    int unhealthy = 0;
    Set<ContainerID> containers =
        nodeManager.getContainers(dn.getDatanodeDetails());
    for(ContainerID cid : containers) {
      try {
        ContainerReplicaCount replicaSet =
            replicationManager.getContainerReplicaCount(cid);
        if (replicaSet.isSufficientlyReplicated()) {
          sufficientlyReplicated++;
        } else {
          underReplicated++;
        }
        if (!replicaSet.isHealthy()) {
          unhealthy++;
        }
      } catch (ContainerNotFoundException e) {
        LOG.warn("ContainerID {} present in node list for {} but not found "+
            "in containerManager", cid, dn.getDatanodeDetails());
      }
    }
    dn.setSufficientlyReplicatedContainers(sufficientlyReplicated);
    dn.setUnderReplicatedContainers(underReplicated);
    dn.setUnHealthyContainers(unhealthy);

    return underReplicated == 0 && unhealthy == 0;
  }

  private void completeDecommission(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException{
    NodeStatus nodeStatus = getNodeStatus(dn.getDatanodeDetails());
    if (nodeStatus.isAlive()) {
      setNodeOpState(dn, NodeOperationalState.DECOMMISSIONED);
      LOG.info("Datanode {} has completed the admin workflow. The operational "+
          "state has been set to {}", dn.getDatanodeDetails(),
          NodeOperationalState.DECOMMISSIONED);
    } else {
      // We cannot move a dead node to decommissioned, as it may not have
      // replicated all its containers before it was marked as dead and they
      // were all removed from the node manager. The node will have been
      // handled as a dead node and here we should set it back to IN_SERVICE
      LOG.warn("Datanode {} is not alive and therefore cannot complete "+
          "decommission. The operational state has been set to {}",
          dn.getDatanodeDetails(), NodeOperationalState.IN_SERVICE);
      putNodeBackInService(dn);
    }
  }

  private void putIntoMaintenance(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException {
    LOG.info("Datanode {} has entered maintenance", dn.getDatanodeDetails());
    setNodeOpState(dn, NodeOperationalState.IN_MAINTENANCE);
  }

  private void completeMaintenance(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException {
    // The end state of Maintenance is to put the node back IN_SERVICE, whether
    // it is dead or not.
    // TODO - if the node is dead do we trigger a dead node event here or leave
    //        it to the heartbeat manager?
    LOG.info("Datanode {} has ended maintenance automatically",
        dn.getDatanodeDetails());
    putNodeBackInService(dn);
  }

  private void startTrackingNode(DatanodeAdminNodeDetails dn) {
    eventQueue.fireEvent(SCMEvents.START_ADMIN_ON_NODE,
        dn.getDatanodeDetails());
    trackedNodes.add(dn);
  }

  private void stopTrackingNode(DatanodeAdminNodeDetails dn) {
    trackedNodes.remove(dn);
  }

  /**
   * If we encounter an unexpected condition in maintenance, we must abort the
   * workflow by setting the node operationalState back to IN_SERVICE and then
   * remove the node from tracking.
   * @param dn The datanode for which to abort tracking
   */
  private void abortWorkflow(DatanodeAdminNodeDetails dn) {
    try {
      putNodeBackInService(dn);
    } catch (NodeNotFoundException e) {
      LOG.error("Unable to set the node OperationalState for {} while "+
          "aborting the datanode admin workflow", dn.getDatanodeDetails());
    }
  }

  private void putNodeBackInService(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException {
    setNodeOpState(dn, NodeOperationalState.IN_SERVICE);
  }

  private void setNodeOpState(DatanodeAdminNodeDetails dn,
      HddsProtos.NodeOperationalState state) throws NodeNotFoundException {
    nodeManager.setNodeOperationalState(dn.getDatanodeDetails(), state);
  }

  // TODO - The nodeManager.getNodeStatus call should really throw
  //        NodeNotFoundException rather than having to handle it here as all
  //        registered nodes must have a status.
  private NodeStatus getNodeStatus(DatanodeDetails dnd)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = nodeManager.getNodeStatus(dnd);
    if (nodeStatus == null) {
      throw new NodeNotFoundException("Unable to retrieve the nodeStatus");
    }
    return nodeStatus;
  }

}