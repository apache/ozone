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
import org.apache.hadoop.ozone.common.statemachine
    .InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
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
  private Queue<DatanodeAdminNodeDetails> completedNodes = new ArrayDeque<>();
  private StateMachine<States, Transitions> workflowSM;

  /**
   * States that a node must pass through when being decommissioned or placed
   * into maintenance.
   */
  public enum States {
    CLOSE_PIPELINES(1),
    REPLICATE_CONTAINERS(2),
    AWAIT_MAINTENANCE_END(3),
    COMPLETE(4);

    private int sequenceNumber;

    States(int sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
    }

    public int getSequenceNumber() {
      return sequenceNumber;
    }
  }

  /**
   * Transition events that occur to move a node from one state to the next.
   */
  public enum Transitions {
    COMPLETE_DECOM_STAGE, COMPLETE_MAINT_STAGE, UNEXPECTED_NODE_STATE
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminMonitor.class);

  public DatanodeAdminMonitor(OzoneConfiguration config) {
    conf = config;
    initializeStateMachine();
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
        new DatanodeAdminNodeDetails(dn, workflowSM.getInitialState(),
            endInHours);
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
    DatanodeAdminNodeDetails nodeDetails = new DatanodeAdminNodeDetails(dn,
        workflowSM.getInitialState(), 0);
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

  /**
   * Return the state machine used to transition a node through the admin
   * workflow.
   * @return The StateMachine used by the admin workflow
   */
  @VisibleForTesting
  public StateMachine<States, Transitions> getWorkflowStateMachine() {
    return workflowSM;
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
    for (DatanodeAdminNodeDetails dn : trackedNodes) {
      try {
        if (!shouldContinueWorkflow(dn)) {
          abortWorkflow(dn);
          continue;
        }
        if (dn.getCurrentState() == States.CLOSE_PIPELINES) {
          checkPipelinesClosedOnNode(dn);
        }
        if (dn.getCurrentState() == States.REPLICATE_CONTAINERS) {
          checkContainersOnNode(dn);
        }
        if (dn.getCurrentState() == States.AWAIT_MAINTENANCE_END) {
          checkMaintenanceEndOnNode(dn);
        }
        if (dn.getCurrentState() == States.COMPLETE) {
          handleCompletedNode(dn);
        }
      } catch (NodeNotFoundException | InvalidStateTransitionException e) {
        LOG.error("An unexpected error occurred processing datanode {}. "+
            "Aborting the admin workflow", dn.getDatanodeDetails(), e);
        abortWorkflow(dn);
      }
    }
    while(!completedNodes.isEmpty()) {
      DatanodeAdminNodeDetails dn = completedNodes.poll();
      stopTrackingNode(dn);
    }
  }

  /**
   * Checks if a node is in an unexpected state or has gone dead while
   * decommissioning or entering maintenance. If the node is not in a valid
   * state to continue the admin workflow, return false, otherwise return true.
   * @param dn The Datanode for which to check the current state
   * @return True if admin can continue, false otherwise
   * @throws NodeNotFoundException
   */
  private boolean shouldContinueWorkflow(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = getNodeStatus(dn.getDatanodeDetails());
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

  private void checkPipelinesClosedOnNode(DatanodeAdminNodeDetails dn)
      throws InvalidStateTransitionException, NodeNotFoundException {
    DatanodeDetails dnd = dn.getDatanodeDetails();
    Set<PipelineID> pipelines = nodeManager.getPipelines(dnd);
    if (pipelines == null || pipelines.size() == 0
        || dn.shouldMaintenanceEnd()) {
      completeStage(dn);
    } else {
      LOG.info("Waiting for pipelines to close for {}. There are {} "+
          "pipelines", dnd, pipelines.size());
    }
  }

  private void checkContainersOnNode(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException, InvalidStateTransitionException {
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
    if (getNodeStatus(dn.getDatanodeDetails()).isDead()) {
      // If the node is dead, we cannot continue decommission so we abort
      LOG.warn("Datanode {} has been marked as dead and the decommission "+
          "workflow cannot continue.", dn.getDatanodeDetails());
      abortWorkflow(dn);
    } else if ((underReplicated == 0 && unhealthy == 0)
        || dn.shouldMaintenanceEnd()) {
      completeStage(dn);
    }
  }

  private void checkMaintenanceEndOnNode(DatanodeAdminNodeDetails dn)
      throws InvalidStateTransitionException, NodeNotFoundException {
    // Move the node to IN_MAINTENANCE if it is not already at that state
    if (!getNodeStatus(dn.getDatanodeDetails()).isInMaintenance()) {
      setNodeOpState(dn, NodeOperationalState.IN_MAINTENANCE);
    }
    if (dn.shouldMaintenanceEnd()) {
      completeStage(dn);
    }
  }

  private void handleCompletedNode(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException {
    DatanodeDetails dnd = dn.getDatanodeDetails();
    NodeStatus nodeStatus = getNodeStatus(dnd);
    if (nodeStatus.isDecommission()) {
      completeDecommission(dn, nodeStatus);
    } else if (nodeStatus.isMaintenance()) {
      completeMaintenance(dn, nodeStatus);
    } else {
      // Node must already be IN_SERVICE
      LOG.warn("Datanode {} has completed the admin workflow but already has "+
          "an operationalState of {}", dnd, nodeStatus.getOperationalState());
    }
    completedNodes.add(dn);
  }

  private void completeDecommission(DatanodeAdminNodeDetails dn,
      NodeStatus nodeStatus) throws NodeNotFoundException{
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

  private void completeMaintenance(DatanodeAdminNodeDetails dn,
      NodeStatus nodeStatus) throws NodeNotFoundException {
    // The end state of Maintenance is to put the node back IN_SERVICE, whether
    // it is dead or not.
    // TODO - if the node is dead do we trigger a dead node event here or leave
    //        it to the heartbeat manager?
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
    completedNodes.add(dn);
  }

  private void completeStage(DatanodeAdminNodeDetails dn)
      throws InvalidStateTransitionException, NodeNotFoundException {
    NodeStatus nodeStatus = getNodeStatus(dn.getDatanodeDetails());
    dn.transitionState(workflowSM, nodeStatus.getOperationalState());
  }

  private void putNodeBackInService(DatanodeAdminNodeDetails dn)
      throws NodeNotFoundException {
    setNodeOpState(dn, NodeOperationalState.IN_SERVICE);
  }

  private void setNodeOpState(DatanodeAdminNodeDetails dn,
      HddsProtos.NodeOperationalState state) throws NodeNotFoundException {
    nodeManager.setNodeOperationalState(dn.getDatanodeDetails(), state);
  }

  /**
   * Setup the state machine with the allowed transitions for a node to move
   * through the maintenance workflow.
   */
  private void initializeStateMachine() {
    Set<States> finalStates = new HashSet<>();
    workflowSM = new StateMachine<>(States.CLOSE_PIPELINES, finalStates);
    workflowSM.addTransition(States.CLOSE_PIPELINES,
        States.REPLICATE_CONTAINERS, Transitions.COMPLETE_DECOM_STAGE);
    workflowSM.addTransition(States.REPLICATE_CONTAINERS, States.COMPLETE,
        Transitions.COMPLETE_DECOM_STAGE);

    workflowSM.addTransition(States.CLOSE_PIPELINES,
        States.REPLICATE_CONTAINERS, Transitions.COMPLETE_MAINT_STAGE);
    workflowSM.addTransition(States.REPLICATE_CONTAINERS,
        States.AWAIT_MAINTENANCE_END, Transitions.COMPLETE_MAINT_STAGE);
    workflowSM.addTransition(States.AWAIT_MAINTENANCE_END,
        States.COMPLETE, Transitions.COMPLETE_MAINT_STAGE);
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