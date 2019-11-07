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
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.common.statemachine.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

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
  private Queue<DatanodeAdminNodeDetails> pendingNodes = new ArrayDeque();
  private Queue<DatanodeAdminNodeDetails> cancelledNodes = new ArrayDeque();
  private Set<DatanodeAdminNodeDetails> trackedNodes = new HashSet<>();
  private StateMachine<States, Transitions> workflowSM;

  /**
   * States that a node must pass through when being decommissioned or placed
   * into maintenance.
   */
  public enum States {
    CLOSE_PIPELINES(1),
    GET_CONTAINERS(2),
    REPLICATE_CONTAINERS(3),
    AWAIT_MAINTENANCE_END(4),
    COMPLETE(5);

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
   * 3. Wait for any nodes which have completed closing pipelines
   */
  @Override
  public void run() {
    try {
      synchronized (this) {
        processCancelledNodes();
        processPendingNodes();
      }
      checkPipelinesClosed();
      if (trackedNodes.size() > 0 || pendingNodes.size() > 0) {
        LOG.info("There are {} nodes tracked for decommission and "+
            "maintenance. {} pending nodes.",
            trackedNodes.size(), pendingNodes.size());
      }
    } catch (Exception e) {
      LOG.error("Caught an error in the DatanodeAdminMonitor", e);
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
      DatanodeAdminNodeDetails dn = cancelledNodes.poll();
      trackedNodes.remove(dn);
      // TODO - fire event to bring node back into service?
    }
  }

  private void processPendingNodes() {
    while(!pendingNodes.isEmpty()) {
      DatanodeAdminNodeDetails dn = pendingNodes.poll();
      // Trigger event to async close the node pipelines.
      eventQueue.fireEvent(SCMEvents.START_ADMIN_ON_NODE,
          dn.getDatanodeDetails());
      trackedNodes.add(dn);
    }
  }

  private void checkPipelinesClosed() {
    for (DatanodeAdminNodeDetails dn : trackedNodes) {
      if (dn.getCurrentState() != States.CLOSE_PIPELINES) {
        continue;
      }
      DatanodeDetails dnd = dn.getDatanodeDetails();
      Set<PipelineID> pipelines = nodeManager.getPipelines(dnd);
      if (pipelines == null || pipelines.size() == 0) {
        NodeStatus nodeStatus = nodeManager.getNodeStatus(dnd);
        try {
          dn.transitionState(workflowSM, nodeStatus.getOperationalState());
        } catch (InvalidStateTransitionException e) {
          LOG.warn("Unexpected state transition", e);
          // TODO - how to handle this? This means the node is not in
          //        an expected state, eg it is IN_SERVICE when it should be
          //        decommissioning, so should we abort decom altogether for it?
          //        This could happen if a node is queued for cancel and not yet
          //        processed.
        }
      } else {
        LOG.info("Waiting for pipelines to close for {}. There are {} "+
            "pipelines", dnd, pipelines.size());
      }
    }
  }

  /**
   * Setup the state machine with the allowed transitions for a node to move
   * through the maintenance workflow.
   */
  private void initializeStateMachine() {
    Set<States> finalStates = new HashSet<>();
    workflowSM = new StateMachine<>(States.CLOSE_PIPELINES, finalStates);
    workflowSM.addTransition(States.CLOSE_PIPELINES,
        States.GET_CONTAINERS, Transitions.COMPLETE_DECOM_STAGE);
    workflowSM.addTransition(States.GET_CONTAINERS, States.REPLICATE_CONTAINERS,
        Transitions.COMPLETE_DECOM_STAGE);
    workflowSM.addTransition(States.REPLICATE_CONTAINERS, States.COMPLETE,
        Transitions.COMPLETE_DECOM_STAGE);

    workflowSM.addTransition(States.CLOSE_PIPELINES,
        States.GET_CONTAINERS, Transitions.COMPLETE_MAINT_STAGE);
    workflowSM.addTransition(States.GET_CONTAINERS, States.REPLICATE_CONTAINERS,
        Transitions.COMPLETE_MAINT_STAGE);
    workflowSM.addTransition(States.REPLICATE_CONTAINERS,
        States.AWAIT_MAINTENANCE_END, Transitions.COMPLETE_MAINT_STAGE);
    workflowSM.addTransition(States.AWAIT_MAINTENANCE_END,
        States.COMPLETE, Transitions.COMPLETE_MAINT_STAGE);
  }

}