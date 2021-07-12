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

package org.apache.hadoop.hdds.scm.container;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMService;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.Time;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.GeneratedMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replication Manager (RM) is the one which is responsible for making sure
 * that the containers are properly replicated. Replication Manager deals only
 * with Quasi Closed / Closed container.
 */
public class ReplicationManager implements MetricsSource, SCMService {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReplicationManager.class);

  public static final String METRICS_SOURCE_NAME = "SCMReplicationManager";

  /**
   * Reference to the ContainerManager.
   */
  private final ContainerManagerV2 containerManager;

  /**
   * PlacementPolicy which is used to identify where a container
   * should be replicated.
   */
  private final PlacementPolicy containerPlacement;

  /**
   * EventPublisher to fire Replicate and Delete container events.
   */
  private final EventPublisher eventPublisher;

  /**
   * SCMContext from StorageContainerManager.
   */
  private final SCMContext scmContext;

  /**
   * Used to lookup the health of a nodes or the nodes operational state.
   */
  private final NodeManager nodeManager;

  /**
   * This is used for tracking container replication commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightReplication;

  /**
   * This is used for tracking container deletion commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final Map<ContainerID, List<InflightAction>> inflightDeletion;

  /**
   * This is used for tracking container move commands
   * which are not yet complete.
   */
  private final Map<ContainerID,
      Pair<DatanodeDetails, DatanodeDetails>> inflightMove;

  /**
   * This is used for indicating the result of move option and
   * the corresponding reason. this is useful for tracking
   * the result of move option
   */
  enum MoveResult {
    // both replication and deletion are completed
    COMPLETED,
    // RM is not running
    RM_NOT_RUNNING,
    // replication fail because the container does not exist in src
    REPLICATION_FAIL_NOT_EXIST_IN_SOURCE,
    // replication fail because the container exists in target
    REPLICATION_FAIL_EXIST_IN_TARGET,
    // replication fail because the container is not cloesed
    REPLICATION_FAIL_CONTAINER_NOT_CLOSED,
    // replication fail because the container is in inflightDeletion
    REPLICATION_FAIL_INFLIGHT_DELETION,
    // replication fail because the container is in inflightReplication
    REPLICATION_FAIL_INFLIGHT_REPLICATION,
    // replication fail because of timeout
    REPLICATION_FAIL_TIME_OUT,
    // replication fail because of node is not in service
    REPLICATION_FAIL_NODE_NOT_IN_SERVICE,
    // replication fail because node is unhealthy
    REPLICATION_FAIL_NODE_UNHEALTHY,
    // replication succeed, but deletion fail because of timeout
    DELETION_FAIL_TIME_OUT,
    // replication succeed, but deletion fail because because
    // node is unhealthy
    DELETION_FAIL_NODE_UNHEALTHY,
    // replication succeed, but if we delete the container from
    // the source datanode , the policy(eg, replica num or
    // rack location) will not be satisfied, so we should not delete
    // the container
    DELETE_FAIL_POLICY,
    //  replicas + target - src does not satisfy placement policy
    PLACEMENT_POLICY_NOT_SATISFIED,
    //unexpected action, remove src at inflightReplication
    UNEXPECTED_REMOVE_SOURCE_AT_INFLIGHT_REPLICATION,
    //unexpected action, remove target at inflightDeletion
    UNEXPECTED_REMOVE_TARGET_AT_INFLIGHT_DELETION
  }

  /**
   * This is used for tracking container move commands
   * which are not yet complete.
   */
  private final Map<ContainerID,
      CompletableFuture<MoveResult>> inflightMoveFuture;

  /**
   * ReplicationManager specific configuration.
   */
  private final ReplicationManagerConfiguration rmConf;

  /**
   * ReplicationMonitor thread is the one which wakes up at configured
   * interval and processes all the containers.
   */
  private Thread replicationMonitor;

  /**
   * Flag used for checking if the ReplicationMonitor thread is running or
   * not.
   */
  private volatile boolean running;

  /**
   * Minimum number of replica in a healthy state for maintenance.
   */
  private int minHealthyForMaintenance;

  /**
   * SCMService related variables.
   * After leaving safe mode, replicationMonitor needs to wait for a while
   * before really take effect.
   */
  private final Lock serviceLock = new ReentrantLock();
  private ServiceStatus serviceStatus = ServiceStatus.PAUSING;
  private final long waitTimeInMillis;
  private long lastTimeToBeReadyInMillis = 0;

  /**
   * Constructs ReplicationManager instance with the given configuration.
   *
   * @param conf OzoneConfiguration
   * @param containerManager ContainerManager
   * @param containerPlacement PlacementPolicy
   * @param eventPublisher EventPublisher
   */
  @SuppressWarnings("parameternumber")
  public ReplicationManager(final ConfigurationSource conf,
                            final ContainerManagerV2 containerManager,
                            final PlacementPolicy containerPlacement,
                            final EventPublisher eventPublisher,
                            final SCMContext scmContext,
                            final SCMServiceManager serviceManager,
                            final NodeManager nodeManager) {
    this.containerManager = containerManager;
    this.containerPlacement = containerPlacement;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.nodeManager = nodeManager;
    this.rmConf = conf.getObject(ReplicationManagerConfiguration.class);
    this.running = false;
    this.inflightReplication = new ConcurrentHashMap<>();
    this.inflightDeletion = new ConcurrentHashMap<>();
    this.inflightMove = new ConcurrentHashMap<>();
    this.inflightMoveFuture = new ConcurrentHashMap<>();
    this.minHealthyForMaintenance = rmConf.getMaintenanceReplicaMinimum();

    this.waitTimeInMillis = conf.getTimeDuration(
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
        TimeUnit.MILLISECONDS);

    // register ReplicationManager to SCMServiceManager.
    serviceManager.register(this);

    // start ReplicationManager.
    start();
  }

  /**
   * Starts Replication Monitor thread.
   */
  @Override
  public synchronized void start() {
    if (!isRunning()) {
      DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
          "SCM Replication manager (closed container replication) related "
              + "metrics",
          this);
      LOG.info("Starting Replication Monitor Thread.");
      running = true;
      replicationMonitor = new Thread(this::run);
      replicationMonitor.setName("ReplicationMonitor");
      replicationMonitor.setDaemon(true);
      replicationMonitor.start();
    } else {
      LOG.info("Replication Monitor Thread is already running.");
    }
  }

  /**
   * Returns true if the Replication Monitor Thread is running.
   *
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    if (!running) {
      synchronized (this) {
        return replicationMonitor != null
            && replicationMonitor.isAlive();
      }
    }
    return true;
  }

  /**
   * Process all the containers immediately.
   */
  @VisibleForTesting
  @SuppressFBWarnings(value="NN_NAKED_NOTIFY",
      justification="Used only for testing")
  public synchronized void processContainersNow() {
    notifyAll();
  }

  /**
   * Stops Replication Monitor thread.
   */
  public synchronized void stop() {
    if (running) {
      LOG.info("Stopping Replication Monitor Thread.");
      inflightReplication.clear();
      inflightDeletion.clear();
      //TODO: replicate inflight move through ratis
      inflightMove.clear();
      inflightMoveFuture.clear();
      running = false;
      DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
      notifyAll();
    } else {
      LOG.info("Replication Monitor Thread is not running.");
    }
  }

  /**
   * ReplicationMonitor thread runnable. This wakes up at configured
   * interval and processes all the containers in the system.
   */
  private synchronized void run() {
    try {
      while (running) {
        final long start = Time.monotonicNow();
        final List<ContainerInfo> containers =
            containerManager.getContainers();
        containers.forEach(this::processContainer);

        LOG.info("Replication Monitor Thread took {} milliseconds for" +
                " processing {} containers.", Time.monotonicNow() - start,
            containers.size());

        wait(rmConf.getInterval());
      }
    } catch (Throwable t) {
      // When we get runtime exception, we should terminate SCM.
      LOG.error("Exception in Replication Monitor Thread.", t);
      ExitUtil.terminate(1, t);
    }
  }

  /**
   * Process the given container.
   *
   * @param container ContainerInfo
   */
  private void processContainer(ContainerInfo container) {
    if (!shouldRun()) {
      return;
    }
    final ContainerID id = container.containerID();
    try {
      // synchronize on the containerInfo object to solve container
      // race conditions with ICR/FCR handlers
      synchronized (container) {
        final Set<ContainerReplica> replicas = containerManager
            .getContainerReplicas(id);
        final LifeCycleState state = container.getState();

        /*
         * We don't take any action if the container is in OPEN state and
         * the container is healthy. If the container is not healthy, i.e.
         * the replicas are not in OPEN state, send CLOSE_CONTAINER command.
         */
        if (state == LifeCycleState.OPEN) {
          if (!isOpenContainerHealthy(container, replicas)) {
            eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER, id);
          }
          return;
        }

        /*
         * If the container is in CLOSING state, the replicas can either
         * be in OPEN or in CLOSING state. In both of this cases
         * we have to resend close container command to the datanodes.
         */
        if (state == LifeCycleState.CLOSING) {
          replicas.forEach(replica -> sendCloseCommand(
              container, replica.getDatanodeDetails(), false));
          return;
        }

        /*
         * If the container is in QUASI_CLOSED state, check and close the
         * container if possible.
         */
        if (state == LifeCycleState.QUASI_CLOSED &&
            canForceCloseContainer(container, replicas)) {
          forceCloseContainer(container, replicas);
          return;
        }

        /*
         * Before processing the container we have to reconcile the
         * inflightReplication and inflightDeletion actions.
         *
         * We remove the entry from inflightReplication and inflightDeletion
         * list, if the operation is completed or if it has timed out.
         */
        updateInflightAction(container, inflightReplication,
            action -> replicas.stream()
                .anyMatch(r -> r.getDatanodeDetails().equals(action.datanode)));

        updateInflightAction(container, inflightDeletion,
            action -> replicas.stream()
                .noneMatch(r ->
                    r.getDatanodeDetails().equals(action.datanode)));

        /*
         * If container is under deleting and all it's replicas are deleted,
         * then make the container as CLEANED,
         * or resend the delete replica command if needed.
         */
        if (state == LifeCycleState.DELETING) {
          handleContainerUnderDelete(container, replicas);
          return;
        }

        /**
         * We don't need to take any action for a DELETE container - eventually
         * it will be removed from SCM.
         */
        if (state == LifeCycleState.DELETED) {
          return;
        }

        ContainerReplicaCount replicaSet =
            getContainerReplicaCount(container, replicas);
        ContainerPlacementStatus placementStatus = getPlacementStatus(
            replicas, container.getReplicationConfig().getRequiredNodes());

        /*
         * We don't have to take any action if the container is healthy.
         *
         * According to ReplicationMonitor container is considered healthy if
         * the container is either in QUASI_CLOSED or in CLOSED state and has
         * exact number of replicas in the same state.
         */
        if (isContainerEmpty(container, replicas)) {
          /*
           *  If container is empty, schedule task to delete the container.
           */
          deleteContainerReplicas(container, replicas);
          return;
        }

        /*
         * Check if the container is under replicated and take appropriate
         * action.
         */
        if (!replicaSet.isSufficientlyReplicated()
            || !placementStatus.isPolicySatisfied()) {
          handleUnderReplicatedContainer(container,
              replicaSet, placementStatus);
          return;
        }

        /*
         * Check if the container is over replicated and take appropriate
         * action.
         */
        if (replicaSet.isOverReplicated()) {
          handleOverReplicatedContainer(container, replicaSet);
          return;
        }

      /*
       If we get here, the container is not over replicated or under replicated
       but it may be "unhealthy", which means it has one or more replica which
       are not in the same state as the container itself.
       */
        if (!replicaSet.isHealthy()) {
          handleUnstableContainer(container, replicas);
        }
      }
    } catch (ContainerNotFoundException ex) {
      LOG.warn("Missing container {}.", id);
    } catch (Exception ex) {
      LOG.warn("Process container {} error: ", id, ex);
    }
  }

  /**
   * Reconciles the InflightActions for a given container.
   *
   * @param container Container to update
   * @param inflightActions inflightReplication (or) inflightDeletion
   * @param filter filter to check if the operation is completed
   */
  private void updateInflightAction(final ContainerInfo container,
      final Map<ContainerID, List<InflightAction>> inflightActions,
      final Predicate<InflightAction> filter) {
    final ContainerID id = container.containerID();
    final long deadline = Time.monotonicNow() - rmConf.getEventTimeout();
    if (inflightActions.containsKey(id)) {
      final List<InflightAction> actions = inflightActions.get(id);

      Iterator<InflightAction> iter = actions.iterator();
      while(iter.hasNext()) {
        try {
          InflightAction a = iter.next();
          NodeStatus status = nodeManager.getNodeStatus(a.datanode);
          boolean isUnhealthy = status.getHealth() != NodeState.HEALTHY;
          boolean isCompleted = filter.test(a);
          boolean isTimeout = a.time < deadline;
          boolean isNotInService = status.getOperationalState() !=
              NodeOperationalState.IN_SERVICE;
          if (isCompleted || isUnhealthy || isTimeout || isNotInService) {
            iter.remove();
            updateMoveIfNeeded(isUnhealthy, isCompleted, isTimeout,
                id, a.datanode, inflightActions);
          }
        } catch (NodeNotFoundException e) {
          // Should not happen, but if it does, just remove the action as the
          // node somehow does not exist;
          iter.remove();
        }
      }
      if (actions.isEmpty()) {
        inflightActions.remove(id);
      }
    }
  }

  /**
   * update inflight move if needed.
   *
   * @param isUnhealthy is the datanode unhealthy
   * @param isCompleted is the action completed
   * @param isTimeout is the action timeout
   * @param id Container to update
   * @param dn datanode which is removed from the inflightActions
   * @param inflightActions inflightReplication (or) inflightDeletion
   */
  private void updateMoveIfNeeded(final boolean isUnhealthy,
                   final boolean isCompleted, final boolean isTimeout,
                   final ContainerID id, final DatanodeDetails dn,
                   final Map<ContainerID,
                       List<InflightAction>> inflightActions) {
    // make sure inflightMove contains the container
    if (!inflightMove.containsKey(id)) {
      return;
    }

    // make sure the datanode , which is removed from inflightActions,
    // is source or target datanode.
    Pair<DatanodeDetails, DatanodeDetails> kv = inflightMove.get(id);
    final boolean isSource = kv.getKey().equals(dn);
    final boolean isTarget = kv.getValue().equals(dn);
    if (!isSource && !isTarget) {
      return;
    }
    final boolean isInflightReplication =
        inflightActions.equals(inflightReplication);

    /*
     * there are some case:
     **********************************************************
     *              * InflightReplication  * InflightDeletion *
     **********************************************************
     *source removed*     unexpected       *    expected      *
     **********************************************************
     *target removed*      expected        *   unexpected     *
     **********************************************************
     * unexpected action may happen somehow. to make it deterministic,
     * if unexpected action happens, we just fail the completableFuture.
     */

    if (isSource && isInflightReplication) {
      inflightMoveFuture.get(id).complete(
          MoveResult.UNEXPECTED_REMOVE_SOURCE_AT_INFLIGHT_REPLICATION);
      inflightMove.remove(id);
      inflightMoveFuture.remove(id);
      return;
    }

    if (isTarget && !isInflightReplication) {
      inflightMoveFuture.get(id).complete(
          MoveResult.UNEXPECTED_REMOVE_TARGET_AT_INFLIGHT_DELETION);
      inflightMove.remove(id);
      inflightMoveFuture.remove(id);
      return;
    }

    //if replication is completed , nothing to do
    if (!(isInflightReplication && isCompleted)) {
      if (isInflightReplication) {
        if (isUnhealthy) {
          inflightMoveFuture.get(id).complete(
              MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
        } else {
          inflightMoveFuture.get(id).complete(
              MoveResult.REPLICATION_FAIL_TIME_OUT);
        }
      } else {
        if (isUnhealthy) {
          inflightMoveFuture.get(id).complete(
              MoveResult.DELETION_FAIL_NODE_UNHEALTHY);
        } else if (isTimeout) {
          inflightMoveFuture.get(id).complete(
              MoveResult.DELETION_FAIL_TIME_OUT);
        } else {
          inflightMoveFuture.get(id).complete(
              MoveResult.COMPLETED);
        }
      }
      inflightMove.remove(id);
      inflightMoveFuture.remove(id);
    }
  }

  /**
   * add a move action for a given container.
   *
   * @param cid Container to move
   * @param srcDn datanode to move from
   * @param targetDn datanode to move to
   */
  public CompletableFuture<MoveResult> move(ContainerID cid,
      DatanodeDetails srcDn, DatanodeDetails targetDn)
      throws ContainerNotFoundException, NodeNotFoundException {
    CompletableFuture<MoveResult> ret = new CompletableFuture<>();
    if (!isRunning()) {
      ret.complete(MoveResult.RM_NOT_RUNNING);
      return ret;
    }

    /*
     * make sure the flowing conditions are met:
     *  1 the given two datanodes are in healthy state
     *  2 the given container exists on the given source datanode
     *  3 the given container does not exist on the given target datanode
     *  4 the given container is in closed state
     *  5 the giver container is not taking any inflight action
     *  6 the given two datanodes are in IN_SERVICE state
     *  7 {Existing replicas + Target_Dn - Source_Dn} satisfies
     *     the placement policy
     *
     * move is a combination of two steps : replication and deletion.
     * if the conditions above are all met, then we take a conservative
     * strategy here : replication can always be executed, but the execution
     * of deletion always depends on placement policy
     */

    NodeStatus currentNodeStat = nodeManager.getNodeStatus(srcDn);
    NodeState healthStat = currentNodeStat.getHealth();
    NodeOperationalState operationalState =
        currentNodeStat.getOperationalState();
    if (healthStat != NodeState.HEALTHY) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      return ret;
    }
    if (operationalState != NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      return ret;
    }

    currentNodeStat = nodeManager.getNodeStatus(targetDn);
    healthStat = currentNodeStat.getHealth();
    operationalState = currentNodeStat.getOperationalState();
    if (healthStat != NodeState.HEALTHY) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      return ret;
    }
    if (operationalState != NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      return ret;
    }

    // we need to synchronize on ContainerInfo, since it is
    // shared by ICR/FCR handler and this.processContainer
    // TODO: use a Read lock after introducing a RW lock into ContainerInfo
    ContainerInfo cif = containerManager.getContainer(cid);
    synchronized (cif) {
      final Set<ContainerReplica> currentReplicas = containerManager
          .getContainerReplicas(cid);
      final Set<DatanodeDetails> replicas = currentReplicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toSet());
      if (replicas.contains(targetDn)) {
        ret.complete(MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET);
        return ret;
      }
      if (!replicas.contains(srcDn)) {
        ret.complete(MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE);
        return ret;
      }

      /*
      * the reason why the given container should not be taking any inflight
      * action is that: if the given container is being replicated or deleted,
      * the num of its replica is not deterministic, so move operation issued
      * by balancer may cause a nondeterministic result, so we should drop
      * this option for this time.
      * */

      if (inflightReplication.containsKey(cid)) {
        ret.complete(MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION);
        return ret;
      }
      if (inflightDeletion.containsKey(cid)) {
        ret.complete(MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION);
        return ret;
      }

      /*
      * here, no need to see whether cid is in inflightMove, because
      * these three map are all synchronized on ContainerInfo, if cid
      * is in infligtMove , it must now being replicated or deleted,
      * so it must be in inflightReplication or in infligthDeletion.
      * thus, if we can not find cid in both of them , this cid must
      * not be in inflightMove.
      */

      LifeCycleState currentContainerStat = cif.getState();
      if (currentContainerStat != LifeCycleState.CLOSED) {
        ret.complete(MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
        return ret;
      }

      // check whether {Existing replicas + Target_Dn - Source_Dn}
      // satisfies current placement policy
      if (!isPolicySatisfiedAfterMove(cif, srcDn, targetDn,
          currentReplicas.stream().collect(Collectors.toList()))) {
        ret.complete(MoveResult.PLACEMENT_POLICY_NOT_SATISFIED);
        return ret;
      }

      inflightMove.putIfAbsent(cid, new ImmutablePair<>(srcDn, targetDn));
      inflightMoveFuture.putIfAbsent(cid, ret);
      sendReplicateCommand(cif, targetDn, Collections.singletonList(srcDn));
    }
    LOG.info("receive a move requset about container {} , from {} to {}",
        cid, srcDn.getUuid(), targetDn.getUuid());
    return ret;
  }

  /**
   * Returns whether {Existing replicas + Target_Dn - Source_Dn}
   * satisfies current placement policy.
   * @param cif Container Info of moved container
   * @param srcDn DatanodeDetails of source data node
   * @param targetDn DatanodeDetails of target data node
   * @param replicas container replicas
   * @return whether the placement policy is satisfied after move
   */
  private boolean isPolicySatisfiedAfterMove(ContainerInfo cif,
                    DatanodeDetails srcDn, DatanodeDetails targetDn,
                    final List<ContainerReplica> replicas){
    Set<ContainerReplica> movedReplicas =
        replicas.stream().collect(Collectors.toSet());
    movedReplicas.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
    movedReplicas.add(ContainerReplica.newBuilder()
        .setDatanodeDetails(targetDn).build());
    ContainerPlacementStatus placementStatus = getPlacementStatus(
        movedReplicas, cif.getReplicationConfig().getRequiredNodes());
    return placementStatus.isPolicySatisfied();
  }

  /**
   * Returns the number replica which are pending creation for the given
   * container ID.
   * @param id The ContainerID for which to check the pending replica
   * @return The number of inflight additions or zero if none
   */
  private int getInflightAdd(final ContainerID id) {
    return inflightReplication.getOrDefault(id, Collections.emptyList()).size();
  }

  /**
   * Returns the number replica which are pending delete for the given
   * container ID.
   * @param id The ContainerID for which to check the pending replica
   * @return The number of inflight deletes or zero if none
   */
  private int getInflightDel(final ContainerID id) {
    return inflightDeletion.getOrDefault(id, Collections.emptyList()).size();
  }

  /**
   * Returns true if the container is empty and CLOSED.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if the container is empty, false otherwise
   */
  private boolean isContainerEmpty(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    return container.getState() == LifeCycleState.CLOSED &&
        (container.getUsedBytes() == 0 && container.getNumberOfKeys() == 0) &&
        replicas.stream().allMatch(r -> r.getState() == State.CLOSED &&
            r.getBytesUsed() == 0 && r.getKeyCount() == 0);
  }

  /**
   * Given a ContainerID, lookup the ContainerInfo and then return a
   * ContainerReplicaCount object for the container.
   * @param containerID The ID of the container
   * @return ContainerReplicaCount for the given container
   * @throws ContainerNotFoundException
   */
  public ContainerReplicaCount getContainerReplicaCount(ContainerID containerID)
      throws ContainerNotFoundException {
    ContainerInfo container = containerManager.getContainer(containerID);
    return getContainerReplicaCount(container);
  }

  /**
   * Given a container, obtain the set of known replica for it, and return a
   * ContainerReplicaCount object. This object will contain the set of replica
   * as well as all information required to determine if the container is over
   * or under replicated, including the delta of replica required to repair the
   * over or under replication.
   *
   * @param container The container to create a ContainerReplicaCount for
   * @return ContainerReplicaCount representing the replicated state of the
   *         container.
   * @throws ContainerNotFoundException
   */
  public ContainerReplicaCount getContainerReplicaCount(ContainerInfo container)
      throws ContainerNotFoundException {
    // TODO: using a RW lock for only read
    synchronized (container) {
      final Set<ContainerReplica> replica = containerManager
          .getContainerReplicas(container.containerID());
      return getContainerReplicaCount(container, replica);
    }
  }

  /**
   * Given a container and its set of replicas, create and return a
   * ContainerReplicaCount representing the container.
   *
   * @param container The container for which to construct a
   *                  ContainerReplicaCount
   * @param replica The set of existing replica for this container
   * @return ContainerReplicaCount representing the current state of the
   *         container
   */
  private ContainerReplicaCount getContainerReplicaCount(
      ContainerInfo container, Set<ContainerReplica> replica) {
    return new ContainerReplicaCount(
        container,
        replica,
        getInflightAdd(container.containerID()),
        getInflightDel(container.containerID()),
        container.getReplicationConfig().getRequiredNodes(),
        minHealthyForMaintenance);
  }

  /**
   * Returns true if more than 50% of the container replicas with unique
   * originNodeId are in QUASI_CLOSED state.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if we can force close the container, false otherwise
   */
  private boolean canForceCloseContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.QUASI_CLOSED);
    final int replicationFactor =
        container.getReplicationConfig().getRequiredNodes();
    final long uniqueQuasiClosedReplicaCount = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED)
        .map(ContainerReplica::getOriginDatanodeId)
        .distinct()
        .count();
    return uniqueQuasiClosedReplicaCount > (replicationFactor / 2);
  }

  /**
   * Delete the container and its replicas.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void deleteContainerReplicas(final ContainerInfo container,
      final Set<ContainerReplica> replicas) throws IOException,
      InvalidStateTransitionException {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.CLOSED);
    Preconditions.assertTrue(container.getNumberOfKeys() == 0);
    Preconditions.assertTrue(container.getUsedBytes() == 0);

    replicas.stream().forEach(rp -> {
      Preconditions.assertTrue(rp.getState() == State.CLOSED);
      Preconditions.assertTrue(rp.getBytesUsed() == 0);
      Preconditions.assertTrue(rp.getKeyCount() == 0);
      sendDeleteCommand(container, rp.getDatanodeDetails(), false);
    });
    containerManager.updateContainerState(container.containerID(),
        HddsProtos.LifeCycleEvent.DELETE);
    LOG.debug("Deleting empty container replicas for {},", container);
  }

  /**
   * Handle the container which is under delete.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleContainerUnderDelete(final ContainerInfo container,
      final Set<ContainerReplica> replicas) throws IOException,
      InvalidStateTransitionException {
    if (replicas.size() == 0) {
      containerManager.updateContainerState(container.containerID(),
          HddsProtos.LifeCycleEvent.CLEANUP);
      LOG.debug("Container {} state changes to DELETED", container);
    } else {
      // Check whether to resend the delete replica command
      final List<DatanodeDetails> deletionInFlight = inflightDeletion
          .getOrDefault(container.containerID(), Collections.emptyList())
          .stream()
          .map(action -> action.datanode)
          .collect(Collectors.toList());
      Set<ContainerReplica> filteredReplicas = replicas.stream().filter(
          r -> !deletionInFlight.contains(r.getDatanodeDetails()))
          .collect(Collectors.toSet());
      // Resend the delete command
      if (filteredReplicas.size() > 0) {
        filteredReplicas.stream().forEach(rp -> {
          sendDeleteCommand(container, rp.getDatanodeDetails(), false);
        });
        LOG.debug("Resend delete Container command for {}", container);
      }
    }
  }

  /**
   * Force close the container replica(s) with highest sequence Id.
   *
   * <p>
   *   Note: We should force close the container only if >50% (quorum)
   *   of replicas with unique originNodeId are in QUASI_CLOSED state.
   * </p>
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void forceCloseContainer(final ContainerInfo container,
                                   final Set<ContainerReplica> replicas) {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.QUASI_CLOSED);

    final List<ContainerReplica> quasiClosedReplicas = replicas.stream()
        .filter(r -> r.getState() == State.QUASI_CLOSED)
        .collect(Collectors.toList());

    final Long sequenceId = quasiClosedReplicas.stream()
        .map(ContainerReplica::getSequenceId)
        .max(Long::compare)
        .orElse(-1L);

    LOG.info("Force closing container {} with BCSID {}," +
        " which is in QUASI_CLOSED state.",
        container.containerID(), sequenceId);

    quasiClosedReplicas.stream()
        .filter(r -> sequenceId != -1L)
        .filter(replica -> replica.getSequenceId().equals(sequenceId))
        .forEach(replica -> sendCloseCommand(
            container, replica.getDatanodeDetails(), true));
  }

  /**
   * If the given container is under replicated, identify a new set of
   * datanode(s) to replicate the container using PlacementPolicy
   * and send replicate container command to the identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicaSet An instance of ContainerReplicaCount, containing the
   *                   current replica count and inflight adds and deletes
   */
  private void handleUnderReplicatedContainer(final ContainerInfo container,
      final ContainerReplicaCount replicaSet,
      final ContainerPlacementStatus placementStatus) {
    LOG.debug("Handling under-replicated container: {}", container);
    Set<ContainerReplica> replicas = replicaSet.getReplica();
    try {

      if (replicaSet.isSufficientlyReplicated()
          && placementStatus.isPolicySatisfied()) {
        LOG.info("The container {} with replicas {} is sufficiently "+
            "replicated and is not mis-replicated",
            container.getContainerID(), replicaSet);
        return;
      }
      int repDelta = replicaSet.additionalReplicaNeeded();
      final ContainerID id = container.containerID();
      final List<DatanodeDetails> deletionInFlight = inflightDeletion
          .getOrDefault(id, Collections.emptyList())
          .stream()
          .map(action -> action.datanode)
          .collect(Collectors.toList());
      final List<DatanodeDetails> replicationInFlight = inflightReplication
          .getOrDefault(id, Collections.emptyList())
          .stream()
          .map(action -> action.datanode)
          .collect(Collectors.toList());
      final List<DatanodeDetails> source = replicas.stream()
          .filter(r ->
              r.getState() == State.QUASI_CLOSED ||
              r.getState() == State.CLOSED)
          // Exclude stale and dead nodes. This is particularly important for
          // maintenance nodes, as the replicas will remain present in the
          // container manager, even when they go dead.
          .filter(r ->
              getNodeStatus(r.getDatanodeDetails()).isHealthy())
          .filter(r -> !deletionInFlight.contains(r.getDatanodeDetails()))
          .sorted((r1, r2) -> r2.getSequenceId().compareTo(r1.getSequenceId()))
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      if (source.size() > 0) {
        final int replicationFactor = container
            .getReplicationConfig().getRequiredNodes();
        // Want to check if the container is mis-replicated after considering
        // inflight add and delete.
        // Create a new list from source (healthy replicas minus pending delete)
        List<DatanodeDetails> targetReplicas = new ArrayList<>(source);
        // Then add any pending additions
        targetReplicas.addAll(replicationInFlight);
        final ContainerPlacementStatus inFlightplacementStatus =
            containerPlacement.validateContainerPlacement(
                targetReplicas, replicationFactor);
        final int misRepDelta = inFlightplacementStatus.misReplicationCount();
        final int replicasNeeded
            = repDelta < misRepDelta ? misRepDelta : repDelta;
        if (replicasNeeded <= 0) {
          LOG.debug("Container {} meets replication requirement with " +
              "inflight replicas", id);
          return;
        }

        final List<DatanodeDetails> excludeList = replicas.stream()
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());
        excludeList.addAll(replicationInFlight);
        final List<DatanodeDetails> selectedDatanodes = containerPlacement
            .chooseDatanodes(excludeList, null, replicasNeeded,
                container.getUsedBytes());
        if (repDelta > 0) {
          LOG.info("Container {} is under replicated. Expected replica count" +
                  " is {}, but found {}.", id, replicationFactor,
              replicationFactor - repDelta);
        }
        int newMisRepDelta = misRepDelta;
        if (misRepDelta > 0) {
          LOG.info("Container: {}. {}",
              id, placementStatus.misReplicatedReason());
          // Check if the new target nodes (original plus newly selected nodes)
          // makes the placement policy valid.
          targetReplicas.addAll(selectedDatanodes);
          newMisRepDelta = containerPlacement.validateContainerPlacement(
              targetReplicas, replicationFactor).misReplicationCount();
        }
        if (repDelta > 0 || newMisRepDelta < misRepDelta) {
          // Only create new replicas if we are missing a replicas or
          // the number of pending mis-replication has improved. No point in
          // creating new replicas for mis-replicated containers unless it
          // improves things.
          for (DatanodeDetails datanode : selectedDatanodes) {
            sendReplicateCommand(container, datanode, source);
          }
        } else {
          LOG.warn("Container {} is mis-replicated, requiring {} additional " +
              "replicas. After selecting new nodes, mis-replication has not " +
              "improved. No additional replicas will be scheduled",
              id, misRepDelta);
        }
      } else {
        LOG.warn("Cannot replicate container {}, no healthy replica found.",
            container.containerID());
      }
    } catch (IOException | IllegalStateException ex) {
      LOG.warn("Exception while replicating container {}.",
          container.getContainerID(), ex);
    }
  }

  /**
   * If the given container is over replicated, identify the datanode(s)
   * to delete the container and send delete container command to the
   * identified datanode(s).
   *
   * @param container ContainerInfo
   * @param replicaSet An instance of ContainerReplicaCount, containing the
   *                   current replica count and inflight adds and deletes
   */
  private void handleOverReplicatedContainer(final ContainerInfo container,
      final ContainerReplicaCount replicaSet) {

    final Set<ContainerReplica> replicas = replicaSet.getReplica();
    final ContainerID id = container.containerID();
    final int replicationFactor =
        container.getReplicationConfig().getRequiredNodes();
    int excess = replicaSet.additionalReplicaNeeded() * -1;
    if (excess > 0) {

      LOG.info("Container {} is over replicated. Expected replica count" +
              " is {}, but found {}.", id, replicationFactor,
          replicationFactor + excess);

      final List<ContainerReplica> eligibleReplicas = new ArrayList<>(replicas);

      // Iterate replicas in deterministic order to avoid potential data loss.
      // See https://issues.apache.org/jira/browse/HDDS-4589.
      // N.B., sort replicas by (containerID, datanodeDetails).
      eligibleReplicas.sort(
          Comparator.comparingLong(ContainerReplica::hashCode));

      final Map<UUID, ContainerReplica> uniqueReplicas =
          new LinkedHashMap<>();

      if (container.getState() != LifeCycleState.CLOSED) {
        replicas.stream()
            .filter(r -> compareState(container.getState(), r.getState()))
            .forEach(r -> uniqueReplicas
                .putIfAbsent(r.getOriginDatanodeId(), r));

        eligibleReplicas.removeAll(uniqueReplicas.values());
      }
      // Replica which are maintenance or decommissioned are not eligible to
      // be removed, as they do not count toward over-replication and they
      // also many not be available
      eligibleReplicas.removeIf(r ->
          r.getDatanodeDetails().getPersistedOpState() !=
              HddsProtos.NodeOperationalState.IN_SERVICE);

      final List<ContainerReplica> unhealthyReplicas = eligibleReplicas
          .stream()
          .filter(r -> !compareState(container.getState(), r.getState()))
          .collect(Collectors.toList());

      // If there are unhealthy replicas, then we should remove them even if it
      // makes the container violate the placement policy, as excess unhealthy
      // containers are not really useful. It will be corrected later as a
      // mis-replicated container will be seen as under-replicated.
      for (ContainerReplica r : unhealthyReplicas) {
        if (excess > 0) {
          sendDeleteCommand(container, r.getDatanodeDetails(), true);
          excess -= 1;
        } else {
          break;
        }
      }
      eligibleReplicas.removeAll(unhealthyReplicas);

      excess -= handleMoveIfNeeded(container, eligibleReplicas);

      removeExcessReplicasIfNeeded(excess, container, eligibleReplicas);
    }
  }

  /**
   * if the container is in inflightMove, handle move if needed.
   *
   * @param cif ContainerInfo
   * @param eligibleReplicas An list of replicas, which may have excess replicas
   * @return minus how many replica is removed through sending delete command
   */
  private int handleMoveIfNeeded(final ContainerInfo cif,
                   final List<ContainerReplica> eligibleReplicas) {
    int minus = 0;
    final ContainerID cid = cif.containerID();
    if (!inflightMove.containsKey(cid)) {
      return minus;
    }

    Pair<DatanodeDetails, DatanodeDetails> movePair =
        inflightMove.get(cid);

    final DatanodeDetails srcDn = movePair.getKey();
    final DatanodeDetails targetDn = movePair.getValue();
    boolean isSourceDnInReplicaSet;
    boolean isTargetDnInReplicaSet;

    isSourceDnInReplicaSet = eligibleReplicas.stream()
        .anyMatch(r -> r.getDatanodeDetails().equals(srcDn));
    isTargetDnInReplicaSet = eligibleReplicas.stream()
        .anyMatch(r -> r.getDatanodeDetails().equals(targetDn));

    // if target datanode is not in replica set , nothing to do
    if (isTargetDnInReplicaSet) {
      Set<ContainerReplica> eligibleReplicaSet =
          eligibleReplicas.stream().collect(Collectors.toSet());
      int replicationFactor =
          cif.getReplicationConfig().getRequiredNodes();
      ContainerPlacementStatus currentCPS =
          getPlacementStatus(eligibleReplicaSet, replicationFactor);
      eligibleReplicaSet.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
      ContainerPlacementStatus afterMoveCPS =
          getPlacementStatus(eligibleReplicaSet, replicationFactor);

      if (isSourceDnInReplicaSet &&
          isPlacementStatusActuallyEqual(currentCPS, afterMoveCPS)) {
        sendDeleteCommand(cif, srcDn, true);
        eligibleReplicas.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
        minus++;
      } else {
        if (!isSourceDnInReplicaSet) {
          // if the target is present, and source disappears somehow,
          // we can consider move is successful.
          inflightMoveFuture.get(cid).complete(MoveResult.COMPLETED);
        } else {
          // if source and target datanode are both in the replicaset,
          // but we can not delete source datanode for now (e.g.,
          // there is only 3 replicas or not policy-statisfied , etc.),
          // we just complete the future without sending a delete command.
          LOG.info("can not remove source replica after successfully " +
              "replicated to target datanode");
          inflightMoveFuture.get(cid).complete(MoveResult.DELETE_FAIL_POLICY);
        }
        inflightMove.remove(cid);
        inflightMoveFuture.remove(cid);
      }
    }

    return minus;
  }

  /**
   * remove execess replicas if needed, replicationFactor and placement policy
   * will be take into consideration.
   *
   * @param excess the excess number after subtracting replicationFactor
   * @param container ContainerInfo
   * @param eligibleReplicas An list of replicas, which may have excess replicas
   */
  private void removeExcessReplicasIfNeeded(int excess,
                    final ContainerInfo container,
                    final List<ContainerReplica> eligibleReplicas) {
    // After removing all unhealthy replicas, if the container is still over
    // replicated then we need to check if it is already mis-replicated.
    // If it is, we do no harm by removing excess replicas. However, if it is
    // not mis-replicated, then we can only remove replicas if they don't
    // make the container become mis-replicated.
    if (excess > 0) {
      Set<ContainerReplica> eligibleSet = new HashSet<>(eligibleReplicas);
      final int replicationFactor =
          container.getReplicationConfig().getRequiredNodes();
      ContainerPlacementStatus ps =
          getPlacementStatus(eligibleSet, replicationFactor);

      for (ContainerReplica r : eligibleReplicas) {
        if (excess <= 0) {
          break;
        }
        // First remove the replica we are working on from the set, and then
        // check if the set is now mis-replicated.
        eligibleSet.remove(r);
        ContainerPlacementStatus nowPS =
            getPlacementStatus(eligibleSet, replicationFactor);
        if (isPlacementStatusActuallyEqual(ps, nowPS)) {
          // Remove the replica if the container was already unsatisfied
          // and losing this replica keep actual placement count unchanged.
          // OR if losing this replica still keep satisfied
          sendDeleteCommand(container, r.getDatanodeDetails(), true);
          excess -= 1;
          continue;
        }
        // If we decided not to remove this replica, put it back into the set
        eligibleSet.add(r);
      }
      if (excess > 0) {
        LOG.info("The container {} is over replicated with {} excess " +
            "replica. The excess replicas cannot be removed without " +
            "violating the placement policy", container, excess);
      }
    }
  }

  /**
   * whether the given two ContainerPlacementStatus are actually equal.
   *
   * @param cps1 ContainerPlacementStatus
   * @param cps2 ContainerPlacementStatus
   */
  private boolean isPlacementStatusActuallyEqual(
                      ContainerPlacementStatus cps1,
                      ContainerPlacementStatus cps2) {
    return cps1.actualPlacementCount() == cps2.actualPlacementCount() ||
        cps1.isPolicySatisfied() && cps2.isPolicySatisfied();
  }

  /**
   * Given a set of ContainerReplica, transform it to a list of DatanodeDetails
   * and then check if the list meets the container placement policy.
   * @param replicas List of containerReplica
   * @param replicationFactor Expected Replication Factor of the containe
   * @return ContainerPlacementStatus indicating if the policy is met or not
   */
  private ContainerPlacementStatus getPlacementStatus(
      Set<ContainerReplica> replicas, int replicationFactor) {
    List<DatanodeDetails> replicaDns = replicas.stream()
        .map(ContainerReplica::getDatanodeDetails)
        .collect(Collectors.toList());
    return containerPlacement.validateContainerPlacement(
        replicaDns, replicationFactor);
  }

  /**
   * Handles unstable container.
   * A container is inconsistent if any of the replica state doesn't
   * match the container state. We have to take appropriate action
   * based on state of the replica.
   *
   * @param container ContainerInfo
   * @param replicas Set of ContainerReplicas
   */
  private void handleUnstableContainer(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    // Find unhealthy replicas
    List<ContainerReplica> unhealthyReplicas = replicas.stream()
        .filter(r -> !compareState(container.getState(), r.getState()))
        .collect(Collectors.toList());

    Iterator<ContainerReplica> iterator = unhealthyReplicas.iterator();
    while (iterator.hasNext()) {
      final ContainerReplica replica = iterator.next();
      final State state = replica.getState();
      if (state == State.OPEN || state == State.CLOSING) {
        sendCloseCommand(container, replica.getDatanodeDetails(), false);
        iterator.remove();
      }

      if (state == State.QUASI_CLOSED) {
        // Send force close command if the BCSID matches
        if (container.getSequenceId() == replica.getSequenceId()) {
          sendCloseCommand(container, replica.getDatanodeDetails(), true);
          iterator.remove();
        }
      }
    }

    // Now we are left with the replicas which are either unhealthy or
    // the BCSID doesn't match. These replicas should be deleted.

    /*
     * If we have unhealthy replicas we go under replicated and then
     * replicate the healthy copy.
     *
     * We also make sure that we delete only one unhealthy replica at a time.
     *
     * If there are two unhealthy replica:
     *  - Delete first unhealthy replica
     *  - Re-replicate the healthy copy
     *  - Delete second unhealthy replica
     *  - Re-replicate the healthy copy
     *
     * Note: Only one action will be executed in a single ReplicationMonitor
     *       iteration. So to complete all the above actions we need four
     *       ReplicationMonitor iterations.
     */

    unhealthyReplicas.stream().findFirst().ifPresent(replica ->
        sendDeleteCommand(container, replica.getDatanodeDetails(), false));

  }

  /**
   * Sends close container command for the given container to the given
   * datanode.
   *
   * @param container Container to be closed
   * @param datanode The datanode on which the container
   *                  has to be closed
   * @param force Should be set to true if we want to close a
   *               QUASI_CLOSED container
   */
  private void sendCloseCommand(final ContainerInfo container,
                                final DatanodeDetails datanode,
                                final boolean force) {

    ContainerID containerID = container.containerID();
    LOG.info("Sending close container command for container {}" +
            " to datanode {}.", containerID, datanode);
    CloseContainerCommand closeContainerCommand =
        new CloseContainerCommand(container.getContainerID(),
            container.getPipelineID(), force);
    try {
      closeContainerCommand.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending close container command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    closeContainerCommand.setEncodedToken(getContainerToken(containerID));
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND,
        new CommandForDatanode<>(datanode.getUuid(), closeContainerCommand));
  }

  private String getContainerToken(ContainerID containerID) {
    StorageContainerManager scm = scmContext.getScm();
    return scm != null
        ? scm.getContainerTokenGenerator().generateEncodedToken(containerID)
        : ""; // unit test
  }

  /**
   * Sends replicate container command for the given container to the given
   * datanode.
   *
   * @param container Container to be replicated
   * @param datanode The destination datanode to replicate
   * @param sources List of source nodes from where we can replicate
   */
  private void sendReplicateCommand(final ContainerInfo container,
                                    final DatanodeDetails datanode,
                                    final List<DatanodeDetails> sources) {

    LOG.info("Sending replicate container command for container {}" +
            " to datanode {} from datanodes {}",
        container.containerID(), datanode, sources);

    final ContainerID id = container.containerID();
    final ReplicateContainerCommand replicateCommand =
        new ReplicateContainerCommand(id.getId(), sources);
    inflightReplication.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, replicateCommand,
        action -> inflightReplication.get(id).add(action));
  }

  /**
   * Sends delete container command for the given container to the given
   * datanode.
   *
   * @param container Container to be deleted
   * @param datanode The datanode on which the replica should be deleted
   * @param force Should be set to true to delete an OPEN replica
   */
  private void sendDeleteCommand(final ContainerInfo container,
                                 final DatanodeDetails datanode,
                                 final boolean force) {

    LOG.info("Sending delete container command for container {}" +
            " to datanode {}", container.containerID(), datanode);

    final ContainerID id = container.containerID();
    final DeleteContainerCommand deleteCommand =
        new DeleteContainerCommand(id.getId(), force);
    inflightDeletion.computeIfAbsent(id, k -> new ArrayList<>());
    sendAndTrackDatanodeCommand(datanode, deleteCommand,
        action -> inflightDeletion.get(id).add(action));
  }

  /**
   * Creates CommandForDatanode with the given SCMCommand and fires
   * DATANODE_COMMAND event to event queue.
   *
   * Tracks the command using the given tracker.
   *
   * @param datanode Datanode to which the command has to be sent
   * @param command SCMCommand to be sent
   * @param tracker Tracker which tracks the inflight actions
   * @param <T> Type of SCMCommand
   */
  private <T extends GeneratedMessage> void sendAndTrackDatanodeCommand(
      final DatanodeDetails datanode,
      final SCMCommand<T> command,
      final Consumer<InflightAction> tracker) {
    try {
      command.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending datanode command,"
          + " since current SCM is not leader.", nle);
      return;
    }
    final CommandForDatanode<T> datanodeCommand =
        new CommandForDatanode<>(datanode.getUuid(), command);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    tracker.accept(new InflightAction(datanode, Time.monotonicNow()));
  }

  /**
   * Wrap the call to nodeManager.getNodeStatus, catching any
   * NodeNotFoundException and instead throwing an IllegalStateException.
   * @param dn The datanodeDetails to obtain the NodeStatus for
   * @return NodeStatus corresponding to the given Datanode.
   */
  private NodeStatus getNodeStatus(DatanodeDetails dn) {
    try {
      return nodeManager.getNodeStatus(dn);
    } catch (NodeNotFoundException e) {
      throw new IllegalStateException("Unable to find NodeStatus for "+dn, e);
    }
  }

  /**
   * Compares the container state with the replica state.
   *
   * @param containerState ContainerState
   * @param replicaState ReplicaState
   * @return true if the state matches, false otherwise
   */
  public static boolean compareState(final LifeCycleState containerState,
                                      final State replicaState) {
    switch (containerState) {
    case OPEN:
      return replicaState == State.OPEN;
    case CLOSING:
      return replicaState == State.CLOSING;
    case QUASI_CLOSED:
      return replicaState == State.QUASI_CLOSED;
    case CLOSED:
      return replicaState == State.CLOSED;
    case DELETING:
      return false;
    case DELETED:
      return false;
    default:
      return false;
    }
  }

  /**
   * An open container is healthy if all its replicas are in the same state as
   * the container.
   * @param container The container to check
   * @param replicas The replicas belonging to the container
   * @return True if the container is healthy, false otherwise
   */
  private boolean isOpenContainerHealthy(
      ContainerInfo container, Set<ContainerReplica> replicas) {
    LifeCycleState state = container.getState();
    return replicas.stream()
        .allMatch(r -> ReplicationManager.compareState(state, r.getState()));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    collector.addRecord(ReplicationManager.class.getSimpleName())
        .addGauge(ReplicationManagerMetrics.INFLIGHT_REPLICATION,
            inflightReplication.size())
        .addGauge(ReplicationManagerMetrics.INFLIGHT_DELETION,
            inflightDeletion.size())
        .addGauge(ReplicationManagerMetrics.INFLIGHT_MOVE,
            inflightMove.size())
        .endRecord();
  }

  /**
   * Wrapper class to hold the InflightAction with its start time.
   */
  private static final class InflightAction {

    private final DatanodeDetails datanode;
    private final long time;

    private InflightAction(final DatanodeDetails datanode,
                           final long time) {
      this.datanode = datanode;
      this.time = time;
    }
  }

  /**
   * Configuration used by the Replication Manager.
   */
  @ConfigGroup(prefix = "hdds.scm.replication")
  public static class ReplicationManagerConfiguration {
    /**
     * The frequency in which ReplicationMonitor thread should run.
     */
    @Config(key = "thread.interval",
        type = ConfigType.TIME,
        defaultValue = "300s",
        tags = {SCM, OZONE},
        description = "There is a replication monitor thread running inside " +
            "SCM which takes care of replicating the containers in the " +
            "cluster. This property is used to configure the interval in " +
            "which that thread runs."
    )
    private long interval = Duration.ofSeconds(300).toMillis();

    /**
     * Timeout for container replication & deletion command issued by
     * ReplicationManager.
     */
    @Config(key = "event.timeout",
        type = ConfigType.TIME,
        defaultValue = "30m",
        tags = {SCM, OZONE},
        description = "Timeout for the container replication/deletion commands "
            + "sent  to datanodes. After this timeout the command will be "
            + "retried.")
    private long eventTimeout = Duration.ofMinutes(30).toMillis();
    public void setInterval(Duration interval) {
      this.interval = interval.toMillis();
    }

    public void setEventTimeout(Duration timeout) {
      this.eventTimeout = timeout.toMillis();
    }

    /**
     * The number of container replica which must be available for a node to
     * enter maintenance.
     */
    @Config(key = "maintenance.replica.minimum",
        type = ConfigType.INT,
        defaultValue = "2",
        tags = {SCM, OZONE},
        description = "The minimum number of container replicas which must " +
            " be available for a node to enter maintenance. If putting a " +
            " node into maintenance reduces the available replicas for any " +
            " container below this level, the node will remain in the " +
            " entering maintenance state until a new replica is created.")
    private int maintenanceReplicaMinimum = 2;

    public void setMaintenanceReplicaMinimum(int replicaCount) {
      this.maintenanceReplicaMinimum = replicaCount;
    }

    public long getInterval() {
      return interval;
    }

    public long getEventTimeout() {
      return eventTimeout;
    }

    public int getMaintenanceReplicaMinimum() {
      return maintenanceReplicaMinimum;
    }
  }

  /**
   * Metric name definitions for Replication manager.
   */
  public enum ReplicationManagerMetrics implements MetricsInfo {

    INFLIGHT_REPLICATION("Tracked inflight container replication requests."),
    INFLIGHT_DELETION("Tracked inflight container deletion requests."),
    INFLIGHT_MOVE("Tracked inflight container move requests.");

    private final String desc;

    ReplicationManagerMetrics(String desc) {
      this.desc = desc;
    }

    @Override
    public String description() {
      return desc;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", this.getClass().getSimpleName() + "{", "}")
          .add("name=" + name())
          .add("description=" + desc)
          .toString();
    }
  }

  @Override
  public void notifyStatusChanged() {
    serviceLock.lock();
    try {
      // 1) SCMContext#isLeaderReady returns true.
      // 2) not in safe mode.
      if (scmContext.isLeaderReady() && !scmContext.isInSafeMode()) {
        // transition from PAUSING to RUNNING
        if (serviceStatus != ServiceStatus.RUNNING) {
          LOG.info("Service {} transitions to RUNNING.", getServiceName());
          lastTimeToBeReadyInMillis = Time.monotonicNow();
          serviceStatus = ServiceStatus.RUNNING;
        }
      } else {
        serviceStatus = ServiceStatus.PAUSING;
      }
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public boolean shouldRun() {
    serviceLock.lock();
    try {
      // If safe mode is off, then this SCMService starts to run with a delay.
      return serviceStatus == ServiceStatus.RUNNING &&
          Time.monotonicNow() - lastTimeToBeReadyInMillis >= waitTimeInMillis;
    } finally {
      serviceLock.unlock();
    }
  }

  @Override
  public String getServiceName() {
    return ReplicationManager.class.getSimpleName();
  }
}