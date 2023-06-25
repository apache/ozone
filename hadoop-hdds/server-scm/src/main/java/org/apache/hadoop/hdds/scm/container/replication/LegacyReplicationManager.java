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

package org.apache.hadoop.hdds.scm.container.replication;

import com.google.protobuf.Message;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.ContainerPlacementStatus;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport;
import org.apache.hadoop.hdds.scm.container.ReplicationManagerReport.HealthState;
import org.apache.hadoop.hdds.scm.container.balancer.MoveManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.MoveDataNodePair;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAInvocationHandler;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServer;
import org.apache.hadoop.hdds.scm.metadata.DBTransactionBuffer;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.NodeStatus;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException;
import org.apache.hadoop.ozone.protocol.commands.CloseContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.DeleteContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.ReplicateContainerCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState.IN_SERVICE;
import static org.apache.hadoop.hdds.protocol.proto.SCMRatisProtocol.RequestType.MOVE;

/**
 * Legacy Replication Manager (RM) is a legacy , which is used to process
 * non-EC container, and hopefully to be replaced int the future.
 */
public class LegacyReplicationManager {

  public static final Logger LOG =
      LoggerFactory.getLogger(LegacyReplicationManager.class);

  static class InflightMap {
    private final Map<ContainerID, List<InflightAction>> map
        = new ConcurrentHashMap<>();
    private final InflightType type;
    private final int sizeLimit;
    private final AtomicInteger inflightCount = new AtomicInteger();

    InflightMap(InflightType type, int sizeLimit) {
      this.type = type;
      this.sizeLimit = sizeLimit > 0 ? sizeLimit : Integer.MAX_VALUE;
    }

    boolean isReplication() {
      return type == InflightType.REPLICATION;
    }

    private List<InflightAction> get(ContainerID id) {
      return map.get(id);
    }

    boolean containsKey(ContainerID id) {
      return map.containsKey(id);
    }

    int inflightActionCount(ContainerID id) {
      return Optional.ofNullable(map.get(id)).map(List::size).orElse(0);
    }

    int containerCount() {
      return map.size();
    }

    boolean isFull() {
      return inflightCount.get() >= sizeLimit;
    }

    void clear() {
      map.clear();
    }

    void iterate(ContainerID id, Predicate<InflightAction> processor) {
      for (; ;) {
        final List<InflightAction> actions = get(id);
        if (actions == null) {
          return;
        }
        synchronized (actions) {
          if (get(id) != actions) {
            continue; //actions is changed, retry
          }
          for (Iterator<InflightAction> i = actions.iterator(); i.hasNext();) {
            final boolean remove = processor.test(i.next());
            if (remove) {
              i.remove();
              inflightCount.decrementAndGet();
            }
          }
          map.computeIfPresent(id,
              (k, v) -> v == actions && v.isEmpty() ? null : v);
          return;
        }
      }
    }

    boolean add(ContainerID id, InflightAction a) {
      final int previous = inflightCount.getAndUpdate(
          n -> n < sizeLimit ? n + 1 : n);
      if (previous >= sizeLimit) {
        return false;
      }
      for (; ;) {
        final List<InflightAction> actions = map.computeIfAbsent(id,
            key -> new LinkedList<>());
        synchronized (actions) {
          if (get(id) != actions) {
            continue; //actions is changed, retry
          }
          final boolean added = actions.add(a);
          if (!added) {
            inflightCount.decrementAndGet();
          }
          return added;
        }
      }
    }

    List<DatanodeDetails> getDatanodeDetails(ContainerID id) {
      for (; ;) {
        final List<InflightAction> actions = get(id);
        if (actions == null) {
          return Collections.emptyList();
        }
        synchronized (actions) {
          if (get(id) != actions) {
            continue; //actions is changed, retry
          }
          return actions.stream()
              .map(InflightAction::getDatanode)
              .collect(Collectors.toList());
        }
      }
    }
  }

  /**
   * Reference to the ContainerManager.
   */
  private final ContainerManager containerManager;

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
  private final InflightMap inflightReplication;

  /**
   * This is used for tracking container deletion commands which are issued
   * by ReplicationManager and not yet complete.
   */
  private final InflightMap inflightDeletion;

  /**
   * This is used for tracking container move commands
   * which are not yet complete.
   */
  private final Map<ContainerID,
      CompletableFuture<MoveManager.MoveResult>> inflightMoveFuture;

  /**
   * ReplicationManager specific configuration.
   */
  private final ReplicationManagerConfiguration rmConf;

  /**
   * Minimum number of replica in a healthy state for maintenance.
   */
  private int minHealthyForMaintenance;

  private final Clock clock;

  /**
   * Current container size as a bound for choosing datanodes with
   * enough space for a replica.
   */
  private long currentContainerSize;

  /**
   * Replication progress related metrics.
   */
  private ReplicationManagerMetrics metrics;

  /**
   * scheduler move option.
   */
  private final MoveScheduler moveScheduler;


  /**
   * Constructs ReplicationManager instance with the given configuration.
   *
   * @param conf OzoneConfiguration
   * @param containerManager ContainerManager
   * @param containerPlacement PlacementPolicy
   * @param eventPublisher EventPublisher
   */
  @SuppressWarnings("parameternumber")
  public LegacyReplicationManager(final ConfigurationSource conf,
            final ContainerManager containerManager,
            final PlacementPolicy containerPlacement,
            final EventPublisher eventPublisher,
            final SCMContext scmContext,
            final NodeManager nodeManager,
            final SCMHAManager scmhaManager,
            final Clock clock,
            final Table<ContainerID, MoveDataNodePair> moveTable)
             throws IOException {
    this.containerManager = containerManager;
    this.containerPlacement = containerPlacement;
    this.eventPublisher = eventPublisher;
    this.scmContext = scmContext;
    this.nodeManager = nodeManager;
    this.rmConf = conf.getObject(ReplicationManagerConfiguration.class);
    LegacyReplicationManagerConfiguration legacyConf = conf
        .getObject(LegacyReplicationManagerConfiguration.class);
    this.inflightReplication = new InflightMap(InflightType.REPLICATION,
        legacyConf.getContainerInflightReplicationLimit());
    this.inflightDeletion = new InflightMap(InflightType.DELETION,
        legacyConf.getContainerInflightDeletionLimit());
    this.inflightMoveFuture = new ConcurrentHashMap<>();
    this.minHealthyForMaintenance = rmConf.getMaintenanceReplicaMinimum();
    this.clock = clock;

    this.currentContainerSize = (long) conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT,
        StorageUnit.BYTES);
    this.metrics = null;

    moveScheduler = new MoveSchedulerImpl.Builder()
        .setDBTransactionBuffer(scmhaManager.getDBTransactionBuffer())
        .setRatisServer(scmhaManager.getRatisServer())
        .setMoveTable(moveTable).build();
  }


  protected synchronized void clearInflightActions() {
    inflightReplication.clear();
    inflightDeletion.clear();
  }

  protected synchronized void setMetrics(ReplicationManagerMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * Process the given container.
   *
   * @param container ContainerInfo
   */
  @SuppressWarnings("checkstyle:methodlength")
  protected void processContainer(ContainerInfo container,
      ReplicationManagerReport report) {
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
            report.incrementAndSample(
                HealthState.OPEN_UNHEALTHY, container.containerID());
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
          setHealthStateForClosing(replicas, container, report);
          for (ContainerReplica replica: replicas) {
            if (replica.getState() != State.UNHEALTHY) {
              sendCloseCommand(
                  container, replica.getDatanodeDetails(), false);
            }
          }

          /*
           * Empty containers in CLOSING state should be CLOSED.
           *
           * These are containers that are allocated in SCM but never got
           * created on Datanodes. Since these containers don't have any
           * replica associated with them, they are stuck in CLOSING state
           * forever as there is no replicas to CLOSE.
           */
          if (replicas.isEmpty() && (container.getNumberOfKeys() == 0)) {
            closeEmptyContainer(container);
          }
          return;
        }

        /*
         * If the container is in QUASI_CLOSED state, check and close the
         * container if possible.
         */
        if (state == LifeCycleState.QUASI_CLOSED) {
          if (canForceCloseContainer(container, replicas)) {
            forceCloseContainer(container, replicas);
            return;
          } else {
            report.incrementAndSample(HealthState.QUASI_CLOSED_STUCK,
                container.containerID());
          }
        }

        if (container.getReplicationType() == HddsProtos.ReplicationType.EC) {
          // TODO We do not support replicating EC containers as yet, so at this
          //      point, after handing the closing etc states, we just return.
          //      EC Support will be added later.
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
            action -> replicas.stream().anyMatch(
                r -> r.getDatanodeDetails().equals(action.getDatanode())),
            () -> metrics.incrReplicaCreateTimeoutTotal(),
            action -> updateCompletedReplicationMetrics(container, action));

        updateInflightAction(container, inflightDeletion,
            action -> replicas.stream().noneMatch(
                r -> r.getDatanodeDetails().equals(action.getDatanode())),
            () -> metrics.incrReplicaDeleteTimeoutTotal(),
            action -> updateCompletedDeletionMetrics(container, action));

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

        RatisContainerReplicaCount replicaSet =
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
          report.incrementAndSample(
              HealthState.EMPTY, container.containerID());
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
        boolean sufficientlyReplicated = replicaSet.isSufficientlyReplicated();
        boolean placementSatisfied = placementStatus.isPolicySatisfied();
        ContainerID containerID = container.containerID();
        if (!placementStatus.isPolicySatisfied()) {
          report.incrementAndSample(HealthState.MIS_REPLICATED, containerID);
        }
        if (!replicaSet.isHealthy()) {
          report.incrementAndSample(HealthState.UNHEALTHY, containerID);
        }
        if (!sufficientlyReplicated || !placementSatisfied) {
          // Replicate container if needed.
          if (!inflightReplication.isFull() || !inflightDeletion.isFull()) {
            if (replicaSet.isUnrecoverable()) {
              // There are no healthy or unhealthy replicas.
              report.incrementAndSample(HealthState.MISSING, containerID);
              report.incrementAndSample(HealthState.UNDER_REPLICATED,
                  containerID);
            } else {
              if (replicaSet.getHealthyReplicaCount() == 0 &&
                  replicaSet.getUnhealthyReplicaCount() != 0) {
                handleAllReplicasUnhealthy(container, replicaSet,
                    placementStatus, report);
              } else {
                handleUnderReplicatedHealthy(container,
                    replicaSet, placementStatus, report);
              }
            }
          }
          return;
        }

        /*
         * Check if the container is over replicated and take appropriate
         * action.
         */
        if (replicaSet.isOverReplicated()) {
          handleOverReplicatedHealthy(container, replicaSet, report);
          return;
        }

      /*
       If we get here, the container is not over replicated or under replicated
       but it may be "unhealthy", which means it has one or more replica which
       are not in the same state as the container itself.
       */
        if (!replicaSet.isHealthy()) {
          handleOverReplicatedExcessUnhealthy(container, replicaSet, report);
        }
      }
    } catch (ContainerNotFoundException ex) {
      LOG.warn("Missing container {}.", id);
    } catch (Exception ex) {
      LOG.warn("Process container {} error: ", id, ex);
    }
  }

  private void updateCompletedReplicationMetrics(ContainerInfo container,
      InflightAction action) {
    metrics.incrReplicasCreatedTotal();
    metrics.incrReplicationBytesCompletedTotal(container.getUsedBytes());
    metrics.addReplicationTime(clock.millis() - action.getTime());
  }

  private void updateCompletedDeletionMetrics(ContainerInfo container,
      InflightAction action) {
    metrics.incrReplicasDeletedTotal();
    metrics.incrDeletionBytesCompletedTotal(container.getUsedBytes());
    metrics.addDeletionTime(clock.millis() - action.getTime());
  }

  /**
   * Reconciles the InflightActions for a given container.
   *
   * @param container Container to update
   * @param inflightActions inflightReplication (or) inflightDeletion
   * @param filter filter to check if the operation is completed
   * @param timeoutCounter update timeout metrics
   * @param completedCounter update completed metrics
   */
  private void updateInflightAction(final ContainerInfo container,
      final InflightMap inflightActions,
      final Predicate<InflightAction> filter,
      final Runnable timeoutCounter,
      final Consumer<InflightAction> completedCounter) throws TimeoutException {
    final ContainerID id = container.containerID();
    final long deadline = clock.millis() - rmConf.getEventTimeout();
    inflightActions.iterate(id, a -> updateInflightAction(
        container, a, filter, timeoutCounter, completedCounter,
        deadline, inflightActions.isReplication()));
  }

  private boolean updateInflightAction(final ContainerInfo container,
      final InflightAction a,
      final Predicate<InflightAction> filter,
      final Runnable timeoutCounter,
      final Consumer<InflightAction> completedCounter,
      final long deadline,
      final boolean isReplication) {
    boolean remove = false;
    try {
      final NodeStatus status = nodeManager.getNodeStatus(a.getDatanode());
      final boolean isUnhealthy = status.getHealth() != NodeState.HEALTHY;
      final boolean isCompleted = filter.test(a);
      final boolean isTimeout = a.getTime() < deadline;
      final boolean isNotInService = status.getOperationalState() !=
          NodeOperationalState.IN_SERVICE;
      if (isCompleted || isUnhealthy || isTimeout || isNotInService) {
        if (isTimeout) {
          timeoutCounter.run();
        } else if (isCompleted) {
          completedCounter.accept(a);
        }

        updateMoveIfNeeded(isUnhealthy, isCompleted, isTimeout,
            isNotInService, container, a.getDatanode(), isReplication);
        remove = true;
      }
    } catch (NodeNotFoundException | ContainerNotFoundException e) {
      // Should not happen, but if it does, just remove the action as the
      // node somehow does not exist;
      remove = true;
    } catch (TimeoutException e) {
      LOG.error("Got exception while updating.", e);
    }
    return remove;
  }

  /**
   * update inflight move if needed.
   *
   * @param isUnhealthy is the datanode unhealthy
   * @param isCompleted is the action completed
   * @param isTimeout is the action timeout
   * @param container Container to update
   * @param dn datanode which is removed from the inflightActions
   * @param isInflightReplication is inflightReplication?
   */
  private void updateMoveIfNeeded(final boolean isUnhealthy,
                   final boolean isCompleted, final boolean isTimeout,
                   final boolean isNotInService,
                   final ContainerInfo container, final DatanodeDetails dn,
                   final boolean isInflightReplication)
      throws ContainerNotFoundException, TimeoutException {
    // make sure inflightMove contains the container
    ContainerID id = container.containerID();

    // make sure the datanode , which is removed from inflightActions,
    // is source or target datanode.
    MoveDataNodePair kv = moveScheduler.getMoveDataNodePair(id);
    if (kv == null) {
      return;
    }
    final boolean isSource = kv.getSrc().equals(dn);
    final boolean isTarget = kv.getTgt().equals(dn);
    if (!isSource && !isTarget) {
      return;
    }

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
      //if RM is reinitialize, inflightMove will be restored,
      //but inflightMoveFuture not. so there will be a case that
      //container is in inflightMove, but not in inflightMoveFuture.
      compleleteMoveFutureWithResult(id,
          MoveManager.MoveResult.FAIL_UNEXPECTED_ERROR);
      LOG.info("Move failed because replication for container {} " +
              "unexpectedly happened at the source {}, not the target {}.",
          container, kv.getSrc().getUuidString(), kv.getTgt().getUuidString());
      moveScheduler.completeMove(id.getProtobuf());
      return;
    }

    if (isTarget && !isInflightReplication) {
      compleleteMoveFutureWithResult(id,
          MoveManager.MoveResult.FAIL_UNEXPECTED_ERROR);
      LOG.info("Move failed because deletion for container {} unexpectedly " +
              "happened at the target {}, not the source {}.", container,
          kv.getTgt().getUuidString(), kv.getSrc().getUuidString());
      moveScheduler.completeMove(id.getProtobuf());
      return;
    }

    if (!(isInflightReplication && isCompleted)) {
      if (isInflightReplication) {
        if (isUnhealthy) {
          compleleteMoveFutureWithResult(id,
              MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
        } else if (isNotInService) {
          compleleteMoveFutureWithResult(id,
              MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
        } else {
          compleleteMoveFutureWithResult(id,
              MoveManager.MoveResult.REPLICATION_FAIL_TIME_OUT);
        }
      } else {
        if (isUnhealthy) {
          compleleteMoveFutureWithResult(id,
              MoveManager.MoveResult.DELETION_FAIL_NODE_UNHEALTHY);
        } else if (isTimeout) {
          compleleteMoveFutureWithResult(id,
              MoveManager.MoveResult.DELETION_FAIL_TIME_OUT);
        } else if (isNotInService) {
          compleleteMoveFutureWithResult(id,
              MoveManager.MoveResult.DELETION_FAIL_NODE_NOT_IN_SERVICE);
        } else {
          compleleteMoveFutureWithResult(id, MoveManager.MoveResult.COMPLETED);
        }
      }
      moveScheduler.completeMove(id.getProtobuf());
    } else {
      deleteSrcDnForMove(container,
          containerManager.getContainerReplicas(id));
    }
  }

  /**
   * add a move action for a given container.
   *
   * @param cid Container to move
   * @param src source datanode
   * @param tgt target datanode
   */
  public CompletableFuture<MoveManager.MoveResult> move(ContainerID cid,
             DatanodeDetails src, DatanodeDetails tgt)
      throws ContainerNotFoundException, NodeNotFoundException,
      TimeoutException {
    return move(cid, new MoveDataNodePair(src, tgt));
  }

  /**
   * add a move action for a given container.
   *
   * @param cid Container to move
   * @param mp MoveDataNodePair which contains source and target datanodes
   */
  private CompletableFuture<MoveManager.MoveResult> move(ContainerID cid,
      MoveDataNodePair mp) throws ContainerNotFoundException,
      NodeNotFoundException, TimeoutException {
    CompletableFuture<MoveManager.MoveResult> ret = new CompletableFuture<>();

    if (!scmContext.isLeader()) {
      ret.complete(MoveManager.MoveResult.FAIL_LEADER_NOT_READY);
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

    DatanodeDetails srcDn = mp.getSrc();
    DatanodeDetails targetDn = mp.getTgt();
    NodeStatus currentNodeStat = nodeManager.getNodeStatus(srcDn);
    NodeState healthStat = currentNodeStat.getHealth();
    NodeOperationalState operationalState =
        currentNodeStat.getOperationalState();
    if (healthStat != NodeState.HEALTHY) {
      ret.complete(MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      LOG.info("Failing move for container {} because source {} is {}", cid,
          srcDn.getUuidString(), healthStat.toString());
      return ret;
    }
    if (operationalState != NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      LOG.info("Failing move for container {} because source {} is {}", cid,
          srcDn.getUuidString(), operationalState.toString());
      return ret;
    }

    currentNodeStat = nodeManager.getNodeStatus(targetDn);
    healthStat = currentNodeStat.getHealth();
    operationalState = currentNodeStat.getOperationalState();
    if (healthStat != NodeState.HEALTHY) {
      ret.complete(MoveManager.MoveResult.REPLICATION_FAIL_NODE_UNHEALTHY);
      LOG.info("Failing move for container {} because target {} is {}", cid,
          targetDn.getUuidString(), healthStat.toString());
      return ret;
    }
    if (operationalState != NodeOperationalState.IN_SERVICE) {
      ret.complete(MoveManager.MoveResult.REPLICATION_FAIL_NODE_NOT_IN_SERVICE);
      LOG.info("Failing move for container {} because target {} is {}", cid,
          targetDn.getUuidString(), operationalState.toString());
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
        ret.complete(MoveManager.MoveResult.REPLICATION_FAIL_EXIST_IN_TARGET);
        return ret;
      }
      if (!replicas.contains(srcDn)) {
        ret.complete(
            MoveManager.MoveResult.REPLICATION_FAIL_NOT_EXIST_IN_SOURCE);
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
        ret.complete(
            MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_REPLICATION);
        return ret;
      }
      if (inflightDeletion.containsKey(cid)) {
        ret.complete(MoveManager.MoveResult.REPLICATION_FAIL_INFLIGHT_DELETION);
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
        ret.complete(
            MoveManager.MoveResult.REPLICATION_FAIL_CONTAINER_NOT_CLOSED);
        return ret;
      }

      // check whether {Existing replicas + Target_Dn - Source_Dn}
      // satisfies current placement policy
      if (!isPolicySatisfiedAfterMove(cif, srcDn, targetDn,
          new ArrayList<>(currentReplicas))) {
        ret.complete(MoveManager.MoveResult.REPLICATION_NOT_HEALTHY_AFTER_MOVE);
        return ret;
      }

      try {
        moveScheduler.startMove(cid.getProtobuf(),
            mp.getProtobufMessage(ClientVersion.CURRENT_VERSION));
      } catch (IOException e) {
        LOG.warn("Exception while starting move for container {}", cid, e);
        ret.complete(MoveManager.MoveResult.FAIL_UNEXPECTED_ERROR);
        return ret;
      }

      inflightMoveFuture.putIfAbsent(cid, ret);
      sendReplicateCommand(cif, targetDn, Collections.singletonList(srcDn));
    }
    LOG.info("receive a move request about container {} , from {} to {}",
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
                    final List<ContainerReplica> replicas) {
    Set<ContainerReplica> movedReplicas = new HashSet<>(replicas);
    movedReplicas.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
    movedReplicas.add(ContainerReplica.newBuilder()
        .setDatanodeDetails(targetDn)
        .setContainerID(cif.containerID())
        .setContainerState(State.CLOSED).build());
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
    return inflightReplication.inflightActionCount(id);
  }

  /**
   * Returns the number replica which are pending delete for the given
   * container ID.
   * @param id The ContainerID for which to check the pending replica
   * @return The number of inflight deletes or zero if none
   */
  private int getInflightDel(final ContainerID id) {
    return inflightDeletion.inflightActionCount(id);
  }

  /**
   * Returns true if the container is empty and CLOSED.
   * A container is deemed empty if its keyCount (num of blocks) is 0. The
   * usedBytes counter is not checked here because usedBytes is not a
   * accurate representation of the committed blocks. There could be orphaned
   * chunks in the container which contribute to the usedBytes.
   *
   * @param container Container to check
   * @param replicas Set of ContainerReplicas
   * @return true if the container is empty, false otherwise
   */
  private boolean isContainerEmpty(final ContainerInfo container,
      final Set<ContainerReplica> replicas) {
    return container.getState() == LifeCycleState.CLOSED &&
        !replicas.isEmpty() &&
        replicas.stream().allMatch(
            r -> r.getState() == State.CLOSED && r.isEmpty());
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
  private RatisContainerReplicaCount getContainerReplicaCount(
      ContainerInfo container, Set<ContainerReplica> replica) {
    return new LegacyRatisContainerReplicaCount(
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
      InvalidStateTransitionException, TimeoutException {
    Preconditions.assertTrue(container.getState() ==
        LifeCycleState.CLOSED);

    replicas.stream().forEach(rp -> {
      Preconditions.assertTrue(rp.getState() == State.CLOSED);
      Preconditions.assertTrue(rp.isEmpty());
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
      InvalidStateTransitionException, TimeoutException {
    if (replicas.size() == 0) {
      containerManager.updateContainerState(container.containerID(),
          HddsProtos.LifeCycleEvent.CLEANUP);
      LOG.debug("Container {} state changes to DELETED", container);
    } else {
      // Check whether to resend the delete replica command
      final List<DatanodeDetails> deletionInFlight
          = inflightDeletion.getDatanodeDetails(container.containerID());
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
  private void handleUnderReplicatedHealthy(final ContainerInfo container,
      final RatisContainerReplicaCount replicaSet,
      final ContainerPlacementStatus placementStatus,
      ReplicationManagerReport report) {
    LOG.debug("Handling under-replicated container: {}", container);
    if (replicaSet.isSufficientlyReplicated()
        && placementStatus.isPolicySatisfied()) {
      LOG.info("The container {} with replicas {} is sufficiently " +
          "replicated and is not mis-replicated",
          container.getContainerID(), replicaSet);
      return;
    }

    List<ContainerReplica> allReplicas = replicaSet.getReplicas();
    int numCloseCommandsSent = closeReplicasIfPossible(container, allReplicas);
    int replicasNeeded =
        replicaSet.additionalReplicaNeeded() - numCloseCommandsSent;

    if (replicasNeeded > 0) {
      report.incrementAndSample(HealthState.UNDER_REPLICATED,
          container.containerID());
    }

    List<ContainerReplica> replicationSources = getReplicationSources(container,
        replicaSet.getReplicas(), State.CLOSED, State.QUASI_CLOSED);
    // This method will handle topology even if replicasNeeded <= 0.
    replicateAnyWithTopology(container, replicationSources,
        placementStatus, replicasNeeded);
  }

  /**
   * If the given container is over replicated, identify the datanode(s)
   * to delete the container and send delete container command to the
   * identified datanode(s). This method ignores unhealthy containers.
   *
   * @param container ContainerInfo
   * @param replicaSet An instance of ContainerReplicaCount, containing the
   *                   current replica count and inflight adds and deletes
   */
  private void handleOverReplicatedHealthy(final ContainerInfo container,
      final RatisContainerReplicaCount replicaSet,
      ReplicationManagerReport report) {

    final ContainerID id = container.containerID();
    final int replicationFactor =
        container.getReplicationConfig().getRequiredNodes();
    int excess = replicaSet.additionalReplicaNeeded() * -1;
    if (excess > 0) {
      LOG.info("Container {} is over replicated. Expected replica count" +
                      " is {}, but found {}.", id, replicationFactor,
              replicationFactor + excess);

      report.incrementAndSample(HealthState.OVER_REPLICATED,
          container.containerID());

      // The list of replicas that we can potentially delete to fix the over
      // replicated state. This method is only concerned with healthy replicas.
      final List<ContainerReplica> deleteCandidates =
          getHealthyDeletionCandidates(container, replicaSet.getReplicas());

      if (container.getState() == LifeCycleState.CLOSED) {
        // Container is closed, so all healthy replicas are equal.
        // We can choose which ones to delete based on topology.
        // TODO Legacy RM implementation can only handle topology when all
        //  container replicas are closed and equal.
        deleteExcessWithTopology(excess, container, deleteCandidates);
      } else {
        // Container is not yet closed. Choose which healthy replicas to
        // delete so that we do not lose any origin node IDs.
        deleteExcessWithNonUniqueOriginNodeIDs(container,
            replicaSet.getReplicas(),
            deleteCandidates, excess);
      }
    }
  }

  /**
   * Processes replicas of the container when all replicas are unhealthy (in
   * a state that does not match the container state).
   *
   * Unhealthy replicas will first be checked to see if they can be closed.
   * If there are more unhealthy replicas than required, some may be deleted.
   * If there are fewer unhealthy replicas than required, some may be
   * replicated.
   */
  private void handleAllReplicasUnhealthy(ContainerInfo container,
      RatisContainerReplicaCount replicaSet,
      ContainerPlacementStatus placementStatus,
      ReplicationManagerReport report) {

    List<ContainerReplica> replicas = replicaSet.getReplicas();
    int excessReplicas = replicas.size() -
        container.getReplicationConfig().getRequiredNodes();
    int missingReplicas = excessReplicas * -1;

    if (missingReplicas > 0) {
      handleUnderReplicatedAllUnhealthy(container, replicas,
          placementStatus, missingReplicas, report);
    } else if (excessReplicas > 0) {
      handleOverReplicatedAllUnhealthy(container, replicas,
          excessReplicas, report);
    } else {
      // We have the correct number of unhealthy replicas. See if any of them
      // can be closed.
      closeReplicasIfPossible(container, replicas);
    }
  }

  /**
   * Handles a container which has the correct number of healthy replicas,
   * but an excess of unhealthy replicas.
   *
   * If the container is closed, the unhealthy replicas can be deleted. If the
   * container is not yet closed, the unhealthy replicas with non-unique
   * origin node IDs can be deleted.
   */
  private void handleOverReplicatedExcessUnhealthy(
      final ContainerInfo container,
      final RatisContainerReplicaCount replicaSet,
      ReplicationManagerReport report) {
    // Note - ReplicationManager would reach here only if the
    // following conditions are met:
    //   1. Container is in either CLOSED or QUASI-CLOSED state
    //   2. We have adequate healthy replicas with extra unhealthy
    //   replicas.

    List<ContainerReplica> replicas = replicaSet.getReplicas();
    List<ContainerReplica> unhealthyReplicas =
        getUnhealthyDeletionCandidates(container, replicas);

    // Only unhealthy replicas which cannot be closed will remain eligible
    // for deletion, since this method is deleting unhealthy containers only.
    closeReplicasIfPossible(container, unhealthyReplicas);
    if (!unhealthyReplicas.isEmpty()) {
      LOG.info("Container {} has {} excess unhealthy replicas. Excess " +
              "unhealthy replicas will be deleted.",
          container.getContainerID(), unhealthyReplicas.size());

      report.incrementAndSample(HealthState.OVER_REPLICATED,
          container.containerID());

      int excessReplicaCount = replicas.size() -
          container.getReplicationConfig().getRequiredNodes();
      if (container.getState() == LifeCycleState.CLOSED) {
        // The container is already closed. The unhealthy replicas are extras
        // and unnecessary.
        deleteExcess(container, unhealthyReplicas, excessReplicaCount);
      } else {
        // Container is not yet closed.
        // We only need to save the unhealthy replicas if they
        // represent unique origin node IDs. If recovering these replicas is
        // possible in the future they could be used to close the container.
        deleteExcessWithNonUniqueOriginNodeIDs(container,
            replicaSet.getReplicas(), unhealthyReplicas, excessReplicaCount);
      }
    }
  }

  /**
   * Returns the replicas from {@code replicas} that:
   *  - Do not have in flight deletions
   *  - Exist on healthy datanodes
   *  - Have a replica state matching one of {@code validReplicaStates}. If
   *  this parameter is empty, any replica state is valid.
   */
  private List<ContainerReplica> getReplicationSources(ContainerInfo container,
      List<ContainerReplica> replicas, State... validReplicaStates) {
    final List<DatanodeDetails> deletionInFlight
        = inflightDeletion.getDatanodeDetails(container.containerID());
    final Set<State> validReplicaStateSet = Arrays.stream(validReplicaStates)
        .collect(Collectors.toSet());
    return replicas.stream()
        // Exclude stale and dead nodes. This is particularly important for
        // maintenance nodes, as the replicas will remain present in the
        // container manager, even when they go dead.
        .filter(r -> getNodeStatus(r.getDatanodeDetails()).isHealthy()
            && !deletionInFlight.contains(r.getDatanodeDetails())
            && (validReplicaStateSet.isEmpty() ||
              validReplicaStateSet.contains(r.getState())))
        .collect(Collectors.toList());
  }

  private List<ContainerReplica> getHealthyDeletionCandidates(
      ContainerInfo container, List<ContainerReplica> replicas) {
    return getDeletionCandidates(container, replicas, true);
  }

  private List<ContainerReplica> getUnhealthyDeletionCandidates(
      ContainerInfo container, List<ContainerReplica> replicas) {
    return getDeletionCandidates(container, replicas, false);
  }

  /**
   * A replica is eligible for deletion if its datanode is healthy and
   * IN_SERVICE.
   */
  private List<ContainerReplica> getDeletionCandidates(ContainerInfo container,
      List<ContainerReplica> replicas, boolean healthy) {
    return replicas.stream()
        .filter(r -> getNodeStatus(r.getDatanodeDetails()).isHealthy()
            && compareState(container.getState(), r.getState()) == healthy
            && r.getDatanodeDetails().getPersistedOpState() == IN_SERVICE)
        .collect(Collectors.toList());
  }

  /**
   * if the container is in inflightMove, handle move.
   * This function assumes replication has been completed
   *
   * @param cif ContainerInfo
   * @param replicaSet An Set of replicas, which may have excess replicas
   */
  private void deleteSrcDnForMove(final ContainerInfo cif,
                   final Set<ContainerReplica> replicaSet)
      throws TimeoutException {
    final ContainerID cid = cif.containerID();
    MoveDataNodePair movePair = moveScheduler.getMoveDataNodePair(cid);
    if (movePair == null) {
      return;
    }
    final DatanodeDetails srcDn = movePair.getSrc();
    ContainerReplicaCount replicaCount =
        getContainerReplicaCount(cif, replicaSet);

    if (!replicaSet.stream()
        .anyMatch(r -> r.getDatanodeDetails().equals(srcDn))) {
      // if the target is present but source disappears somehow,
      // we can consider move is successful.
      compleleteMoveFutureWithResult(cid, MoveManager.MoveResult.COMPLETED);
      moveScheduler.completeMove(cid.getProtobuf());
      return;
    }

    int replicationFactor =
        cif.getReplicationConfig().getRequiredNodes();
    ContainerPlacementStatus currentCPS =
        getPlacementStatus(replicaSet, replicationFactor);
    Set<ContainerReplica> newReplicaSet = new HashSet<>(replicaSet);
    newReplicaSet.removeIf(r -> r.getDatanodeDetails().equals(srcDn));
    ContainerPlacementStatus newCPS =
        getPlacementStatus(newReplicaSet, replicationFactor);

    if (replicaCount.isOverReplicated() &&
        isPlacementStatusActuallyEqual(currentCPS, newCPS)) {
      sendDeleteCommand(cif, srcDn, true);
    } else {
      // if source and target datanode are both in the replicaset,
      // but we can not delete source datanode for now (e.g.,
      // there is only 3 replicas or not policy-statisfied , etc.),
      // we just complete the future without sending a delete command.
      LOG.info("can not remove source replica after successfully " +
          "replicated to target datanode");
      compleleteMoveFutureWithResult(cid,
          MoveManager.MoveResult.DELETE_FAIL_POLICY);
      moveScheduler.completeMove(cid.getProtobuf());
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
    return (!cps1.isPolicySatisfied() &&
        cps1.actualPlacementCount() == cps2.actualPlacementCount()) ||
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
    if (scmContext.getScm() instanceof StorageContainerManager) {
      StorageContainerManager scm =
              (StorageContainerManager) scmContext.getScm();
      return scm.getContainerTokenGenerator().generateEncodedToken(containerID);
    }
    return ""; // unit test
  }

  private boolean addInflight(InflightType type, ContainerID id,
      InflightAction action) {
    final boolean added = getInflightMap(type).add(id, action);
    if (!added) {
      metrics.incrInflightSkipped(type);
    }
    return added;
  }

  /**
   * Sends replicate container command for the given container to the given
   * datanode.
   *
   * @param container Container to be replicated
   * @param target The destination datanode to replicate
   * @param sources List of source nodes from where we can replicate
   */
  private void sendReplicateCommand(final ContainerInfo container,
                                    final DatanodeDetails target,
                                    final List<DatanodeDetails> sources) {

    final ContainerID id = container.containerID();
    final long containerID = id.getId();
    final ReplicateContainerCommand replicateCommand =
        ReplicateContainerCommand.fromSources(containerID, sources);
    LOG.info("Sending {} to {}", replicateCommand, target);

    final boolean sent = sendAndTrackDatanodeCommand(target, replicateCommand,
        action -> addInflight(InflightType.REPLICATION, id, action));

    if (sent) {
      metrics.incrReplicationCmdsSentTotal();
      metrics.incrReplicationBytesTotal(container.getUsedBytes());
    }
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
    final boolean sent = sendAndTrackDatanodeCommand(datanode, deleteCommand,
        action -> addInflight(InflightType.DELETION, id, action));

    if (sent) {
      metrics.incrDeletionCmdsSentTotal();
      metrics.incrDeletionBytesTotal(container.getUsedBytes());
    }
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
  private <T extends Message> boolean sendAndTrackDatanodeCommand(
      final DatanodeDetails datanode,
      final SCMCommand<T> command,
      final Predicate<InflightAction> tracker) {
    try {
      command.setTerm(scmContext.getTermOfLeader());
    } catch (NotLeaderException nle) {
      LOG.warn("Skip sending datanode command,"
          + " since current SCM is not leader.", nle);
      return false;
    }
    final boolean allowed = tracker.test(
        new InflightAction(datanode, clock.millis()));
    if (!allowed) {
      return false;
    }
    final CommandForDatanode<T> datanodeCommand =
        new CommandForDatanode<>(datanode.getUuid(), command);
    eventPublisher.fireEvent(SCMEvents.DATANODE_COMMAND, datanodeCommand);
    return true;
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
      throw new IllegalStateException("Unable to find NodeStatus for " + dn, e);
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
        .allMatch(r -> compareState(state, r.getState()));
  }

  private void setHealthStateForClosing(Set<ContainerReplica> replicas,
                                        ContainerInfo container,
                                        ReplicationManagerReport report) {
    if (replicas.size() == 0) {
      report.incrementAndSample(HealthState.MISSING, container.containerID());
      report.incrementAndSample(HealthState.UNDER_REPLICATED,
              container.containerID());
      report.incrementAndSample(HealthState.MIS_REPLICATED,
              container.containerID());
    }
  }

  public boolean isContainerReplicatingOrDeleting(ContainerID containerID) {
    return inflightReplication.containsKey(containerID) ||
        inflightDeletion.containsKey(containerID);
  }

  /**
   * Configuration used by the Replication Manager.
   */
  @ConfigGroup(prefix = "hdds.scm.replication")
  public static class LegacyReplicationManagerConfiguration {

    @Config(key = "container.inflight.replication.limit",
        type = ConfigType.INT,
        defaultValue = "0", // 0 means unlimited.
        tags = {SCM, OZONE},
        description = "This property is used to limit" +
            " the maximum number of inflight replication."
    )
    private int containerInflightReplicationLimit = 0;

    @Config(key = "container.inflight.deletion.limit",
        type = ConfigType.INT,
        defaultValue = "0", // 0 means unlimited.
        tags = {SCM, OZONE},
        description = "This property is used to limit" +
            " the maximum number of inflight deletion."
    )
    private int containerInflightDeletionLimit = 0;

    public void setContainerInflightReplicationLimit(int replicationLimit) {
      this.containerInflightReplicationLimit = replicationLimit;
    }

    public void setContainerInflightDeletionLimit(int deletionLimit) {
      this.containerInflightDeletionLimit = deletionLimit;
    }

    public int getContainerInflightReplicationLimit() {
      return containerInflightReplicationLimit;
    }

    public int getContainerInflightDeletionLimit() {
      return containerInflightDeletionLimit;
    }

  }

  protected void notifyStatusChanged() {
    //now, as the current scm is leader and it`s state is up-to-date,
    //we need to take some action about replicated inflight move options.
    onLeaderReadyAndOutOfSafeMode();
  }

  private InflightMap getInflightMap(InflightType type) {
    switch (type) {
    case REPLICATION: return inflightReplication;
    case DELETION: return inflightDeletion;
    default: throw new IllegalStateException("Unexpected type " + type);
    }
  }

  int getInflightCount(InflightType type) {
    return getInflightMap(type).containerCount();
  }

  DatanodeDetails getFirstDatanode(InflightType type, ContainerID id) {
    return getInflightMap(type).get(id).get(0).getDatanode();
  }

  public Map<ContainerID, CompletableFuture<MoveManager.MoveResult>>
      getInflightMove() {
    return inflightMoveFuture;
  }

  /**
  * make move option HA aware.
  */
  public interface MoveScheduler {
    /**
     * completeMove a move action for a given container.
     *
     * @param contianerIDProto Container to which the move option is finished
     */
    @Replicate
    void completeMove(HddsProtos.ContainerID contianerIDProto)
        throws TimeoutException;

    /**
     * start a move action for a given container.
     *
     * @param contianerIDProto Container to move
     * @param mp encapsulates the source and target datanode infos
     */
    @Replicate
    void startMove(HddsProtos.ContainerID contianerIDProto,
              HddsProtos.MoveDataNodePairProto mp)
        throws IOException, TimeoutException;

    /**
     * get the MoveDataNodePair of the giver container.
     *
     * @param cid Container to move
     * @return null if cid is not found in MoveScheduler,
     *          or the corresponding MoveDataNodePair
     */
    MoveDataNodePair getMoveDataNodePair(ContainerID cid);

    /**
     * Reinitialize the MoveScheduler with DB if become leader.
     */
    void reinitialize(Table<ContainerID,
        MoveDataNodePair> moveTable) throws IOException;

    /**
     * get all the inflight move info.
     */
    Map<ContainerID, MoveDataNodePair> getInflightMove();
  }

  /**
   * @return the moveScheduler of RM
   */
  public MoveScheduler getMoveScheduler() {
    return moveScheduler;
  }

  /**
   * Ratis based MoveScheduler, db operations are stored in
   * DBTransactionBuffer until a snapshot is taken.
   */
  public static final class MoveSchedulerImpl implements MoveScheduler {
    private Table<ContainerID, MoveDataNodePair> moveTable;
    private final DBTransactionBuffer transactionBuffer;
    /**
     * This is used for tracking container move commands
     * which are not yet complete.
     */
    private final Map<ContainerID, MoveDataNodePair> inflightMove;

    private MoveSchedulerImpl(Table<ContainerID, MoveDataNodePair> moveTable,
                DBTransactionBuffer transactionBuffer) throws IOException {
      this.moveTable = moveTable;
      this.transactionBuffer = transactionBuffer;
      this.inflightMove = new ConcurrentHashMap<>();
      initialize();
    }

    @Override
    public void completeMove(HddsProtos.ContainerID contianerIDProto) {
      ContainerID cid = null;
      try {
        cid = ContainerID.getFromProtobuf(contianerIDProto);
        transactionBuffer.removeFromBuffer(moveTable, cid);
      } catch (IOException e) {
        LOG.warn("Exception while completing move {}", cid);
      }
      inflightMove.remove(cid);
    }

    @Override
    public void startMove(HddsProtos.ContainerID contianerIDProto,
                          HddsProtos.MoveDataNodePairProto mdnpp)
        throws IOException {
      ContainerID cid = null;
      MoveDataNodePair mp = null;
      try {
        cid = ContainerID.getFromProtobuf(contianerIDProto);
        mp = MoveDataNodePair.getFromProtobuf(mdnpp);
        if (!inflightMove.containsKey(cid)) {
          transactionBuffer.addToBuffer(moveTable, cid, mp);
          inflightMove.putIfAbsent(cid, mp);
        }
      } catch (IOException e) {
        LOG.warn("Exception while completing move {}", cid);
      }
    }

    @Override
    public MoveDataNodePair getMoveDataNodePair(ContainerID cid) {
      return inflightMove.get(cid);
    }

    @Override
    public void reinitialize(Table<ContainerID,
        MoveDataNodePair> mt) throws IOException {
      moveTable = mt;
      inflightMove.clear();
      initialize();
    }

    private void initialize() throws IOException {
      try (TableIterator<ContainerID,
          ? extends Table.KeyValue<ContainerID, MoveDataNodePair>> iterator =
               moveTable.iterator()) {

        while (iterator.hasNext()) {
          Table.KeyValue<ContainerID, MoveDataNodePair> kv = iterator.next();
          final ContainerID cid = kv.getKey();
          final MoveDataNodePair mp = kv.getValue();
          Preconditions.assertNotNull(cid,
              "moved container id should not be null");
          Preconditions.assertNotNull(mp,
              "MoveDataNodePair container id should not be null");
          inflightMove.put(cid, mp);
        }
      }
    }

    @Override
    public Map<ContainerID, MoveDataNodePair> getInflightMove() {
      return inflightMove;
    }

    /**
     * Builder for Ratis based MoveSchedule.
     */
    public static class Builder {
      private Table<ContainerID, MoveDataNodePair> moveTable;
      private DBTransactionBuffer transactionBuffer;
      private SCMRatisServer ratisServer;

      public Builder setRatisServer(final SCMRatisServer scmRatisServer) {
        ratisServer = scmRatisServer;
        return this;
      }

      public Builder setMoveTable(
          final Table<ContainerID, MoveDataNodePair> mt) {
        moveTable = mt;
        return this;
      }

      public Builder setDBTransactionBuffer(DBTransactionBuffer trxBuffer) {
        transactionBuffer = trxBuffer;
        return this;
      }

      public MoveScheduler build() throws IOException {
        Preconditions.assertNotNull(moveTable, "moveTable is null");
        Preconditions.assertNotNull(transactionBuffer,
            "transactionBuffer is null");

        final MoveScheduler impl =
            new MoveSchedulerImpl(moveTable, transactionBuffer);

        final SCMHAInvocationHandler invocationHandler
            = new SCMHAInvocationHandler(MOVE, impl, ratisServer);

        return (MoveScheduler) Proxy.newProxyInstance(
            SCMHAInvocationHandler.class.getClassLoader(),
            new Class<?>[]{MoveScheduler.class},
            invocationHandler);
      }
    }
  }

  /**
  * when scm become LeaderReady and out of safe mode, some actions
  * should be taken. for now , it is only used for handle replicated
  * infligtht move.
  */
  private void onLeaderReadyAndOutOfSafeMode() {
    List<HddsProtos.ContainerID> needToRemove = new LinkedList<>();
    moveScheduler.getInflightMove().forEach((k, v) -> {
      Set<ContainerReplica> replicas;
      ContainerInfo cif;
      try {
        replicas = containerManager.getContainerReplicas(k);
        cif = containerManager.getContainer(k);
      } catch (ContainerNotFoundException e) {
        needToRemove.add(k.getProtobuf());
        LOG.error("can not find container {} " +
            "while processing replicated move", k);
        return;
      }
      boolean isSrcExist = replicas.stream()
          .anyMatch(r -> r.getDatanodeDetails().equals(v.getSrc()));
      boolean isTgtExist = replicas.stream()
          .anyMatch(r -> r.getDatanodeDetails().equals(v.getTgt()));

      if (isSrcExist) {
        if (isTgtExist) {
          //the former scm leader may or may not send the deletion command
          //before reelection.here, we just try to send the command again.
          try {
            deleteSrcDnForMove(cif, replicas);
          } catch (TimeoutException ex) {
            LOG.error("Exception while cleaning up excess replicas.", ex);
          }
        } else {
          // resenting replication command is ok , no matter whether there is an
          // on-going replication
          sendReplicateCommand(cif, v.getTgt(),
              Collections.singletonList(v.getSrc()));
        }
      } else {
        // if container does not exist in src datanode, no matter it exists
        // in target datanode, we can not take more actions to this option,
        // so just remove it through ratis
        needToRemove.add(k.getProtobuf());
      }
    });

    for (HddsProtos.ContainerID containerID : needToRemove) {
      try {
        moveScheduler.completeMove(containerID);
      } catch (TimeoutException ex) {
        LOG.error("Exception while moving container.", ex);
      }
    }
  }

  /**
   * complete the CompletableFuture of the container in the given Map with
   * the given MoveManager.MoveResult.
   */
  private void compleleteMoveFutureWithResult(ContainerID cid,
      MoveManager.MoveResult mr) {
    if (inflightMoveFuture.containsKey(cid)) {
      inflightMoveFuture.get(cid).complete(mr);
      inflightMoveFuture.remove(cid);
    }
  }

  private int closeReplicasIfPossible(ContainerInfo container,
                                      List<ContainerReplica> replicas) {
    // This method should not be used on open containers.
    if (container.getState() == LifeCycleState.OPEN) {
      return 0;
    }

    int numCloseCmdsSent = 0;
    Iterator<ContainerReplica> iterator = replicas.iterator();
    while (iterator.hasNext()) {
      final ContainerReplica replica = iterator.next();
      final State state = replica.getState();
      if (state == State.OPEN || state == State.CLOSING) {
        sendCloseCommand(container, replica.getDatanodeDetails(), false);
        numCloseCmdsSent++;
        iterator.remove();
      } else if (state == State.QUASI_CLOSED &&
          container.getState() == LifeCycleState.CLOSED) {
        // Send force close command if the BCSID matches
        if (container.getSequenceId() == replica.getSequenceId()) {
          sendCloseCommand(container, replica.getDatanodeDetails(), true);
          numCloseCmdsSent++;
          iterator.remove();
        }
      }
    }

    return numCloseCmdsSent;
  }

  /* HELPER METHODS FOR UNHEALTHY OVER AND UNDER REPLICATED CONTAINERS */

  /**
   * Process a container with more replicas than required where all replicas
   * are unhealthy.
   *
   * First try to close any replicas that are unhealthy due to pending
   * closure. Replicas that can be closed will become healthy and will not be
   * processed by this method.
   * If the container is closed, delete replicas with lower BCSIDs first.
   * If the container is not yet closed, delete replicas with origin node IDs
   * already represented by other replicas.
   */
  private void handleOverReplicatedAllUnhealthy(ContainerInfo container,
      List<ContainerReplica> replicas, int excess,
      ReplicationManagerReport report) {
    List<ContainerReplica> deleteCandidates =
        getUnhealthyDeletionCandidates(container, replicas);

    // Only unhealthy replicas which cannot be closed will remain eligible
    // for deletion, since this method is deleting unhealthy containers only.
    closeReplicasIfPossible(container, deleteCandidates);
    if (deleteCandidates.isEmpty()) {
      return;
    }

    if (excess > 0) {
      report.incrementAndSample(HealthState.OVER_REPLICATED,
          container.containerID());
      int replicationFactor = container.getReplicationFactor().getNumber();
      LOG.info("Container {} has all unhealthy replicas and is over " +
              "replicated. Expected replica count" +
              " is {}, but found {}.", container.getContainerID(),
          replicationFactor, replicationFactor + excess);
    }

    if (container.getState() == LifeCycleState.CLOSED) {
      // Prefer to delete unhealthy replicas with lower BCS IDs.
      // If the replica became unhealthy after the container was closed but
      // before the replica could be closed, it may have a smaller BCSID.
      deleteExcessLowestBcsIDs(container, deleteCandidates, excess);
    } else {
      // Container is not yet closed.
      // We only need to save the unhealthy replicas if they
      // represent unique origin node IDs. If recovering these replicas is
      // possible in the future they could be used to close the container.
      deleteExcessWithNonUniqueOriginNodeIDs(container,
          replicas, deleteCandidates, excess);
    }
  }

  /**
   * Processes container replicas when all replicas are unhealthy and there
   * are fewer than the required number of replicas.
   *
   * If any of these replicas unhealthy because they are pending closure and
   * they can be closed, close them to create a healthy replica that can be
   * replicated.
   * If none of the replicas can be closed, use one of the unhealthy replicas
   * to restore replica count while satisfying topology requirements.
   */
  private void handleUnderReplicatedAllUnhealthy(ContainerInfo container,
      List<ContainerReplica> replicas, ContainerPlacementStatus placementStatus,
      int additionalReplicasNeeded, ReplicationManagerReport report) {

    report.incrementAndSample(HealthState.UNDER_REPLICATED,
        container.containerID());
    int numCloseCmdsSent = closeReplicasIfPossible(container, replicas);
    // Only replicate unhealthy containers if none of the unhealthy replicas
    // could be closed. If we sent a close command to an unhealthy replica,
    // we should wait for that to complete and replicate it when it becomes
    // healthy on a future iteration.
    if (numCloseCmdsSent == 0) {
      LOG.info("Container {} is under replicated missing {} replicas with all" +
          " replicas unhealthy. Copying unhealthy replicas.",
          container.getContainerID(), additionalReplicasNeeded);
      // TODO Datanodes currently shuffle sources, so we cannot prioritize
      //  some replicas based on BCSID or origin node ID.
      replicateAnyWithTopology(container,
          getReplicationSources(container, replicas), placementStatus,
          additionalReplicasNeeded);
    }
  }

  /* HELPER METHODS FOR ALL OVER AND UNDER REPLICATED CONTAINERS */

  /**
   * Deletes the first {@code excess} replicas from {@code deleteCandidates}.
   * Replicas whose datanode operation state is not IN_SERVICE will be skipped.
   */
  private void deleteExcess(ContainerInfo container,
      List<ContainerReplica> deleteCandidates, int excess) {
    // Replica which are maintenance or decommissioned are not eligible to
    // be removed, as they do not count toward over-replication and they
    // also may not be available
    deleteCandidates.removeIf(r ->
        r.getDatanodeDetails().getPersistedOpState() !=
            NodeOperationalState.IN_SERVICE);

    deleteCandidates.stream().limit(excess).forEach(r ->
        sendDeleteCommand(container, r.getDatanodeDetails(), true));
  }

  /**
   * remove execess replicas if needed, replicationFactor and placement policy
   * will be take into consideration.
   *
   * @param excess the excess number after subtracting replicationFactor
   * @param container ContainerInfo
   * @param eligibleReplicas An list of replicas, which may have excess replicas
   */
  private void deleteExcessWithTopology(int excess,
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

  private void deleteExcessWithNonUniqueOriginNodeIDs(ContainerInfo container,
      List<ContainerReplica> allReplicas,
      List<ContainerReplica> deleteCandidates, int excess) {
    // Remove delete candidates whose origin node ID is not already covered
    // by an existing replica.
    // TODO topology handling must be improved to make an optimal
    //  choice as to which replica to keep.

    // Gather the origin node IDs of replicas which are not candidates for
    // deletion.
    Set<UUID> existingOriginNodeIDs = allReplicas.stream()
        .filter(r -> !deleteCandidates.contains(r))
        .map(ContainerReplica::getOriginDatanodeId)
        .collect(Collectors.toSet());

    List<ContainerReplica> nonUniqueDeleteCandidates = new ArrayList<>();
    for (ContainerReplica replica: deleteCandidates) {
      if (existingOriginNodeIDs.contains(replica.getOriginDatanodeId())) {
        nonUniqueDeleteCandidates.add(replica);
      } else {
        // Spare this replica with this new origin node ID from deletion.
        // delete candidates seen later in the loop with this same origin
        // node ID can be deleted.
        existingOriginNodeIDs.add(replica.getOriginDatanodeId());
      }
    }

    if (LOG.isDebugEnabled() && nonUniqueDeleteCandidates.size() < excess) {
      LOG.debug("Unable to delete {} excess replicas of container {}. Only {}" +
          " replicas can be deleted to preserve unique origin node IDs for " +
          "this unclosed container.", excess, container.getContainerID(),
          nonUniqueDeleteCandidates.size());
    }
    deleteExcess(container, nonUniqueDeleteCandidates, excess);
  }

  /**
   * Delete {@code excess} replicas from {@code deleteCandidates}, deleting
   * those with lowest BCSIDs first.
   */
  private void deleteExcessLowestBcsIDs(ContainerInfo container,
      List<ContainerReplica> deleteCandidates, int excess) {
    // Sort containers with lowest BCSID first. These will be the first ones
    // deleted.
    deleteCandidates.sort(
        Comparator.comparingLong(ContainerReplica::getSequenceId).reversed());
    deleteExcess(container, deleteCandidates, excess);
  }

  /**
   * Choose {@code additionalReplicasNeeded} datanodes to make copies of some
   * of the container replicas to restore replication factor or satisfy
   * topology requirements.
   */
  private void replicateAnyWithTopology(ContainerInfo container,
      List<ContainerReplica> replicas,
      ContainerPlacementStatus placementStatus, int additionalReplicasNeeded) {
    try {
      final ContainerID id = container.containerID();

      final List<DatanodeDetails> sourceDNs = replicas.stream()
          .map(ContainerReplica::getDatanodeDetails)
          .collect(Collectors.toList());
      final List<DatanodeDetails> replicationInFlight
          = inflightReplication.getDatanodeDetails(id);
      if (sourceDNs.size() > 0) {
        final int replicationFactor = container
            .getReplicationConfig().getRequiredNodes();
        // Want to check if the container is mis-replicated after considering
        // inflight add and delete.
        // Create a new list from source (healthy replicas minus pending delete)
        List<DatanodeDetails> targetReplicas = new ArrayList<>(sourceDNs);
        // Then add any pending additions
        targetReplicas.addAll(replicationInFlight);
        final ContainerPlacementStatus inFlightplacementStatus =
            containerPlacement.validateContainerPlacement(
                targetReplicas, replicationFactor);
        final int misRepDelta = inFlightplacementStatus.misReplicationCount();
        final int replicasNeeded =
            Math.max(additionalReplicasNeeded, misRepDelta);
        if (replicasNeeded <= 0) {
          LOG.debug("Container {} meets replication requirement with " +
              "inflight replicas", id);
          return;
        }

        // We should ensure that the target datanode has enough space
        // for a complete container to be created, but since the container
        // size may be changed smaller than origin, we should be defensive.
        final long dataSizeRequired = Math.max(container.getUsedBytes(),
            currentContainerSize);
        final List<DatanodeDetails> excludeList = replicas.stream()
                .filter(r -> !r.getDatanodeDetails().isDecommissioned())
            .map(ContainerReplica::getDatanodeDetails)
            .collect(Collectors.toList());
        excludeList.addAll(replicationInFlight);
        final List<DatanodeDetails> selectedDatanodes = containerPlacement
            .chooseDatanodes(excludeList, null, replicasNeeded,
                0, dataSizeRequired);
        if (additionalReplicasNeeded > 0) {
          LOG.info("Container {} is under replicated. Expected replica count" +
                  " is {}, but found {}.", id, replicationFactor,
              replicationFactor - additionalReplicasNeeded);
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
        if (additionalReplicasNeeded > 0 || newMisRepDelta < misRepDelta) {
          // Only create new replicas if we are missing a replicas or
          // the number of pending mis-replication has improved. No point in
          // creating new replicas for mis-replicated containers unless it
          // improves things.
          for (DatanodeDetails datanode : selectedDatanodes) {
            sendReplicateCommand(container, datanode, sourceDNs);
          }
        } else {
          LOG.warn("Container {} is mis-replicated, requiring {} additional " +
                  "replicas. After selecting new nodes, mis-replication has" +
                  "not improved. No additional replicas will be scheduled",
              id, misRepDelta);
        }
      } else {
        LOG.warn("Cannot replicate container {}, no healthy datanodes with " +
                "replica found.",
            container.containerID());
      }
    } catch (IOException | IllegalStateException ex) {
      LOG.warn("Exception while replicating container {}.",
          container.getContainerID(), ex);
    }
  }

  private void closeEmptyContainer(ContainerInfo containerInfo) {
    /*
     * We should wait for sometime before moving the container to CLOSED state.
     * This will give enough time for Datanodes to report the container,
     * in cases where the container creation was successful on Datanodes.
     *
     * Should we have a separate configuration for this wait time?
     * For now, we are using ReplicationManagerThread Interval * 5 as the wait
     * time.
     */

    final Duration waitTime = rmConf.getInterval().multipliedBy(5);
    final Instant closingTime = containerInfo.getStateEnterTime();

    try {
      if (clock.instant().isAfter(closingTime.plus(waitTime))) {
        containerManager.updateContainerState(containerInfo.containerID(),
            HddsProtos.LifeCycleEvent.CLOSE);
      }
    } catch (IOException | InvalidStateTransitionException |
             TimeoutException e) {
      LOG.error("Failed to CLOSE the container {}",
          containerInfo.containerID(), e);
    }
  }
}
