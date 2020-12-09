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
package org.apache.hadoop.ozone.container.common.statemachine;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.protobuf.Descriptors.Descriptor;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.InitDatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.RunningDatanodeState;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlockCommandStatus.DeleteBlockCommandStatusBuilder;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;

import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessage;
import static java.lang.Math.min;
import org.apache.commons.collections.CollectionUtils;

import static org.apache.hadoop.hdds.utils.HddsServerUtil.getLogWarnInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmHeartbeatInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Current Context of State Machine.
 */
public class StateContext {

  @VisibleForTesting
  final static String CONTAINER_REPORTS_PROTO_NAME =
      ContainerReportsProto.getDescriptor().getFullName();
  @VisibleForTesting
  final static String NODE_REPORT_PROTO_NAME =
      NodeReportProto.getDescriptor().getFullName();
  @VisibleForTesting
  final static String PIPELINE_REPORTS_PROTO_NAME =
      PipelineReportsProto.getDescriptor().getFullName();
  @VisibleForTesting
  final static String COMMAND_STATUS_REPORTS_PROTO_NAME =
      CommandStatusReportsProto.getDescriptor().getFullName();
  @VisibleForTesting
  final static String INCREMENTAL_CONTAINER_REPORT_PROTO_NAME =
      IncrementalContainerReportProto.getDescriptor().getFullName();
  // Accepted types of reports that can be queued to incrementalReportsQueue
  private final static Set<String> ACCEPTED_INCREMENTAL_REPORT_TYPE_SET =
      Sets.newHashSet(COMMAND_STATUS_REPORTS_PROTO_NAME,
          INCREMENTAL_CONTAINER_REPORT_PROTO_NAME);

  static final Logger LOG =
      LoggerFactory.getLogger(StateContext.class);
  private final Queue<SCMCommand> commandQueue;
  private final Map<Long, CommandStatus> cmdStatusMap;
  private final Lock lock;
  private final DatanodeStateMachine parent;
  private final AtomicLong stateExecutionCount;
  private final ConfigurationSource conf;
  private final Set<InetSocketAddress> endpoints;
  // Only the latest full report of each type is kept
  private final AtomicReference<GeneratedMessage> containerReports;
  private final AtomicReference<GeneratedMessage> nodeReport;
  private final AtomicReference<GeneratedMessage> pipelineReports;
  // Incremental reports are queued in the map below
  private final Map<InetSocketAddress, List<GeneratedMessage>>
      incrementalReportsQueue;
  private final Map<InetSocketAddress, Queue<ContainerAction>> containerActions;
  private final Map<InetSocketAddress, Queue<PipelineAction>> pipelineActions;
  private DatanodeStateMachine.DatanodeStates state;
  private boolean shutdownOnError = false;
  private boolean shutdownGracefully = false;
  private final AtomicLong threadPoolNotAvailableCount;

  /**
   * Starting with a 2 sec heartbeat frequency which will be updated to the
   * real HB frequency after scm registration. With this method the
   * initial registration could be significant faster.
   */
  private AtomicLong heartbeatFrequency = new AtomicLong(2000);

  /**
   * Constructs a StateContext.
   *
   * @param conf   - Configration
   * @param state  - State
   * @param parent Parent State Machine
   */
  public StateContext(ConfigurationSource conf,
      DatanodeStateMachine.DatanodeStates
          state, DatanodeStateMachine parent) {
    this.conf = conf;
    this.state = state;
    this.parent = parent;
    commandQueue = new LinkedList<>();
    cmdStatusMap = new ConcurrentHashMap<>();
    incrementalReportsQueue = new HashMap<>();
    containerReports = new AtomicReference<>();
    nodeReport = new AtomicReference<>();
    pipelineReports = new AtomicReference<>();
    endpoints = new HashSet<>();
    containerActions = new HashMap<>();
    pipelineActions = new HashMap<>();
    lock = new ReentrantLock();
    stateExecutionCount = new AtomicLong(0);
    threadPoolNotAvailableCount = new AtomicLong(0);
  }

  /**
   * Returns the ContainerStateMachine class that holds this state.
   *
   * @return ContainerStateMachine.
   */
  public DatanodeStateMachine getParent() {
    return parent;
  }

  /**
   * Returns true if we are entering a new state.
   *
   * @return boolean
   */
  boolean isEntering() {
    return stateExecutionCount.get() == 0;
  }

  /**
   * Returns true if we are exiting from the current state.
   *
   * @param newState - newState.
   * @return boolean
   */
  boolean isExiting(DatanodeStateMachine.DatanodeStates newState) {
    boolean isExiting = state != newState && stateExecutionCount.get() > 0;
    if(isExiting) {
      stateExecutionCount.set(0);
    }
    return isExiting;
  }

  /**
   * Returns the current state the machine is in.
   *
   * @return state.
   */
  public DatanodeStateMachine.DatanodeStates getState() {
    return state;
  }

  /**
   * Sets the current state of the machine.
   *
   * @param state state.
   */
  public void setState(DatanodeStateMachine.DatanodeStates state) {
    if (this.state != state) {
      if (this.state.isTransitionAllowedTo(state)) {
        this.state = state;
      } else {
        LOG.warn("Ignore disallowed transition from {} to {}",
            this.state, state);
      }
    }
  }

  /**
   * Sets the shutdownOnError. This method needs to be called when we
   * set DatanodeState to SHUTDOWN when executing a task of a DatanodeState.
   */
  private void setShutdownOnError() {
    this.shutdownOnError = true;
  }

  /**
   * Indicate to the StateContext that StateMachine shutdown was called.
   */
  void setShutdownGracefully() {
    this.shutdownGracefully = true;
  }

  /**
   * Get shutdownStateMachine.
   * @return boolean
   */
  public boolean getShutdownOnError() {
    return shutdownOnError;
  }

  /**
   * Adds the report to report queue.
   *
   * @param report report to be added
   */
  public void addReport(GeneratedMessage report) {
    if (report == null) {
      return;
    }
    final Descriptor descriptor = report.getDescriptorForType();
    Preconditions.checkState(descriptor != null);
    final String reportType = descriptor.getFullName();
    Preconditions.checkState(reportType != null);
    for (InetSocketAddress endpoint : endpoints) {
      if (reportType.equals(CONTAINER_REPORTS_PROTO_NAME)) {
        containerReports.set(report);
      } else if (reportType.equals(NODE_REPORT_PROTO_NAME)) {
        nodeReport.set(report);
      } else if (reportType.equals(PIPELINE_REPORTS_PROTO_NAME)) {
        pipelineReports.set(report);
      } else if (ACCEPTED_INCREMENTAL_REPORT_TYPE_SET.contains(reportType)) {
        synchronized (incrementalReportsQueue) {
          incrementalReportsQueue.get(endpoint).add(report);
        }
      } else {
        throw new IllegalArgumentException(
            "Unidentified report message type: " + reportType);
      }
    }
  }

  /**
   * Adds the reports which could not be sent by heartbeat back to the
   * reports list.
   *
   * @param reportsToPutBack list of reports which failed to be sent by
   *                         heartbeat.
   */
  public void putBackReports(List<GeneratedMessage> reportsToPutBack,
                             InetSocketAddress endpoint) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("endpoint: {}, size of reportsToPutBack: {}",
          endpoint, reportsToPutBack.size());
    }
    // We don't expect too much reports to be put back
    for (GeneratedMessage report : reportsToPutBack) {
      final Descriptor descriptor = report.getDescriptorForType();
      Preconditions.checkState(descriptor != null);
      final String reportType = descriptor.getFullName();
      Preconditions.checkState(reportType != null);
      if (!ACCEPTED_INCREMENTAL_REPORT_TYPE_SET.contains(reportType)) {
        throw new IllegalArgumentException(
            "Unaccepted report message type: " + reportType);
      }
    }
    synchronized (incrementalReportsQueue) {
      if (incrementalReportsQueue.containsKey(endpoint)){
        incrementalReportsQueue.get(endpoint).addAll(0, reportsToPutBack);
      }
    }
  }

  /**
   * Returns all the available reports from the report queue, or empty list if
   * the queue is empty.
   *
   * @return List of reports
   */
  public List<GeneratedMessage> getAllAvailableReports(
      InetSocketAddress endpoint) {
    return getReports(endpoint, Integer.MAX_VALUE);
  }

  List<GeneratedMessage> getIncrementalReports(
      InetSocketAddress endpoint, int maxLimit) {
    List<GeneratedMessage> reportsToReturn = new LinkedList<>();
    synchronized (incrementalReportsQueue) {
      List<GeneratedMessage> reportsForEndpoint =
          incrementalReportsQueue.get(endpoint);
      if (reportsForEndpoint != null) {
        List<GeneratedMessage> tempList = reportsForEndpoint.subList(
            0, min(reportsForEndpoint.size(), maxLimit));
        reportsToReturn.addAll(tempList);
        tempList.clear();
      }
    }
    return reportsToReturn;
  }

  /**
   * Returns available reports from the report queue with a max limit on
   * list size, or empty list if the queue is empty.
   *
   * @return List of reports
   */
  public List<GeneratedMessage> getReports(InetSocketAddress endpoint,
                                           int maxLimit) {
    List<GeneratedMessage> reportsToReturn =
        getIncrementalReports(endpoint, maxLimit);
    GeneratedMessage report = containerReports.get();
    if (report != null) {
      reportsToReturn.add(report);
    }
    report = nodeReport.get();
    if (report != null) {
      reportsToReturn.add(report);
    }
    report = pipelineReports.get();
    if (report != null) {
      reportsToReturn.add(report);
    }
    return reportsToReturn;
  }


  /**
   * Adds the ContainerAction to ContainerAction queue.
   *
   * @param containerAction ContainerAction to be added
   */
  public void addContainerAction(ContainerAction containerAction) {
    synchronized (containerActions) {
      for (InetSocketAddress endpoint : endpoints) {
        containerActions.get(endpoint).add(containerAction);
      }
    }
  }

  /**
   * Add ContainerAction to ContainerAction queue if it's not present.
   *
   * @param containerAction ContainerAction to be added
   */
  public void addContainerActionIfAbsent(ContainerAction containerAction) {
    synchronized (containerActions) {
      for (InetSocketAddress endpoint : endpoints) {
        if (!containerActions.get(endpoint).contains(containerAction)) {
          containerActions.get(endpoint).add(containerAction);
        }
      }
    }
  }

  /**
   * Returns all the pending ContainerActions from the ContainerAction queue,
   * or empty list if the queue is empty.
   *
   * @return {@literal List<ContainerAction>}
   */
  public List<ContainerAction> getAllPendingContainerActions(
      InetSocketAddress endpoint) {
    return getPendingContainerAction(endpoint, Integer.MAX_VALUE);
  }

  /**
   * Returns pending ContainerActions from the ContainerAction queue with a
   * max limit on list size, or empty list if the queue is empty.
   *
   * @return {@literal List<ContainerAction>}
   */
  public List<ContainerAction> getPendingContainerAction(
      InetSocketAddress endpoint,
      int maxLimit) {
    List<ContainerAction> containerActionList = new ArrayList<>();
    synchronized (containerActions) {
      if (!containerActions.isEmpty() &&
          CollectionUtils.isNotEmpty(containerActions.get(endpoint))) {
        Queue<ContainerAction> actions = containerActions.get(endpoint);
        int size = actions.size();
        int limit = size > maxLimit ? maxLimit : size;
        for (int count = 0; count < limit; count++) {
          // we need to remove the action from the containerAction queue
          // as well
          ContainerAction action = actions.poll();
          Preconditions.checkNotNull(action);
          containerActionList.add(action);
        }
      }
      return containerActionList;
    }
  }

  /**
   * Add PipelineAction to PipelineAction queue if it's not present.
   *
   * @param pipelineAction PipelineAction to be added
   */
  public void addPipelineActionIfAbsent(PipelineAction pipelineAction) {
    synchronized (pipelineActions) {
      /**
       * If pipelineAction queue already contains entry for the pipeline id
       * with same action, we should just return.
       * Note: We should not use pipelineActions.contains(pipelineAction) here
       * as, pipelineAction has a msg string. So even if two msgs differ though
       * action remains same on the given pipeline, it will end up adding it
       * multiple times here.
       */
      for (InetSocketAddress endpoint : endpoints) {
        Queue<PipelineAction> actionsForEndpoint =
            this.pipelineActions.get(endpoint);
        for (PipelineAction pipelineActionIter : actionsForEndpoint) {
          if (pipelineActionIter.getAction() == pipelineAction.getAction()
              && pipelineActionIter.hasClosePipeline() && pipelineAction
              .hasClosePipeline()
              && pipelineActionIter.getClosePipeline().getPipelineID()
              .equals(pipelineAction.getClosePipeline().getPipelineID())) {
            break;
          }
        }
        actionsForEndpoint.add(pipelineAction);
      }
    }
  }

  /**
   * Returns pending PipelineActions from the PipelineAction queue with a
   * max limit on list size, or empty list if the queue is empty.
   *
   * @return {@literal List<ContainerAction>}
   */
  public List<PipelineAction> getPendingPipelineAction(
      InetSocketAddress endpoint,
      int maxLimit) {
    List<PipelineAction> pipelineActionList = new ArrayList<>();
    synchronized (pipelineActions) {
      if (!pipelineActions.isEmpty() &&
          CollectionUtils.isNotEmpty(pipelineActions.get(endpoint))) {
        Queue<PipelineAction> actionsForEndpoint =
            this.pipelineActions.get(endpoint);
        int size = actionsForEndpoint.size();
        int limit = size > maxLimit ? maxLimit : size;
        for (int count = 0; count < limit; count++) {
          pipelineActionList.add(actionsForEndpoint.poll());
        }
      }
      return pipelineActionList;
    }
  }

  /**
   * Returns the next task to get executed by the datanode state machine.
   * @return A callable that will be executed by the
   * {@link DatanodeStateMachine}
   */
  @SuppressWarnings("unchecked")
  public DatanodeState<DatanodeStateMachine.DatanodeStates> getTask() {
    switch (this.state) {
    case INIT:
      return new InitDatanodeState(this.conf, parent.getConnectionManager(),
          this);
    case RUNNING:
      return new RunningDatanodeState(this.conf, parent.getConnectionManager(),
          this);
    case SHUTDOWN:
      return null;
    default:
      throw new IllegalArgumentException("Not Implemented yet.");
    }
  }

  @VisibleForTesting
  public boolean isThreadPoolAvailable(ExecutorService executor) {
    if (!(executor instanceof ThreadPoolExecutor)) {
      return true;
    }

    ThreadPoolExecutor ex = (ThreadPoolExecutor) executor;
    if (ex.getQueue().size() == 0) {
      return true;
    }

    return false;
  }

  /**
   * Executes the required state function.
   *
   * @param service - Executor Service
   * @param time    - seconds to wait
   * @param unit    - Seconds.
   * @throws InterruptedException
   * @throws ExecutionException
   * @throws TimeoutException
   */
  public void execute(ExecutorService service, long time, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    stateExecutionCount.incrementAndGet();
    DatanodeState<DatanodeStateMachine.DatanodeStates> task = getTask();

    // Adding not null check, in a case where datanode is still starting up, but
    // we called stop DatanodeStateMachine, this sets state to SHUTDOWN, and
    // there is a chance of getting task as null.
    if (task != null) {
      if (this.isEntering()) {
        task.onEnter();
      }

      if (!isThreadPoolAvailable(service)) {
        long count = threadPoolNotAvailableCount.getAndIncrement();
        if (count % getLogWarnInterval(conf) == 0) {
          LOG.warn("No available thread in pool for past {} seconds.",
              unit.toSeconds(time) * (count + 1));
        }
        return;
      }

      threadPoolNotAvailableCount.set(0);
      task.execute(service);
      DatanodeStateMachine.DatanodeStates newState = task.await(time, unit);
      if (this.state != newState) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Task {} executed, state transited from {} to {}",
              task.getClass().getSimpleName(), this.state, newState);
        }
        if (isExiting(newState)) {
          task.onExit();
        }
        this.setState(newState);
      }

      if (!shutdownGracefully &&
          this.state == DatanodeStateMachine.DatanodeStates.SHUTDOWN) {
        LOG.error("Critical error occurred in StateMachine, setting " +
            "shutDownMachine");
        // When some exception occurred, set shutdownStateMachine to true, so
        // that we can terminate the datanode.
        setShutdownOnError();
      }
    }
  }

  /**
   * Returns the next command or null if it is empty.
   *
   * @return SCMCommand or Null.
   */
  public SCMCommand getNextCommand() {
    lock.lock();
    try {
      return commandQueue.poll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Adds a command to the State Machine queue.
   *
   * @param command - SCMCommand.
   */
  public void addCommand(SCMCommand command) {
    lock.lock();
    try {
      commandQueue.add(command);
    } finally {
      lock.unlock();
    }
    this.addCmdStatus(command);
  }

  /**
   * Returns the count of the Execution.
   * @return long
   */
  public long getExecutionCount() {
    return stateExecutionCount.get();
  }

  /**
   * Returns the next {@link CommandStatus} or null if it is empty.
   *
   * @return {@link CommandStatus} or Null.
   */
  public CommandStatus getCmdStatus(Long key) {
    return cmdStatusMap.get(key);
  }

  /**
   * Adds a {@link CommandStatus} to the State Machine.
   *
   * @param status - {@link CommandStatus}.
   */
  public void addCmdStatus(Long key, CommandStatus status) {
    cmdStatusMap.put(key, status);
  }

  /**
   * Adds a {@link CommandStatus} to the State Machine for given SCMCommand.
   *
   * @param cmd - {@link SCMCommand}.
   */
  public void addCmdStatus(SCMCommand cmd) {
    if (cmd.getType() == SCMCommandProto.Type.deleteBlocksCommand) {
      addCmdStatus(cmd.getId(),
          DeleteBlockCommandStatusBuilder.newBuilder()
              .setCmdId(cmd.getId())
              .setStatus(Status.PENDING)
              .setType(cmd.getType())
              .build());
    }
  }

  /**
   * Get map holding all {@link CommandStatus} objects.
   *
   */
  public Map<Long, CommandStatus> getCommandStatusMap() {
    return cmdStatusMap;
  }

  /**
   * Updates status of a pending status command.
   * @param cmdId       command id
   * @param cmdStatusUpdater Consumer to update command status.
   * @return true if command status updated successfully else false.
   */
  public boolean updateCommandStatus(Long cmdId,
      Consumer<CommandStatus> cmdStatusUpdater) {
    if(cmdStatusMap.containsKey(cmdId)) {
      cmdStatusUpdater.accept(cmdStatusMap.get(cmdId));
      return true;
    }
    return false;
  }

  public void configureHeartbeatFrequency(){
    heartbeatFrequency.set(getScmHeartbeatInterval(conf));
  }

  /**
   * Return current heartbeat frequency in ms.
   */
  public long getHeartbeatFrequency() {
    return heartbeatFrequency.get();
  }

  public void addEndpoint(InetSocketAddress endpoint) {
    if (!endpoints.contains(endpoint)) {
      this.endpoints.add(endpoint);
      this.containerActions.put(endpoint, new LinkedList<>());
      this.pipelineActions.put(endpoint, new LinkedList<>());
      this.incrementalReportsQueue.put(endpoint, new LinkedList<>());
    }
  }

  @VisibleForTesting
  public GeneratedMessage getContainerReports() {
    return containerReports.get();
  }

  @VisibleForTesting
  public GeneratedMessage getNodeReport() {
    return nodeReport.get();
  }

  @VisibleForTesting
  public GeneratedMessage getPipelineReports() {
    return pipelineReports.get();
  }
}
