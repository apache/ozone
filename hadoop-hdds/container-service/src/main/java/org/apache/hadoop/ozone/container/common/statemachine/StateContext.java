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

package org.apache.hadoop.ozone.container.common.statemachine;

import static java.lang.Math.min;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getInitialReconHeartbeatInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getLogWarnInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getReconHeartbeatInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmHeartbeatInterval;
import static org.apache.hadoop.hdds.utils.HddsServerUtil.getScmInitialHeartbeatInterval;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatus.Status;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.CommandStatusReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.IncrementalContainerReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineAction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReport;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.PipelineReportsProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.ozone.container.common.statemachine.commandhandler.ClosePipelineCommandHandler;
import org.apache.hadoop.ozone.container.common.states.DatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.InitDatanodeState;
import org.apache.hadoop.ozone.container.common.states.datanode.RunningDatanodeState;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.CommandStatus;
import org.apache.hadoop.ozone.protocol.commands.DeleteBlockCommandStatus.DeleteBlockCommandStatusBuilder;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Current Context of State Machine.
 */
public class StateContext {

  @VisibleForTesting
  static final String CONTAINER_REPORTS_PROTO_NAME =
      ContainerReportsProto.getDescriptor().getFullName();
  @VisibleForTesting
  static final String NODE_REPORT_PROTO_NAME =
      NodeReportProto.getDescriptor().getFullName();
  @VisibleForTesting
  static final String PIPELINE_REPORTS_PROTO_NAME =
      PipelineReportsProto.getDescriptor().getFullName();
  @VisibleForTesting
  static final String COMMAND_STATUS_REPORTS_PROTO_NAME =
      CommandStatusReportsProto.getDescriptor().getFullName();
  @VisibleForTesting
  static final String INCREMENTAL_CONTAINER_REPORT_PROTO_NAME =
      IncrementalContainerReportProto.getDescriptor().getFullName();

  static final Logger LOG =
      LoggerFactory.getLogger(StateContext.class);
  private final Queue<SCMCommand<?>> commandQueue;
  private final Map<Long, CommandStatus> cmdStatusMap;
  private final Lock lock;
  private final DatanodeStateMachine parentDatanodeStateMachine;
  private final AtomicLong stateExecutionCount;
  private final ConfigurationSource conf;
  private final Set<InetSocketAddress> endpoints;
  // Only the latest full report of each type is kept
  private final AtomicReference<Message> containerReports;
  private final AtomicReference<Message> nodeReport;
  private final AtomicReference<Message> pipelineReports;
  // Incremental reports are queued in the map below
  private final Map<InetSocketAddress, List<Message>>
      incrementalReportsQueue;
  private final Map<InetSocketAddress, Queue<ContainerAction>> containerActions;
  private final Map<InetSocketAddress, PipelineActionMap> pipelineActions;
  private DatanodeStateMachine.DatanodeStates state;
  private boolean shutdownOnError = false;
  private boolean shutdownGracefully = false;
  private final AtomicLong threadPoolNotAvailableCount;
  private final AtomicLong lastHeartbeatSent;
  // Endpoint -> ReportType -> Boolean of whether the full report should be
  //  queued in getFullReports call.
  private final Map<InetSocketAddress,
      Map<String, AtomicBoolean>> isFullReportReadyToBeSent;
  // List of supported full report types.
  private final List<String> fullReportTypeList;
  // ReportType -> Report.
  private final Map<String, AtomicReference<Message>> type2Reports;

  /**
   * term of latest leader SCM, extract from SCMCommand.
   *
   * Only leader SCM (both latest and stale) can send out SCMCommand,
   * which will save its term in SCMCommand. Since latest leader SCM
   * always has the highest term, term can be used to detect SCMCommand
   * from stale leader SCM.
   *
   * For non-HA mode, term of SCMCommand will be 0.
   */
  private OptionalLong termOfLeaderSCM = OptionalLong.empty();

  /**
   * Starting with a 2 sec heartbeat frequency which will be updated to the
   * real HB frequency after scm registration. With this method the
   * initial registration could be significant faster.
   */
  private final AtomicLong heartbeatFrequency;

  private final AtomicLong reconHeartbeatFrequency;

  private final int maxCommandQueueLimit;

  private final String threadNamePrefix;

  private RunningDatanodeState runningDatanodeState;

  /**
   * Constructs a StateContext.
   *
   * @param conf   - Configuration
   * @param state  - State
   * @param parent Parent State Machine
   */
  public StateContext(ConfigurationSource conf,
      DatanodeStateMachine.DatanodeStates
          state, DatanodeStateMachine parent, String threadNamePrefix) {
    this.conf = conf;
    DatanodeConfiguration dnConf =
        conf.getObject(DatanodeConfiguration.class);
    maxCommandQueueLimit = dnConf.getCommandQueueLimit();
    this.state = state;
    this.parentDatanodeStateMachine = parent;
    commandQueue = new LinkedList<>();
    cmdStatusMap = new ConcurrentHashMap<>();
    // ConcurrentHashMap because addEndpoint / removeEndpoint /
    // migrateEndpoint mutate this map's KEYSET (put/remove) without
    // holding the same monitor that producers (addIncrementalReport,
    // putBackReports, getIncrementalReports) take. The producers'
    // `synchronized(incrementalReportsQueue)` blocks still guard
    // operations on the inner LinkedList values across threads --
    // CHM only fixes the map-structure race. The metric readers
    // (getIncrementalReportQueueSize) iterate the entrySet without
    // any monitor; weakly-consistent CHM iteration is safe here.
    incrementalReportsQueue = new ConcurrentHashMap<>();
    containerReports = new AtomicReference<>();
    nodeReport = new AtomicReference<>();
    pipelineReports = new AtomicReference<>();
    // CopyOnWriteArraySet so producers in addContainerAction /
    // addIncrementalReport / addPipelineActionIfAbsent can iterate
    // endpoints lock-free and a concurrent migrateEndpoint() (fired
    // from the heartbeat thread on DNS-refresh recovery) cannot raise
    // ConcurrentModificationException. Endpoint membership changes
    // are infrequent (startup add, reconfig, refresh swap) so the
    // copy-on-write cost is negligible.
    endpoints = new CopyOnWriteArraySet<>();
    // ConcurrentHashMap for the same reason as incrementalReportsQueue
    // above: addEndpoint / removeEndpoint / migrateEndpoint mutate the
    // map structure without taking the producer's monitor. Inner Queue
    // operations are still guarded by `synchronized(containerActions)`
    // in the producers.
    containerActions = new ConcurrentHashMap<>();
    pipelineActions = new ConcurrentHashMap<>();
    lock = new ReentrantLock();
    stateExecutionCount = new AtomicLong(0);
    threadPoolNotAvailableCount = new AtomicLong(0);
    lastHeartbeatSent = new AtomicLong(0);
    // ConcurrentHashMap because reads from getFullReports /
    // refreshFullReport / migrateEndpoint can race against startup
    // addEndpoint and against migrateEndpoint's putIfAbsent. Plain
    // HashMap was unsafe even pre-PR; migrateEndpoint promoted that
    // latent race into a regular occurrence under K8s pod churn.
    isFullReportReadyToBeSent = new ConcurrentHashMap<>();
    fullReportTypeList = new ArrayList<>();
    type2Reports = new HashMap<>();
    this.threadNamePrefix = threadNamePrefix;
    heartbeatFrequency = new AtomicLong(getScmInitialHeartbeatInterval(conf));
    reconHeartbeatFrequency = new AtomicLong(getInitialReconHeartbeatInterval(conf));
    initReportTypeCollection();
  }

  /**
   * init related ReportType Collections.
   */
  private void initReportTypeCollection() {
    fullReportTypeList.add(CONTAINER_REPORTS_PROTO_NAME);
    type2Reports.put(CONTAINER_REPORTS_PROTO_NAME, containerReports);
    fullReportTypeList.add(NODE_REPORT_PROTO_NAME);
    type2Reports.put(NODE_REPORT_PROTO_NAME, nodeReport);
    fullReportTypeList.add(PIPELINE_REPORTS_PROTO_NAME);
    type2Reports.put(PIPELINE_REPORTS_PROTO_NAME, pipelineReports);
  }

  /**
   * Returns the DatanodeStateMachine class that holds this state.
   *
   * @return DatanodeStateMachine.
   */
  public DatanodeStateMachine getParent() {
    return parentDatanodeStateMachine;
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
    if (isExiting) {
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
  public void addIncrementalReport(Message report) {
    if (report == null) {
      return;
    }
    final Descriptor descriptor = report.getDescriptorForType();
    Objects.requireNonNull(descriptor, "descriptor == null");
    final String reportType = descriptor.getFullName();
    Objects.requireNonNull(reportType, "reportType == null");
    // in some case, we want to add a fullReportType message
    // as an incremental message.
    // see XceiverServerRatis#sendPipelineReport
    synchronized (incrementalReportsQueue) {
      for (InetSocketAddress endpoint : endpoints) {
        // Same migration race shape as addPipelineActionIfAbsent: an
        // endpoint observed in the COW set may have just had its
        // queue removed by migrateEndpoint. Skip rather than NPE.
        List<Message> queue = incrementalReportsQueue.get(endpoint);
        if (queue != null) {
          queue.add(report);
        }
      }
    }
  }

  /**
   * refresh Full report.
   *
   * @param report report to be refreshed
   */
  public void refreshFullReport(Message report) {
    if (report == null) {
      return;
    }
    final Descriptor descriptor = report.getDescriptorForType();
    Objects.requireNonNull(descriptor, "descriptor == null");
    final String reportType = descriptor.getFullName();
    Objects.requireNonNull(reportType, "reportType == null");
    if (!fullReportTypeList.contains(reportType)) {
      throw new IllegalArgumentException(
          "not full report message type: " + reportType);
    }
    type2Reports.get(reportType).set(report);
    if (isFullReportReadyToBeSent != null) {
      for (Map<String, AtomicBoolean> mp : isFullReportReadyToBeSent.values()) {
        mp.get(reportType).set(true);
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
  public void putBackReports(List<Message> reportsToPutBack,
                             InetSocketAddress endpoint) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("endpoint: {}, size of reportsToPutBack: {}",
          endpoint, reportsToPutBack.size());
    }
    // We don't expect too much reports to be put back
    for (Message report : reportsToPutBack) {
      final Descriptor descriptor = report.getDescriptorForType();
      Objects.requireNonNull(descriptor, "descriptor == null");
      final String reportType = descriptor.getFullName();
      Objects.requireNonNull(reportType, "reportType == null");
    }
    synchronized (incrementalReportsQueue) {
      if (incrementalReportsQueue.containsKey(endpoint)) {
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
  public List<Message> getAllAvailableReports(
      InetSocketAddress endpoint
  ) {
    int maxLimit = Integer.MAX_VALUE;
    // TODO: It is highly unlikely that we will reach maxLimit for the number
    //       for the number of reports, specially as it does not apply to the
    //       number of entries in a report. But if maxLimit is hit, should a
    //       heartbeat be scheduled ASAP? Should full reports not included be
    //       dropped? Currently this code will keep the full reports not sent
    //       and include it in the next heartbeat.
    return getAllAvailableReportsUpToLimit(endpoint, maxLimit);
  }

  /**
   * Gets a point in time snapshot of all containers, any pending incremental
   * container reports (ICR) for containers will be included in this report
   * and this call will drop any pending ICRs.
   * @return Full Container Report
   */
  public ContainerReportsProto getFullContainerReportDiscardPendingICR()
      throws IOException {

    // Block ICRs from being generated
    synchronized (parentDatanodeStateMachine
        .getContainer()) {
      synchronized (incrementalReportsQueue) {
        for (Map.Entry<InetSocketAddress, List<Message>>
            entry : incrementalReportsQueue.entrySet()) {
          if (entry.getValue() != null) {
            entry.getValue().removeIf(
                generatedMessage ->
                    generatedMessage instanceof
                        IncrementalContainerReportProto);
          }
        }
      }
      return parentDatanodeStateMachine
          .getContainer()
          .getContainerSet()
          .getContainerReport();
    }
  }

  @VisibleForTesting
  List<Message> getAllAvailableReportsUpToLimit(
      InetSocketAddress endpoint,
      int limit) {
    List<Message> reports = getFullReports(endpoint, limit);
    List<Message> incrementalReports = getIncrementalReports(endpoint,
        limit - reports.size()); // get all (MAX_VALUE)
    reports.addAll(incrementalReports);
    return reports;
  }

  List<Message> getIncrementalReports(
      InetSocketAddress endpoint, int maxLimit) {
    List<Message> reportsToReturn = new LinkedList<>();
    synchronized (incrementalReportsQueue) {
      List<Message> reportsForEndpoint =
          incrementalReportsQueue.get(endpoint);
      if (reportsForEndpoint != null) {
        List<Message> tempList = reportsForEndpoint.subList(
            0, min(reportsForEndpoint.size(), maxLimit));
        reportsToReturn.addAll(tempList);
        tempList.clear();
      }
    }
    return reportsToReturn;
  }

  List<Message> getFullReports(
      InetSocketAddress endpoint, int maxLimit) {
    int count = 0;
    Map<String, AtomicBoolean> mp = isFullReportReadyToBeSent.get(endpoint);
    List<Message> fullReports = new LinkedList<>();
    if (null != mp) {
      for (Map.Entry<String, AtomicBoolean> kv : mp.entrySet()) {
        if (count == maxLimit) {
          break;
        }
        if (kv.getValue().get()) {
          String reportType = kv.getKey();
          final AtomicReference<Message> ref =
              type2Reports.get(reportType);
          if (ref == null) {
            throw new RuntimeException(reportType + " is not a valid full "
                + "report type!");
          }
          final Message msg = ref.get();
          if (msg != null) {
            fullReports.add(msg);
            // Mark the report as not ready to be sent, until another refresh.
            mp.get(reportType).set(false);
            count++;
          }
        }
      }
    }
    return fullReports;
  }

  /**
   * Adds the ContainerAction to ContainerAction queue.
   *
   * @param containerAction ContainerAction to be added
   */
  public void addContainerAction(ContainerAction containerAction) {
    synchronized (containerActions) {
      for (InetSocketAddress endpoint : endpoints) {
        Queue<ContainerAction> q = containerActions.get(endpoint);
        if (q != null) {
          q.add(containerAction);
        }
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
        Queue<ContainerAction> q = containerActions.get(endpoint);
        if (q != null && !q.contains(containerAction)) {
          q.add(containerAction);
        }
      }
    }
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
          ContainerAction action = actions.poll();
          Objects.requireNonNull(action, "action == null");
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
  public boolean addPipelineActionIfAbsent(PipelineAction pipelineAction) {
    // Put only if the pipeline id with the same action is absent.
    final PipelineKey key = new PipelineKey(pipelineAction);
    boolean added = false;
    for (InetSocketAddress endpoint : endpoints) {
      // Endpoints are migrated under the DNS-refresh path
      // (migrateEndpoint) which removes the old pipelineActions entry
      // before it removes the endpoint from this set. With
      // CopyOnWriteArraySet iteration semantics we may still observe
      // an endpoint whose pipelineActions entry was just removed --
      // skip rather than NPE.
      PipelineActionMap actions = pipelineActions.get(endpoint);
      if (actions == null) {
        continue;
      }
      added = actions.putIfAbsent(key, pipelineAction) || added;
    }
    return added;
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
    final PipelineActionMap map = pipelineActions.get(endpoint);
    if (map == null) {
      return Collections.emptyList();
    }
    final OzoneContainer ozoneContainer = parentDatanodeStateMachine.
        getContainer();
    if (ozoneContainer == null) {
      return Collections.emptyList();
    }
    final PipelineReportsProto reports = ozoneContainer.getPipelineReport();
    return map.getActions(reports.getPipelineReportList(), maxLimit);
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
      return new InitDatanodeState(this.conf,
          parentDatanodeStateMachine.getConnectionManager(),
          this);
    case RUNNING:
      if (runningDatanodeState == null) {
        runningDatanodeState = new RunningDatanodeState(this.conf,
            parentDatanodeStateMachine.getConnectionManager(), this);
      }
      return runningDatanodeState;
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
    if (ex.getQueue().isEmpty()) {
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
    if (task == null) {
      return;
    }

    try {
      if (this.isEntering()) {
        task.onEnter();
      }

      if (!isThreadPoolAvailable(service)) {
        long count = threadPoolNotAvailableCount.incrementAndGet();
        long unavailableTime = Time.monotonicNow() - lastHeartbeatSent.get();
        if (unavailableTime > time && count % getLogWarnInterval(conf) == 0) {
          LOG.warn("No available thread in pool for the past {} seconds " +
              "and {} times.", unit.toSeconds(unavailableTime), count);
        }
        return;
      }
      threadPoolNotAvailableCount.set(0);
      task.execute(service);
      lastHeartbeatSent.set(Time.monotonicNow());
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
    } finally {
      task.clear();
    }
  }

  /**
   * After startup, datanode needs detect latest leader SCM before handling
   * any SCMCommand, so that it won't be disturbed by stale leader SCM.
   *
   * The rule is: after majority SCMs are in HEARTBEAT state and has
   * heard from leader SCMs (commandQueue is not empty), datanode will init
   * termOfLeaderSCM with the max term found in commandQueue.
   *
   * The init process also works for non-HA mode. In that case, term of all
   * SCMCommands will be SCMContext.INVALID_TERM.
   */
  private void initTermOfLeaderSCM() {
    // only init once
    if (termOfLeaderSCM.isPresent()) {
      return;
    }

    AtomicInteger scmNum = new AtomicInteger(0);
    AtomicInteger activeScmNum = new AtomicInteger(0);

    getParent().getConnectionManager().getValues()
        .forEach(endpoint -> {
          if (endpoint.isPassive()) {
            return;
          }
          scmNum.incrementAndGet();
          if (endpoint.getState()
              == EndpointStateMachine.EndPointStates.HEARTBEAT) {
            activeScmNum.incrementAndGet();
          }
        });

    // majority SCMs should be in HEARTBEAT state.
    if (activeScmNum.get() < scmNum.get() / 2 + 1) {
      return;
    }

    // if commandQueue is not empty, init termOfLeaderSCM
    // with the largest term found in commandQueue
    termOfLeaderSCM = commandQueue.stream()
        .mapToLong(SCMCommand::getTerm)
        .max();
  }

  /**
   * monotonically increase termOfLeaderSCM.
   * Always record the latest term that has seen.
   */
  private void updateTermOfLeaderSCM(SCMCommand<?> command) {
    updateTermOfLeaderSCM(command.getTerm());
  }

  public void updateTermOfLeaderSCM(final long newTerm) {
    if (!termOfLeaderSCM.isPresent()) {
      return;
    }

    final long currentTerm = termOfLeaderSCM.getAsLong();
    if (currentTerm < newTerm) {
      setTermOfLeaderSCM(newTerm);
    }
  }

  @VisibleForTesting
  public void setTermOfLeaderSCM(long term) {
    termOfLeaderSCM = OptionalLong.of(term);
  }

  public OptionalLong getTermOfLeaderSCM() {
    return termOfLeaderSCM;
  }

  /**
   * Returns the next command or null if it is empty.
   *
   * @return SCMCommand or Null.
   */
  public SCMCommand<?> getNextCommand() {
    lock.lock();
    try {
      initTermOfLeaderSCM();
      if (!termOfLeaderSCM.isPresent()) {
        return null;      // not ready yet
      }

      while (true) {
        SCMCommand<?> command = commandQueue.poll();
        if (command == null) {
          return null;
        }

        updateTermOfLeaderSCM(command);
        final long currentTerm = termOfLeaderSCM.getAsLong();
        if (command.getTerm() == currentTerm) {
          return command;
        }

        LOG.warn("Detect and drop a SCMCommand {} from stale leader SCM," +
            " stale term {}, latest term {}.",
            command, command.getTerm(), currentTerm);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Adds a command to the State Machine queue.
   *
   * @param command - SCMCommand.
   */
  public void addCommand(SCMCommand<?> command) {
    lock.lock();
    try {
      if (commandQueue.size() >= maxCommandQueueLimit) {
        LOG.warn("Ignore command as command queue crosses max limit {}.",
            maxCommandQueueLimit);
        return;
      }
      updateTermOfLeaderSCM(command);
      commandQueue.add(command);
    } finally {
      lock.unlock();
    }
    this.addCmdStatus(command);
  }

  public EnumCounters<SCMCommandProto.Type> getCommandQueueSummary() {
    EnumCounters<SCMCommandProto.Type> summary = new EnumCounters<>(SCMCommandProto.Type.class);
    lock.lock();
    try {
      for (SCMCommand<?> cmd : commandQueue) {
        summary.add(cmd.getType(), 1);
      }
    } finally {
      lock.unlock();
    }
    return summary;
  }

  public boolean isPipelineCloseInProgress(UUID pipelineID) {
    ClosePipelineCommandHandler handler = parentDatanodeStateMachine.getCommandDispatcher()
        .getClosePipelineCommandHandler();
    return handler.isPipelineCloseInProgress(pipelineID);
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
  public void addCmdStatus(SCMCommand<?> cmd) {
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
   * Updates the command status of a pending command.
   * @param cmdId       command id
   * @param cmdStatusUpdater Consumer to update command status.
   * @return true if command status updated successfully else if the command
   * associated with the command id does not exist in the context.
   */
  public boolean updateCommandStatus(Long cmdId,
      Consumer<CommandStatus> cmdStatusUpdater) {
    CommandStatus updatedCommandStatus = cmdStatusMap.computeIfPresent(cmdId,
        (key, value) -> {
          cmdStatusUpdater.accept(value);
          return value;
        }
    );
    return updatedCommandStatus != null;
  }

  public void configureHeartbeatFrequency() {
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
      this.pipelineActions.put(endpoint, new PipelineActionMap());
      this.incrementalReportsQueue.put(endpoint, new LinkedList<>());
      Map<String, AtomicBoolean> mp = new HashMap<>();
      fullReportTypeList.forEach(e -> {
        mp.putIfAbsent(e, new AtomicBoolean(true));
      });
      this.isFullReportReadyToBeSent.putIfAbsent(endpoint, mp);
      if (getQueueMetrics() != null) {
        getQueueMetrics().addEndpoint(endpoint);
      }
    }
  }

  public void removeEndpoint(InetSocketAddress endpoint) {
    this.endpoints.remove(endpoint);
    this.containerActions.remove(endpoint);
    this.pipelineActions.remove(endpoint);
    this.incrementalReportsQueue.remove(endpoint);
    this.isFullReportReadyToBeSent.remove(endpoint);
    if (getQueueMetrics() != null) {
      getQueueMetrics().removeEndpoint(endpoint);
    }
  }

  /**
   * Re-key all per-endpoint state from {@code oldEndpoint} to
   * {@code newEndpoint}, preserving any reports / actions already
   * queued for the old key. Used by the DNS-refresh-on-failure path
   * ({@code HeartbeatEndpointTask#maybeRefreshScmAddress}) so that an
   * SCM peer pod-IP change does not silently drop in-flight
   * incremental container reports, container actions, or pipeline
   * actions queued under the stale {@link InetSocketAddress}.
   * <p>
   * Concurrency / lock-order rules:
   * <ul>
   *   <li>{@link #endpoints} is a {@link CopyOnWriteArraySet} so
   *       producers iterating it (in {@link #addContainerAction},
   *       {@link #addIncrementalReport}, {@link #addPipelineActionIfAbsent})
   *       cannot CME against the {@code remove}+{@code add} pair below. </li>
   *   <li>{@link #incrementalReportsQueue} mutations take the same
   *       {@code synchronized(incrementalReportsQueue)} monitor that
   *       producers use. </li>
   *   <li>{@link #containerActions} mutations take the same
   *       {@code synchronized(containerActions)} monitor that
   *       producers use. </li>
   *   <li>{@link #pipelineActions} is {@link ConcurrentHashMap}; we
   *       guard the read-modify-write sequence with the per-key atomic
   *       {@code compute} call. </li>
   *   <li>{@link #isFullReportReadyToBeSent} is now a
   *       {@link ConcurrentHashMap} so its mutations are individually
   *       atomic and concurrent {@link #refreshFullReport} iterations
   *       are weakly-consistent rather than fail-fast. </li>
   * </ul>
   * Migration order: producers' queues first (so a producer racing in
   * during the migration finds the new endpoint key already populated),
   * then the {@code endpoints} set last (so any thread that observes
   * the new endpoint in {@code endpoints} also observes its queues
   * present). Each step is idempotent at the per-key level.
   * <p>
   * Collision case (the freshly-resolved IP collides with another
   * already-registered endpoint key): this method leaves the existing
   * collided entry untouched and only drops the old key's state. Two
   * distinct SCM peers' queues are NEVER merged; doing so would deliver
   * one peer's incremental reports / pipeline actions to a different
   * peer that did not request them.
   * <p>
   * If {@code oldEndpoint} was not registered, this is a no-op. The
   * new endpoint is NOT added in that case.
   *
   * @param oldEndpoint the InetSocketAddress under which queues are
   *                    currently keyed
   * @param newEndpoint the freshly-resolved InetSocketAddress that the
   *                    new EndpointStateMachine is bound to
   */
  public void migrateEndpoint(InetSocketAddress oldEndpoint,
                              InetSocketAddress newEndpoint) {
    if (oldEndpoint == null || newEndpoint == null
        || oldEndpoint.equals(newEndpoint)) {
      return;
    }
    if (!endpoints.contains(oldEndpoint)) {
      return;
    }
    final boolean newAlreadyRegistered = endpoints.contains(newEndpoint);

    // Invariant we must preserve at every observable point:
    //   "for every e in endpoints, queues[e] exists for all four
    //    per-endpoint maps."
    // Producers iterate `endpoints` lock-free (CopyOnWriteArraySet) and
    // then look up the per-key queue. If we removed a queue before
    // removing the endpoint from the set, a producer iterating between
    // the two operations would see the endpoint, fail to find a queue,
    // and silently drop its report. The producer-side null-skip we
    // added previously was a band-aid for exactly this race.
    //
    // To preserve the invariant, the ordering is:
    //   1. PUBLISH: install new-key queues alongside the old-key queues.
    //      During this step both keys are valid.
    //   2. SWITCH: add newEndpoint to the endpoints set; remove
    //      oldEndpoint from the endpoints set. After this step,
    //      producers iterating `endpoints` see only the new key (which
    //      has its queue from step 1).
    //   3. RETIRE: drop the old-key queues. By this point no producer
    //      iterating `endpoints` can reach the old key.
    //
    // Collision case (newEndpoint already a registered peer): we MUST
    // NOT install our incoming queues over the colliding peer's queues
    // (would deliver one peer's reports to another). On collision we
    // skip the publish step's "install" and the switch step's "add",
    // and only retire the old-key queues. Any old-key in-flight reports
    // are dropped (logged) -- the alternative of merging into a foreign
    // peer's queue is worse.

    // Step 1 (PUBLISH): install new-key queues. On collision, do not
    // touch newEndpoint's existing queues (foreign peer owns them).
    synchronized (incrementalReportsQueue) {
      if (!newAlreadyRegistered) {
        List<Message> oldQueue = incrementalReportsQueue.get(oldEndpoint);
        if (oldQueue != null) {
          // Both keys reference the SAME LinkedList. Safe because
          // producers serialize on `incrementalReportsQueue`'s monitor
          // and the inner list is not mutated unsynchronized. After
          // step 3 removes the old key, only the new key references
          // the list.
          incrementalReportsQueue.put(newEndpoint, oldQueue);
        } else {
          incrementalReportsQueue.putIfAbsent(newEndpoint, new LinkedList<>());
        }
      }
    }
    synchronized (containerActions) {
      if (!newAlreadyRegistered) {
        Queue<ContainerAction> oldActions = containerActions.get(oldEndpoint);
        if (oldActions != null) {
          containerActions.put(newEndpoint, oldActions);
        } else {
          containerActions.putIfAbsent(newEndpoint, new LinkedList<>());
        }
      }
    }
    if (!newAlreadyRegistered) {
      PipelineActionMap oldPipelineActions = pipelineActions.get(oldEndpoint);
      pipelineActions.computeIfAbsent(newEndpoint,
          k -> oldPipelineActions != null
              ? oldPipelineActions
              : new PipelineActionMap());

      // Always seed all-true flags for the new peer. A rescheduled SCM
      // pod is a fresh process and needs a full container/node/pipeline
      // report on the next heartbeat regardless of what the old peer
      // had already received.
      Map<String, AtomicBoolean> flags = new HashMap<>();
      for (String e : fullReportTypeList) {
        flags.put(e, new AtomicBoolean(true));
      }
      isFullReportReadyToBeSent.putIfAbsent(newEndpoint, flags);
    }

    // Step 2 (SWITCH): swap endpoint membership under the same monitors
    // producers hold when fanning out incremental reports and container
    // actions, so no producer observes both keys while they alias the
    // same underlying queue.
    synchronized (incrementalReportsQueue) {
      synchronized (containerActions) {
        if (!newAlreadyRegistered) {
          endpoints.add(newEndpoint);
        }
        endpoints.remove(oldEndpoint);
      }
    }

    // Step 3 (RETIRE): drop the old-key queues. From this point no
    // producer iterating `endpoints` reaches the old key.
    synchronized (incrementalReportsQueue) {
      List<Message> oldQueue =
          incrementalReportsQueue.remove(oldEndpoint);
      if (newAlreadyRegistered && oldQueue != null && !oldQueue.isEmpty()) {
        LOG.warn("DNS refresh produced an endpoint collision: dropping "
                + "{} incremental reports queued under {} because {} is "
                + "already a registered endpoint with its own queue.",
            oldQueue.size(), oldEndpoint, newEndpoint);
      }
    }
    synchronized (containerActions) {
      Queue<ContainerAction> oldActions = containerActions.remove(oldEndpoint);
      if (newAlreadyRegistered && oldActions != null && !oldActions.isEmpty()) {
        LOG.warn("DNS refresh produced an endpoint collision: dropping "
                + "{} container actions queued under {} because {} is "
                + "already a registered endpoint with its own queue.",
            oldActions.size(), oldEndpoint, newEndpoint);
      }
    }
    PipelineActionMap retiredPipelineActions =
        pipelineActions.remove(oldEndpoint);
    if (newAlreadyRegistered && retiredPipelineActions != null
        && retiredPipelineActions.size() > 0) {
      LOG.warn("DNS refresh produced an endpoint collision: dropping "
              + "{} pipeline actions queued under {} because {} is "
              + "already a registered endpoint with its own map.",
          retiredPipelineActions.size(), oldEndpoint, newEndpoint);
    }
    isFullReportReadyToBeSent.remove(oldEndpoint);

    if (getQueueMetrics() != null) {
      getQueueMetrics().removeEndpoint(oldEndpoint);
      if (!newAlreadyRegistered) {
        getQueueMetrics().addEndpoint(newEndpoint);
      }
    }
  }

  @VisibleForTesting
  public Message getContainerReports() {
    return containerReports.get();
  }

  @VisibleForTesting
  public Message getNodeReport() {
    return nodeReport.get();
  }

  @VisibleForTesting
  public Message getPipelineReports() {
    return pipelineReports.get();
  }

  public void configureReconHeartbeatFrequency() {
    reconHeartbeatFrequency.set(getReconHeartbeatInterval(conf));
  }

  /**
   * Return current Datanode to Recon heartbeat frequency in ms.
   */
  public long getReconHeartbeatFrequency() {
    return reconHeartbeatFrequency.get();
  }

  public Map<InetSocketAddress, Integer> getPipelineActionQueueSize() {
    return pipelineActions.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
  }

  public Map<InetSocketAddress, Integer> getContainerActionQueueSize() {
    return containerActions.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
  }

  public Map<InetSocketAddress, Integer> getIncrementalReportQueueSize() {
    return incrementalReportsQueue.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
  }

  public DatanodeQueueMetrics getQueueMetrics() {
    return parentDatanodeStateMachine.getQueueMetrics();
  }

  public String getThreadNamePrefix() {
    return threadNamePrefix;
  }

  static class PipelineActionMap {
    private final LinkedHashMap<PipelineKey, PipelineAction> map =
        new LinkedHashMap<>();

    synchronized int size() {
      return map.size();
    }

    synchronized boolean putIfAbsent(PipelineKey key,
        PipelineAction pipelineAction) {
      return map.putIfAbsent(key, pipelineAction) == null;
    }

    synchronized List<PipelineAction> getActions(List<PipelineReport> reports,
        int max) {
      if (map.isEmpty()) {
        return Collections.emptyList();
      }
      final List<PipelineAction> pipelineActionList = new ArrayList<>();
      final int limit = Math.min(map.size(), max);
      final Iterator<Map.Entry<PipelineKey, PipelineAction>> i =
          map.entrySet().iterator();
      for (int count = 0; count < limit && i.hasNext(); count++) {
        final Map.Entry<PipelineKey, PipelineAction> entry = i.next();
        final PipelineAction action = entry.getValue();
        // Add closePipeline back to the pipelineAction queue until
        // pipeline is closed and removed from the DN.
        if (action.hasClosePipeline()) {
          if (reports.stream().noneMatch(entry.getKey()::equalsId)) {
            // pipeline is removed from the DN, this action is no longer needed.
            i.remove();
            continue;
          }
          // pipeline is closed but not yet removed from the DN.
        } else {
          i.remove();
        }
        pipelineActionList.add(action);
      }
      // add all
      return pipelineActionList;
    }
  }

  static class PipelineKey {
    private final HddsProtos.PipelineID pipelineID;
    private final PipelineAction.Action action;

    PipelineKey(PipelineAction p) {
      this.pipelineID = p.getClosePipeline().getPipelineID();
      this.action = p.getAction();
    }

    boolean equalsId(PipelineReport report) {
      return pipelineID.equals(report.getPipelineID());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(pipelineID);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (!(obj instanceof PipelineKey)) {
        return false;
      }
      final PipelineKey that = (PipelineKey) obj;
      return Objects.equals(this.action, that.action)
          && Objects.equals(this.pipelineID, that.pipelineID);
    }
  }
}
