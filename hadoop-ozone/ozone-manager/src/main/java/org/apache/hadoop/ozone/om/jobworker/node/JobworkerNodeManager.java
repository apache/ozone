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

package org.apache.hadoop.ozone.om.jobworker.node;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.ozone.OzoneConsts.CLUSTER_ID;
import static org.apache.hadoop.ozone.OzoneConsts.OM_ID;
import static org.apache.hadoop.ozone.OzoneConsts.OM_SERVICE_ID;
import static org.apache.hadoop.ozone.conf.OMJobworkerConfiguration.getJobworkerServiceConfigKey;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USE_JOBWORKER_HOSTNAME_DEFAULT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_USE_JOBWORKER_HOSTNAME_KEY;
import static org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents.STALE_JOBWORKER;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.Message;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionRequest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.GetOMVersionResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerNodeReportProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.RegisterJobworkerResponse.ReturnCode;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.net.NetworkTopology;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.conf.OMJobworkerConfiguration;
import org.apache.hadoop.ozone.jobworker.commands.OMJobworkerCommand;
import org.apache.hadoop.ozone.jobworker.protocol.JobworkerNodeProtocol;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.ozone.om.jobworker.OMJobworkerCommandQueue;
import org.apache.hadoop.ozone.om.jobworker.OMJobworkerEvents;
import org.apache.hadoop.ozone.om.jobworker.node.JobworkerNodeStateMachine.NodeLifeCycleEvent;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeAlreadyExistsException;
import org.apache.hadoop.ozone.om.jobworker.states.JobworkerNodeNotFoundException;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.util.ProtobufUtils;
import org.apache.hadoop.ozone.util.RemoteAddressInterceptor;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains information about the jobworker on OM side.
 */
public class JobworkerNodeManager implements JobworkerNodeProtocol, Closeable {

  public static final Logger LOG = LoggerFactory.getLogger(JobworkerNodeManager.class);

  private final JobworkerNodeStateManager nodeStateManager;
  private final boolean useHostname;
  private final Function<String, String> nodeResolver;
  private final NetworkTopology clusterMap;
  private final OMStorage omStorage;
  private final OMNodeDetails omNodeDetails;
  private final OMJobworkerCommandQueue commandQueue;
  private final int softwareLayoutVersion;
  private final EventPublisher eventPublisher;
  private final ScheduledExecutorService executorService;
  private final long heartbeatCheckerIntervalMs;
  private final long staleNodeIntervalMs;
  private final long removalTimeoutMs;
  private long lastHealthCheck;
  private final JobworkerNodeStateMachine nodeStateMachine;

  public JobworkerNodeManager(Function<String, String> nodeResolver, NetworkTopology clusterMap,
                              OMStorage omStorage, OMNodeDetails omNodeDetails,
                              OzoneConfiguration conf, EventPublisher eventPublisher) {
    Objects.requireNonNull(nodeResolver);
    Objects.requireNonNull(clusterMap);
    Objects.requireNonNull(omStorage);
    Objects.requireNonNull(omNodeDetails);
    Objects.requireNonNull(eventPublisher);
    this.nodeStateManager = new JobworkerNodeStateManager();
    this.useHostname = conf.getBoolean(OZONE_OM_USE_JOBWORKER_HOSTNAME_KEY,
        OZONE_OM_USE_JOBWORKER_HOSTNAME_DEFAULT);
    this.nodeResolver = nodeResolver;
    this.clusterMap = clusterMap;
    this.omStorage = omStorage;
    this.omNodeDetails = omNodeDetails;
    this.commandQueue = new OMJobworkerCommandQueue();
    this.softwareLayoutVersion = OMLayoutVersionManager.maxLayoutVersion();
    this.eventPublisher = eventPublisher;
    this.nodeStateMachine = new JobworkerNodeStateMachine();

    // Initialize configuration values
    OMJobworkerConfiguration jwConfig =
        conf.getObject(OMJobworkerConfiguration.class);
    this.heartbeatCheckerIntervalMs = jwConfig.getHeartbeatProcessIntervalMs();
    this.staleNodeIntervalMs = jwConfig.getStaleNodeIntervalMs();
    this.removalTimeoutMs = jwConfig.getRemovalTimeoutMs();
    Preconditions.checkState(heartbeatCheckerIntervalMs > 0,
        getJobworkerServiceConfigKey() +  " should be greater than 0.");

    String threadNamePrefix = omNodeDetails != null ?
        omNodeDetails.threadNamePrefix() : "JobworkerNodeManager";
    executorService = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat(threadNamePrefix + "-%d")
            .build()
    );

    // Start the health check process
    scheduleHealthCheck();
  }

  @Override
  public GetOMVersionResponse getVersion(GetOMVersionRequest versionRequest) {
    return GetOMVersionResponse.newBuilder()
        .setSoftwareVersion(softwareLayoutVersion)
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(OM_ID)
            .setValue(omStorage.getOmId()))
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(CLUSTER_ID)
            .setValue(omStorage.getClusterID()))
        .addKeys(HddsProtos.KeyValue.newBuilder()
            .setKey(OM_SERVICE_ID)
            .setValue(omNodeDetails.getServiceId()))
        .build();
  }

  @Override
  public RegisterJobworkerResponse registerJobworker(JobworkerDetails jobworkerDetails) throws IOException {
    InetAddress jobworkerAddress = RemoteAddressInterceptor.REMOTE_ADDR.get();
    jobworkerDetails.setNetworkName(jobworkerDetails.getUuidString());
    final UUID uuid = jobworkerDetails.getUuid();
    if (jobworkerAddress != null) {
      final String ipAddress = jobworkerAddress.getHostAddress();
      final String hostName = jobworkerAddress.getHostName();
      jobworkerDetails.setIpAddress(ipAddress);
      jobworkerDetails.setHostName(hostName);
      String networkLocation = nodeResolver.apply(
          useHostname ? hostName : ipAddress);
      if (networkLocation != null) {
        jobworkerDetails.setNetworkLocation(networkLocation);
      }
    }

    if (!isJobworkerNodeRegistered(uuid)) {
      try {
        clusterMap.add(jobworkerDetails);
        nodeStateManager.addNode(jobworkerDetails);
        JobworkerInfo nodeInfo = nodeStateManager.getNodeInfo(uuid);
        Preconditions.checkState(nodeInfo.getParent() != null);
        LOG.info("Registered jobworker: {}", jobworkerDetails.toDebugString());

        processNodeReport(jobworkerDetails, null);

        eventPublisher.fireEvent(OMJobworkerEvents.NEW_JOBWORKER, jobworkerDetails);
      } catch (JobworkerNodeAlreadyExistsException e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Jobworker is already registered: {}",
              jobworkerDetails);
        }
      } catch (JobworkerNodeNotFoundException e) {
        LOG.error("Cannot find jobworker {} from nodeStateManager",
            jobworkerDetails);
      }
    } else {
      // Jobworker is a stateless service, so after the jobworker restart, its UUID will change,
      // So jobworker should not re-register to OM
      LOG.warn("Jobworker is already registered {}", jobworkerDetails.toDebugString());
    }

    return getRegisterJobworkerResponse(jobworkerDetails, omStorage, omNodeDetails, ReturnCode.SUCCESS);
  }

  @Override
  public Boolean isJobworkerNodeRegistered(UUID jobworkerID) {
    try {
      nodeStateManager.getNodeInfo(jobworkerID);
      return true;
    } catch (JobworkerNodeNotFoundException e) {
      return false;
    }
  }

  @Override
  public void processHeartbeat(JobworkerDetails jobworkerDetails) {
    try {
      nodeStateManager.updateLastHeartbeatTime(jobworkerDetails);
      if (eventPublisher != null) {
        // Check current state and fire event if state has changed
        JobworkerInfo nodeInfo = nodeStateManager.getNodeInfo(jobworkerDetails.getUuid());
        if (nodeInfo.getNodeStatus().isStale()) {
          // The Node was previously stale but is now healthy
          try {
            updateNodeState(nodeInfo, time -> true, NodeLifeCycleEvent.RESTORE);
          } catch (JobworkerNodeNotFoundException e) {
            LOG.error("Node is not found when updating state: {}", jobworkerDetails, e);
          }
        }
      }
    } catch (JobworkerNodeNotFoundException e) {
      LOG.error("OM trying to process heartbeat from an " +
          "unregistered node {}. Ignoring the heartbeat.", jobworkerDetails);
    }
  }

  @Override
  public List<OMJobworkerCommand> pollJobworkerCommand(UUID jobworkerId) {
    return commandQueue.pollCommand(jobworkerId);
  }

  @Override
  public void close() throws IOException {
    if (executorService != null) {
      ServerUtils.executorServiceShutdownGraceful(executorService);
    }

    if (nodeStateManager != null) {
      nodeStateManager.close();
    }
  }

  public void processNodeReport(JobworkerDetails jobworkerDetails,
                                JobworkerNodeReportProto nodeReport) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing node report from [jobworker={}]",
          jobworkerDetails.getHostName());
    }
    try {
      JobworkerInfo jobworkerInfo = nodeStateManager.getNodeInfo(jobworkerDetails.getUuid());
      if (nodeReport != null) {
        jobworkerInfo.updateStorageReports(nodeReport.getStorageReportList());
      }
    } catch (JobworkerNodeNotFoundException e) {
      LOG.warn("Got node report from unregistered datanode {}", jobworkerDetails);
    }
  }

  private RegisterJobworkerResponse getRegisterJobworkerResponse(
      JobworkerDetails jobworkerDetails, OMStorage storage, OMNodeDetails omNode, ReturnCode returnCode) {
    RegisterJobworkerResponse.Builder builder = RegisterJobworkerResponse.newBuilder()
        .setReturnCode(returnCode)
        .setJobworkerUUID(ProtobufUtils.toProtobuf(jobworkerDetails.getUuid()))
        .setClusterID(storage.getClusterID())
        .setOmServiceId(omNode.getServiceId());
    if (!Strings.isNullOrEmpty(jobworkerDetails.getIpAddress())) {
      builder.setIpAddress(jobworkerDetails.getIpAddress());
    }
    if (!Strings.isNullOrEmpty(jobworkerDetails.getHostName())) {
      builder.setHostname(jobworkerDetails.getHostName());
    }
    if (!Strings.isNullOrEmpty(jobworkerDetails.getNetworkName())) {
      builder.setNetworkName(jobworkerDetails.getNetworkName());
    }
    if (!Strings.isNullOrEmpty(jobworkerDetails.getNetworkLocation())) {
      builder.setNetworkLocation(jobworkerDetails.getNetworkLocation());
    }
    return builder.build();
  }

  /**
   * Add a {@link OMJobworkerCommand} to the command queue, which are
   * handled by HB thread asynchronously.
   * @param uuid jobwoker uuid
   * @param command The command need to add to the command queue
   */
  public void addOMJobworkerCommand(UUID uuid, OMJobworkerCommand<? extends Message> command) {
    this.commandQueue.addCommand(uuid, command);
  }

  /**
   * Remove the queued commands of all jobworkers, for example after this OM lost leadership.
   */
  public void clearAllCommands() {
    this.commandQueue.clear();
  }

  @VisibleForTesting
  public JobworkerNodeStateManager getNodeStateManager() {
    return nodeStateManager;
  }

  /**
   * Checks the health of all nodes and transitions their states as needed.
   * This method implements a health check mechanism for Jobworker nodes with the following logic:
   * 1. We get the current time and look back at the time when we got a heartbeat from a node.
   * 2. If the last heartbeat was recent (within staleNodeIntervalMs), we mark it as HEALTHY.
   * 3. If the last HB timestamp is older and falls beyond the staleNodeIntervalMs window,
   *    we will mark it as STALE.
   * 4. For nodes already in STALE state:
   *    1. If they send a heartbeat (becomes recent again), they are restored to HEALTHY.
   *    2. If they remain without heartbeats beyond removalTimeoutMs, they are completely
   *       removed from the OM include related resource.
   * This health check runs periodically based on the configured heartbeatCheckerIntervalMs
   * to ensure timely detection of node state changes and resource cleanup.
   */
  public void checkNodesHealth() {
    long processingStartTime = Time.monotonicNow();
    long healthyNodeDeadline = processingStartTime - staleNodeIntervalMs;
    long removalDeadline = processingStartTime - removalTimeoutMs;

    Predicate<Long> healthyNodeCondition =
        (lastHbTime) -> lastHbTime >= healthyNodeDeadline;
    Predicate<Long> staleNodeCondition =
        (lastHbTime) -> lastHbTime < healthyNodeDeadline;
    Predicate<Long> removalCondition =
        (lastHbTime) -> lastHbTime < removalDeadline;

    try {
      List<UUID> nodesToRemove = new ArrayList<>();
      for (JobworkerInfo node : nodeStateManager.getAllJobworkerInfos()) {
        JobworkerNodeStatus status = node.getNodeStatus();

        // Check for stale nodes to be removed from memory
        if (status.isStale() && removalCondition.test(node.getLastHeartbeatTime())) {
          LOG.info("Removing stale JobWorker {} from memory as it has not sent " +
              "heartbeats for too long", node);
          nodesToRemove.add(node.getUuid());
          continue;
        }

        switch (status.getHealthState()) {
        case HEALTHY:
          // Move the node to STALE if the last heartbeat time is older than
          // the configured stale-node interval.
          updateNodeState(node, staleNodeCondition, NodeLifeCycleEvent.TIMEOUT);
          break;
        case STALE:
          // Restore the node if we have received a recent heartbeat
          updateNodeState(node, healthyNodeCondition, NodeLifeCycleEvent.RESTORE);
          break;
        default:
          // This shouldn't happen with our state model
          LOG.warn("JobWorker {} is in an unexpected state: {}",
              node, status.getHealthState());
        }
      }

      removeNodes(nodesToRemove);
    } catch (JobworkerNodeNotFoundException e) {
      // This should not happen unless someone else is directly modifying NodeStateMap
      LOG.error("Inconsistent JobWorkerNodeStateMap! {}", nodeStateManager);
    }

    long processingEndTime = Time.monotonicNow();
    if ((processingEndTime - processingStartTime) > heartbeatCheckerIntervalMs) {
      LOG.warn("Total time spent processing JobWorker HBs ({} ms) is greater than " +
              "configured heartbeat interval ({} ms). Consider adjusting heartbeat configs.",
          (processingEndTime - processingStartTime), heartbeatCheckerIntervalMs);
    }
  }

  public void removeNodes(List<UUID> nodesToRemove) {
    for (UUID nodeId : nodesToRemove) {
      try {
        JobworkerDetails node = nodeStateManager.getNodeInfo(nodeId);
        clusterMap.remove(node);
        commandQueue.clear(nodeId);

        nodeStateManager.removeNode(nodeId);

        LOG.info("Removed stale JobWorker {} from node manager", node);
      } catch (JobworkerNodeNotFoundException e) {
        LOG.warn("Attempted to remove node {} but it was already gone", nodeId);
      }
    }
  }

  /**
   * Updates the node state if the condition satisfies.
   *
   * @param node JobworkerInfo
   * @param condition condition to check
   * @param lifeCycleEvent NodeLifeCycleEvent to be applied if condition matches
   *
   * @throws JobworkerNodeNotFoundException if the node is not present
   */
  private void updateNodeState(JobworkerInfo node, Predicate<Long> condition,
                               NodeLifeCycleEvent lifeCycleEvent)
      throws JobworkerNodeNotFoundException {
    try {
      if (condition.test(node.getLastHeartbeatTime())) {
        HddsProtos.NodeState currentState = node.getNodeStatus().getHealthState();
        HddsProtos.NodeState newState = nodeStateMachine.getNextState(
            currentState, lifeCycleEvent);

        if (currentState != newState) {
          LOG.info("JobWorker {} state transition: {} -> {}",
              node, currentState, newState);

          JobworkerNodeStatus oldStatus = node.getNodeStatus();
          JobworkerNodeStatus newStatus = new JobworkerNodeStatus(
              oldStatus.getOperationalState(), newState);
          node.setNodeStatus(newStatus);

          if (eventPublisher != null) {
            if (newState == HEALTHY) {
              LOG.info("JobWorker {} recovery HEALTHY state ", node);
              // No need target future Event for the recovered HEALTHY Jobworker currently.
            } else if (newState == STALE) {
              eventPublisher.fireEvent(STALE_JOBWORKER, node);
            }
          }
        }
      }
    } catch (org.apache.hadoop.ozone.common.statemachine.InvalidStateTransitionException e) {
      LOG.warn("Invalid state transition of JobWorker {}." +
              " Current state: {}, life cycle event: {}",
          node, node.getNodeStatus().getHealthState(), lifeCycleEvent);
    }
  }

  /**
   * Runnable task for the scheduled node check.
   */
  public void run() {
    try {
      if (shouldSkipCheck()) {
        LOG.warn("Detected long delay in scheduling HB processing thread. "
            + "Skipping heartbeat checks for one iteration.");
      } else {
        checkNodesHealth();
      }
      lastHealthCheck = Time.monotonicNow();
    } catch (Exception e) {
      LOG.error("Error during node health check", e);
    }
  }

  private void scheduleHealthCheck() {
    LOG.info("scheduling heartbeat check for jobworker, heartbeat check interval {}ms",
        heartbeatCheckerIntervalMs);
    lastHealthCheck = Time.monotonicNow();
    executorService.scheduleWithFixedDelay(this::run, 0,
        heartbeatCheckerIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * If the time since last check exceeds the stale node interval, skip.
   * Such long delays might be caused by a JVM pause. OM cannot make reliable
   * conclusions about jobworker health in such situations.
   * @return : true indicates skip HB checks
   */
  private boolean shouldSkipCheck() {
    long currentTime = Time.monotonicNow();
    return ((currentTime - lastHealthCheck) >= staleNodeIntervalMs);
  }

  @VisibleForTesting
  public void setLastHealthCheck(long lastHealthCheck) {
    this.lastHealthCheck = lastHealthCheck;
  }

  @VisibleForTesting
  public int getCommandCount(UUID jobworkerUuid, OMJobworkerCommandProto.Type commandType) {
    return commandQueue.getJobworkerCommandCount(jobworkerUuid, commandType);
  }

}
