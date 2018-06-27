/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.VersionInfo;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeMetric;
import org.apache.hadoop.hdds.scm.container.placement.metrics.SCMNodeStat;
import org.apache.hadoop.hdds.server.events.Event;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdds.server.events.TypedEvent;
import org.apache.hadoop.hdfs.protocol.UnregisteredNodeException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.NodeReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMRegisteredResponseProto
    .ErrorCode;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.SCMVersionRequestProto;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hadoop.ozone.protocol.StorageContainerNodeProtocol;
import org.apache.hadoop.ozone.protocol.VersionResponse;
import org.apache.hadoop.ozone.protocol.commands.CommandForDatanode;
import org.apache.hadoop.ozone.protocol.commands.RegisteredCommand;
import org.apache.hadoop.ozone.protocol.commands.ReregisterCommand;
import org.apache.hadoop.ozone.protocol.commands.SCMCommand;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import com.google.protobuf.GeneratedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ObjectName;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.DEAD;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .HEALTHY;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState
    .INVALID;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeState.STALE;
import static org.apache.hadoop.util.Time.monotonicNow;

/**
 * Maintains information about the Datanodes on SCM side.
 * <p>
 * Heartbeats under SCM is very simple compared to HDFS heartbeatManager.
 * <p>
 * Here we maintain 3 maps, and we propagate a node from healthyNodesMap to
 * staleNodesMap to deadNodesMap. This moving of a node from one map to another
 * is controlled by 4 configuration variables. These variables define how many
 * heartbeats must go missing for the node to move from one map to another.
 * <p>
 * Each heartbeat that SCMNodeManager receives is  put into heartbeatQueue. The
 * worker thread wakes up and grabs that heartbeat from the queue. The worker
 * thread will lookup the healthynodes map and set the timestamp if the entry
 * is there. if not it will look up stale and deadnodes map.
 * <p>
 * The getNode(byState) functions make copy of node maps and then creates a list
 * based on that. It should be assumed that these get functions always report
 * *stale* information. For example, getting the deadNodeCount followed by
 * getNodes(DEAD) could very well produce totally different count. Also
 * getNodeCount(HEALTHY) + getNodeCount(DEAD) + getNodeCode(STALE), is not
 * guaranteed to add up to the total nodes that we know off. Please treat all
 * get functions in this file as a snap-shot of information that is inconsistent
 * as soon as you read it.
 */
public class SCMNodeManager
    implements NodeManager, StorageContainerNodeProtocol,
    EventHandler<CommandForDatanode> {

  @VisibleForTesting
  static final Logger LOG =
      LoggerFactory.getLogger(SCMNodeManager.class);

  /**
   * Key = NodeID, value = timestamp.
   */
  private final ConcurrentHashMap<UUID, Long> healthyNodes;
  private final ConcurrentHashMap<UUID, Long> staleNodes;
  private final ConcurrentHashMap<UUID, Long> deadNodes;
  private final Queue<HeartbeatQueueItem> heartbeatQueue;
  private final ConcurrentHashMap<UUID, DatanodeDetails> nodes;
  // Individual live node stats
  private final ConcurrentHashMap<UUID, SCMNodeStat> nodeStats;
  // Aggregated node stats
  private SCMNodeStat scmStat;
  // TODO: expose nodeStats and scmStat as metrics
  private final AtomicInteger healthyNodeCount;
  private final AtomicInteger staleNodeCount;
  private final AtomicInteger deadNodeCount;
  private final AtomicInteger totalNodes;
  private long staleNodeIntervalMs;
  private final long deadNodeIntervalMs;
  private final long heartbeatCheckerIntervalMs;
  private final long datanodeHBIntervalSeconds;
  private final ScheduledExecutorService executorService;
  private long lastHBcheckStart;
  private long lastHBcheckFinished = 0;
  private long lastHBProcessedCount;
  private int chillModeNodeCount;
  private final int maxHBToProcessPerLoop;
  private final String clusterID;
  private final VersionInfo version;
  /**
   * During start up of SCM, it will enter into chill mode and will be there
   * until number of Datanodes registered reaches {@code chillModeNodeCount}.
   * This flag is for tracking startup chill mode.
   */
  private AtomicBoolean inStartupChillMode;
  /**
   * Administrator can put SCM into chill mode manually.
   * This flag is for tracking manual chill mode.
   */
  private AtomicBoolean inManualChillMode;
  private final CommandQueue commandQueue;
  // Node manager MXBean
  private ObjectName nmInfoBean;

  // Node pool manager.
  private final SCMNodePoolManager nodePoolManager;
  private final StorageContainerManager scmManager;

  public static final Event<CommandForDatanode> DATANODE_COMMAND =
      new TypedEvent<>(CommandForDatanode.class, "DATANODE_COMMAND");

  /**
   * Constructs SCM machine Manager.
   */
  public SCMNodeManager(OzoneConfiguration conf, String clusterID,
      StorageContainerManager scmManager) throws IOException {
    heartbeatQueue = new ConcurrentLinkedQueue<>();
    healthyNodes = new ConcurrentHashMap<>();
    deadNodes = new ConcurrentHashMap<>();
    staleNodes = new ConcurrentHashMap<>();
    nodes = new ConcurrentHashMap<>();
    nodeStats = new ConcurrentHashMap<>();
    scmStat = new SCMNodeStat();

    healthyNodeCount = new AtomicInteger(0);
    staleNodeCount = new AtomicInteger(0);
    deadNodeCount = new AtomicInteger(0);
    totalNodes = new AtomicInteger(0);
    this.clusterID = clusterID;
    this.version = VersionInfo.getLatestVersion();
    commandQueue = new CommandQueue();

    // TODO: Support this value as a Percentage of known machines.
    chillModeNodeCount = 1;

    staleNodeIntervalMs = HddsServerUtil.getStaleNodeInterval(conf);
    deadNodeIntervalMs = HddsServerUtil.getDeadNodeInterval(conf);
    heartbeatCheckerIntervalMs =
        HddsServerUtil.getScmheartbeatCheckerInterval(conf);
    datanodeHBIntervalSeconds = HddsServerUtil.getScmHeartbeatInterval(conf);
    maxHBToProcessPerLoop = HddsServerUtil.getMaxHBToProcessPerLoop(conf);

    executorService = HadoopExecutors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("SCM Heartbeat Processing Thread - %d").build());

    LOG.info("Entering startup chill mode.");
    this.inStartupChillMode = new AtomicBoolean(true);
    this.inManualChillMode = new AtomicBoolean(false);

    Preconditions.checkState(heartbeatCheckerIntervalMs > 0);
    executorService.schedule(this, heartbeatCheckerIntervalMs,
        TimeUnit.MILLISECONDS);

    registerMXBean();

    this.nodePoolManager = new SCMNodePoolManager(conf);
    this.scmManager = scmManager;
  }

  private void registerMXBean() {
    this.nmInfoBean = MBeans.register("SCMNodeManager",
        "SCMNodeManagerInfo", this);
  }

  private void unregisterMXBean() {
    if(this.nmInfoBean != null) {
      MBeans.unregister(this.nmInfoBean);
      this.nmInfoBean = null;
    }
  }

  /**
   * Removes a data node from the management of this Node Manager.
   *
   * @param node - DataNode.
   * @throws UnregisteredNodeException
   */
  @Override
  public void removeNode(DatanodeDetails node) {
    // TODO : Fix me when adding the SCM CLI.

  }

  /**
   * Gets all datanodes that are in a certain state. This function works by
   * taking a snapshot of the current collection and then returning the list
   * from that collection. This means that real map might have changed by the
   * time we return this list.
   *
   * @return List of Datanodes that are known to SCM in the requested state.
   */
  @Override
  public List<DatanodeDetails> getNodes(NodeState nodestate)
      throws IllegalArgumentException {
    Map<UUID, Long> set;
    switch (nodestate) {
    case HEALTHY:
      synchronized (this) {
        set = Collections.unmodifiableMap(new HashMap<>(healthyNodes));
      }
      break;
    case STALE:
      synchronized (this) {
        set = Collections.unmodifiableMap(new HashMap<>(staleNodes));
      }
      break;
    case DEAD:
      synchronized (this) {
        set = Collections.unmodifiableMap(new HashMap<>(deadNodes));
      }
      break;
    default:
      throw new IllegalArgumentException("Unknown node state requested.");
    }

    return set.entrySet().stream().map(entry -> nodes.get(entry.getKey()))
        .collect(Collectors.toList());
  }

  /**
   * Returns all datanodes that are known to SCM.
   *
   * @return List of DatanodeDetails
   */
  @Override
  public List<DatanodeDetails> getAllNodes() {
    Map<UUID, DatanodeDetails> set;
    synchronized (this) {
      set = Collections.unmodifiableMap(new HashMap<>(nodes));
    }
    return set.entrySet().stream().map(entry -> nodes.get(entry.getKey()))
        .collect(Collectors.toList());
  }

  /**
   * Get the minimum number of nodes to get out of Chill mode.
   *
   * @return int
   */
  @Override
  public int getMinimumChillModeNodes() {
    return chillModeNodeCount;
  }

  /**
   * Sets the Minimum chill mode nodes count, used only in testing.
   *
   * @param count - Number of nodes.
   */
  @VisibleForTesting
  public void setMinimumChillModeNodes(int count) {
    chillModeNodeCount = count;
  }

  /**
   * Returns chill mode Status string.
   * @return String
   */
  @Override
  public String getChillModeStatus() {
    if (inStartupChillMode.get()) {
      return "Still in chill mode, waiting on nodes to report in." +
          String.format(" %d nodes reported, minimal %d nodes required.",
              totalNodes.get(), getMinimumChillModeNodes());
    }
    if (inManualChillMode.get()) {
      return "Out of startup chill mode, but in manual chill mode." +
          String.format(" %d nodes have reported in.", totalNodes.get());
    }
    return "Out of chill mode." +
        String.format(" %d nodes have reported in.", totalNodes.get());
  }

  /**
   * Forcefully exits the chill mode even if we have not met the minimum
   * criteria of exiting the chill mode. This will exit from both startup
   * and manual chill mode.
   */
  @Override
  public void forceExitChillMode() {
    if(inStartupChillMode.get()) {
      LOG.info("Leaving startup chill mode.");
      inStartupChillMode.set(false);
    }
    if(inManualChillMode.get()) {
      LOG.info("Leaving manual chill mode.");
      inManualChillMode.set(false);
    }
  }

  /**
   * Puts the node manager into manual chill mode.
   */
  @Override
  public void enterChillMode() {
    LOG.info("Entering manual chill mode.");
    inManualChillMode.set(true);
  }

  /**
   * Brings node manager out of manual chill mode.
   */
  @Override
  public void exitChillMode() {
    LOG.info("Leaving manual chill mode.");
    inManualChillMode.set(false);
  }

  /**
   * Returns true if node manager is out of chill mode, else false.
   * @return true if out of chill mode, else false
   */
  @Override
  public boolean isOutOfChillMode() {
    return !(inStartupChillMode.get() || inManualChillMode.get());
  }

  /**
   * Returns the Number of Datanodes by State they are in.
   *
   * @return int -- count
   */
  @Override
  public int getNodeCount(NodeState nodestate) {
    switch (nodestate) {
    case HEALTHY:
      return healthyNodeCount.get();
    case STALE:
      return staleNodeCount.get();
    case DEAD:
      return deadNodeCount.get();
    case INVALID:
      // This is unknown due to the fact that some nodes can be in
      // transit between the other states. Returning a count for that is not
      // possible. The fact that we have such state is to deal with the fact
      // that this information might not be consistent always.
      return 0;
    default:
      return 0;
    }
  }

  /**
   * Used for testing.
   *
   * @return true if the HB check is done.
   */
  @VisibleForTesting
  @Override
  public boolean waitForHeartbeatProcessed() {
    return lastHBcheckFinished != 0;
  }

  /**
   * Returns the node state of a specific node.
   *
   * @param datanodeDetails - Datanode Details
   * @return Healthy/Stale/Dead/Unknown.
   */
  @Override
  public NodeState getNodeState(DatanodeDetails datanodeDetails) {
    // There is a subtle race condition here, hence we also support
    // the NODEState.UNKNOWN. It is possible that just before we check the
    // healthyNodes, we have removed the node from the healthy list but stil
    // not added it to Stale Nodes list.
    // We can fix that by adding the node to stale list before we remove, but
    // then the node is in 2 states to avoid this race condition. Instead we
    // just deal with the possibilty of getting a state called unknown.

    UUID id = datanodeDetails.getUuid();
    if(healthyNodes.containsKey(id)) {
      return HEALTHY;
    }

    if(staleNodes.containsKey(id)) {
      return STALE;
    }

    if(deadNodes.containsKey(id)) {
      return DEAD;
    }

    return INVALID;
  }

  /**
   * This is the real worker thread that processes the HB queue. We do the
   * following things in this thread.
   * <p>
   * Process the Heartbeats that are in the HB Queue. Move Stale or Dead node to
   * healthy if we got a heartbeat from them. Move Stales Node to dead node
   * table if it is needed. Move healthy nodes to stale nodes if it is needed.
   * <p>
   * if it is a new node, we call register node and add it to the list of nodes.
   * This will be replaced when we support registration of a node in SCM.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    lastHBcheckStart = monotonicNow();
    lastHBProcessedCount = 0;

    // Process the whole queue.
    while (!heartbeatQueue.isEmpty() &&
        (lastHBProcessedCount < maxHBToProcessPerLoop)) {
      HeartbeatQueueItem hbItem = heartbeatQueue.poll();
      synchronized (this) {
        handleHeartbeat(hbItem);
      }
      // we are shutting down or something give up processing the rest of
      // HBs. This will terminate the HB processing thread.
      if (Thread.currentThread().isInterrupted()) {
        LOG.info("Current Thread is isInterrupted, shutting down HB " +
            "processing thread for Node Manager.");
        return;
      }
    }

    if (lastHBProcessedCount >= maxHBToProcessPerLoop) {
      LOG.error("SCM is being flooded by heartbeats. Not able to keep up with" +
          " the heartbeat counts. Processed {} heartbeats. Breaking out of" +
          " loop. Leaving rest to be processed later. ", lastHBProcessedCount);
    }

    // Iterate over the Stale nodes and decide if we need to move any node to
    // dead State.
    long currentTime = monotonicNow();
    for (Map.Entry<UUID, Long> entry : staleNodes.entrySet()) {
      if (currentTime - entry.getValue() > deadNodeIntervalMs) {
        synchronized (this) {
          moveStaleNodeToDead(entry);
        }
      }
    }

    // Iterate over the healthy nodes and decide if we need to move any node to
    // Stale State.
    currentTime = monotonicNow();
    for (Map.Entry<UUID, Long> entry : healthyNodes.entrySet()) {
      if (currentTime - entry.getValue() > staleNodeIntervalMs) {
        synchronized (this) {
          moveHealthyNodeToStale(entry);
        }
      }
    }
    lastHBcheckFinished = monotonicNow();

    monitorHBProcessingTime();

    // we purposefully make this non-deterministic. Instead of using a
    // scheduleAtFixedFrequency  we will just go to sleep
    // and wake up at the next rendezvous point, which is currentTime +
    // heartbeatCheckerIntervalMs. This leads to the issue that we are now
    // heart beating not at a fixed cadence, but clock tick + time taken to
    // work.
    //
    // This time taken to work can skew the heartbeat processor thread.
    // The reason why we don't care is because of the following reasons.
    //
    // 1. checkerInterval is general many magnitudes faster than datanode HB
    // frequency.
    //
    // 2. if we have too much nodes, the SCM would be doing only HB
    // processing, this could lead to SCM's CPU starvation. With this
    // approach we always guarantee that  HB thread sleeps for a little while.
    //
    // 3. It is possible that we will never finish processing the HB's in the
    // thread. But that means we have a mis-configured system. We will warn
    // the users by logging that information.
    //
    // 4. And the most important reason, heartbeats are not blocked even if
    // this thread does not run, they will go into the processing queue.

    if (!Thread.currentThread().isInterrupted() &&
        !executorService.isShutdown()) {
      executorService.schedule(this, heartbeatCheckerIntervalMs, TimeUnit
          .MILLISECONDS);
    } else {
      LOG.info("Current Thread is interrupted, shutting down HB processing " +
          "thread for Node Manager.");
    }
  }

  /**
   * If we have taken too much time for HB processing, log that information.
   */
  private void monitorHBProcessingTime() {
    if (TimeUnit.MILLISECONDS.toSeconds(lastHBcheckFinished -
        lastHBcheckStart) > datanodeHBIntervalSeconds) {
      LOG.error("Total time spend processing datanode HB's is greater than " +
              "configured values for datanode heartbeats. Please adjust the" +
              " heartbeat configs. Time Spend on HB processing: {} seconds " +
              "Datanode heartbeat Interval: {} seconds , heartbeats " +
              "processed: {}",
          TimeUnit.MILLISECONDS
              .toSeconds(lastHBcheckFinished - lastHBcheckStart),
          datanodeHBIntervalSeconds, lastHBProcessedCount);
    }
  }

  /**
   * Moves a Healthy node to a Stale node state.
   *
   * @param entry - Map Entry
   */
  private void moveHealthyNodeToStale(Map.Entry<UUID, Long> entry) {
    LOG.trace("Moving healthy node to stale: {}", entry.getKey());
    healthyNodes.remove(entry.getKey());
    healthyNodeCount.decrementAndGet();
    staleNodes.put(entry.getKey(), entry.getValue());
    staleNodeCount.incrementAndGet();

    if (scmManager != null) {
      // remove stale node's container report
      scmManager.removeContainerReport(entry.getKey().toString());
    }
  }

  /**
   * Moves a Stale node to a dead node state.
   *
   * @param entry - Map Entry
   */
  private void moveStaleNodeToDead(Map.Entry<UUID, Long> entry) {
    LOG.trace("Moving stale node to dead: {}", entry.getKey());
    staleNodes.remove(entry.getKey());
    staleNodeCount.decrementAndGet();
    deadNodes.put(entry.getKey(), entry.getValue());
    deadNodeCount.incrementAndGet();

    // Update SCM node stats
    SCMNodeStat deadNodeStat = nodeStats.get(entry.getKey());
    scmStat.subtract(deadNodeStat);
    nodeStats.remove(entry.getKey());
  }

  /**
   * Handles a single heartbeat from a datanode.
   *
   * @param hbItem - heartbeat item from a datanode.
   */
  private void handleHeartbeat(HeartbeatQueueItem hbItem) {
    lastHBProcessedCount++;

    DatanodeDetails datanodeDetails = hbItem.getDatanodeDetails();
    UUID datanodeUuid = datanodeDetails.getUuid();
    NodeReportProto nodeReport = hbItem.getNodeReport();
    long recvTimestamp = hbItem.getRecvTimestamp();
    long processTimestamp = Time.monotonicNow();
    if (LOG.isTraceEnabled()) {
      //TODO: add average queue time of heartbeat request as metrics
      LOG.trace("Processing Heartbeat from datanode {}: queueing time {}",
          datanodeUuid, processTimestamp - recvTimestamp);
    }

    // If this node is already in the list of known and healthy nodes
    // just set the last timestamp and return.
    if (healthyNodes.containsKey(datanodeUuid)) {
      healthyNodes.put(datanodeUuid, processTimestamp);
      updateNodeStat(datanodeUuid, nodeReport);
      return;
    }

    // A stale node has heartbeat us we need to remove the node from stale
    // list and move to healthy list.
    if (staleNodes.containsKey(datanodeUuid)) {
      staleNodes.remove(datanodeUuid);
      healthyNodes.put(datanodeUuid, processTimestamp);
      healthyNodeCount.incrementAndGet();
      staleNodeCount.decrementAndGet();
      updateNodeStat(datanodeUuid, nodeReport);
      return;
    }

    // A dead node has heartbeat us, we need to remove that node from dead
    // node list and move it to the healthy list.
    if (deadNodes.containsKey(datanodeUuid)) {
      deadNodes.remove(datanodeUuid);
      healthyNodes.put(datanodeUuid, processTimestamp);
      deadNodeCount.decrementAndGet();
      healthyNodeCount.incrementAndGet();
      updateNodeStat(datanodeUuid, nodeReport);
      return;
    }

    LOG.warn("SCM receive heartbeat from unregistered datanode {}",
        datanodeUuid);
    this.commandQueue.addCommand(datanodeUuid,
        new ReregisterCommand());
  }

  private void updateNodeStat(UUID dnId, NodeReportProto nodeReport) {
    SCMNodeStat stat = nodeStats.get(dnId);
    if (stat == null) {
      LOG.debug("SCM updateNodeStat based on heartbeat from previous" +
          "dead datanode {}", dnId);
      stat = new SCMNodeStat();
    }

    if (nodeReport != null && nodeReport.getStorageReportCount() > 0) {
      long totalCapacity = 0;
      long totalRemaining = 0;
      long totalScmUsed = 0;
      List<StorageReportProto> storageReports = nodeReport
          .getStorageReportList();
      for (StorageReportProto report : storageReports) {
        totalCapacity += report.getCapacity();
        totalRemaining +=  report.getRemaining();
        totalScmUsed+= report.getScmUsed();
      }
      scmStat.subtract(stat);
      stat.set(totalCapacity, totalScmUsed, totalRemaining);
      nodeStats.put(dnId, stat);
      scmStat.add(stat);
    }
  }

  /**
   * Closes this stream and releases any system resources associated with it. If
   * the stream is already closed then invoking this method has no effect.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    unregisterMXBean();
    nodePoolManager.close();
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
      }

      if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.error("Unable to shutdown NodeManager properly.");
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  long getLastHBProcessedCount() {
    return lastHBProcessedCount;
  }

  /**
   * Gets the version info from SCM.
   *
   * @param versionRequest - version Request.
   * @return - returns SCM version info and other required information needed by
   * datanode.
   */
  @Override
  public VersionResponse getVersion(SCMVersionRequestProto versionRequest) {
    return VersionResponse.newBuilder()
        .setVersion(this.version.getVersion())
        .build();
  }

  /**
   * Register the node if the node finds that it is not registered with any
   * SCM.
   *
   * @param datanodeDetails - Send datanodeDetails with Node info.
   *                   This function generates and assigns new datanode ID
   *                   for the datanode. This allows SCM to be run independent
   *                   of Namenode if required.
   * @param nodeReport NodeReport.
   *
   * @return SCMHeartbeatResponseProto
   */
  @Override
  public RegisteredCommand register(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport) {

    String hostname = null;
    String ip = null;
    InetAddress dnAddress = Server.getRemoteIp();
    if (dnAddress != null) {
      // Mostly called inside an RPC, update ip and peer hostname
      hostname = dnAddress.getHostName();
      ip = dnAddress.getHostAddress();
      datanodeDetails.setHostName(hostname);
      datanodeDetails.setIpAddress(ip);
    }
    RegisteredCommand responseCommand = verifyDatanodeUUID(datanodeDetails);
    if (responseCommand != null) {
      return responseCommand;
    }
    UUID dnId = datanodeDetails.getUuid();
    nodes.put(dnId, datanodeDetails);
    totalNodes.incrementAndGet();
    healthyNodes.put(dnId, monotonicNow());
    healthyNodeCount.incrementAndGet();
    nodeStats.put(dnId, new SCMNodeStat());

    if(inStartupChillMode.get() &&
        totalNodes.get() >= getMinimumChillModeNodes()) {
      inStartupChillMode.getAndSet(false);
      LOG.info("Leaving startup chill mode.");
    }

    // TODO: define node pool policy for non-default node pool.
    // For now, all nodes are added to the "DefaultNodePool" upon registration
    // if it has not been added to any node pool yet.
    try {
      if (nodePoolManager.getNodePool(datanodeDetails) == null) {
        nodePoolManager.addNode(SCMNodePoolManager.DEFAULT_NODEPOOL,
            datanodeDetails);
      }
    } catch (IOException e) {
      // TODO: make sure registration failure is handled correctly.
      return RegisteredCommand.newBuilder()
          .setErrorCode(ErrorCode.errorNodeNotPermitted)
          .build();
    }
    // Updating Node Report, as registration is successful
    updateNodeStat(datanodeDetails.getUuid(), nodeReport);
    LOG.info("Data node with ID: {} Registered.",
        datanodeDetails.getUuid());
    RegisteredCommand.Builder builder =
        RegisteredCommand.newBuilder().setErrorCode(ErrorCode.success)
            .setDatanodeUUID(datanodeDetails.getUuidString())
            .setClusterID(this.clusterID);
    if (hostname != null && ip != null) {
      builder.setHostname(hostname).setIpAddress(ip);
    }
    return builder.build();
  }

  /**
   * Verifies the datanode does not have a valid UUID already.
   *
   * @param datanodeDetails - Datanode Details.
   * @return SCMCommand
   */
  private RegisteredCommand verifyDatanodeUUID(
      DatanodeDetails datanodeDetails) {
    if (datanodeDetails.getUuid() != null &&
        nodes.containsKey(datanodeDetails.getUuid())) {
      LOG.trace("Datanode is already registered. Datanode: {}",
          datanodeDetails.toString());
      return RegisteredCommand.newBuilder()
          .setErrorCode(ErrorCode.success)
          .setClusterID(this.clusterID)
          .setDatanodeUUID(datanodeDetails.getUuidString())
          .build();
    }
    return null;
  }

  /**
   * Send heartbeat to indicate the datanode is alive and doing well.
   *
   * @param datanodeDetails - DatanodeDetailsProto.
   * @param nodeReport - node report.
   * @return SCMheartbeat response.
   * @throws IOException
   */
  @Override
  public List<SCMCommand> sendHeartbeat(
      DatanodeDetails datanodeDetails, NodeReportProto nodeReport) {

    Preconditions.checkNotNull(datanodeDetails, "Heartbeat is missing " +
        "DatanodeDetails.");
    heartbeatQueue.add(
        new HeartbeatQueueItem.Builder()
            .setDatanodeDetails(datanodeDetails)
            .setNodeReport(nodeReport)
            .build());
    return commandQueue.getCommand(datanodeDetails.getUuid());
  }

  /**
   * Returns the aggregated node stats.
   * @return the aggregated node stats.
   */
  @Override
  public SCMNodeStat getStats() {
    return new SCMNodeStat(this.scmStat);
  }

  /**
   * Return a map of node stats.
   * @return a map of individual node stats (live/stale but not dead).
   */
  @Override
  public Map<UUID, SCMNodeStat> getNodeStats() {
    return Collections.unmodifiableMap(nodeStats);
  }

  /**
   * Return the node stat of the specified datanode.
   * @param datanodeDetails - datanode ID.
   * @return node stat if it is live/stale, null if it is dead or does't exist.
   */
  @Override
  public SCMNodeMetric getNodeStat(DatanodeDetails datanodeDetails) {
    return new SCMNodeMetric(nodeStats.get(datanodeDetails.getUuid()));
  }

  @Override
  public NodePoolManager getNodePoolManager() {
    return nodePoolManager;
  }

  @Override
  public Map<String, Integer> getNodeCount() {
    Map<String, Integer> nodeCountMap = new HashMap<String, Integer>();
    for(NodeState state : NodeState.values()) {
      nodeCountMap.put(state.toString(), getNodeCount(state));
    }
    return nodeCountMap;
  }

  @Override
  public void addDatanodeCommand(UUID dnId, SCMCommand command) {
    this.commandQueue.addCommand(dnId, command);
  }

  @VisibleForTesting
  public void setStaleNodeIntervalMs(long interval) {
    this.staleNodeIntervalMs = interval;
  }

  @Override
  public void onMessage(CommandForDatanode commandForDatanode,
      EventPublisher publisher) {
    addDatanodeCommand(commandForDatanode.getDatanodeId(),
        commandForDatanode.getCommand());
  }
}
