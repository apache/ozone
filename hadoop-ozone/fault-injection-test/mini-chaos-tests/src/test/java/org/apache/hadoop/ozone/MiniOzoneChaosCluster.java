/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

/**
 * This class causes random failures in the chaos cluster.
 */
public abstract class MiniOzoneChaosCluster extends MiniOzoneHAClusterImpl {

  static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneChaosCluster.class);

  private final int numDatanodes;
  private final int numOzoneManagers;

  // Number of Nodes of the service (Datanode or OM) on which chaos will be
  // unleashed
  private int numNodes;

  private FailureService failureService;
  private long failureIntervalInMS;

  private final ScheduledExecutorService executorService;

  private ScheduledFuture scheduledFuture;

  private enum FailureMode {
    NODES_RESTART,
    NODES_SHUTDOWN
  }

  // The service on which chaos will be unleashed.
  enum FailureService {
    DATANODE,
    OZONE_MANAGER;

    public String toString() {
      if (this == DATANODE) {
        return "Datanode";
      } else {
        return "OzoneManager";
      }
    }

    public static FailureService of(String serviceName) {
      if (serviceName.equalsIgnoreCase("Datanode")) {
        return DATANODE;
      } else if (serviceName.equalsIgnoreCase("OzoneManager")) {
        return OZONE_MANAGER;
      }
      throw new IllegalArgumentException("Unrecognized value for " +
          "FailureService enum: " + serviceName);
    }
  }

  public MiniOzoneChaosCluster(OzoneConfiguration conf,
      List<OzoneManager> ozoneManagers, StorageContainerManager scm,
      List<HddsDatanodeService> hddsDatanodes, String omServiceID,
      FailureService service) {
    super(conf, ozoneManagers, scm, hddsDatanodes, omServiceID);

    this.executorService =  Executors.newSingleThreadScheduledExecutor();
    this.numDatanodes = getHddsDatanodes().size();
    this.numOzoneManagers = ozoneManagers.size();
    this.failureService = service;
    LOG.info("Starting MiniOzoneChaosCluster with {} OzoneManagers and {} " +
        "Datanodes, chaos on service: {}",
        numOzoneManagers, numDatanodes, failureService);
  }

  protected int getNumNodes() {
    return numNodes;
  }

  protected void setNumNodes(int numOfNodes) {
    this.numNodes = numOfNodes;
  }

  protected long getFailureIntervalInMS() {
    return failureIntervalInMS;
  }

  /**
   * Is the cluster ready for chaos.
   */
  protected boolean isClusterReady() {
    return true;
  }

  protected void getClusterReady() {
    // Do nothing
  }

  // Get the number of nodes to fail in the cluster.
  protected int getNumberOfNodesToFail() {
    return RandomUtils.nextBoolean() ? 1 : 2;
  }

  // Should the failed node wait for SCM to register even before
  // restart, i.e fast restart or not.
  protected boolean isFastRestart() {
    return RandomUtils.nextBoolean();
  }

  // Should the selected node be stopped or started.
  protected boolean shouldStop() {
    return RandomUtils.nextBoolean();
  }

  // Get the node index of the node to fail.
  private int getNodeToFail() {
    return RandomUtils.nextInt() % numNodes;
  }

  protected abstract void restartNode(int failedNodeIndex,
      boolean waitForNodeRestart)
      throws TimeoutException, InterruptedException, IOException;

  protected abstract void shutdownNode(int failedNodeIndex)
      throws ExecutionException, InterruptedException;

  protected abstract String getFailedNodeID(int failedNodeIndex);

  private void restartNodes() {
    final int numNodesToFail = getNumberOfNodesToFail();
    LOG.info("Will restart {} nodes to simulate failure", numNodesToFail);
    for (int i = 0; i < numNodesToFail; i++) {
      boolean failureMode = isFastRestart();
      int failedNodeIndex = getNodeToFail();
      String failString = failureMode ? "Fast" : "Slow";
      String failedNodeID = getFailedNodeID(failedNodeIndex);
      try {
        LOG.info("{} Restarting {}: {}", failString, failureService,
            failedNodeID);
        restartNode(failedNodeIndex, failureMode);
        LOG.info("{} Completed restarting {}: {}", failString, failureService,
            failedNodeID);
      } catch (Exception e) {
        LOG.error("Failed to restartNodes {}: {}", failedNodeID,
            failureService, e);
      }
    }
  }

  private void shutdownNodes() {
    final int numNodesToFail = getNumberOfNodesToFail();
    LOG.info("Will shutdown {} nodes to simulate failure", numNodesToFail);
    for (int i = 0; i < numNodesToFail; i++) {
      boolean shouldStop = shouldStop();
      int failedNodeIndex = getNodeToFail();
      String stopString = shouldStop ? "Stopping" : "Restarting";
      String failedNodeID = getFailedNodeID(failedNodeIndex);
      try {
        LOG.info("{} {} {}", stopString, failureService, failedNodeID);
        if (shouldStop) {
          shutdownNode(failedNodeIndex);
        } else {
          restartNode(failedNodeIndex, false);
        }
        LOG.info("Completed {} {} {}", stopString, failureService,
            failedNodeID);
      } catch (Exception e) {
        LOG.error("Failed {} {} {}", stopString, failureService,
            failedNodeID, e);
      }
    }
  }

  private FailureMode getFailureMode() {
    return FailureMode.
        values()[RandomUtils.nextInt() % FailureMode.values().length];
  }

  // Fail nodes randomly at configured timeout period.
  private void fail() {
    if (isClusterReady()) {
      FailureMode mode = getFailureMode();
      switch (mode) {
      case NODES_RESTART:
        restartNodes();
        break;
      case NODES_SHUTDOWN:
        shutdownNodes();
        break;

      default:
        LOG.error("invalid failure mode:{}", mode);
        break;
      }
    } else {
      // Cluster is not ready for failure yet. Skip failing this time and get
      // the cluster ready by restarting any OM that is not running.
      LOG.info("Cluster is not ready for failure.");
      getClusterReady();
    }
  }

  void startChaos(long initialDelay, long period, TimeUnit timeUnit) {
    LOG.info("Starting Chaos with failure period:{} unit:{} numDataNodes:{} " +
            "numOzoneManagers:{}", period, timeUnit, numDatanodes,
        numOzoneManagers);
    this.failureIntervalInMS = TimeUnit.MILLISECONDS.convert(period, timeUnit);
    scheduledFuture = executorService.scheduleAtFixedRate(this::fail,
        initialDelay, period, timeUnit);
  }

  void stopChaos() throws Exception {
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
      scheduledFuture.get();
    }
  }

  public void shutdown() {
    try {
      stopChaos();
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.DAYS);
      //this should be called after stopChaos to be sure that the
      //datanode collection is not modified during the shutdown
      super.shutdown();
    } catch (Exception e) {
      LOG.error("failed to shutdown MiniOzoneChaosCluster", e);
    }
  }

  /**
   * Builder for configuring the MiniOzoneChaosCluster to run.
   */
  public static class Builder extends MiniOzoneHAClusterImpl.Builder {

    private FailureService failureService;

    /**
     * Creates a new Builder.
     *
     * @param conf configuration
     */
    public Builder(OzoneConfiguration conf) {
      super(conf);
    }

    /**
     * Sets the number of HddsDatanodes to be started as part of
     * MiniOzoneChaosCluster.
     * @param val number of datanodes
     * @return MiniOzoneChaosCluster.Builder
     */
    public Builder setNumDatanodes(int val) {
      super.setNumDatanodes(val);
      return this;
    }

    /**
     * Sets the number of OzoneManagers to be started as part of
     * MiniOzoneChaosCluster.
     * @param val number of OzoneManagers
     * @return MiniOzoneChaosCluster.Builder
     */
    public Builder setNumOzoneManagers(int val) {
      super.setNumOfOzoneManagers(val);
      super.setNumOfActiveOMs(val);
      return this;
    }

    /**
     * Sets OM Service ID.
     */
    public Builder setOMServiceID(String omServiceID) {
      super.setOMServiceId(omServiceID);
      return this;
    }

    public Builder setFailureService(String serviceName) {
      this.failureService = FailureService.of(serviceName);
      return this;
    }

    protected void initializeConfiguration() throws IOException {
      super.initializeConfiguration();
      conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY,
          4, StorageUnit.KB);
      conf.setStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
          32, StorageUnit.KB);
      conf.setStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_FLUSH_SIZE,
          8, StorageUnit.KB);
      conf.setStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_MAX_SIZE,
          16, StorageUnit.KB);
      conf.setStorageSize(OzoneConfigKeys.OZONE_CLIENT_STREAM_BUFFER_SIZE,
          4, StorageUnit.KB);
      conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
          1, StorageUnit.MB);
      conf.setTimeDuration(ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT, 1000,
          TimeUnit.MILLISECONDS);
      conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, 10,
          TimeUnit.SECONDS);
      conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, 20,
          TimeUnit.SECONDS);
      conf.setTimeDuration(HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setTimeDuration(HddsConfigKeys.HDDS_PIPELINE_REPORT_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setTimeDuration(
          ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT, 5,
          TimeUnit.SECONDS);
      conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
          1, TimeUnit.SECONDS);
      conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setInt(
          OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_KEY,
          4);
      conf.setInt(
          OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY,
          2);
      conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);
      conf.setInt("hdds.scm.replication.thread.interval", 10 * 1000);
      conf.setInt("hdds.scm.replication.event.timeout", 20 * 1000);
      conf.setInt(OzoneConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_KEY, 100);
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_PURGE_GAP, 100);
    }

    @Override
    public MiniOzoneChaosCluster build() throws IOException {

      if (failureService == FailureService.OZONE_MANAGER && numOfOMs < 3) {
        throw new IllegalArgumentException("Not enough number of " +
            "OzoneManagers to test chaos on OzoneManagers. Set number of " +
            "OzoneManagers to at least 3");
      }

      DefaultMetricsSystem.setMiniClusterMode(true);
      initializeConfiguration();
      if (numOfOMs > 1) {
        initOMRatisConf();
      }

      StorageContainerManager scm;
      List<OzoneManager> omList;
      try {
        scm = createSCM();
        scm.start();
        if (numOfOMs > 1) {
          omList = createOMService();
        } else {
          OzoneManager om = createOM();
          om.start();
          omList = Arrays.asList(om);
        }
      } catch (AuthenticationException ex) {
        throw new IOException("Unable to build MiniOzoneCluster. ", ex);
      }

      final List<HddsDatanodeService> hddsDatanodes = createHddsDatanodes(
          scm, null);

      MiniOzoneChaosCluster cluster;
      if (failureService == FailureService.DATANODE) {
        cluster = new MiniOzoneDatanodeChaosCluster(conf, omList, scm,
            hddsDatanodes, omServiceId);
      } else {
        cluster = new MiniOzoneOMChaosCluster(conf, omList, scm,
            hddsDatanodes, omServiceId);
      }

      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }
  }
}
