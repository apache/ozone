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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Set;
import java.util.Collections;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.failure.FailureManager;
import org.apache.hadoop.ozone.failure.Failures;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * This class causes random failures in the chaos cluster.
 */
public class MiniOzoneChaosCluster extends MiniOzoneHAClusterImpl {

  static final Logger LOG =
      LoggerFactory.getLogger(MiniOzoneChaosCluster.class);

  private final int numDatanodes;
  private final int numOzoneManagers;

  private final FailureManager failureManager;

  private final int waitForClusterToBeReadyTimeout = 120000; // 2 min

  private final Set<OzoneManager> failedOmSet;
  private final Set<DatanodeDetails> failedDnSet;

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
      List<Class<? extends Failures>> clazzes) {
    super(conf, ozoneManagers, scm, hddsDatanodes, omServiceID);
    this.numDatanodes = getHddsDatanodes().size();
    this.numOzoneManagers = ozoneManagers.size();

    this.failedOmSet = new HashSet<>();
    this.failedDnSet = new HashSet<>();

    this.failureManager = new FailureManager(this, conf, clazzes);
    LOG.info("Starting MiniOzoneChaosCluster with {} OzoneManagers and {} " +
        "Datanodes", numOzoneManagers, numDatanodes);
    clazzes.forEach(c -> LOG.info("added failure:{}", c.getSimpleName()));
  }

  void startChaos(long initialDelay, long period, TimeUnit timeUnit) {
    LOG.info("Starting Chaos with failure period:{} unit:{} numDataNodes:{} " +
            "numOzoneManagers:{}", period, timeUnit, numDatanodes,
        numOzoneManagers);
    failureManager.start(initialDelay, period, timeUnit);
  }

  public void shutdown() {
    try {
      failureManager.stop();
      //this should be called after stopChaos to be sure that the
      //datanode collection is not modified during the shutdown
      super.shutdown();
    } catch (Exception e) {
      LOG.error("failed to shutdown MiniOzoneChaosCluster", e);
    }
  }

  /**
   * Check if cluster is ready for a restart or shutdown of an OM node. If
   * yes, then set isClusterReady to false so that another thread cannot
   * restart/ shutdown OM till all OMs are up again.
   */
  @Override
  public void waitForClusterToBeReady()
      throws TimeoutException, InterruptedException {
    super.waitForClusterToBeReady();
    GenericTestUtils.waitFor(() -> {
      for (OzoneManager om : getOzoneManagersList()) {
        if (!om.isRunning()) {
          return false;
        }
      }
      return true;
    }, 1000, waitForClusterToBeReadyTimeout);
  }

  /**
   * Builder for configuring the MiniOzoneChaosCluster to run.
   */
  public static class Builder extends MiniOzoneHAClusterImpl.Builder {

    private final List<Class<? extends Failures>> clazzes = new ArrayList<>();

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

    public Builder addFailures(Class<? extends Failures> clazz) {
      this.clazzes.add(clazz);
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
      conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL,
          1, TimeUnit.SECONDS);
      conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, 1,
          TimeUnit.SECONDS);
      conf.setInt(
          OzoneConfigKeys
              .DFS_CONTAINER_RATIS_NUM_WRITE_CHUNK_THREADS_PER_VOLUME_KEY,
          4);
      conf.setInt(
          OzoneConfigKeys.DFS_CONTAINER_RATIS_NUM_CONTAINER_OP_EXECUTORS_KEY,
          2);
      conf.setInt(OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE, 2);
      ReplicationManagerConfiguration replicationConf =
          conf.getObject(ReplicationManagerConfiguration.class);
      replicationConf.setInterval(Duration.ofSeconds(10));
      replicationConf.setEventTimeout(Duration.ofSeconds(20));
      conf.setFromObject(replicationConf);
      conf.setInt(OzoneConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_KEY, 100);
      conf.setInt(OzoneConfigKeys.DFS_CONTAINER_RATIS_LOG_PURGE_GAP, 100);
      conf.setInt(OMConfigKeys.OZONE_OM_RATIS_LOG_PURGE_GAP, 100);

      conf.setInt(OMConfigKeys.
          OZONE_OM_RATIS_SNAPSHOT_AUTO_TRIGGER_THRESHOLD_KEY, 100);
    }

    /**
     * Sets the number of data volumes per datanode.
     *
     * @param val number of volumes per datanode.
     *
     * @return MiniOzoneCluster.Builder
     */
    public Builder setNumDataVolumes(int val) {
      numDataVolumes = val;
      return this;
    }

    @Override
    public MiniOzoneChaosCluster build() throws IOException {

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

      MiniOzoneChaosCluster cluster =
          new MiniOzoneChaosCluster(conf, omList, scm, hddsDatanodes,
              omServiceId, clazzes);

      if (startDataNodes) {
        cluster.startHddsDatanodes();
      }
      return cluster;
    }
  }

  // OzoneManager specifc
  public static int getNumberOfOmToFail() {
    return 1;
  }

  public Set<OzoneManager> omToFail() {
    int numNodesToFail = getNumberOfOmToFail();
    if (failedOmSet.size() >= numOzoneManagers/2) {
      return Collections.emptySet();
    }

    int numOms = getOzoneManagersList().size();
    Set<OzoneManager> oms = new HashSet<>();
    for (int i = 0; i < numNodesToFail; i++) {
      int failedNodeIndex = FailureManager.getBoundedRandomIndex(numOms);
      oms.add(getOzoneManager(failedNodeIndex));
    }
    return oms;
  }

  public void shutdownOzoneManager(OzoneManager om) {
    super.shutdownOzoneManager(om);
    failedOmSet.add(om);
  }

  public void restartOzoneManager(OzoneManager om, boolean waitForOM)
      throws IOException, TimeoutException, InterruptedException {
    super.restartOzoneManager(om, waitForOM);
    failedOmSet.remove(om);
  }

  // Should the selected node be stopped or started.
  public boolean shouldStop() {
    if (failedOmSet.size() >= numOzoneManagers/2) {
      return false;
    }
    return RandomUtils.nextBoolean();
  }

  // Datanode specifc
  private int getNumberOfDnToFail() {
    return RandomUtils.nextBoolean() ? 1 : 2;
  }

  public Set<DatanodeDetails> dnToFail() {
    int numNodesToFail = getNumberOfDnToFail();
    int numDns = getHddsDatanodes().size();
    Set<DatanodeDetails> dns = new HashSet<>();
    for (int i = 0; i < numNodesToFail; i++) {
      int failedNodeIndex = FailureManager.getBoundedRandomIndex(numDns);
      dns.add(getHddsDatanodes().get(failedNodeIndex).getDatanodeDetails());
    }
    return dns;
  }
  
  @Override
  public void restartHddsDatanode(DatanodeDetails dn, boolean waitForDatanode)
      throws InterruptedException, TimeoutException, IOException {
    failedDnSet.add(dn);
    super.restartHddsDatanode(dn, waitForDatanode);
    failedDnSet.remove(dn);
  }

  @Override
  public void shutdownHddsDatanode(DatanodeDetails dn) throws IOException {
    failedDnSet.add(dn);
    super.shutdownHddsDatanode(dn);
  }

  // Should the selected node be stopped or started.
  public boolean shouldStop(DatanodeDetails dn) {
    return !failedDnSet.contains(dn);
  }
}
