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

package org.apache.hadoop.hdds.scm.simulation;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;

/**
 * Settings of one simulation run. Intervals are production defaults scaled down so that a few simulated hours cover
 * many failure and recovery cycles; their ratios (heartbeat vs. stale vs. dead, and so on) are kept.
 */
final class SimConfig {

  private static final long GB = 1L << 30;

  private long seed;
  private int initialDatanodes = 8;
  private int racks = 3;
  private int maxDatanodes = 12;
  private long chaosDurationMs = TimeUnit.HOURS.toMillis(2);
  private long settleDurationMs = TimeUnit.HOURS.toMillis(1);
  private boolean faults = true;
  private int maxUnavailableDatanodes = 2;
  private double slowHandlerProbability = 0.05;
  private long maxSlowHandlerMs = 10_000;
  private boolean keepFullTrace;
  private Path traceDir = Paths.get("target", "scm-simulation");

  private long heartbeatIntervalMs = 3_000;
  private long nodeReportIntervalMs = 60_000;
  private long containerReportIntervalMs = 300_000;
  private long pipelineReportIntervalMs = 60_000;
  private long heartbeatProcessIntervalMs = 3_000;
  private long staleNodeIntervalMs = 30_000;
  private long deadNodeIntervalMs = 90_000;
  private long replicationIntervalMs = 60_000;
  private long underReplicatedIntervalMs = 10_000;
  private long overReplicatedIntervalMs = 10_000;
  private long replicationEventTimeoutMs = 240_000;
  private long replicationDatanodeOffsetMs = 120_000;
  private long pipelineCreationIntervalMs = 60_000;
  private long pipelineScrubIntervalMs = 60_000;
  private long pipelineAllocatedTimeoutMs = 120_000;
  private long pipelineDestroyTimeoutMs = 60_000;
  private long closeContainerWaitMs = 30_000;
  private long replicaOpScrubIntervalMs = 60_000;
  private long adminMonitorIntervalMs = 30_000;
  private long safeModeExitWaitMs = 10_000;
  private long containerSizeBytes = GB;
  private long datanodeCapacityBytes = 2048 * GB;
  private long writeIntervalMs = 2_000;
  private long faultIntervalMs = 120_000;
  /** Relative weights of chaos faults which differ from their defaults, by fault name. */
  private final Map<String, Integer> faultWeights = new TreeMap<>();

  SimConfig(long seed) {
    this.seed = seed;
  }

  /**
   * Builds the SCM configuration for these settings.
   * @param placementSeed seed of the random choices of pipeline placement
   */
  OzoneConfiguration toOzoneConfiguration(Path metadataDir, long placementSeed) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metadataDir.toString());
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, metadataDir.toString());
    // SCM creates its RPC servers, though it is never started.
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY, "127.0.0.1:0");
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY, "127.0.0.1:0");
    conf.setClass(ScmConfigKeys.OZONE_SCM_PIPELINE_PLACEMENT_IMPL_KEY, SimPipelinePlacementPolicy.class,
        PlacementPolicy.class);
    conf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY, SimContainerPlacementPolicy.class,
        PlacementPolicy.class);
    conf.setLong(SimPipelinePlacementPolicy.SEED_KEY, placementSeed);
    conf.setTimeDuration(HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL, heartbeatIntervalMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, heartbeatProcessIntervalMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL, staleNodeIntervalMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL, deadNodeIntervalMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL, pipelineCreationIntervalMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, pipelineScrubIntervalMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_ALLOCATED_TIMEOUT, pipelineAllocatedTimeoutMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, pipelineDestroyTimeoutMs, MILLISECONDS);
    conf.setTimeDuration(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, closeContainerWaitMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_EXPIRED_CONTAINER_REPLICA_OP_SCRUB_INTERVAL,
        replicaOpScrubIntervalMs, MILLISECONDS);
    conf.setTimeDuration(ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL, adminMonitorIntervalMs, MILLISECONDS);
    // Nothing but logging runs on the safe mode log thread; keep it idle.
    conf.setTimeDuration(HddsConfigKeys.HDDS_SCM_SAFEMODE_LOG_INTERVAL, 3650, TimeUnit.DAYS);
    conf.setTimeDuration(HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, safeModeExitWaitMs, MILLISECONDS);
    // Validate safe mode rules inline, on the handler that reports progress, instead of a refresh thread.
    conf.setTimeDuration(HddsConfigKeys.HDDS_SCM_SAFEMODE_RULE_REFRESH_INTERVAL, 0, MILLISECONDS);
    conf.setInt(HddsConfigKeys.HDDS_SCM_SAFEMODE_MIN_DATANODE, Math.min(3, initialDatanodes));
    conf.setStorageSize(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE, containerSizeBytes, StorageUnit.BYTES);

    ReplicationManagerConfiguration rmConf = conf.getObject(ReplicationManagerConfiguration.class);
    rmConf.setInterval(java.time.Duration.ofMillis(replicationIntervalMs));
    rmConf.setUnderReplicatedInterval(java.time.Duration.ofMillis(underReplicatedIntervalMs));
    rmConf.setOverReplicatedInterval(java.time.Duration.ofMillis(overReplicatedIntervalMs));
    rmConf.setEventTimeout(java.time.Duration.ofMillis(replicationEventTimeoutMs));
    rmConf.setDatanodeTimeoutOffset(replicationDatanodeOffsetMs);
    conf.setFromObject(rmConf);
    return conf;
  }

  /** How long a datanode takes before it handles a command of the given type. */
  long getCommandDelayMs(SCMCommandProto.Type type, Random random) {
    switch (type) {
    case createPipelineCommand:
      return 500 + random.nextInt(2_500);
    case replicateContainerCommand:
      return 1_000 + random.nextInt(10_000);
    case deleteContainerCommand:
      return 200 + random.nextInt(3_000);
    default:
      return random.nextInt(2_000);
    }
  }

  long getElectionDelayMs(Random random) {
    return 500 + random.nextInt(3_000);
  }

  /** Time to push a replica of the given size to another datanode, at roughly 100 MB/s. */
  long getReplicationTransferMs(long bytes, Random random) {
    return 1_000 + bytes / 100_000 + random.nextInt(5_000);
  }

  long getContainerCloseThresholdBytes() {
    return (long) (containerSizeBytes * 0.9);
  }

  long getSeed() {
    return seed;
  }

  int getInitialDatanodes() {
    return initialDatanodes;
  }

  SimConfig setInitialDatanodes(int value) {
    initialDatanodes = value;
    return this;
  }

  int getRacks() {
    return racks;
  }

  /** Spreads the datanodes over the given number of racks, with enough datanodes to fill each rack. */
  SimConfig setRacks(int value) {
    racks = value;
    initialDatanodes = Math.max(initialDatanodes, value + 2);
    maxDatanodes = Math.max(maxDatanodes, initialDatanodes + 4);
    return this;
  }

  int getMaxDatanodes() {
    return maxDatanodes;
  }

  SimConfig setMaxDatanodes(int value) {
    maxDatanodes = value;
    return this;
  }

  long getChaosDurationMs() {
    return chaosDurationMs;
  }

  SimConfig setChaosDurationMs(long value) {
    chaosDurationMs = value;
    return this;
  }

  long getSettleDurationMs() {
    return settleDurationMs;
  }

  SimConfig setSettleDurationMs(long value) {
    settleDurationMs = value;
    return this;
  }

  boolean isFaults() {
    return faults;
  }

  SimConfig setFaults(boolean value) {
    faults = value;
    return this;
  }

  int getMaxUnavailableDatanodes() {
    return maxUnavailableDatanodes;
  }

  SimConfig setMaxUnavailableDatanodes(int value) {
    maxUnavailableDatanodes = value;
    return this;
  }

  double getSlowHandlerProbability() {
    return slowHandlerProbability;
  }

  SimConfig setSlowHandlerProbability(double value) {
    slowHandlerProbability = value;
    return this;
  }

  long getMaxSlowHandlerMs() {
    return maxSlowHandlerMs;
  }

  /** Where the trace of a failed run (or of every run, with keepFullTrace) is written. */
  Path getTraceDir() {
    return traceDir;
  }

  SimConfig setTraceDir(Path value) {
    traceDir = value;
    return this;
  }

  boolean isKeepFullTrace() {
    return keepFullTrace;
  }

  SimConfig setKeepFullTrace(boolean value) {
    keepFullTrace = value;
    return this;
  }

  long getHeartbeatIntervalMs() {
    return heartbeatIntervalMs;
  }

  long getNodeReportIntervalMs() {
    return nodeReportIntervalMs;
  }

  long getContainerReportIntervalMs() {
    return containerReportIntervalMs;
  }

  long getPipelineReportIntervalMs() {
    return pipelineReportIntervalMs;
  }

  long getHeartbeatProcessIntervalMs() {
    return heartbeatProcessIntervalMs;
  }

  long getStaleNodeIntervalMs() {
    return staleNodeIntervalMs;
  }

  long getDeadNodeIntervalMs() {
    return deadNodeIntervalMs;
  }

  long getReplicationIntervalMs() {
    return replicationIntervalMs;
  }

  long getUnderReplicatedIntervalMs() {
    return underReplicatedIntervalMs;
  }

  long getOverReplicatedIntervalMs() {
    return overReplicatedIntervalMs;
  }

  long getPipelineCreationIntervalMs() {
    return pipelineCreationIntervalMs;
  }

  long getPipelineScrubIntervalMs() {
    return pipelineScrubIntervalMs;
  }

  long getReplicaOpScrubIntervalMs() {
    return replicaOpScrubIntervalMs;
  }

  long getAdminMonitorIntervalMs() {
    return adminMonitorIntervalMs;
  }

  long getSafeModeExitWaitMs() {
    return safeModeExitWaitMs;
  }

  long getCloseContainerWaitMs() {
    return closeContainerWaitMs;
  }

  long getContainerSizeBytes() {
    return containerSizeBytes;
  }

  long getDatanodeCapacityBytes() {
    return datanodeCapacityBytes;
  }

  long getWriteIntervalMs() {
    return writeIntervalMs;
  }

  long getFaultIntervalMs() {
    return faultIntervalMs;
  }

  SimConfig setFaultIntervalMs(long value) {
    faultIntervalMs = value;
    return this;
  }

  int getFaultWeight(String fault, int defaultWeight) {
    return faultWeights.getOrDefault(fault, defaultWeight);
  }

  Map<String, Integer> getFaultWeights() {
    return faultWeights;
  }

  SimConfig setFaultWeight(String fault, int weight) {
    faultWeights.put(fault, weight);
    return this;
  }

  @Override
  public String toString() {
    return "seed=" + seed + ", datanodes=" + initialDatanodes + "/" + maxDatanodes + ", racks=" + racks
        + ", chaos=" + chaosDurationMs / 1000 + "s, settle=" + settleDurationMs / 1000 + "s, faults=" + faults
        + (faultWeights.isEmpty() ? "" : ", faultWeights=" + faultWeights);
  }
}
