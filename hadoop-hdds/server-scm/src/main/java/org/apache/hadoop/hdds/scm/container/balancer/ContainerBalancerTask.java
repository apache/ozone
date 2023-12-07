/*
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

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.balancer.iteration.ContainerBalanceIteration;
import org.apache.hadoop.hdds.scm.node.DatanodeUsageInfo;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL_DEFAULT;

/**
 * Container balancer task performs move of containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancerTask {

  public static final Logger LOG = LoggerFactory.getLogger(ContainerBalancerTask.class);
  private final ContainerBalancer containerBalancer;
  private final StorageContainerManager scm;

  private final ContainerBalancerConfiguration config;
  private volatile Status taskStatus = Status.RUNNING;
  private ContainerBalanceIteration it;

  /**
   * Constructs ContainerBalancerTask with the specified arguments.
   *
   * @param scm                the storage container manager
   * @param containerBalancer  the container balancer
   * @param config             the config
   */
  public ContainerBalancerTask(
      @Nonnull StorageContainerManager scm,
      @Nonnull ContainerBalancer containerBalancer,
      @Nonnull ContainerBalancerConfiguration config
  ) {
    this.scm = scm;
    this.containerBalancer = containerBalancer;
    this.config = config;
  }

  /**
   * Run the container balancer task.
   */
  public void run(int nextIterationIndex, boolean delayStart) {
    try {
      if (delayStart) {
        long delayDuration = scm.getConfiguration().getTimeDuration(
            HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
            HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT_DEFAULT,
            TimeUnit.SECONDS);
        LOG.info("ContainerBalancer will sleep for {} seconds before" +
                " starting balancing.",
            delayDuration);
        Thread.sleep(Duration.ofSeconds(delayDuration).toMillis());
      }
      balance(nextIterationIndex, config.getIterations());
    } catch (Exception e) {
      LOG.error("Container Balancer is stopped abnormally, ", e);
    } finally {
      synchronized (this) {
        taskStatus = Status.STOPPED;
      }
    }
  }

  /**
   * Changes the status from RUNNING to STOPPING.
   */
  public void stop() {
    synchronized (this) {
      if (taskStatus == Status.RUNNING) {
        taskStatus = Status.STOPPING;
      }
    }
  }

  private void balance(int nextIterationIndex, int iterationCount) {
    // nextIterationIndex is the iteration that balancer should start from on
    // leader change or restart
    for (int i = nextIterationIndex; i < iterationCount && isBalancerRunning(); ++i) {
      // reset some variables and metrics for this iteration
      if (it != null) {
        it.resetState();
      }

      if (!balancerIsOk()) {
        return;
      }

      List<DatanodeUsageInfo> datanodeUsageInfos = getDatanodeUsageInfos();
      if (datanodeUsageInfos == null) {
        return;
      }

      it = new ContainerBalanceIteration(containerBalancer.getMetrics(), config,
          scm, datanodeUsageInfos);

      // initialize this iteration. stop balancing on initialization failure
      if (!it.findUnBalancedNodes(this::isBalancerRunning, datanodeUsageInfos)) {
        // Try to stop balancer
        tryStopWithSaveConfiguration("Could not initialize " +
            "ContainerBalancer's iteration number " + i);
        return;
      }

      IterationResult iterationResult =
          it.doIteration(this::isBalancerRunning, config);

      LOG.info("Result of this iteration of Container Balancer: {}",
          iterationResult);

      // if no new move option is generated, it means the cluster cannot be
      // balanced anymore; so just stop balancer
      if (iterationResult == IterationResult.CAN_NOT_BALANCE_ANY_MORE) {
        tryStopWithSaveConfiguration(iterationResult.toString());
        return;
      }

      // persist next iteration index
      if (iterationResult == IterationResult.ITERATION_COMPLETED) {
        try {
          saveConfiguration(config, true, i + 1);
        } catch (IOException | TimeoutException e) {
          LOG.warn("Could not persist next iteration index value for " +
              "ContainerBalancer after completing an iteration", e);
        }
      }

      // return if balancing has been stopped
      if (!isBalancerRunning()) {
        return;
      }

      // wait for configured time before starting next iteration, unless
      // this was the final iteration
      if (i != iterationCount - 1) {
        try {
          Thread.sleep(config.getBalancingInterval().toMillis());
        } catch (InterruptedException e) {
          LOG.info("Container Balancer was interrupted while waiting for" +
              " next iteration.");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }

    tryStopWithSaveConfiguration("Completed all iterations.");
  }

  private @Nullable List<DatanodeUsageInfo> getDatanodeUsageInfos() {
    // sorted list in order from most to least used
    List<DatanodeUsageInfo> datanodeUsageInfos =
        scm.getScmNodeManager().getMostOrLeastUsedDatanodes(true);
    if (datanodeUsageInfos.isEmpty()) {
      LOG.warn("Received an empty list of datanodes from Node Manager when " +
          "trying to identify which nodes to balance");
      return null;
    }
    // include/exclude nodes from balancing according to configs
    datanodeUsageInfos.removeIf(dnUsageInfo -> shouldExcludeDatanode(config,
        dnUsageInfo.getDatanodeDetails()));
    return datanodeUsageInfos;
  }

  private boolean balancerIsOk() {
    boolean result = isBalancerRunning();
    result = config.getTriggerDuEnable() ? runCommandDU() : result;
    return result & scmStateIsValid();
  }

  private boolean runCommandDU() {
    // before starting a new iteration, we trigger all the datanode
    // to run `du`. this is an aggressive action, with which we can
    // get more precise usage info of all datanodes before moving.
    // this is helpful for container balancer to make more appropriate
    // decisions. this will increase the disk io load of data nodes, so
    // please enable it with caution.
    scm.getScmNodeManager().refreshAllHealthyDnUsageInfo();
    try {
      long nodeReportInterval = scm.getConfiguration().getTimeDuration(
          HDDS_NODE_REPORT_INTERVAL,
          HDDS_NODE_REPORT_INTERVAL_DEFAULT,
          TimeUnit.MILLISECONDS
      );
      // one for sending command , one for running du, and one for
      // reporting back make it like this for now, a more suitable
      // value. can be set in the future if needed
      long sleepTime = 3 * nodeReportInterval;
      LOG.info("ContainerBalancer will sleep for {} ms while waiting " +
          "for updated usage information from Datanodes.", sleepTime);
      Thread.sleep(sleepTime);
      return true;
    } catch (InterruptedException e) {
      LOG.info("Container Balancer was interrupted while waiting for" +
          "datanodes refreshing volume usage info");
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Logs the reason for stop and save configuration and stop the task.
   *
   * @param stopReason a string specifying the reason for stop
   */
  private void tryStopWithSaveConfiguration(@Nonnull String stopReason) {
    synchronized (this) {
      try {
        LOG.info("Save Configuration for stopping. Reason: {}", stopReason);
        saveConfiguration(config, false, 0);
        stop();
      } catch (IOException | TimeoutException e) {
        LOG.warn("Save configuration failed. Reason for " +
            "stopping: {}", stopReason, e);
      }
    }
  }

  private void saveConfiguration(
      @Nonnull ContainerBalancerConfiguration configuration,
      boolean shouldRun,
      int index
  ) throws IOException, TimeoutException {
    if (!scmStateIsValid()) {
      LOG.warn("Save configuration is not allowed as not in valid State.");
      return;
    }
    synchronized (this) {
      if (isBalancerRunning()) {
        containerBalancer.saveConfiguration(configuration, shouldRun, index);
      }
    }
  }

  private boolean scmStateIsValid() {
    if (scm.getScmContext().isInSafeMode()) {
      LOG.error("Container Balancer cannot operate while SCM is in Safe Mode.");
      return false;
    }
    if (!scm.getScmContext().isLeaderReady()) {
      LOG.warn("Current SCM is not the leader.");
      return false;
    }
    return true;
  }

  /**
   * Consults the configurations
   * {@link ContainerBalancerConfiguration#includeNodes} and
   * {@link ContainerBalancerConfiguration#excludeNodes} to check
   * if the specified Datanode should be excluded from balancing.
   *
   * @param config
   * @param datanode DatanodeDetails to check
   * @return true if Datanode should be excluded, else false
   */
  private static boolean shouldExcludeDatanode(
      @Nonnull ContainerBalancerConfiguration config,
      @Nonnull DatanodeDetails datanode
  ) {
    Set<String> excludeNodes = config.getExcludeNodes();
    String hostName = datanode.getHostName();
    String ipAddress = datanode.getIpAddress();
    if (excludeNodes.contains(hostName) || excludeNodes.contains(ipAddress)) {
      return true;
    } else {
      Set<String> includeNodes = config.getIncludeNodes();
      return
          !includeNodes.isEmpty() &&
              !includeNodes.contains(hostName) &&
              !includeNodes.contains(ipAddress);
    }
  }

  /**
   * Gets the list of under utilized nodes in the cluster.
   *
   * @return List of DatanodeUsageInfo containing under utilized nodes.
   */
  @VisibleForTesting
  List<DatanodeUsageInfo> getUnderUtilizedNodes() {
    return it.getUnderUtilizedNodes();
  }

  /**
   * Gets the list of over utilized nodes in the cluster.
   *
   * @return List of DatanodeUsageInfo containing over utilized nodes.
   */
  @VisibleForTesting
  List<DatanodeUsageInfo> getOverUtilizedNodes() {
    return it.getOverUtilizedNodes();
  }

  /**
   * Gets a map with selected containers and their source datanodes.
   *
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToSourceMap() {
    return it.getContainerToSourceMap();
  }

  /**
   * Gets a map with selected containers and target datanodes.
   *
   * @return map with mappings from {@link ContainerID} to
   * {@link DatanodeDetails}.
   */
  @VisibleForTesting
  Map<ContainerID, DatanodeDetails> getContainerToTargetMap() {
    return it.getContainerToTargetMap();
  }

  @VisibleForTesting
  Set<DatanodeDetails> getSelectedTargets() {
    return it.getSelectedTargets();
  }

  @VisibleForTesting
  int getCountDatanodesInvolvedPerIteration() {
    return it.getDatanodeCountUsedIteration();
  }

  @VisibleForTesting
  public long getSizeScheduledForMoveInLatestIteration() {
    return it.getSizeScheduledForMoveInLatestIteration();
  }

  @VisibleForTesting
  public ContainerBalancerMetrics getMetrics() {
    return it.getMetrics();
  }

  @VisibleForTesting
  IterationResult getIterationResult() {
    return it.getIterationResult();
  }

  public boolean isBalancerRunning() {
    return taskStatus == Status.RUNNING;
  }

  public boolean isStopped() {
    return taskStatus == Status.STOPPED;
  }

  public Status getBalancerStatus() {
    return taskStatus;
  }

  @Override
  public String toString() {
    String status = String.format("%nContainer Balancer Task status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", isBalancerRunning());
    return status + config.toString();
  }

  /**
   * The result of {@link ContainerBalancerTask#doIteration}.
   */
  public enum IterationResult {
    ITERATION_COMPLETED,
    ITERATION_INTERRUPTED,
    CAN_NOT_BALANCE_ANY_MORE
  }

  /**
   * The status of {@link ContainerBalancerTask}.
   */
  enum Status {
    RUNNING,
    STOPPING,
    STOPPED
  }
}
