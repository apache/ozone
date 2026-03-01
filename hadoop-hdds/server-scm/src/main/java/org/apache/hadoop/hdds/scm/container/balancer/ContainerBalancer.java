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

package org.apache.hadoop.hdds.scm.container.balancer;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.fs.DUFactory;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerBalancerConfigurationProto;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.StatefulService;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Container balancer is a service in SCM to move containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancer extends StatefulService {

  private static final AtomicInteger ID = new AtomicInteger();

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancer.class);

  private StorageContainerManager scm;
  private final SCMContext scmContext;
  private OzoneConfiguration ozoneConfiguration;
  private ContainerBalancerConfiguration config;
  private ContainerBalancerMetrics metrics;
  private volatile Thread currentBalancingThread;
  private volatile ContainerBalancerTask task = null;
  private ReentrantLock lock;
  private OffsetDateTime startedAt;

  /**
   * Constructs ContainerBalancer with the specified arguments. Initializes
   * ContainerBalancerMetrics. Container Balancer does not start on
   * construction.
   *
   * @param scm the storage container manager
   */
  public ContainerBalancer(StorageContainerManager scm) {
    super(scm.getStatefulServiceStateManager());
    this.scm = scm;
    this.ozoneConfiguration = scm.getConfiguration();
    this.config = ozoneConfiguration.getObject(
        ContainerBalancerConfiguration.class);
    this.scmContext = scm.getScmContext();
    this.metrics = ContainerBalancerMetrics.create();

    this.lock = new ReentrantLock();
    scm.getSCMServiceManager().register(this);
  }

  /**
   * Receives a notification for raft or safe mode related status changes.
   * Stops ContainerBalancer if it's running and the current SCM becomes a
   * follower or enters safe mode. Starts ContainerBalancer if the current
   * SCM becomes leader, is out of safe mode and balancer should run.
   */
  @Override
  public void notifyStatusChanged() {
    if (!scmContext.isLeader() || scmContext.isInSafeMode()) {
      boolean shouldStop;
      lock.lock();
      try {
        shouldStop = canBalancerStop();
      } finally {
        lock.unlock();
      }
      if (shouldStop) {
        LOG.info("Stopping ContainerBalancer in this scm on status change");
        stop();
      }
      return;
    }

    lock.lock();
    try {
      // else check for start
      boolean shouldRun = shouldRun();
      if (shouldRun && !canBalancerStart()) {
        LOG.warn("Could not start ContainerBalancer on notify," +
            " might be stopped");
      }
      if (shouldRun && canBalancerStart()) {
        LOG.info("Starting ContainerBalancer in this scm on status change");
        try {
          start();
        } catch (IllegalContainerBalancerStateException |
                 InvalidContainerBalancerConfigurationException e) {
          LOG.warn("Could not start ContainerBalancer on raft/safe-mode " +
              "status change.", e);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Checks if ContainerBalancer should start (after a leader change, restart
   * etc.) by reading persisted state.
   * @return true if the persisted state is true, otherwise false
   */
  @Override
  public boolean shouldRun() {
    try {
      ContainerBalancerConfigurationProto proto =
          readConfiguration(ContainerBalancerConfigurationProto.class);
      if (proto == null) {
        LOG.warn("Could not find persisted configuration for {} when checking" +
            " if ContainerBalancer should run. ContainerBalancer should not " +
            "run now.", this.getServiceName());
        return false;
      }
      return proto.getShouldRun();
    } catch (IOException e) {
      LOG.warn("Could not read persisted configuration for checking if " +
          "ContainerBalancer should start. ContainerBalancer should not start" +
          " now.", e);
      return false;
    }
  }

  /**
   * Checks if ContainerBalancer is currently running in this SCM.
   *
   * @return true if balancer started, otherwise false
   */
  public boolean isBalancerRunning() {
    return (null != task
        && task.getBalancerStatus() == ContainerBalancerTask.Status.RUNNING);
  }

  /**
   * Checks if ContainerBalancer in valid state and can be started.
   *
   * @return true if balancer can be started, otherwise false
   */
  private boolean canBalancerStart() {
    return (null == task
        || task.getBalancerStatus() == ContainerBalancerTask.Status.STOPPED);
  }

  /**
   * get the Container Balancer state.
   *
   * @return true if balancer started, otherwise false
   */
  public ContainerBalancerTask.Status getBalancerStatus() {
    return null != task ? task.getBalancerStatus()
        : ContainerBalancerTask.Status.STOPPED;
  }

  /**
   * Get balancer status info.
   *
   * @return balancer status info if balancer started
   */
  public ContainerBalancerStatusInfo getBalancerStatusInfo() throws IOException {
    lock.lock();
    try {
      if (isBalancerRunning()) {
        return new ContainerBalancerStatusInfo(
            this.startedAt,
            config.toProtobufBuilder().setShouldRun(true).build(),
            task.getCurrentIterationsStatistic()
        );
      }
      return null;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Checks if ContainerBalancer is in valid state to call stop.
   *
   * @return true if balancer can be stopped, otherwise false
   */
  private boolean canBalancerStop() {
    return isBalancerRunning();
  }

  /**
   * @return Name of this service.
   */
  @Override
  public String getServiceName() {
    return ContainerBalancer.class.getSimpleName();
  }

  /**
   * Starts ContainerBalancer as an SCMService. Validates state, reads and
   * validates persisted configuration, and then starts the balancing
   * thread.
   * @throws IllegalContainerBalancerStateException if balancer should not
   * run according to persisted configuration
   * @throws InvalidContainerBalancerConfigurationException if failed to
   * retrieve persisted configuration, or the configuration is null
   */
  @Override
  public void start() throws IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException {
    startedAt = OffsetDateTime.now();
    lock.lock();
    try {
      // should be leader-ready, out of safe mode, and not running already
      validateState(false);
      ContainerBalancerConfigurationProto proto;
      try {
        proto = readConfiguration(ContainerBalancerConfigurationProto.class);
      } catch (IOException e) {
        throw new InvalidContainerBalancerConfigurationException("Could not " +
            "retrieve persisted configuration while starting " +
            "Container Balancer as an SCMService. Will not start now.", e);
      }
      if (proto == null) {
        throw new InvalidContainerBalancerConfigurationException("Persisted " +
            "configuration for ContainerBalancer is null during start. Will " +
            "not start now.");
      }
      if (!proto.getShouldRun()) {
        throw new IllegalContainerBalancerStateException("According to " +
            "persisted configuration, ContainerBalancer should not run.");
      }
      ContainerBalancerConfiguration configuration =
          ContainerBalancerConfiguration.fromProtobuf(proto,
              ozoneConfiguration);
      validateConfiguration(configuration);
      this.config = configuration;
      startBalancingThread(proto.getNextIterationIndex(), true);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Starts Container Balancer after checking its state and validating
   * configuration.
   *
   * @throws IllegalContainerBalancerStateException if ContainerBalancer is
   * not in a start-appropriate state
   * @throws InvalidContainerBalancerConfigurationException if
   * {@link ContainerBalancerConfiguration} config file is incorrectly
   * configured
   * @throws IOException on failure to persist
   * {@link ContainerBalancerConfiguration}
   */
  public void startBalancer(ContainerBalancerConfiguration configuration)
      throws IllegalContainerBalancerStateException,
      InvalidContainerBalancerConfigurationException, IOException {
    startedAt = OffsetDateTime.now();
    lock.lock();
    try {
      // validates state, config, and then saves config
      validateState(false);
      validateConfiguration(configuration);
      saveConfiguration(configuration, true, 0);
      this.config = configuration;

      //start balancing task
      startBalancingThread(0, false);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Starts a new balancing thread asynchronously.
   */
  private void startBalancingThread(int nextIterationIndex,
      boolean delayStart) {
    String prefix = scmContext.threadNamePrefix();
    task = new ContainerBalancerTask(scm, nextIterationIndex, this, metrics,
        config, delayStart);
    Thread thread = new Thread(task);
    thread.setName(prefix + "ContainerBalancerTask-" + ID.incrementAndGet());
    thread.setDaemon(true);
    thread.start();
    currentBalancingThread = thread;
    LOG.info("Starting Container Balancer {}... {}", thread, this);
  }

  /**
   * Validates balancer's eligibility based on SCM state.
   * Confirms SCM is leader-ready and out of safe mode.
   *
   * @throws IllegalContainerBalancerStateException if SCM is not
   * leader-ready or is in safe mode
   */
  private void validateEligibility()
      throws IllegalContainerBalancerStateException {
    if (!scmContext.isLeaderReady()) {
      LOG.warn("SCM is not leader ready");
      throw new IllegalContainerBalancerStateException("SCM is not leader " +
          "ready");
    }
    if (scmContext.isInSafeMode()) {
      LOG.warn("SCM is in safe mode");
      throw new IllegalContainerBalancerStateException("SCM is in safe mode");
    }
  }

  /**
   * Validates balancer's state based on the specified expectedRunning.
   *
   * @param expectedRunning true if ContainerBalancer is expected to be
   *                        running, else false
   * @throws IllegalContainerBalancerStateException if state does not
   * match the specified expected state
   */
  private void validateState(boolean expectedRunning)
      throws IllegalContainerBalancerStateException {
    validateEligibility();
    if (!expectedRunning && !canBalancerStart()) {
      throw new IllegalContainerBalancerStateException(
          "Expect ContainerBalancer as not running state" +
              ", but running state is actually " + getBalancerStatus());
    }
    if (expectedRunning && !canBalancerStop()) {
      throw new IllegalContainerBalancerStateException(
          "Expect ContainerBalancer as running state" +
              ", but running state is actually " + getBalancerStatus());
    }
  }

  /**
   * Stops the ContainerBalancer thread in this SCM.
   */
  @Override
  public void stop() {
    lock.lock();
    Thread balancingThread;
    try {
      if (!canBalancerStop()) {
        LOG.warn("Cannot stop Container Balancer because it's not running or " +
            "stopping");
        return;
      }
      LOG.info("Trying to stop ContainerBalancer in this SCM.");
      task.stop();
      balancingThread = currentBalancingThread;
    } finally {
      lock.unlock();
    }

    blockTillTaskStop(balancingThread);
  }

  private static void blockTillTaskStop(Thread balancingThread) {
    // NOTE: join should be called outside the lock in hierarchy
    // to avoid locking others waiting
    // wait for balancingThread to die with interrupt
    LOG.info("Container Balancer waiting for {} to stop", balancingThread);
    try {
      while (balancingThread.isAlive()) {
        // retry interrupt every 5ms to avoid waiting when thread is sleeping
        balancingThread.interrupt();
        balancingThread.join(5);
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
    LOG.info("Container Balancer stopped successfully.");
  }

  /**
   * Stops ContainerBalancer gracefully. Persists state such that
   * {@link ContainerBalancer#shouldRun()} will return false. This is the
   * "stop" command.
   */
  public void stopBalancer()
      throws IOException, IllegalContainerBalancerStateException {
    Thread balancingThread = null;
    lock.lock();
    try {
      validateEligibility();
      saveConfiguration(config, false, 0);
      if (isBalancerRunning()) {
        LOG.info("Trying to stop ContainerBalancer service.");
        task.stop();
        balancingThread = currentBalancingThread;
      }
    } finally {
      lock.unlock();
    }
    if (balancingThread != null) {
      blockTillTaskStop(balancingThread);
    }
  }

  public void saveConfiguration(ContainerBalancerConfiguration configuration,
                                boolean shouldRun, int index)
      throws IOException {
    config = configuration;
    saveConfiguration(configuration.toProtobufBuilder()
        .setShouldRun(shouldRun)
        .setNextIterationIndex(index)
        .build());
  }

  @VisibleForTesting
  public ContainerBalancerConfiguration getConfig() {
    return this.config;
  }

  private void validateConfiguration(ContainerBalancerConfiguration conf)
      throws InvalidContainerBalancerConfigurationException {
    // maxSizeEnteringTarget and maxSizeLeavingSource should by default be
    // greater than container size
    long size = (long) ozoneConfiguration.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, StorageUnit.BYTES);

    if (conf.getMaxSizeEnteringTarget() <= size) {
      LOG.warn("hdds.container.balancer.size.entering.target.max {} should " +
              "be greater than ozone.scm.container.size {}",
          conf.getMaxSizeEnteringTarget(), size);
      throw new InvalidContainerBalancerConfigurationException(
          "hdds.container.balancer.size.entering.target.max should be greater" +
              " than ozone.scm.container.size");
    }
    if (conf.getMaxSizeLeavingSource() <= size) {
      LOG.warn("hdds.container.balancer.size.leaving.source.max {} should " +
              "be greater than ozone.scm.container.size {}",
          conf.getMaxSizeLeavingSource(), size);
      throw new InvalidContainerBalancerConfigurationException(
          "hdds.container.balancer.size.leaving.source.max should be greater" +
              " than ozone.scm.container.size");
    }

    // balancing interval should be greater than DUFactory refresh period
    DUFactory.Conf duConf = ozoneConfiguration.getObject(DUFactory.Conf.class);
    long refreshPeriod = duConf.getRefreshPeriod().toMillis();
    if (conf.getBalancingInterval().toMillis() <= refreshPeriod) {
      LOG.warn("hdds.container.balancer.balancing.iteration.interval {} " +
              "should be greater than hdds.datanode.du.refresh.period {}",
          conf.getBalancingInterval().toMillis(), refreshPeriod);
    }

    // "move.replication.timeout" should be lesser than "move.timeout"
    if (conf.getMoveReplicationTimeout().toMillis() >=
        conf.getMoveTimeout().toMillis()) {
      LOG.warn("hdds.container.balancer.move.replication.timeout {} should " +
              "be less than hdds.container.balancer.move.timeout {}.",
          conf.getMoveReplicationTimeout().toMinutes(),
          conf.getMoveTimeout().toMinutes());
      throw new InvalidContainerBalancerConfigurationException(
          "hdds.container.balancer.move.replication.timeout should " +
          "be less than hdds.container.balancer.move.timeout.");
    }

    // (move.timeout - move.replication.timeout - event.timeout.datanode.offset)
    // should be greater than or equal to 9 minutes
    long datanodeOffset = ozoneConfiguration.getTimeDuration("hdds.scm.replication.event.timeout.datanode.offset",
        Duration.ofMinutes(6).toMillis(), TimeUnit.MILLISECONDS);
    if ((conf.getMoveTimeout().toMillis() - conf.getMoveReplicationTimeout().toMillis() - datanodeOffset)
        < Duration.ofMinutes(9).toMillis()) {
      String msg = String.format("(hdds.container.balancer.move.timeout (%sms) - " +
              "hdds.container.balancer.move.replication.timeout (%sms) - " +
              "hdds.scm.replication.event.timeout.datanode.offset (%sms)) " +
              "should be greater than or equal to 540000ms or 9 minutes.",
          conf.getMoveTimeout().toMillis(),
          conf.getMoveReplicationTimeout().toMillis(),
          Duration.ofMillis(datanodeOffset).toMillis());
      LOG.warn(msg);
      throw new InvalidContainerBalancerConfigurationException(msg);
    }

    validateNodeList(conf.getIncludeNodes(), "included");
    validateNodeList(conf.getExcludeNodes(), "excluded");
  }

  public ContainerBalancerMetrics getMetrics() {
    return metrics;
  }

  @VisibleForTesting
  Thread getCurrentBalancingThread() {
    return currentBalancingThread;
  }

  @Override
  public String toString() {
    String status = String.format("%nContainer Balancer status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", isBalancerRunning());
    return status + config.toString();
  }

  /**
   * Validates if the provided datanodes are known by SCM.
   *
   * @param nodes set of datanode hostnames or IP addresses
   * @param type context label for the error message
   * @throws InvalidContainerBalancerConfigurationException if a node is unknown
   */
  private void validateNodeList(Set<String> nodes, String type)
      throws InvalidContainerBalancerConfigurationException {
    if (nodes == null || nodes.isEmpty()) {
      return;
    }

    for (String node : nodes) {
      // Check if SCM knows about this node by hostname or IP
      if (scm.getScmNodeManager().getNodesByAddress(node).isEmpty()) {
        String errorMessage = String.format("Invalid configuration: The %s datanode '%s' " +
                "does not exist or is not registered with SCM. Please check the hostname/IP.",
            type, node);
        LOG.warn(errorMessage);
        throw new InvalidContainerBalancerConfigurationException(errorMessage);
      }
    }
  }
}
