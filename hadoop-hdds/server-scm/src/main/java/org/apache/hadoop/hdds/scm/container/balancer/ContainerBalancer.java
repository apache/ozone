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

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Container balancer is a service in SCM to move containers between over- and
 * under-utilized datanodes.
 */
public class ContainerBalancer extends StatefulService {

  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerBalancer.class);

  private StorageContainerManager scm;
  private final SCMContext scmContext;
  private OzoneConfiguration ozoneConfiguration;
  private ContainerBalancerConfiguration config;
  private ContainerBalancerMetrics metrics;
  private volatile Thread currentBalancingThread;
  private volatile ContainerBalancerTask task = null;
  private ReentrantLock lock;

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
      startBalancingThread(proto.getNextIterationIndex());
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
      InvalidContainerBalancerConfigurationException, IOException,
      TimeoutException {
    lock.lock();
    try {
      // validates state, config, and then saves config
      validateState(false);
      validateConfiguration(configuration);
      saveConfiguration(configuration, true, 0);
      this.config = configuration;

      //start balancing task
      startBalancingThread(0);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Starts a new balancing thread asynchronously.
   */
  private void startBalancingThread(int nextIterationIndex) {
    task = new ContainerBalancerTask(scm, nextIterationIndex, this, metrics,
        config);
    currentBalancingThread = new Thread(task);
    currentBalancingThread.setName("ContainerBalancerTask");
    currentBalancingThread.setDaemon(true);
    currentBalancingThread.start();
    LOG.info("Starting Container Balancer... {}", this);
  }

  /**
   * Validates balancer's state based on the specified expectedRunning.
   * Confirms SCM is leader-ready and out of safe mode.
   *
   * @param expectedRunning true if ContainerBalancer is expected to be
   *                        running, else false
   * @throws IllegalContainerBalancerStateException if SCM is not
   * leader-ready, is in safe mode, or state does not match the specified
   * expected state
   */
  private void validateState(boolean expectedRunning)
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
    balancingThread.interrupt();
    try {
      balancingThread.join();
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
      throws IOException, IllegalContainerBalancerStateException,
      TimeoutException {
    Thread balancingThread;
    lock.lock();
    try {
      validateState(true);
      saveConfiguration(config, false, 0);
      task.stop();
      balancingThread = currentBalancingThread;
    } finally {
      lock.unlock();
    }
    blockTillTaskStop(balancingThread);
  }

  public void saveConfiguration(ContainerBalancerConfiguration configuration,
                                boolean shouldRun, int index)
      throws IOException, TimeoutException {
    config = configuration;
    saveConfiguration(configuration.toProtobufBuilder()
        .setShouldRun(shouldRun)
        .setNextIterationIndex(index)
        .build());
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
          conf.getBalancingInterval(), refreshPeriod);
    }
  }

  public ContainerBalancerMetrics getMetrics() {
    return metrics;
  }

  @Override
  public String toString() {
    String status = String.format("%nContainer Balancer status:%n" +
        "%-30s %s%n" +
        "%-30s %b%n", "Key", "Value", "Running", getBalancerStatus());
    return status + config.toString();
  }
}
