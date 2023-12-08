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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ContainerBalancer}.
 */
@Timeout(60)
public class TestContainerBalancer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancer.class);

  private ContainerBalancer containerBalancer;
  private StorageContainerManager scm;
  private ContainerBalancerConfiguration balancerConfiguration;
  private Map<String, ByteString> serviceToConfigMap = new HashMap<>();
  private StatefulServiceStateManager serviceStateManager;
  private OzoneConfiguration conf;

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException,
      TimeoutException {
    conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT,
        5, TimeUnit.SECONDS);
    conf.setTimeDuration(HDDS_NODE_REPORT_INTERVAL, 2, TimeUnit.SECONDS);
    scm = Mockito.mock(StorageContainerManager.class);
    serviceStateManager = Mockito.mock(StatefulServiceStateManagerImpl.class);
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(10);
    // Note: this will make container balancer task to wait for running
    // for 6 sec as default and ensure below test case have sufficient
    // time to verify, and interrupt when stop.
    balancerConfiguration.setTriggerDuEnable(true);
    conf.setFromObject(balancerConfiguration);
    GenericTestUtils.setLogLevel(ContainerBalancer.LOG, Level.DEBUG);

    when(scm.getScmNodeManager()).thenReturn(mock(NodeManager.class));
    when(scm.getScmContext()).thenReturn(SCMContext.emptyContext());
    when(scm.getConfiguration()).thenReturn(conf);
    when(scm.getStatefulServiceStateManager()).thenReturn(serviceStateManager);
    when(scm.getSCMServiceManager()).thenReturn(mock(SCMServiceManager.class));
    when(scm.getMoveManager()).thenReturn(Mockito.mock(MoveManager.class));

    /*
    When StatefulServiceStateManager#saveConfiguration is called, save to
    in-memory serviceToConfigMap and read from same.
     */
    Mockito.doAnswer(i -> {
      serviceToConfigMap.put(i.getArgument(0, String.class), i.getArgument(1,
          ByteString.class));
      return null;
    }).when(serviceStateManager).saveConfiguration(
        Mockito.any(String.class),
        Mockito.any(ByteString.class));
    when(serviceStateManager.readConfiguration(Mockito.anyString())).thenAnswer(
        i -> serviceToConfigMap.get(i.getArgument(0, String.class)));

    containerBalancer = new ContainerBalancer(scm);
  }

  @Test
  public void testShouldRun() throws Exception {
    boolean doRun = containerBalancer.shouldRun();
    Assertions.assertFalse(doRun);
    containerBalancer.saveConfiguration(balancerConfiguration, true, 0);
    doRun = containerBalancer.shouldRun();
    Assertions.assertTrue(doRun);
    containerBalancer.saveConfiguration(balancerConfiguration, false, 0);
    doRun = containerBalancer.shouldRun();
    Assertions.assertFalse(doRun);
  }

  @Test
  public void testStartBalancerStop() throws Exception {
    startBalancer(balancerConfiguration);
    try {
      containerBalancer.startBalancer(balancerConfiguration);
      Assertions.assertTrue(false,
          "Exception should be thrown when startBalancer again");
    } catch (IllegalContainerBalancerStateException e) {
      // start failed again, valid case
    }

    try {
      containerBalancer.start();
      Assertions.assertTrue(false,
          "Exception should be thrown when start again");
    } catch (IllegalContainerBalancerStateException e) {
      // start failed again, valid case
    }

    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);

    stopBalancer();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);

    try {
      containerBalancer.stopBalancer();
      Assertions.assertTrue(false,
          "Exception should be thrown when stop again");
    } catch (Exception e) {
      // stop failed as already stopped, valid case
    }
  }

  @Test
  public void testStartStopSCMCalls() throws Exception {
    containerBalancer.saveConfiguration(balancerConfiguration, true, 0);
    containerBalancer.start();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);
    containerBalancer.notifyStatusChanged();
    try {
      containerBalancer.start();
      Assertions.assertTrue(false,
          "Exception should be thrown when start again");
    } catch (IllegalContainerBalancerStateException e) {
      // start failed when triggered again, valid case
    }

    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);

    containerBalancer.stop();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);
    containerBalancer.saveConfiguration(balancerConfiguration, false, 0);
  }

  @Test
  public void testNotifyStateChangeStopStart() throws Exception {
    containerBalancer.startBalancer(balancerConfiguration);

    scm.getScmContext().updateLeaderAndTerm(false, 1);
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);
    containerBalancer.notifyStatusChanged();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);
    scm.getScmContext().updateLeaderAndTerm(true, 2);
    scm.getScmContext().setLeaderReady();
    containerBalancer.notifyStatusChanged();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.RUNNING);

    containerBalancer.stop();
    Assertions.assertTrue(containerBalancer.getBalancerStatus()
        == ContainerBalancerTask.Status.STOPPED);
  }

  /**
   * This tests that ContainerBalancer rejects certain invalid configurations
   * while starting. It should fail to start in some cases.
   */
  @Test
  public void testValidationOfConfigurations() {
    conf = new OzoneConfiguration();

    conf.setTimeDuration(
        "hdds.container.balancer.move.replication.timeout", 60,
        TimeUnit.MINUTES);

    conf.setTimeDuration("hdds.container.balancer.move.timeout", 59,
        TimeUnit.MINUTES);

    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    Assertions.assertThrowsExactly(
        InvalidContainerBalancerConfigurationException.class,
        () -> containerBalancer.startBalancer(balancerConfiguration),
        "hdds.container.balancer.move.replication.timeout should " +
            "be less than hdds.container.balancer.move.timeout.");
  }

  /**
   * Tests that ContainerBalancerTask starts with a delay of
   * "hdds.scm.wait.time.after.safemode.exit" when ContainerBalancer receives
   * status change notification in
   * {@link ContainerBalancer#notifyStatusChanged()}.
   */
  @Test
  public void testDelayedStartOnSCMStatusChange()
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException,
      InterruptedException {
    long delayDuration = conf.getTimeDuration(
        HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT, 10, TimeUnit.SECONDS);
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);

    // Start the ContainerBalancer service.
    containerBalancer.startBalancer(balancerConfiguration);
    GenericTestUtils.waitFor(() -> containerBalancer.isBalancerRunning(), 1,
        20);
    Assertions.assertTrue(containerBalancer.isBalancerRunning());

    // Balancer should stop the current balancing thread when it receives a
    // status change notification
    scm.getScmContext().updateLeaderAndTerm(false, 1);
    containerBalancer.notifyStatusChanged();
    Assertions.assertFalse(containerBalancer.isBalancerRunning());

    GenericTestUtils.LogCapturer logCapturer =
        GenericTestUtils.LogCapturer.captureLogs(ContainerBalancerTask.LOG);
    String expectedLog = "ContainerBalancer will sleep for " + delayDuration +
        " seconds before starting balancing.";
    /*
     Send a status change notification again and check whether balancer
     starts balancing. We're actually just checking for the expected log
     line here.
     */
    scm.getScmContext().updateLeaderAndTerm(true, 2);
    scm.getScmContext().setLeaderReady();
    containerBalancer.notifyStatusChanged();
    Assertions.assertTrue(containerBalancer.isBalancerRunning());
    Thread balancingThread = containerBalancer.getCurrentBalancingThread();
    GenericTestUtils.waitFor(
        () -> balancingThread.getState() == Thread.State.TIMED_WAITING, 2, 20);
    Assertions.assertTrue(logCapturer.getOutput().contains(expectedLog));
    stopBalancer();
  }

  private void startBalancer(ContainerBalancerConfiguration config)
      throws IllegalContainerBalancerStateException, IOException,
      InvalidContainerBalancerConfigurationException, TimeoutException {
    containerBalancer.startBalancer(config);
  }

  private void stopBalancer() {
    try {
      if (containerBalancer.isBalancerRunning()) {
        containerBalancer.stopBalancer();
      }
    } catch (IOException | IllegalContainerBalancerStateException e) {
      LOG.warn("Failed to stop balancer", e);
    }
  }
}
