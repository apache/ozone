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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_NODE_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_WAIT_TIME_AFTER_SAFE_MODE_EXIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManagerImpl;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Tests for {@link ContainerBalancer}.
 */
public class TestContainerBalancer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestContainerBalancer.class);

  private ContainerBalancer containerBalancer;
  private StorageContainerManager scm;
  private ContainerBalancerConfiguration balancerConfiguration;
  private Map<String, ByteString> serviceToConfigMap = new HashMap<>();
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
    scm = mock(StorageContainerManager.class);
    StatefulServiceStateManager serviceStateManager = mock(StatefulServiceStateManagerImpl.class);
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(10);
    // Note: this will make container balancer task to wait for running
    // for 6 sec as default and ensure below test case have sufficient
    // time to verify, and interrupt when stop.
    balancerConfiguration.setTriggerDuEnable(true);
    conf.setFromObject(balancerConfiguration);
    GenericTestUtils.setLogLevel(ContainerBalancer.class, Level.DEBUG);

    when(scm.getScmNodeManager()).thenReturn(mock(NodeManager.class));
    when(scm.getScmContext()).thenReturn(SCMContext.emptyContext());
    when(scm.getConfiguration()).thenReturn(conf);
    when(scm.getStatefulServiceStateManager()).thenReturn(serviceStateManager);
    when(scm.getSCMServiceManager()).thenReturn(mock(SCMServiceManager.class));
    when(scm.getMoveManager()).thenReturn(mock(MoveManager.class));

    /*
    When StatefulServiceStateManager#saveConfiguration is called, save to
    in-memory serviceToConfigMap and read from same.
     */
    doAnswer(i -> {
      serviceToConfigMap.put(i.getArgument(0, String.class), i.getArgument(1,
          ByteString.class));
      return null;
    }).when(serviceStateManager).saveConfiguration(
        any(String.class),
        any(ByteString.class));
    when(serviceStateManager.readConfiguration(anyString())).thenAnswer(
        i -> serviceToConfigMap.get(i.getArgument(0, String.class)));

    containerBalancer = new ContainerBalancer(scm);
  }

  @Test
  public void testShouldRun() throws Exception {
    boolean doRun = containerBalancer.shouldRun();
    assertFalse(doRun);
    containerBalancer.saveConfiguration(balancerConfiguration, true, 0);
    doRun = containerBalancer.shouldRun();
    assertTrue(doRun);
    containerBalancer.saveConfiguration(balancerConfiguration, false, 0);
    doRun = containerBalancer.shouldRun();
    assertFalse(doRun);
  }

  @Test
  public void testStartBalancerStop() throws Exception {
    //stop should not throw an exception as it is idempotent
    assertDoesNotThrow(() -> containerBalancer.stopBalancer());
    startBalancer(balancerConfiguration);
    assertThrows(IllegalContainerBalancerStateException.class,
        () -> containerBalancer.startBalancer(balancerConfiguration),
        "Exception should be thrown when startBalancer again");

    assertThrows(IllegalContainerBalancerStateException.class,
        () -> containerBalancer.start(),
        "Exception should be thrown when start again");

    assertSame(ContainerBalancerTask.Status.RUNNING, containerBalancer.getBalancerStatus());

    stopBalancer();
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());

    // If the balancer is already stopped, the stop command should do nothing
    // and return successfully as stopBalancer is idempotent
    assertDoesNotThrow(() -> containerBalancer.stopBalancer());
  }

  @Test
  public void testStartStopSCMCalls() throws Exception {
    containerBalancer.saveConfiguration(balancerConfiguration, true, 0);
    containerBalancer.start();
    assertSame(ContainerBalancerTask.Status.RUNNING, containerBalancer.getBalancerStatus());
    containerBalancer.notifyStatusChanged();

    assertThrows(IllegalContainerBalancerStateException.class,
        () -> containerBalancer.start(),
        "Exception should be thrown when start again");

    assertSame(ContainerBalancerTask.Status.RUNNING, containerBalancer.getBalancerStatus());

    containerBalancer.stop();
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());
    containerBalancer.saveConfiguration(balancerConfiguration, false, 0);
  }

  @Test
  public void testNotifyStateChangeStopStart() throws Exception {
    containerBalancer.startBalancer(balancerConfiguration);

    scm.getScmContext().updateLeaderAndTerm(false, 1);
    assertSame(ContainerBalancerTask.Status.RUNNING, containerBalancer.getBalancerStatus());
    containerBalancer.notifyStatusChanged();
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());
    scm.getScmContext().updateLeaderAndTerm(true, 2);
    scm.getScmContext().setLeaderReady();
    containerBalancer.notifyStatusChanged();
    assertSame(ContainerBalancerTask.Status.RUNNING, containerBalancer.getBalancerStatus());

    containerBalancer.stop();
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());
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
    assertThrowsExactly(
        InvalidContainerBalancerConfigurationException.class,
        () -> containerBalancer.startBalancer(balancerConfiguration),
        "hdds.container.balancer.move.replication.timeout should " +
            "be less than hdds.container.balancer.move.timeout.");
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());

    conf.setTimeDuration("hdds.container.balancer.move.timeout", 60,
        TimeUnit.MINUTES);
    conf.setTimeDuration(
        "hdds.container.balancer.move.replication.timeout", 50,
        TimeUnit.MINUTES);

    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    InvalidContainerBalancerConfigurationException ex =
        assertThrowsExactly(
            InvalidContainerBalancerConfigurationException.class,
            () -> containerBalancer.startBalancer(balancerConfiguration),
            "(hdds.container.balancer.move.timeout - hdds.container.balancer.move.replication.timeout " +
                "- hdds.scm.replication.event.timeout.datanode.offset) should be greater than or equal to 9 minutes.");
    assertTrue(ex.getMessage().contains("should be greater than or equal to 540000ms or 9 minutes"),
        "Exception message should contain 'should be greater than or equal to 540000ms or 9 minutes'");
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());
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
    assertTrue(containerBalancer.isBalancerRunning());

    // Balancer should stop the current balancing thread when it receives a
    // status change notification
    scm.getScmContext().updateLeaderAndTerm(false, 1);
    containerBalancer.notifyStatusChanged();
    assertFalse(containerBalancer.isBalancerRunning());

    LogCapturer logCapturer = LogCapturer.captureLogs(ContainerBalancerTask.class);
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
    assertTrue(containerBalancer.isBalancerRunning());
    Thread balancingThread = containerBalancer.getCurrentBalancingThread();
    GenericTestUtils.waitFor(
        () -> balancingThread.getState() == Thread.State.TIMED_WAITING, 2, 20);
    assertThat(logCapturer.getOutput()).contains(expectedLog);
    stopBalancer();
  }

  @Test
  public void testGetBalancerStatusInfo() throws Exception {
    startBalancer(balancerConfiguration);
    assertSame(ContainerBalancerTask.Status.RUNNING, containerBalancer.getBalancerStatus());

    // Assert the configuration fields that were explicitly set
    ContainerBalancerStatusInfo status = containerBalancer.getBalancerStatusInfo();
    assertEquals(balancerConfiguration.getThreshold(),
        Double.parseDouble(status.getConfiguration().getUtilizationThreshold()));
    assertEquals(balancerConfiguration.getIterations(), status.getConfiguration().getIterations());
    assertEquals(balancerConfiguration.getTriggerDuEnable(), status.getConfiguration().getTriggerDuBeforeMoveEnable());

    stopBalancer();
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());
  }

  @Test
  public void testStartBalancerWithInvalidNodes() throws Exception {
    NodeManager nm = scm.getScmNodeManager();
    String validHost = "1.2.3.4";
    String invalidHost = "invalid-host-name";

    when(nm.getNodesByAddress(invalidHost)).thenReturn(Collections.emptyList());
    when(nm.getNodesByAddress(validHost)).thenReturn(Collections.singletonList(mock(DatanodeDetails.class)));

    // Test invalid includeNodes
    balancerConfiguration.setIncludeNodes(invalidHost);
    InvalidContainerBalancerConfigurationException ex =
        assertThrows(InvalidContainerBalancerConfigurationException.class,
            () -> containerBalancer.startBalancer(balancerConfiguration));
    assertThat(ex.getMessage()).contains(invalidHost);
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());

    // Test invalid excludeNodes
    balancerConfiguration.setIncludeNodes("");
    balancerConfiguration.setExcludeNodes(invalidHost);
    ex = assertThrows(InvalidContainerBalancerConfigurationException.class,
        () -> containerBalancer.startBalancer(balancerConfiguration));
    assertThat(ex.getMessage()).contains(invalidHost);
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());

    // Test a valid case
    balancerConfiguration.setExcludeNodes("");
    balancerConfiguration.setIncludeNodes(validHost);
    assertDoesNotThrow(() -> startBalancer(balancerConfiguration));
    assertSame(ContainerBalancerTask.Status.RUNNING, containerBalancer.getBalancerStatus());

    stopBalancer();
    assertSame(ContainerBalancerTask.Status.STOPPED, containerBalancer.getBalancerStatus());
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
