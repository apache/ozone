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
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  private StatefulServiceStateManager serviceStateManager;

  /**
   * Sets up configuration values and creates a mock cluster.
   */
  @BeforeEach
  public void setup() throws IOException, NodeNotFoundException,
      TimeoutException {
    OzoneConfiguration conf = new OzoneConfiguration();
    scm = Mockito.mock(StorageContainerManager.class);
    serviceStateManager = Mockito.mock(StatefulServiceStateManagerImpl.class);
    balancerConfiguration =
        conf.getObject(ContainerBalancerConfiguration.class);
    balancerConfiguration.setThreshold(10);
    balancerConfiguration.setIterations(10);
    // Note: this will make container balancer task to wait for running
    // for 60 sec as default and ensure below test case have sufficient
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
    } catch (IOException | IllegalContainerBalancerStateException |
             TimeoutException e) {
      LOG.warn("Failed to stop balancer", e);
    }
  }
}
