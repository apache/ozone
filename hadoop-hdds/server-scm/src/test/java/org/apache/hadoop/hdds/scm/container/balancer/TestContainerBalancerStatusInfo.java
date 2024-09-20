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
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.OzoneConsts;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ContainerBalancerStatusInfo}.
 */
class TestContainerBalancerStatusInfo {

  @Test
  void testGetIterationStatistics() {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(20, OzoneConsts.GB));

    ContainerBalancerConfiguration config = new OzoneConfiguration().getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    config.setBalancingInterval(0);
    config.setMaxSizeToMovePerIteration(50 * OzoneConsts.GB);

    ContainerBalancerTask task = mockedScm.startBalancerTask(config);
    List<ContainerBalancerTaskIterationStatusInfo> iterationStatistics = task.getCurrentIterationsStatistic();
    assertEquals(3, iterationStatistics.size());
    iterationStatistics.forEach(is -> {
      assertTrue(is.getContainerMovesCompleted() > 0);
      assertEquals(0, is.getContainerMovesFailed());
      assertEquals(0, is.getContainerMovesTimeout());
      assertFalse(is.getSizeEnteringNodesGB().isEmpty());
      assertFalse(is.getSizeLeavingNodesGB().isEmpty());
    });

  }

  /**
   * @see <a href="https://issues.apache.org/jira/browse/HDDS-11350">HDDS-11350</a>
   */
  @Test
  void testGetCurrentIterationsStatisticDoesNotThrowNullPointerExceptionWhenBalancingThreadIsSleeping() {
    MockedSCM mockedScm = new MockedSCM(new TestableCluster(10, OzoneConsts.GB));
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ContainerBalancerConfiguration config = ozoneConfig.getObject(ContainerBalancerConfiguration.class);

    config.setIterations(2);
    // the following config makes the balancing thread go to sleep while waiting for DU to be triggered in DNs and
    // updated storage reports to arrive via DN heartbeats - of course, this is a unit test and NodeManager, DNs etc.
    // are all mocked
    config.setTriggerDuEnable(true);
    mockedScm.init(config, ozoneConfig);

    // run ContainerBalancerTask in a new thread and have the current thread call getCurrentIterationsStatistic
    StorageContainerManager scm = mockedScm.getStorageContainerManager();
    ContainerBalancer cb = new ContainerBalancer(scm);
    ContainerBalancerTask task = new ContainerBalancerTask(scm, 0, cb, cb.getMetrics(), config, false);
    Thread thread = new Thread(task);
    thread.setDaemon(true);
    thread.start();
    Assertions.assertDoesNotThrow(task::getCurrentIterationsStatistic);
  }
}
