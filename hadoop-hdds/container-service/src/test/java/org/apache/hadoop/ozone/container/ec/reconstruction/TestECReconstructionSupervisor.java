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
package org.apache.hadoop.ozone.container.ec.reconstruction;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.SortedMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * Tests the ECReconstructionSupervisor.
 */
public class TestECReconstructionSupervisor {

  @Test
  public void testAddTaskShouldExecuteTheGivenTask()
      throws InterruptedException, TimeoutException, IOException {
    final CountDownLatch runnableInvoked = new CountDownLatch(1);
    final CountDownLatch holdProcessing = new CountDownLatch(1);
    ECReconstructionSupervisor supervisor =
        new ECReconstructionSupervisor(null, null, 5,
            new ECReconstructionCoordinator(new OzoneConfiguration(), null,
                ECReconstructionMetrics.create()) {
              @Override
              public void reconstructECContainerGroup(long containerID,
                  ECReplicationConfig repConfig,
                  SortedMap<Integer, DatanodeDetails> sourceNodeMap,
                  SortedMap<Integer, DatanodeDetails> targetNodeMap)
                  throws IOException {
                runnableInvoked.countDown();
                try {
                  holdProcessing.await();
                } catch (InterruptedException e) {
                }
                super.reconstructECContainerGroup(containerID, repConfig,
                    sourceNodeMap, targetNodeMap);
              }
            }) {
        };
    supervisor.addTask(
        new ECReconstructionCommandInfo(1, new ECReplicationConfig(3, 2),
            new byte[0], ImmutableList.of(), ImmutableList.of()));
    runnableInvoked.await();
    Assertions.assertEquals(1, supervisor.getInFlightReplications());
    holdProcessing.countDown();
    GenericTestUtils
        .waitFor(() -> supervisor.getInFlightReplications() == 0, 100, 15000);
  }
}
