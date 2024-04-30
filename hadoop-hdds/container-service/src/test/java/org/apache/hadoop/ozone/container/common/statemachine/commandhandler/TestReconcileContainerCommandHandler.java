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

package org.apache.hadoop.ozone.container.common.statemachine.commandhandler;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.ContainerTestUtils;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerLayoutVersion;
import org.apache.hadoop.ozone.container.common.impl.ContainerSet;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.report.IncrementalReportSender;
import org.apache.hadoop.ozone.container.common.statemachine.StateContext;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.ContainerLayoutTestInfo;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerController;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.protocol.commands.ReconcileContainerCommand;
import org.apache.ozone.test.GenericTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.min;
import static java.util.Collections.singletonMap;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests Datanode handling of reconcile container commands.
 */
public class TestReconcileContainerCommandHandler {
  public static final Logger LOG = LoggerFactory.getLogger(TestReconcileContainerCommandHandler.class);

  private static final long CONTAINER_ID = 123L;

  private OzoneContainer ozoneContainer;
  private StateContext context;
  private Container container;
  private Handler containerHandler;
  private ContainerController controller;
  private ContainerSet containerSet;
  private ReconcileContainerCommandHandler subject;

  private ContainerLayoutVersion layoutVersion;

  public void init(ContainerLayoutVersion layout, IncrementalReportSender<Container> icrSender)
      throws Exception {
    this.layoutVersion = layout;

    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeDetails dnDetails = randomDatanodeDetails();
    subject = new ReconcileContainerCommandHandler("");
    context = ContainerTestUtils.getMockContext(dnDetails, conf);

    KeyValueContainerData data = new KeyValueContainerData(CONTAINER_ID, layoutVersion, GB,
        PipelineID.randomId().toString(), randomDatanodeDetails().getUuidString());
    container = new KeyValueContainer(data, conf);
    containerSet = new ContainerSet(1000);
    containerSet.addContainer(container);

    containerHandler = new KeyValueHandler(new OzoneConfiguration(), dnDetails.getUuidString(), containerSet,
        mock(VolumeSet.class), mock(ContainerMetrics.class), icrSender);
    controller = new ContainerController(containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer, containerHandler));
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
  }

  @ContainerLayoutTestInfo.ContainerTest
  public void testReconcileContainerCommandReports(ContainerLayoutVersion layout) throws Exception {
    Map<ContainerID, ContainerReplicaProto> containerReportsSent = new HashMap<>();
    IncrementalReportSender<Container> icrSender = c -> {
      try {
        ContainerID id = ContainerID.valueOf(c.getContainerData().getContainerID());
        containerReportsSent.put(id, c.getContainerReport());
        LOG.info("Added container report for container {}", id);
      } catch (Exception ex) {
        LOG.error("ICR sender failed", ex);
      }
    };
    init(layout, icrSender);

    // These two commands are for a container existing in the datanode.
    ReconcileContainerCommand cmd = new ReconcileContainerCommand(CONTAINER_ID, Collections.emptyList());
    subject.handle(cmd, ozoneContainer, context, null);
    subject.handle(cmd, ozoneContainer, context, null);

    // This container was
    ReconcileContainerCommand cmd2 = new ReconcileContainerCommand(CONTAINER_ID + 1, Collections.emptyList());
    subject.handle(cmd2, ozoneContainer, context, null);

    waitForAllCommandsToFinish();

    verifyContainerReportsSent(containerReportsSent, new HashSet<>(Arrays.asList(CONTAINER_ID, CONTAINER_ID + 1)));
  }

  // TODO test is flaky on the second container layout run only.
  @ContainerLayoutTestInfo.ContainerTest
  public void testReconcileContainerCommandMetrics(ContainerLayoutVersion layout) throws Exception {
    // Used to block ICR sending so that queue metrics can be checked before the reconcile task completes.
    CountDownLatch icrLatch = new CountDownLatch(1);
    // Wait this long before completing the task.
    // This provides a lower bound on execution time.
    final long minExecTimeMillis = 500;

    IncrementalReportSender<Container> icrSender = c -> {
      try {
        // Block the caller until the latch is counted down.
        // Caller can check queue metrics in the meantime.
        LOG.info("ICR sender waiting for latch");
        assertTrue(icrLatch.await(30, TimeUnit.SECONDS));
        LOG.info("ICR sender proceeding after latch");

        Thread.sleep(minExecTimeMillis);
      } catch (Exception ex) {
        LOG.error("ICR sender failed", ex);
      }
    };

    init(layout, icrSender);

    ReconcileContainerCommand cmd = new ReconcileContainerCommand(CONTAINER_ID, Collections.emptyList());
    // Queue two commands for processing.
    // Both commands will be blocked until the latch is counted down.
    subject.handle(cmd, ozoneContainer, context, null);
    subject.handle(cmd, ozoneContainer, context, null);

    // The first command was invoked when submitted, and is now blocked in the ICR sender.
    // The second command is blocked on the first since handling is single threaded in the current implementation.
    // Since neither command has finished they both count towards queue count, which is incremented synchronously.
    assertEquals(2, subject.getQueuedCount());
    assertEquals(0, subject.getTotalRunTime());
    assertEquals(0, subject.getAverageRunTime());

    // This will resume handling of the two tasks.
    icrLatch.countDown();
    // Two tasks were fired, and each one should have taken at least minExecTime.
    final long expectedTotalMinExecTimeMillis = minExecTimeMillis * 2;

    waitForAllCommandsToFinish();

    assertEquals(2, subject.getInvocationCount());
    long totalRunTime = subject.getTotalRunTime();
    assertTrue(totalRunTime >= expectedTotalMinExecTimeMillis,
        "Total run time " + totalRunTime + "ms was not larger than the minimum total exec time " +
            expectedTotalMinExecTimeMillis + "ms");
    long avgRunTime = subject.getAverageRunTime();
    assertTrue(avgRunTime >= minExecTimeMillis,
        "Average run time " + avgRunTime + "ms was not larger than the minimum per task exec time " +
            minExecTimeMillis + "ms");
  }

  private void waitForAllCommandsToFinish() throws Exception {
    // Queue count should be decremented only after the task completes, so the other metrics should be consistent when
    // it reaches zero.
    GenericTestUtils.waitFor(() -> {
      int qCount = subject.getQueuedCount();
      LOG.info("Waiting for queued command count to reach 0. Currently at " + qCount);
      return qCount == 0;
    }, 500, 3000);
  }

  private void verifyContainerReportsSent(Map<ContainerID, ContainerReplicaProto> reportsSent,
      Set<Long> expectedContainerIDs) throws Exception {

    assertEquals(expectedContainerIDs.size(), reportsSent.size());

    for (Map.Entry<ContainerID, ContainerReplicaProto> entry: reportsSent.entrySet()) {
      ContainerID id = entry.getKey();
      assertTrue(expectedContainerIDs.contains(id.getId()));

      String sentDataChecksum = entry.getValue().getDataChecksum();
      // Current implementation is incomplete, and uses this as a mocked checksum.
      String expectedDataChecksum = ContainerUtils.getChecksum(Long.toString(id.getId()));
      assertEquals(expectedDataChecksum, sentDataChecksum, "Checksum mismatch in report of container " + id);
    }
  }
}
