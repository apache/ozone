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
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.apache.hadoop.ozone.OzoneConsts.GB;
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
  // Used to block ICR sending so that queue metrics can be checked before the reconcile task completes.
  private CountDownLatch icrLatch;

  private ContainerLayoutVersion layoutVersion;

  // As data hashes are calculated during the test, they are written back here.
  private final Map<ContainerID, ContainerReplicaProto> containerReportsSent = new HashMap<>();

  public void initLayoutVersion(ContainerLayoutVersion layout)
      throws Exception {
    this.layoutVersion = layout;
    init();
  }

  private void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeDetails dnDetails = randomDatanodeDetails();
    subject = new ReconcileContainerCommandHandler("");
    context = ContainerTestUtils.getMockContext(dnDetails, conf);

    KeyValueContainerData data = new KeyValueContainerData(CONTAINER_ID, layoutVersion, GB,
        PipelineID.randomId().toString(), randomDatanodeDetails().getUuidString());
    container = new KeyValueContainer(data, conf);
    containerSet = new ContainerSet(1000);
    containerSet.addContainer(container);

    icrLatch = new CountDownLatch(1);
    IncrementalReportSender<Container> icrSender = c -> {
      try {
        containerReportsSent.put(ContainerID.valueOf(c.getContainerData().getContainerID()), c.getContainerReport());

        // Block the caller until the latch is counted down.
        // Caller can check queue metrics in the meantime.
        LOG.info("ICR sender waiting for latch");
        Assertions.assertTrue(icrLatch.await(30, TimeUnit.SECONDS));
        LOG.info("ICR sender proceeding after latch");
        // Reset the latch for the next iteration.
        // This assumes requests are executed by a single thread reading the latch.
        icrLatch = new CountDownLatch(1);
      } catch (Exception ex) {
        LOG.error("ICR sender failed", ex);
      }
    };

    containerHandler = new KeyValueHandler(new OzoneConfiguration(), dnDetails.getUuidString(), containerSet,
        mock(VolumeSet.class), mock(ContainerMetrics.class), icrSender);
    controller = new ContainerController(containerSet,
        singletonMap(ContainerProtos.ContainerType.KeyValueContainer, containerHandler));
    ozoneContainer = mock(OzoneContainer.class);
    when(ozoneContainer.getController()).thenReturn(controller);
    when(ozoneContainer.getContainerSet()).thenReturn(containerSet);
  }

  // TODO test is flaky on the second container layout run only.
  @ContainerLayoutTestInfo.ContainerTest
  public void testReconcileContainerCommandHandled(ContainerLayoutVersion layout) throws Exception {
    initLayoutVersion(layout);

    ReconcileContainerCommand cmd = new ReconcileContainerCommand(CONTAINER_ID, Collections.emptyList());
    // Queue two commands for processing.
    // Handler is blocked until we count down the ICR latch.
    subject.handle(cmd, ozoneContainer, context, null);
    subject.handle(cmd, ozoneContainer, context, null);

    // The first command was invoked when submitted, and is now blocked in the ICR sender.
    // Since neither command has finished, they both count towards queue count.
    Assertions.assertEquals(1, subject.getInvocationCount());
    Assertions.assertEquals(2, subject.getQueuedCount());
    Assertions.assertEquals(0, subject.getTotalRunTime());
    Assertions.assertEquals(0, subject.getAverageRunTime());

    // Wait this long before unblocking the ICR sender. This is the lower bound on simulated execution time.
    long minExecTimeMillis = 500;
    Thread.sleep(minExecTimeMillis);
    icrLatch.countDown();

    // Decrementing queue count indicates the task completed.
    waitForQueueCount(1);
    // The other command is invoked but blocked in the ICR sender.
    Assertions.assertEquals(2, subject.getInvocationCount());
    long firstTotalRunTime = subject.getTotalRunTime();
    long firstAvgRunTime = subject.getAverageRunTime();
    Assertions.assertTrue(firstTotalRunTime >= minExecTimeMillis,
        "Total run time " + firstTotalRunTime + "ms was not larger than min exec time " + minExecTimeMillis + "ms");

    // Wait a little longer before firing the second command.
    Thread.sleep(minExecTimeMillis + 100);
    icrLatch.countDown();
    // Decrementing queue count indicates the task completed.
    waitForQueueCount(0);
    Assertions.assertEquals(2, subject.getInvocationCount());
    long secondTotalRunTime = subject.getTotalRunTime();
    long secondAvgRunTime = subject.getAverageRunTime();
    Assertions.assertTrue(secondTotalRunTime >= firstTotalRunTime + minExecTimeMillis);
    Assertions.assertTrue(secondAvgRunTime >= minExecTimeMillis);
    // We slept the thread a little longer on the second invocation, which should have increased the average run time
    // from the first run.
    Assertions.assertTrue(secondAvgRunTime >= firstAvgRunTime);

    verifyContainerReportsSent();
  }

  private void waitForQueueCount(int expectedQueueCount) throws Exception {
    GenericTestUtils.waitFor(() -> {
      int qCount = subject.getQueuedCount();
      LOG.info("Waiting for queued command count to reach " + expectedQueueCount + ". Currently at " + qCount);
      return qCount == expectedQueueCount;
    }, 500, 3000);
  }

  private void verifyContainerReportsSent() throws Exception {
    for (ContainerID id: containerReportsSent.keySet()) {
      String sentDataChecksum = containerReportsSent.get(id).getDataChecksum();
      String expectedDataChecksum = ContainerUtils.getChecksum(Long.toString(id.getId()));
      Assertions.assertEquals(expectedDataChecksum, sentDataChecksum, "Checksum mismatch in report of container " + id);
    }
  }
}
